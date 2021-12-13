import argparse
import csv
import ftplib
import io
import os
import shutil
import zipfile
from copy import copy
from datetime import datetime
from netrc import netrc
from tempfile import TemporaryFile
from typing import Dict, Iterable, List, NamedTuple, Optional, Set, Tuple
from warnings import warn

import osmiter
import pytz
import requests

__title__ = "PKPIntercityGTFS"
__license__ = "MIT"
__author__ = "Mikołaj Kuranowski"
__email__ = "".join(chr(i) for i in [109, 105, 107, 111, 108, 97, 106, 64, 109, 107,
                                     117, 114, 97, 110, 46, 112, 108])

# Types

Color = Tuple[str, str]  # background, text
CsvRow = Dict[str, str]


class StopData(NamedTuple):
    id: str
    name: str
    ibnr: str = ""
    lat: float = 0.0
    lon: float = 0.0


# Static Data

STOPS_URL = "https://raw.githubusercontent.com/MKuranowski/PLRailMap/master/plrailmap.osm"
FTP_ADDR = "ftps.intercity.pl"

ARCH_FTP_PATH = "rozklad/KPD_Rozklad.zip"
ARCH_CSV_FILE = "KPD_Rozklad.csv"

DEFAULT_COLOR: Color = ("DE4E4E", "FFFFFF")

ROUTE_COLORS: Dict[str, Color] = {
    "TLK": ("8505A3", "FFFFFF"),
    "TLK IC": ("8505A3", "FFFFFF"),
    "IC": ("F25E18", "FFFFFF"),
    "IC EIC": ("898989", "FFFFFF"),
    "EC": ("9D740F", "FFFFFF"),
    "EIC": ("898989", "FFFFFF"),
    "EIC IC": ("898989", "FFFFFF"),
    "EIP": ("002664", "FFFFFF"),
    "EN": ("000000", "FFFFFF"),
}


class FTP_TLS_Patched(ftplib.FTP_TLS):
    """A patched FTP client"""

    def makepasv(self) -> Tuple[str, int]:
        """Parse PASV response, but ignore provided IP.
        PKP IC's FTP sends incorrect addresses."""
        _, port = super().makepasv()
        return self.host, port

    def mod_time(self, filename: str) -> datetime:
        """Get modification time of file on the server.
        Returns an aware datetime object."""
        resp = self.voidcmd("MDTM " + filename)
        date = resp.split(" ")[1]

        if len(date) == 14:
            date = datetime.strptime(date, "%Y%m%d%H%M%S")
        elif len(date) > 15:
            date = datetime.strptime(date[:21], "%Y%m%d%H%M%S.%f")
        else:
            raise ValueError(f"invalid MDTM command response {resp}")

        # reinterpret date as UTC
        date = date.replace(tzinfo=pytz.utc)
        return date


def row_dep_only(row: CsvRow, set_bus: Optional[str] = None) -> CsvRow:
    """Return copy of this row, but only with departure data"""
    row = copy(row)
    if set_bus is not None:
        row["BUS"] = set_bus
    row["Przyjazd"] = row["Odjazd"] or row["Przyjazd"]
    row["PeronWjazd"] = row["PeronWyjazd"] or row["PeronWjazd"]
    row["TorWjazd"] = row["TorWyjazd"] or row["TorWjazd"]
    return row


def row_arr_only(row: CsvRow, set_bus: Optional[str] = None) -> CsvRow:
    """Return copy of this row, but only with arrival data"""
    row = copy(row)
    if set_bus is not None:
        row["BUS"] = set_bus
    row["Odjazd"] = row["Przyjazd"] or row["Odjazd"]
    row["PeronWyjazd"] = row["PeronWjazd"] or row["PeronWyjazd"]
    row["TorWyjazd"] = row["TorWjazd"] or row["TorWyjazd"]
    return row


def train_loader(file_name: str) -> Iterable[List[CsvRow]]:
    """Generate trains from the CSV file."""
    previous_train_id: Tuple[str, str] = ("", "")
    previous_train_data: List[CsvRow] = []

    with open(file_name, mode="r", encoding="utf8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            train_id = (row["DataOdjazdu"], row["NrPociagu"])

            if previous_train_id != train_id:

                if previous_train_data:
                    yield previous_train_data

                previous_train_id = copy(train_id)
                previous_train_data = [row]

            else:
                previous_train_data.append(row)

        if previous_train_data:
            yield previous_train_data


def train_fixup(rows: List[CsvRow]) -> List[CsvRow]:
    """Fixes a train - changes 'Przyjazd' and 'Odjazd' times to GTFS-compliant strings,
    removes non-passenger stations"""
    # Remove non-passenger stops and sort those stops
    rows = sorted([i for i in rows if i["StacjaHandlowa"] == "1"],
                  key=lambda i: int(i["Lp"]))

    # Fix times
    previous_dep = [0, 0, 0]

    for row in rows:
        arr = time_to_list(row["Przyjazd"])
        dep = time_to_list(row["Odjazd"])

        while arr < previous_dep:
            arr[0] += 24

        while dep < arr:
            dep[0] += 24

        previous_dep = dep
        row["Przyjazd"] = time_to_str(*arr)
        row["Odjazd"] = time_to_str(*dep)

    return rows


def train_legs(rows: List[CsvRow]) -> List[List[CsvRow]]:
    """Generate all legs of a train from its routes."""
    all_legs: List[List[CsvRow]] = []
    leg_so_far: List[CsvRow] = []
    previous_bus: bool = rows[0]["BUS"] == "1"

    for row in rows:
        current_bus: bool = row["BUS"] == "1"

        # Bus value flips - arrival same as `previous_bus`, departure as `current_bus`
        # Also, flips on the last stop are ignored as they make no sense.
        if previous_bus != current_bus and row is not rows[-1]:
            if len(leg_so_far) > 1:
                leg_so_far.append(row_arr_only(row, set_bus="1" if previous_bus else "0"))
                all_legs.append(leg_so_far)

            leg_so_far = [row_dep_only(row)]
            previous_bus = current_bus

        else:
            leg_so_far.append(row)

    if len(leg_so_far) > 1:
        all_legs.append(leg_so_far)

    return all_legs


def time_to_list(text: str) -> List[int]:
    """Convert 'HH:MM:SS' string into a [h, m, s] list"""
    h, m, s = map(int, text.split(":"))
    return [h, m, s]


def time_to_str(h: int, m: int, s: int) -> str:
    """Create a 'HH:MM:SS' string from h, m, s ints."""
    return f"{h:0>2}:{m:0>2}:{s:0>2}"


def file_mtime(file_name: str) -> str:
    """Get file modification time."""
    s = os.stat(file_name).st_mtime
    d = datetime.fromtimestamp(s)
    return d.strftime("%Y-%m-%d %H:%M:%S")


def resolve_ftp_login() -> Tuple[str, str]:
    # If username and password is provided in arguments, just return them
    if "PKPIC_FTPUSER" in os.environ and "PKPIC_FTPPASS" in os.environ:
        return os.environ["PKPIC_FTPUSER"], os.environ["PKPIC_FTPPASS"]

    # Otherwise, check for file login.netrc
    elif os.path.exists("login.netrc"):
        n = netrc("login.netrc")

        if FTP_ADDR not in n.hosts:
            raise ValueError(f"entry for machine {FTP_ADDR} in login.netrc is missing!")

        ftp_user, _, ftp_pass = n.authenticators(FTP_ADDR)  # type: ignore - checked earlier

        if ftp_user is None or ftp_pass is None:
            raise ValueError(f"entry for machine {FTP_ADDR} in login.netrc doesn't define "
                             "username and/or password")

        return ftp_user, ftp_pass

    # Last resort, check ~/.netrc (Windows %USERPROFILE%/.netrc)
    elif os.path.exists(os.path.expanduser("~/.netrc")):
        n = netrc()

        if FTP_ADDR not in n.hosts:
            raise ValueError(f"entry for machine {FTP_ADDR} in ~/.netrc is missing!")

        ftp_user, _, ftp_pass = n.authenticators(FTP_ADDR)  # type: ignore - checked earlier

        if ftp_user is None or ftp_pass is None:
            raise ValueError(f"entry for machine {FTP_ADDR} in ~/.netrc doesn't define "
                             "username and/or password")

        return ftp_user, ftp_pass

    # Nothing found: raise an error
    else:
        raise ValueError(f"login and password to ftp {FTP_ADDR} must be provided via "
                         "env variables or present in files: login.netrc (alongside pkpic.py) "
                         "or ~/.netrc")


def escape_csv(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


class PKPIntercityGTFS:
    def __init__(self):
        self.version: str = ""

        self.stops: Dict[str, StopData] = {}
        self.stop_id_swap: Dict[str, str] = {}
        self.stop_names_db: Dict[str, str] = {}
        self.stops_used: Set[str] = set()
        self.stops_invalid: Set[StopData] = set()

        self.routes: Set[str] = set()
        self.services: Set[str] = set()

    def version_check(self) -> bool:
        """Check if data on FTP is newer then previously-parsed data."""
        if not os.path.exists("version.txt"):
            current_version = ""

        else:
            with open("version.txt", mode="r") as f:
                current_version = f.read().strip()

        if current_version != self.version:
            with open("version.txt", mode="w") as f:
                f.write(self.version + "\n")

            return True

        return False

    def get_file(self, ftp_user: str, ftp_pass: str) -> None:
        """Connect to PKP IC FTP server,
        download archive with schedules,
        and extract the CSV file from it to rozklad.csv."""
        # TemporaryFile for the archive
        with TemporaryFile(mode="w+b", suffix=".zip") as temp_zip:

            # Connect to the FTP and retrieve the file
            with FTP_TLS_Patched(FTP_ADDR, ftp_user, ftp_pass) as ftp:
                ftp.prot_p()

                # Get file modification date for the version
                file_mod = ftp.mod_time(ARCH_FTP_PATH)
                file_mod = file_mod.astimezone(pytz.timezone("Europe/Warsaw"))
                self.version = file_mod.strftime("%Y-%m-%d %H:%M:%S")

                ftp.retrbinary(f"RETR {ARCH_FTP_PATH}", temp_zip.write)

            temp_zip.seek(0)

            # Open the archive
            with zipfile.ZipFile(temp_zip, mode="r") as arch:
                arch_files = arch.namelist()

                # Check if the CSV file is inside the archive
                if ARCH_CSV_FILE not in arch_files:
                    raise FileNotFoundError(
                        f"file {ARCH_CSV_FILE!r} not found in PKPIC archive. "
                        f"List of all files inside archive: {arch_files}"
                    )

                # Extract the CSV, switch the encoding to UTF-8 and change the delimiter to ','
                with open("rozklad.csv", mode="w", encoding="utf-8", newline="") as out_file, \
                        arch.open(ARCH_CSV_FILE) as in_buff:

                    in_file = io.TextIOWrapper(in_buff, encoding="cp1250", newline="")
                    in_csv = csv.reader(in_file, delimiter=";")

                    out_csv = csv.writer(out_file)

                    for line in in_csv:
                        # Replace "NULL" with "", something's wrong in PKP IC export
                        # process, whatever
                        line = ("" if i == "NULL" else i for i in line)
                        out_csv.writerow(line)

    def get_stops(self) -> None:
        # Request the XML file
        with requests.get(STOPS_URL) as r:
            r.raise_for_status()
            buffer = io.BytesIO(r.content)

        # Parse it
        for elem in osmiter.iter_from_osm(buffer, file_format="xml", filter_attrs=set()):
            # Only care about railway=station nodes
            if elem["type"] != "node" or elem["tag"].get("railway") != "station":
                continue

            if "ref:2" in elem["tag"]:
                self.stop_id_swap[elem["tag"]["ref:2"]] = elem["tag"]["ref"]

            self.stops[elem["tag"]["ref"]] = StopData(
                id=elem["tag"]["ref"],
                ibnr=elem["tag"].get("ref:ibnr", ""),
                name=elem["tag"]["name"],
                lat=elem["lat"],
                lon=elem["lon"])

    def save_trip_leg(self, wrtr_trips: "csv._writer", wrtr_times: "csv._writer",
                      gtfs_trip: List[str], leg: List[CsvRow]) -> None:
        """Writes a single leg to trips.txt and stop_times.txt.
        Checks and modifies the route_id in the case of a bus legs."""
        if leg[0]["BUS"] == "1":
            gtfs_trip[0] = "ZKA " + gtfs_trip[0]
        self.routes.add(gtfs_trip[0])

        dist_offset = int(leg[0]["DrogaKumulowanaMetry"])
        wrtr_trips.writerow(gtfs_trip)

        for seq, row in enumerate(leg):
            stop_id = row["NumerStacji"]
            stop_name = row["NazwaStacji"]

            if stop_id not in self.stops:
                self.stops_invalid.add(StopData(stop_id, stop_name))
                continue

            # Mark stop as used
            self.stops_used.add(stop_id)
            self.stop_names_db[stop_id] = stop_name

            # Platforms
            platform_arr = row["PeronWjazd"]
            platform_dep = row["PeronWyjazd"]

            if platform_arr.upper() == "BUS":
                platform_arr = ""

            if platform_dep.upper() == "BUS":
                platform_dep = ""

            platform = platform_dep or platform_arr
            dist = int(row["DrogaKumulowanaMetry"]) - dist_offset

            # Dump to GTFS
            wrtr_times.writerow([gtfs_trip[2], seq, stop_id, row["Przyjazd"], row["Odjazd"],
                                 platform, dist])

    def save_trip_multiple_legs(self, wrtr_trips: "csv._writer", wrtr_times: "csv._writer",
                                wrtr_transfers: "csv._writer", base_trip: List[str],
                                legs: List[List[CsvRow]]) -> None:
        previous_leg_id: str = ""

        for suffix, leg in enumerate(legs):
            leg_id = f"{base_trip[2]}_{suffix}"

            # Update trips.txt entry
            leg_trip = base_trip.copy()
            leg_trip[2] = leg_id

            # Write to trips.txt and transfers.txt (which handles bus legs)
            self.save_trip_leg(wrtr_trips, wrtr_times, leg_trip, leg)

            # Write to transfers.txt
            transfer_stop = leg[0]["NumerStacji"]
            if previous_leg_id and transfer_stop in self.stops:
                wrtr_transfers.writerow([
                    transfer_stop, transfer_stop, previous_leg_id, leg_id, "1",
                ])

            previous_leg_id = leg_id

    def save_trips(self) -> None:
        """Parse data from rozklad.csv and save trips and stop_times."""
        file_trips = open("gtfs/trips.txt", mode="w", encoding="utf8", newline="")
        wrtr_trips = csv.writer(file_trips)
        wrtr_trips.writerow([
            "route_id", "service_id", "trip_id", "trip_headsign", "trip_short_name",
        ])

        file_times = open("gtfs/stop_times.txt", mode="w", encoding="utf8", newline="")
        wrtr_times = csv.writer(file_times)
        wrtr_times.writerow([
            "trip_id", "stop_sequence", "stop_id",
            "arrival_time", "departure_time", "platform",
            "official_dist_traveled",
        ])

        file_transfers = open("gtfs/transfers.txt", mode="w", encoding="utf8", newline="")
        wrtr_transfers = csv.writer(file_transfers)
        wrtr_transfers.writerow([
            "from_stop_id", "to_stop_id", "from_trip_id",
            "to_trip_id", "transfer_type",
        ])

        for rows in train_loader("rozklad.csv"):
            # Filter "stations" without passanger exchange
            rows = train_fixup(rows)

            # Swap stop_ids
            for row in rows:
                row["NumerStacji"] = self.stop_id_swap.get(row["NumerStacji"], row["NumerStacji"])

            # Get some info about the train
            category = rows[0]["KategoriaHandlowa"].replace("  ", " ")
            number = rows[0]["NrPociaguHandlowy"]
            name = rows[0]["NazwaPociagu"]

            # Hotfix for missing NrPociaguHandlowy
            if number == "":
                number, _, _ = rows[0]["NrPociagu"].partition("/")

            service_id = rows[0]["DataOdjazdu"]
            train_id = service_id + "_" + rows[0]["NrPociagu"].replace("/", "-")

            print("\033[1A\033[K" f"Parsing trips: {train_id}")

            # User-facing text info
            headsign = rows[-1]["NazwaStacji"]

            if name and number in name:
                gtfs_name = name.title().replace("Zka", "ZKA")
            elif name:
                gtfs_name = f"{number} {name.title()}"
            else:
                gtfs_name = number

            # Create a base trips.txt entry
            gtfs_trip = [category, service_id, train_id, headsign, gtfs_name]

            # Write to GTFS
            self.services.add(service_id)
            legs = train_legs(rows)

            if len(legs) > 1:
                self.save_trip_multiple_legs(wrtr_trips, wrtr_times, wrtr_transfers,
                                             gtfs_trip, legs)
            else:
                self.save_trip_leg(wrtr_trips, wrtr_times, gtfs_trip, legs[0])

        file_trips.close()
        file_times.close()

    def save_stops(self) -> None:
        file = open("gtfs/stops.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.writer(file)
        writer.writerow(["stop_id", "stop_name", "stop_lat", "stop_lon", "stop_IBNR"])

        for stop in map(lambda i: self.stops[i], self.stops_used):
            writer.writerow([stop.id, stop.name, stop.lat, stop.lon, stop.ibnr])

            db_name = self.stop_names_db.get(stop.id, "")
            if stop.name.casefold() != db_name.casefold():
                warn(f"Dissimilar stop names for id: {stop.id} - {stop.name!r} vs {db_name!r}")

        file.close()

        file = open("stops_missing.csv", mode="w", encoding="utf-8", newline="")
        writer = csv.writer(file)
        writer.writerow(["stop_id", "stop_name"])

        for stop in self.stops_invalid:
            writer.writerow([stop.id, stop.name])

        file.close()

    def save_routes(self) -> None:
        file = open("gtfs/routes.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.writer(file)
        writer.writerow([
            "agency_id", "route_id", "route_short_name",
            "route_long_name", "route_type", "route_color", "route_text_color"
        ])

        for route_id in sorted(self.routes):
            route_color, route_text = ROUTE_COLORS.get(route_id, DEFAULT_COLOR)
            route_type = "3" if "ZKA" in route_id else "2"
            writer.writerow([
                "0", route_id, route_id, "", route_type,
                route_color, route_text
            ])

        file.close()

    def save_dates(self) -> None:
        file = open("gtfs/calendar_dates.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.writer(file)
        writer.writerow([
            "date", "service_id", "exception_type",
        ])

        for service_id in sorted(self.services):
            writer.writerow([
                service_id.replace("-", ""), service_id, "1",
            ])

        file.close()

    def save_static(self, pub_name: str, pub_url: str) -> None:
        # make sure current version is defined
        assert isinstance(self.version, str)

        pkpic_tstamp = file_mtime("rozklad.csv")

        # Agency
        file = open("gtfs/agency.txt", mode="w", encoding="utf-8", newline="\r\n")
        file.write("agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone\n")
        file.write('0,PKP Intercity,"https://intercity.pl/",Europe/Warsaw,pl,+48703200200')
        file.close()

        # Attributions
        file = open("gtfs/attributions.txt", mode="w", encoding="utf-8", newline="\r\n")
        file.write("organization_name,is_producer,is_operator,is_authority,"
                   "is_data_source,attribution_url\n")

        file.write(f'"Schedules provided by: PKP Intercity S.A. (retrieved {pkpic_tstamp})",0,1,0,'
                   '1,"https://intercity.pl/"\n')

        file.close()

        # Feed Info
        if pub_name and pub_url:
            file = open("gtfs/feed_info.txt", mode="w", encoding="utf-8", newline="\r\n")
            file.write("feed_publisher_name,feed_publisher_url,feed_lang,feed_version\n")
            file.write(",".join([
                escape_csv(pub_name), escape_csv(pub_url), "pl", self.version
            ]) + "\n")
            file.close()

    def compress(self, target: str = "pkpic.zip") -> None:
        "Compress all created files to pkpic.zip"
        with zipfile.ZipFile(target, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            for file in os.listdir("gtfs"):
                if file.endswith(".txt"):
                    archive.write(os.path.join("gtfs", file), arcname=file)

    @classmethod
    def create(cls, ftp_user: str, ftp_pass: str, ignore_version: str = "",
               pub_name: str = "", pub_url: str = ""):
        self = cls()

        print("Downloading file")
        self.get_file(ftp_user, ftp_pass)

        print("\033[1A\033[K" "Checking if new version is available")
        new_version = self.version_check()

        if (not new_version) and (not ignore_version):
            print("\033[1A\033[K" "Current version matches previous version, aborting.")
            return

        if not os.path.exists("gtfs"):
            print("\033[1A\033[K" "creating the gtfs directory")
            os.mkdir("gtfs")

        else:
            print("\033[1A\033[K" "clearing the gtfs directory")
            for f in os.scandir("gtfs"):
                if f.is_dir():
                    shutil.rmtree(f.path)
                else:
                    os.remove(f.path)

        print("\033[1A\033[K" "Downloading stops")
        self.get_stops()

        print("\033[1A\033[K" "Parsing trips")
        self.save_trips()
        print("\033[1A\033[K" "Parsing trips: done")

        print("\033[1A\033[K" "Parsing stops")
        self.save_stops()

        print("\033[1A\033[K" "Parsing routes")
        self.save_routes()

        print("\033[1A\033[K" "Saving calendar_dates")
        self.save_dates()

        print("\033[1A\033[K" "Saving static files")
        self.save_static(pub_name, pub_url)

        print("\033[1A\033[K" "Compressing")
        self.compress()


if __name__ == "__main__":
    argprs = argparse.ArgumentParser()

    argprs.add_argument(
        "-i", "--ignore-version",
        action="store_true",
        required=False,
        help="force recreating the GTFS, even if the feed_version won't change",
    )

    argprs.add_argument(
        "-pn", "--publisher-name",
        required=False,
        metavar="NAME",
        help="value of feed_publisher_name (--publisher-url is also required to create feed_info)",
        default="",
    )

    argprs.add_argument(
        "-pu", "--publisher-url",
        required=False,
        metavar="URL",
        help="value of feed_publisher_url (--publisher-name is also required to create feed_info)",
        default="",
    )

    args = argprs.parse_args()
    ftp_user, ftp_pass = resolve_ftp_login()

    PKPIntercityGTFS.create(ftp_user, ftp_pass, args.ignore_version,
                            args.publisher_name, args.publisher_url)
