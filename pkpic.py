from tempfile import TemporaryFile
from datetime import datetime
from netrc import netrc
from copy import copy
import argparse
import overpass
import zipfile
import ftplib
import shutil
import pytz
import json
import csv
import io
import os

__title__ = "PKPIntercityGTFS"
__license__ = "MIT"
__author__ = "Mikołaj Kuranowski"
__email__ = "".join(chr(i) for i in [109, 105, 107, 111, 108, 97, 106, 64, 109, 107,
                                     117, 114, 97, 110, 46, 112, 108])

FTP_ADDR = "ftps.intercity.pl"

ARCH_FTP_PATH = "rozklad/KPD_Rozklad.zip"
ARCH_CSV_FILE = "KPD_Rozklad.csv"

ADD_STOPS = {
    "Warszawa Zachodnia (Peron 8)": (52.221609, 20.961388),
    "Zduńska Wola Karsznice": (51.58046, 19.00512),
}

FIX_STOPS = {
    "BOHUMIN": "BOGUMIN",
    "RABKA ZDRÓJ": "RABKA-ZDRÓJ",
    "MOSTISKA 2": "MOŚCISKA 2",
    "PIWNICZNA ZDRÓJ": "PIWNICZNA-ZDRÓJ",
    "KUDOWA ZDRÓJ": "KUDOWA-ZDRÓJ",
    "PETROVICE U KARVINE": "PETROVICE U KARVINÉ",
    "WARSZAWA ZACHODNIA P8": "WARSZAWA ZACHODNIA (PERON 8)",
    "KRYNICA": "KRYNICA ZDRÓJ",
    "GUTKOWO": "OLSZTYN GUTKOWO",
    "NAKŁO N/NOTECIĄ": "NAKŁO NAD NOTECIĄ",
    "CHEŁM": "CHEŁM GŁÓWNY",
    "JAGODIN": "JAGODZIN",
    "CZECHOWICE DZIEDZICE": "CZECHOWICE-DZIEDZICE",
    "RUDNIK N/SANEM": "RUDNIK NAD SANEM",
    "MAŁASZEWICZE PRZYSTANEK": "MAŁASZEWICZE",
    "GORZÓW WIELKOPOLSKI TEATRALNA": "GORZÓW WIELKOPOLSKI WSCHODNI",
    "SKALITE": "SKALITÉ",
    "SKARŻYSKO KAMIENNA": "SKARŻYSKO-KAMIENNA",
    "KĘDZIERZYN KOŹLE": "KĘDZIERZYN-KOŹLE",
    "STRZYŻÓW N/WISŁOKIEM": "STRZYŻÓW NAD WISŁOKIEM",
    "BREST CENTRALNY": "BRZEŚĆ CENTRALNY",
    "FRANKFURT/ODER": "FRANKFURT (ODER)",
    "KUŹNICA": "KUŹNICA (HEL)",
    "WIELEŃ PÓŁNOCNY": "WIELEŃ",
}

DEFAULT_COLOR = ("DE4E4E", "FFFFFF")

ROUTE_COLORS = {
    "TLK": ("8505A3", "FFFFFF"),
    "IC": ("F25E18", "FFFFFF"),
    "IC EIC": ("898989", "FFFFFF"),
    "EC": ("9D740F", "FFFFFF"),
    "EIC": ("898989", "FFFFFF"),
    "EIP": ("002664", "FFFFFF"),
    "EN": ("000000", "FFFFFF"),
}


class FTP_TLS_Patched(ftplib.FTP_TLS):
    """A patched FTP client"""

    def makepasv(self):
        """Parse PASV response, but ignore provided IP.
        PKP IC's FTP sends incorrect addresses."""
        _, port = super().makepasv()
        return self.host, port

    def mod_time(self, filename):
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


def row_dep_only(row, set_bus=None):
    """Return copy of this row, but only with departure data"""
    row = copy(row)
    if set_bus is not None:
        row["BUS"] = set_bus
    row["Przyjazd"] = row["Odjazd"] or row["Przyjazd"]
    row["PeronWjazd"] = row["PeronWyjazd"] or row["PeronWjazd"]
    row["TorWjazd"] = row["TorWyjazd"] or row["TorWjazd"]
    return row


def row_arr_only(row, set_bus=None):
    """Return copy of this row, but only with arrival data"""
    row = copy(row)
    if set_bus is not None:
        row["BUS"] = set_bus
    row["Odjazd"] = row["Przyjazd"] or row["Odjazd"]
    row["PeronWyjazd"] = row["PeronWjazd"] or row["PeronWyjazd"]
    row["TorWyjazd"] = row["TorWjazd"] or row["TorWyjazd"]
    return row


def train_loader(file_name):
    """Generate trains from the CSV file."""
    previous_train_id = None
    previous_train_data = []

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


def train_legs(rows):
    """Generate all legs of a train from its routes."""
    previous_bus_value = None
    leg_so_far = []

    # first BUS=1 after a train → train arrival, bus departure
    # first BUS=0 after a bus → bus arrival, train departure

    for row in rows:
        current_bus_value = int(row["BUS"])

        if current_bus_value != previous_bus_value:

            if len(leg_so_far) > 1:
                leg_so_far.append(row_arr_only(row, set_bus=(current_bus_value ^ 1)))
                yield leg_so_far

            previous_bus_value = copy(current_bus_value)
            leg_so_far = [row_dep_only(row)]

        else:
            leg_so_far.append(row)

    if len(leg_so_far) > 1:
        yield leg_so_far


def time_to_list(text):
    """Convert 'HH:MM:SS' string into a [h, m, s] list"""
    h, m, s = map(int, text.split(":"))
    return [h, m, s]


def time_to_str(h, m, s):
    """Create a 'HH:MM:SS' string from h, m, s ints."""
    return f"{h:0>2}:{m:0>2}:{s:0>2}"


def file_mtime(file_name):
    """Get file modification time."""
    s = os.stat(file_name).st_mtime
    d = datetime.fromtimestamp(s)
    return d.strftime("%Y-%m-%d %H:%M:%S")


def resolve_ftp_login():
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


def escape_csv(value):
    return '"' + value.replace('"', '""') + '"'


class PKPIntercityGTFS:
    def __init__(self):
        self.version = None
        self.data_fetch = None

        self.stops = {}
        self.stops_used = set()
        self.stops_invalid = set()
        self.stops_duplicate = set()

        self.routes = set()
        self.services = set()
        self.transfers = []

    def version_check(self):
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

        else:
            return False

    def get_file(self, ftp_user, ftp_pass):
        """Connect to PKP IC FTP server,
        download archive with schedules,
        and extract the CSV file ftom it."""
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
                        out_csv.writerow(line)

    def get_stops_overpass(self):
        """Get stop positions from OpenStreetMap"""
        api = overpass.API(timeout=600)
        data: dict = api.get(
            "[out:json][timeout:600][bbox:46.4,12,55,30];"
            "("
            "node[railway=station];"
            "way[railway=station];"
            "node[railway=halt];"
            "way[railway=halt];"
            ");"
            "out center;",
            build=False
        )  # type: ignore

        # Parse stations
        for feature in data["elements"]:
            # Station name
            if "name:pl" in feature["tags"]:
                name = feature["tags"]["name:pl"].upper()
            elif "name" in feature["tags"]:
                name = feature["tags"]["name"].upper()
            else:
                continue

            # Some ignore rules
            # Parkowa Kolejka Maltańska has confusing station names
            if feature["tags"].get("operator") == "MPK Poznań":
                continue

            # Also ignore Jagodzin halt in Lower Silesian vv.
            # In PKP IC "Jagodzin" refers to Ukrainian station Ягодин (right after Dorohusk)
            if feature["id"] == 2146607462:
                continue

            # Position
            if "center" in feature:
                lat = feature["center"]["lat"]
                lon = feature["center"]["lon"]
            else:
                lat = feature["lat"]
                lon = feature["lon"]

            # Save info
            if name in self.stops:
                self.stops_duplicate.add(name)
            self.stops[name] = (lat, lon)

        # add stops from ADD_STOPS
        for name, pos in ADD_STOPS.items():
            self.stops[name.upper()] = pos

        # save stations
        with open("stops.json", mode="w", encoding="utf-8") as f:
            json.dump(self.stops, f, ensure_ascii=False)

    def get_stops_local(self):
        """Get stop positions from local file, stops.json"""
        with open("stops.json", mode="r", encoding="utf-8") as f:
            self.stops = json.load(f)

    def save_trips(self):
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
        ])

        for train_rows in train_loader("rozklad.csv"):
            # Filter "stations" without passanger exchange
            valid_rows = sorted([i for i in train_rows if i["StacjaHandlowa"] == "1"],
                                key=lambda i: int(i["Lp"]))

            # Ignore trains with only one valid station
            if len(valid_rows) <= 1:
                continue

            # Get some info about the train
            category = train_rows[0]["KategoriaHandlowa"].replace("  ", " ")
            number = train_rows[0]["NrPociaguHandlowy"]
            name = train_rows[0]["NazwaPociagu"]

            service_id = train_rows[0]["DataOdjazdu"]
            train_id = service_id + "_" + train_rows[0]["NrPociagu"].replace("/", "-")

            print("\033[1A\033[K" f"Parsing trips: {train_id}")

            # User-facing text info
            headsign = valid_rows[-1]["NazwaStacji"]

            if name and number in name:
                gtfs_name = name.title().replace("Zka", "ZKA")
            elif name:
                gtfs_name = f"{number} {name.title()}"
            else:
                gtfs_name = number

            multiple_legs = len({i["BUS"] for i in valid_rows}) > 1

            if not multiple_legs:
                is_bus = int(valid_rows[0]["BUS"]) == 1
                category = f"ZKA {category}" if is_bus else category

                if is_bus:
                    print("\033[1A\033[K" f"BUS: {train_id}")

                wrtr_trips.writerow([category, service_id, train_id, headsign, gtfs_name])

                self.routes.add(category)
                self.services.add(service_id)

                previous_dep = [0, 0, 0]

                for seq, row in enumerate(valid_rows):
                    stop_id = row["NumerStacji"]
                    stop_name = row["NazwaStacji"].upper()

                    stop_name = FIX_STOPS.get(stop_name, stop_name)

                    if stop_name in self.stops:
                        self.stops_used.add((stop_id, stop_name))

                        # Fix time values
                        arr = time_to_list(row["Przyjazd"])
                        dep = time_to_list(row["Odjazd"])

                        while arr < previous_dep:
                            arr[0] += 24

                        while dep < arr:
                            dep[0] += 24

                        previous_dep = copy(dep)

                        arr = time_to_str(*arr)
                        dep = time_to_str(*dep)

                        # Platforms
                        platform_arr = row["PeronWjazd"]
                        platform_dep = row["PeronWyjazd"]

                        if platform_arr.upper() in {"BUS", "NULL"}:
                            platform_arr = ""

                        if platform_dep.upper() in {"BUS", "NULL"}:
                            platform_dep = ""

                        platform = platform_dep or platform_arr

                        # Dump to GTFS
                        wrtr_times.writerow([
                            train_id, seq, stop_id,
                            arr, dep, platform,
                        ])

                    else:
                        self.stops_invalid.add((stop_id, stop_name))

            else:
                previous_dep = [0, 0, 0]

                for leg_suffix, leg_rows in enumerate(train_legs(valid_rows)):
                    leg_id = train_id + "_" + str(leg_suffix)

                    leg_is_bus = int(leg_rows[0]["BUS"]) == 1
                    leg_cat = f"ZKA {category}" if leg_is_bus else category

                    wrtr_trips.writerow([leg_cat, service_id, leg_id, headsign, gtfs_name])

                    self.routes.add(leg_cat)
                    self.services.add(service_id)

                    # Transfer
                    if leg_suffix > 0:
                        transfer_stop = leg_rows[0]["NumerStacji"]
                        self.transfers.append((
                            transfer_stop,
                            train_id + "_" + str(leg_suffix - 1),
                            leg_id,
                        ))

                    for seq, row in enumerate(leg_rows):
                        stop_id = row["NumerStacji"]
                        stop_name = row["NazwaStacji"].upper()

                        stop_name = FIX_STOPS.get(stop_name, stop_name)

                        if stop_name in self.stops:
                            self.stops_used.add((stop_id, stop_name))

                            # Fix time values
                            arr = time_to_list(row["Przyjazd"])
                            dep = time_to_list(row["Odjazd"])

                            while arr < previous_dep:
                                arr[0] += 24

                            while dep < arr:
                                dep[0] += 24

                            previous_dep = copy(dep)

                            arr = time_to_str(*arr)
                            dep = time_to_str(*dep)

                            # Platforms
                            platform_arr = row["PeronWjazd"]
                            platform_dep = row["PeronWyjazd"]

                            if platform_arr.upper() in {"BUS", "NULL"}:
                                platform_arr = ""

                            if platform_dep.upper() in {"BUS", "NULL"}:
                                platform_dep = ""

                            platform = platform_dep or platform_arr

                            # Dump to GTFS
                            wrtr_times.writerow([
                                leg_id, seq, stop_id,
                                arr, dep, platform,
                            ])

                        else:
                            self.stops_invalid.add((stop_id, stop_name))

        file_trips.close()
        file_times.close()

    def save_stops(self):
        file = open("gtfs/stops.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.writer(file)
        writer.writerow(["stop_id", "stop_name", "stop_lat", "stop_lon"])

        for stop_id, stop_name in self.stops_used:
            stop_lat, stop_lon = self.stops[stop_name]
            writer.writerow([stop_id, stop_name.title(), stop_lat, stop_lon])

        file.close()

        file = open("stops_missing.csv", mode="w", encoding="utf-8", newline="")
        writer = csv.writer(file)
        writer.writerow(["stop_id", "stop_name"])

        for stop_id, stop_name in self.stops_invalid:
            writer.writerow([stop_id, stop_name])

        file.close()

    def save_routes(self):
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

    def save_transfers(self):
        file = open("gtfs/transfers.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.writer(file)
        writer.writerow([
            "from_stop_id", "to_stop_id", "from_trip_id",
            "to_trip_id", "transfer_type",
        ])

        for stop_id, from_trip, to_trip in self.transfers:
            writer.writerow([
                stop_id, stop_id, from_trip, to_trip, "1",
            ])

        file.close()

    def save_dates(self):
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

    def save_static(self, pub_name, pub_url):
        # make sure current version is defined
        assert isinstance(self.version, str)

        pkpic_tstamp = file_mtime("rozklad.csv")
        osm_tstamp = file_mtime("stops.json")

        # Agency
        file = open("gtfs/agency.txt", mode="w", encoding="utf-8", newline="\r\n")
        file.write("agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone\n")
        file.write('0,PKP Intercity,"https://intercity.pl/",Europe/Warsaw,pl,+48703200200')
        file.close()

        # Attributions
        file = open("gtfs/attributions.txt", mode="w", encoding="utf-8", newline="\r\n")
        file.write("organization_name,is_producer,is_operator,is_authority,"
                   "is_data_source,attribution_url\n")

        file.write('"Stop positions provided by: © OpenStreetMap contributors '
                   f'(under ODbL license, retrieved {osm_tstamp})",0,0,1,'
                   '1,"https://www.openstreetmap.org/copyright/"\n')

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

    def compress(self, target="pkpic.zip"):
        "Compress all created files to pkpic.zip"
        with zipfile.ZipFile(target, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            for file in os.listdir("gtfs"):
                if file.endswith(".txt"):
                    archive.write(os.path.join("gtfs", file), arcname=file)

    @classmethod
    def create(cls, ftp_user, ftp_pass, ignore_version="", refresh_osm=False,
               pub_name="", pub_url=""):
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

        if refresh_osm:
            print("\033[1A\033[K" "Downloading stops")
            self.get_stops_overpass()

        else:
            print("\033[1A\033[K" "Loading stops")
            self.get_stops_local()

        print("\033[1A\033[K" "Parsing trips")
        self.save_trips()
        print("\033[1A\033[K" "Parsing trips: done")

        print("\033[1A\033[K" "Parsing stops")
        self.save_stops()

        print("\033[1A\033[K" "Parsing routes")
        self.save_routes()

        print("\033[1A\033[K" "Saving transfers")
        self.save_transfers()

        print("\033[1A\033[K" "Saving calendar_dates")
        self.save_dates()

        print("\033[1A\033[K" "Saving static files")
        self.save_static(pub_name, pub_url)

        print("\033[1A\033[K" "Compressing")
        self.compress()


if __name__ == "__main__":
    argprs = argparse.ArgumentParser()

    argprs.add_argument(
        "-o", "--refresh-osm",
        action="store_true",
        required=False,
        help="re-download file stops.json"
    )

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

    PKPIntercityGTFS.create(ftp_user, ftp_pass, args.ignore_version, args.refresh_osm,
                            args.publisher_name, args.publisher_url)
