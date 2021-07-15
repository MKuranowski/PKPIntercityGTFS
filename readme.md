# PKPIntercityGTFS


## Description

Creates GTFS fata for [PKP Intercity](https://intercity.pl).
Data comes from the [Polish National Access Point to multimodal travel information services](https://dane.gov.pl/dataset/1739,krajowy-punkt-dostepowy-kpd-multimodalne-usugi-informacji-o-podrozach).
You need to get login credentials for the FTP server by writing an email to PKP Intercity's address, available in the NAP.

Stop data comes from my other project, [PLRailMap](https://github.com/MKuranowski/PLRailMap),
and is shared as public data.


## Dependencies

Script is written in Python and is tested on versions 3.8+.

Additionally, 3 external packages are required:
- [osmiter](https://pypi.org/project/osmiter/),
- [requests](https://pypi.org/project/requests/),
- [pytz](https://pypi.org/project/pytz/).

You can install them with `pip3 install -U -r requirements.txt`


## Running

Login credentials for the FTPS server can be provided via `PKPIC_FTPUSER` and `PKPIC_FTPPASS`
environment variables (username and password accordingly), or via a .netrc file.
This script first checks `login.netrc` in working directory, then the main `~/.netrc` file.

Run `python3 pkpic.py --help` to see all available options.

The script creates a `pkpic.zip` file in the working directory.
Sometimes, stops cannot be matched to anything in OSM,
in this case a `stops_missing.csv` file is created.

## Issues

Currently, merging/joining trains are not correctly supported.

International trains have data only within Polish borders,
this is due to a limitation in PKP IC's internal systems.


## License

*PKPIntercityGTFS* is provided under the MIT license, see the `license.md` file.
