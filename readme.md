PKPIntercityGTFS
============


Description
-----------

Creates GTFS fata for [PKP Intercity](https://intercity.pl).
Data comes from the [Polish National Access Point to multimodal travel information services](https://dane.gov.pl/dataset/1739,krajowy-punkt-dostepowy-kpd-multimodalne-usugi-informacji-o-podrozach).
You need to get login credentials for the FTP server by writing an email to PKP Intercity's address, available in the NAP.

Stop data comes from my other project, [PLRailMap](https://github.com/MKuranowski/PLRailMap).


Issues
------

Currently, merging/joining trains are not correctly supported.

International trains have data only within Polish borders,
this is due to a limitation in PKP IC's internal systems.


Running
-------

PKPIntercityGTFS is written in Python with the [Impuls framework](https://github.com/MKuranowski/Impuls).

To set up the project, run:

```terminal
$ python3 -m venv .venv
$ . .venv/bin/activate
$ pip install -Ur requirements.txt
```

Then, run:

```terminal
$ export INTERCITY_FTP_CREDENTIALS=ftp_username,ftp_password
$ python3 -m polregio_gtfs
```

Substituting `ftp_username` and `ftp_password` for login credentials to the PKP IC's FTP server.

The resulting schedules will be put in a file called `pkpic.zip`.

Most IDEs will have some support for .env files –
use them to avoid having to export environment variables
when running the script. As an alternative, PKPIntercityGTFS
also accepts Docker-style secret passing. Instead of setting
INTERCITY_FTP_CREDENTIALS, the credentials can be saved in a file,
and path to that file can be set through the INTERCITY_FTP_CREDENTIALS_FILE
environment variables. Beware that the direct/non-file env
variables take precedence.


License
-------

_PKPIntercityGTFS_ is provided under the MIT license, included in the `LICENSE` file.
