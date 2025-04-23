# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import os
from collections.abc import Iterator
from datetime import datetime, timezone
from ftplib import FTP_TLS

from impuls.errors import InputNotModified
from impuls.resource import ConcreteResource

FTP_HOST = "ftps.intercity.pl"


class FTP_TLS_Patched(FTP_TLS):
    """A patched FTP client"""

    def makepasv(self) -> tuple[str, int]:
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
        date = date.replace(tzinfo=timezone.utc)
        return date

    def iter_binary(self, cmd: str, blocksize: int = 8192) -> Iterator[bytes]:
        self.voidcmd("TYPE I")
        with self.transfercmd(cmd) as conn:
            while data := conn.recv(blocksize):
                yield data
        return self.voidresp()


class FTPResource(ConcreteResource):
    def __init__(self, filename: str) -> None:
        super().__init__()
        self.filename = filename

    def fetch(self, conditional: bool) -> Iterator[bytes]:
        username, password = get_credentials()
        with FTP_TLS_Patched(FTP_HOST, username, password) as ftp:
            ftp.prot_p()

            current_last_modified = ftp.mod_time(self.filename)
            if conditional and current_last_modified <= self.last_modified:
                raise InputNotModified

            self.last_modified = current_last_modified
            self.fetch_time = datetime.now(timezone.utc)
            yield from ftp.iter_binary(f"RETR {self.filename}")


def get_credentials() -> tuple[str, str]:
    c = os.getenv("INTERCITY_FTP_CREDENTIALS")
    if not c and (p := os.getenv("INTERCITY_FTP_CREDENTIALS_FILE")):
        with open(p, "r", encoding="utf-8-sig") as f:
            c = f.read()

    if not c:
        raise ValueError("INTERCITY_FTP_CREDENTIALS environment variable not set")

    username, _, password = c.strip().partition(",")
    return username, password
