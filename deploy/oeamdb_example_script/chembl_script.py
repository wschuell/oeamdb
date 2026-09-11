#!/usr/bin/env python3
"""Restore selected ChEMBL tables into PostgreSQL.

Downloads the official PostgreSQL dump only if at least one of the requested
tables is missing, then runs ``pg_restore`` with one ``-t`` per missing table.

Connection parameters come from the usual libpq environment variables
(PGHOST, PGPORT, PGUSER, PGPASSWORD, ...); ``--dbname`` overrides PGDATABASE.

Requires psycopg 3 and the PostgreSQL client binaries on PATH.
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import re
import shutil
import subprocess
import sys
import tarfile
import urllib.request
from pathlib import Path
from typing import Iterable, Sequence

import psycopg

log = logging.getLogger("chembl_restore")

BASE_URL = "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases"
IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
FTP_ROOT = "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb"
VERSION_RE = re.compile(r"chembl[_-](\d+)", re.IGNORECASE)

DEFAULT_TABLES = (
    "molecule_dictionary",
    "molecule_synonyms",
    "compound_structures",
    "compound_properties",
    "atc_classification",
    "molecule_atc_classification",
)


def ensure_database(dbname: str, fallback_dbname: str="postgres", user: str="postgres", host: str="localhost", password: str|None = None) -> None:
    """CREATE DATABASE if it is not there yet."""
    with psycopg.connect(dbname=fallback_dbname, autocommit=True, user=user, host=host, password=password) as conn:
        exists = conn.execute(
            "select 1 from pg_database where datname = %s", (dbname,)
        ).fetchone()
        if exists:
            return
        log.info("creating database %s", dbname)
        conn.execute(f'create database "{dbname}"')


def missing_tables(dbname: str, tables: Sequence[str], schema: str, user: str="postgres", host: str="localhost", password: str|None = None) -> list[str]:
    """Return the subset of *tables* that does not exist as a relation."""
    with psycopg.connect(dbname=dbname,user=user,host=host, password=password) as conn:
        rows = conn.execute(
            "select t, to_regclass(format('%%I.%%I', %s::text, t)) is null"
            " from unnest(%s::text[]) as t",
            (schema, list(tables)),
        ).fetchall()
    return [name for name, absent in rows if absent]


def download(url: str, dest: Path) -> None:
    """Download *url* to *dest*, resuming a partial ``.part`` file if present."""
    if dest.exists():
        return
    part = dest.with_suffix(dest.suffix + ".part")
    offset = part.stat().st_size if part.exists() else 0

    req = urllib.request.Request(url)
    if offset:
        req.add_header("Range", f"bytes={offset}-")
        log.info("resuming download of %s at %.1f GiB", url, offset / 2**30)
    else:
        log.info("downloading %s (several GiB)", url)

    with urllib.request.urlopen(req) as resp:
        # Server ignored the Range header: start over.
        mode = "ab" if resp.status == 206 else "wb"
        with part.open(mode) as fh:
            shutil.copyfileobj(resp, fh, length=1 << 20)

    part.rename(dest)


def extract_member(tarball: Path, name: str, dest: Path) -> None:
    """Stream *name* out of *tarball* into *dest*."""
    log.info("extracting %s", name)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with tarfile.open(tarball, "r|gz") as tar:
        for member in tar:
            if not member.isfile() or Path(member.name).name != name:
                continue
            src = tar.extractfile(member)
            assert src is not None
            with src, tmp.open("wb") as fh:
                shutil.copyfileobj(src, fh, length=1 << 20)
            tmp.rename(dest)
            return
    raise FileNotFoundError(f"{name} not found in {tarball}")


def pg_restore(dump: Path, dbname: str, tables: Iterable[str],
    schema: str, jobs: int, user: str="postgres", host: str="localhost", password: str|None = None) -> None:
    env = {**os.environ, "LC_MESSAGES": "C"}
    if password:
        env["PGPASSWORD"] = password
    cmd = [
        "pg_restore",
        f"--dbname={dbname}",
        "--no-owner",
        "--no-privileges",
        f"--jobs={jobs}",
        f"--schema={schema}",
        f"--user={user}",
        f"--host={host}"
    ]
    for table in tables:
        cmd += ["-t", table]
    cmd.append(str(dump))

    log.info("running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True,env=env)



def get_latest_version(timeout: float = 30.0) -> int:
    """Return the highest ChEMBL release number published on the EBI FTP site."""
    for url in (f"{FTP_ROOT}/latest/", f"{FTP_ROOT}/releases/"):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                listing = resp.read().decode("utf-8", errors="replace")
        except OSError as exc:
            log.warning("could not list %s: %s", url, exc)
            continue
        versions = {int(m) for m in VERSION_RE.findall(listing)}
        if versions:
            return max(versions)
        log.warning("no chembl_NN entries found in %s", url)
    raise RuntimeError("could not determine the latest ChEMBL version")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("tables", nargs="*", default=list(DEFAULT_TABLES),
                   help="tables to ensure (default: %(default)s)")
    p.add_argument("--chembl-version", default=os.environ.get("CHEMBL_VERSION", "latest"))
    p.add_argument("--dbname", default=None,
                   help="target database (default: PGDATABASE or chembl_<version>)")
    p.add_argument("--fallbackdbname", default=os.environ.get("POSTGRES_FALLBACK","postgres"),
                   help="Fallback database to connect to to create the target database")
    p.add_argument("--password", default=os.environ.get("POSTGRES_PASSWORD"))
    p.add_argument("--schema", default=os.environ.get("POSTGRES_SCHEMA","public"))
    p.add_argument("--user", default=os.environ.get("POSTGRES_USER","postgres"))
    p.add_argument("--host", default=os.environ.get("POSTGRES_HOST","localhost"))
    p.add_argument("--workdir", type=Path,
                   default=Path(os.environ.get("WORKDIR", "/var/tmp/chembl")))
    p.add_argument("--jobs", type=int, default=int(os.environ.get("JOBS", "4")))
    p.add_argument("--keep-download", action="store_true", default=True)
    p.add_argument("--no-keep-download", dest="keep_download", action="store_false",
                   help="delete the tarball once the dump is extracted")
    return p.parse_args(argv)


def cleanup(workdir: Path, version: int|None=None) -> None:
    pattern = f"chembl_{'*' if version is None else version}_postgresql.*"
    for p in sorted(workdir.glob(pattern)):
        if p.suffix in {".gz", ".dmp"} or p.name.endswith(".tar.gz"):
            log.info("removing %s", p)
            p.unlink(missing_ok=True)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    args = parse_args(argv)

    bad = [t for t in args.tables if not IDENT_RE.match(t)]
    if bad:
        log.error("not plain lowercase identifiers: %s", ", ".join(bad))
        return 2

    version = args.chembl_version
    if version == "latest":
        version = get_latest_version()
    dbname = args.dbname or os.environ.get("PGDATABASE") or f"chembl_{version}"
    stem = f"chembl_{version}_postgresql"
    url = f"{BASE_URL}/chembl_{version}/{stem}.tar.gz"
    tarball = args.workdir / f"{stem}.tar.gz"
    dump = args.workdir / f"{stem}.dmp"

    ensure_database(
        dbname=dbname,
        fallback_dbname=args.fallbackdbname,
        user=args.user,
        host=args.host,
        password=args.password,
        )

    missing = missing_tables(
        dbname=dbname,
        tables=args.tables,
        schema=args.schema,
        user=args.user,
        host=args.host,
        password=args.password,
        )
    if not missing:
        log.info("all %d table(s) already present in %s", len(args.tables), dbname)
        return 0
    log.info("missing: %s", ", ".join(missing))

    args.workdir.mkdir(parents=True, exist_ok=True)
    if not dump.exists():
        download(url, tarball)
        extract_member(tarball, f"{stem}.dmp", dump)
        if not args.keep_download:
            tarball.unlink(missing_ok=True)

    pg_restore(
        dump=dump,
        dbname=dbname,
        tables=missing,
        schema=args.schema,
        jobs=args.jobs,
        user=args.user,
        host=args.host,
        password=args.password,
        )
    log.info("done")
    if not args.keep_download:
        cleanup(args.workdir)

    return 0


if __name__ == "__main__":
    sys.exit(main())