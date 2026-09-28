from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Request, Query, Response
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker


import oeamdb
import os
from pathlib import Path

from sqlalchemy import URL

import csv
import io
from typing import Literal


from dataclasses import dataclass, field
 
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.routing import APIRoute
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=BASE_DIR / "templates")

@dataclass
class Endpoint:
    path: str
    description: str = ""
    highlight: bool = False
    color: str | None = None          # per-row highlight color, e.g. "#f93e3e"

@dataclass
class Section:
    name: str
    endpoints: list[Endpoint] = field(default_factory=list)


def get_engine_url() -> URL | str:
    """Build the database URL from the environment.

    Precedence:
      1. OEAMDB_WEB_DATABASE_URL             - explicit, wins outright (sqlite, external host, tests)
      2. OEAMDB_WEB_POSTGRES_* components    - assembled with correct escaping
      3. SQLite fallback                     - only if OEAMDB_WEB_SQLITE_PATH is set

    Raises RuntimeError if nothing is configured, rather than silently
    connecting somewhere unintended.
    """
    if url := os.getenv("OEAMDB_WEB_DATABASE_URL"):
        return url

    if sqlite_path := os.getenv("OEAMDB_WEB_SQLITE_PATH"):
        return f"sqlite:///{Path(sqlite_path).expanduser().resolve()}"

    database = os.getenv("OEAMDB_WEB_POSTGRES_DB")
    user = os.getenv("OEAMDB_WEB_POSTGRES_USER")
    if not (database and user):
        raise RuntimeError(
            "No database configured: set OEAMDB_WEB_DATABASE_URL, or OEAMDB_WEB_POSTGRES_DB and "
            "OEAMDB_WEB_POSTGRES_USER (plus OEAMDB_WEB_POSTGRES_PASSWORD), or OEAMDB_WEB_SQLITE_PATH."
        )

    query: dict[str, str] = {}
    if sslmode := os.getenv("OEAMDB_WEB_POSTGRES_SSLMODE"):
        query["sslmode"] = sslmode
    if schema := os.getenv("OEAMDB_WEB_POSTGRES_SCHEMA"):
        query["options"] = f"-csearch_path={schema}"

    return URL.create(
        drivername=os.getenv("OEAMDB_WEB_DB_DRIVER", "postgresql+psycopg"),
        username=user,
        password=os.getenv("OEAMDB_WEB_POSTGRES_PASSWORD"),
        host=os.getenv("OEAMDB_WEB_POSTGRES_HOST", "db"),
        port=int(os.getenv("OEAMDB_WEB_POSTGRES_PORT", "5432")),
        database=database,
        query=query,
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    oeam_db = oeamdb.Oeamdb(create_metadata=False,engine_url=get_engine_url())
    engine = oeam_db.engine
    app.state.oeam_db = oeam_db
    app.state.engine = engine
    app.state.sessionmaker = sessionmaker(engine, expire_on_commit=False)
    yield
    engine.dispose()

app = FastAPI(
            lifespan=lifespan,
            title="ÖAMDB",
            version=oeamdb.__version__,
            description="Database for Austrian approved medication and usage in University courses.")

def get_session(request: Request):
    with request.app.state.sessionmaker() as session:
        yield session

@app.get("/results")
def results(session: Session = Depends(get_session)):
    rows = session.execute(
        text("SELECT * FROM substance;"),
    ).mappings().all()
    return list(rows)

# --- 1. Docs links, taken from the app config (follow any URL changes) -------
def docs_section() -> Section:
    eps = []
    if app.docs_url:
        eps.append(Endpoint(app.docs_url, "Interactive API documentation, with list of all available parameters (Swagger UI)", highlight=True))
    if app.redoc_url:
        eps.append(Endpoint(app.redoc_url, "Alternative reference documentation (ReDoc)"))
    if app.openapi_url:
        eps.append(Endpoint(app.openapi_url, "Raw OpenAPI schema"))
    return Section("Documentation", eps)


# --- 2. Auto-discovered: any GET route that opts in with openapi_extra -------
def discovered_section() -> Section:
    eps = []
    for r in app.routes:
        if not isinstance(r, APIRoute) or "GET" not in r.methods:
            continue
        opts = (r.openapi_extra or {}).get("x-index")
        if opts is None or "{" in r.path:       # skip non-opted-in and parametrised paths
            continue
        desc = r.summary or (r.description or "").strip().split("\n")[0]
        eps.append(Endpoint(r.path, desc, opts.get("highlight", False), opts.get("color")))
    return Section("Service", eps)


# --- 3. Values computed per request ------------------------------------------
def service_is_healthy() -> bool:
    return True  # replace with a real check (DB ping, etc.)

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def index(request: Request):
    service = discovered_section()
    # Example: turn /health red when the check fails
    for ep in service.endpoints:
        if ep.path == "/health" and not service_is_healthy():
            ep.highlight, ep.color = True, "#f93e3e"
            ep.description += " — currently failing"
 
    return templates.TemplateResponse(request, "index.html", {
        "title": app.title,
        "version": app.version,
        "description": app.description,
        "base_url": request.url.netloc + request.scope.get("root_path", "") + "/", #str(request.base_url),
        "sections": [s for s in (docs_section(), service) if s.endpoints],
    })


# --- Example routes that opt in to the index ---------------------------------
@app.get("/health", summary="Health check — returns service status",
         openapi_extra={"x-index": {"highlight": False}})
def health():
    return {"status": "ok"}

@app.get("/dbaccess_tuto", summary="Instructions to access the DB directly",
         openapi_extra={"x-index": {"highlight": False}})
def dbaccess_tuto():
    return {"status": "Work In Progress -- this will be a proper page with screenshots",
    "instructions":[
        "install DBeaver https://dbeaver.com/docs/dbeaver/Installation/\n",
        "Launch DBeaver\n",
        "Create new connection with provided configuration/credentials\n",
        "Browse to database - schemas - public - Tables\n",
        ]}

@app.get("/spreadsheet", summary="Download the spreadsheet of the last version",
         openapi_extra={"x-index": {"highlight": True}})
def spreadsheet(
    session: Session = Depends(get_session),
    fmt: Literal["csv", "json"] = Query("csv", alias="format"),
    download: bool = True,
    schema: Literal["public","oeamdb_2025","oeamdb_2026"] = Query("oeamdb_2026", alias="schema"),
    ):
    rows = session.execute(text("""
        SELECT DISTINCT
            DATE_PART('year', p.approval_date) = 2026 AS new_in_2026,
            DATE_PART('year', p.approval_date) = 2025 AS new_in_2025,
            p.product_key,
            vu.description AS vo_unit,
            nimr.reason AS not_in_m_reason,
            p.category,
            p.orig_category,
            DATE_PART('year', p.approval_date) AS approval_year,
            p.approval_date ,
            s.name_en AS substance_name_english,
            s.chembl_id ,
            s.pubchem_cid ,
            STRING_AGG(ac.atc_code, ';') OVER (
                PARTITION BY s.id,p.id
                ORDER BY ac.atc_code
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                AS atc_codes,
            p.human_usage,
            p.vet_usage,
            ac.atc_code_short,
            s.canonical_smiles,
            cm.*
        FROM product p
        INNER JOIN product_substances ps
        ON ps.product_id =p.id
        INNER JOIN substance s
        ON s.id=ps.substance_id 
        INNER JOIN product_atc pa
        ON p.id=pa.product_id
        INNER JOIN atc_code ac
        ON ac.atc_code=pa.atc_code 
        INNER JOIN substance_atc sa
        ON s.id=sa.substance_id
        AND pa.atc_code=sa.atc_code
        LEFT OUTER JOIN course_pivot AS cm
        ON p.id=cm.product_id
        AND s.id=cm.substance_id
        AND ac.atc_code_short=cm.atc_code
        LEFT OUTER JOIN vo_unit vu
        ON p.id=vu.product_id
        AND s.id=vu.substance_id
        AND ac.atc_code_short=vu.atc_code
        LEFT OUTER JOIN not_in_m_reason nimr
        ON p.id=nimr.product_id
        AND s.id=nimr.substance_id
        AND ac.atc_code_short=nimr.atc_code
        ;"""))

    if fmt == "json":
        return rows.mappings().all()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(rows.keys())
    writer.writerows(rows)
    if download:
        return Response(
            content=buf.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="spreadsheet.csv"'},
        )

    return PlainTextResponse(buf.getvalue())