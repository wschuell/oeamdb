import hashlib, subprocess
from html import escape

from fastapi import APIRouter, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse
from sqlalchemy import MetaData

import pygraphviz as pgv

from ..sql_model import Base   # NOT from main


from fastapi.templating import Jinja2Templates
from pathlib import Path

templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
MERMAID_VERSION = "11.17.2"


router = APIRouter()

def to_dot(md: MetaData) -> str:
    out = ['digraph schema {', '  rankdir=LR;',
           '  node [shape=plain fontname="Helvetica" fontsize=10];']
    for t in md.sorted_tables:
        rows = "".join(
            f'<tr><td port="{escape(c.name)}" align="left">'
            f'{"<b>" if c.primary_key else ""}{escape(c.name)}{"</b>" if c.primary_key else ""}</td>'
            f'<td align="left"><font color="#666666">{escape(type(c.type).__name__)}</font></td></tr>'
            for c in t.columns)
        out.append(
            f'  "{t.name}" [label=<<table border="0" cellborder="1" cellspacing="0">'
            f'<tr><td colspan="2" bgcolor="#e8eef7"><b>{escape(t.name)}</b></td></tr>'
            f'{rows}</table>>];')
    for t in md.sorted_tables:
        for fk in t.foreign_key_constraints:
            for el in fk.elements:
                out.append(f'  "{t.name}":"{el.parent.name}" -> '
                           f'"{el.column.table.name}":"{el.column.name}";')
    out.append("}")
    return "\n".join(out)

def render_svg(dot: str) -> bytes:
        return pgv.AGraph(string=dot).draw(format="svg", prog="dot")

def etag_matches(header: str | None, etag: str) -> bool:
    if not header:
        return False
    if header.strip() == "*":
        return True
    tags = {t.strip().removeprefix("W/") for t in header.split(",")}
    return etag in tags

def to_mermaid(md: MetaData) -> str:
    out = ["erDiagram"]
    for t in md.sorted_tables:
        out.append(f"  {t.name} {{")
        for c in t.columns:
            keys = [k for k, on in (("PK", c.primary_key), ("FK", bool(c.foreign_keys))) if on]
            out.append(f"    {type(c.type).__name__} {c.name} {','.join(keys)}".rstrip())
        out.append("  }")
    for t in md.sorted_tables:
        for fk in t.foreign_key_constraints:
            left = "|o" if any(c.nullable for c in fk.columns) else "||"
            label = ",".join(c.name for c in fk.columns)
            out.append(f'  {fk.referred_table.name} {left}--o{{ {t.name} : "{label}"')
    return "\n".join(out)

def cached_response(request: Request, body: str, media_type: str) -> Response:
    data = body.encode()
    etag = '"' + hashlib.sha256(data).hexdigest()[:16] + '"'
    headers = {"ETag": etag, "Cache-Control": "no-cache"}
    if etag_matches(request.headers.get("if-none-match"), etag):
        return Response(status_code=304, headers=headers)
    return Response(data, media_type=media_type, headers=headers)

_current: tuple[str, bytes] | None = None

async def get_svg() -> tuple[bytes, str]:
    global _current
    dot = to_dot(Base.metadata)
    etag = '"' + hashlib.sha256(dot.encode()).hexdigest()[:16] + '"'
    if _current is None or _current[0] != etag:
        _current = (etag, await run_in_threadpool(render_svg, dot))
    return _current[1], etag

@router.get("/schema.svg")
async def schema_svg(request: Request):
    svg, etag = await get_svg()
    headers = {"ETag": etag, "Cache-Control": "no-cache"}
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers=headers)
    return Response(svg, media_type="image/svg+xml", headers=headers)

@router.get("/schema", response_class=HTMLResponse)
async def schema_page():
    svg, _ = await get_svg()
    text = svg.decode()
    return f"<!doctype html><title>Schema</title><body>{text[text.index('<svg'):]}</body>"

@router.get("/schema.mmd")
async def schema_mmd(request: Request):
    """Raw Mermaid source."""
    return cached_response(request, to_mermaid(Base.metadata), "text/plain; charset=utf-8")

@router.get("/schema/mermaid")
async def schema_mermaid_page(request: Request):
    response = templates.TemplateResponse(
        request, "schema_mermaid.html",
        {"diagram": to_mermaid(Base.metadata), "mermaid_version": MERMAID_VERSION},
    )
    etag = '"' + hashlib.sha256(response.body).hexdigest()[:16] + '"'
    headers = {"ETag": etag, "Cache-Control": "no-cache"}
    if etag_matches(request.headers.get("if-none-match"), etag):
        return Response(status_code=304, headers=headers)
    response.headers.update(headers)
    return response