import io
import re
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

import httpx
from fastapi import Body, FastAPI, Form, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from fastapi.templating import Jinja2Templates

from crawler import AsyncCrawler

app = FastAPI(title="Web-to-KnowledgeBase")
templates = Jinja2Templates(directory="templates")


@app.get("/")
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


def _parse_list_field(raw_value: Optional[str]) -> List[str]:
    if not raw_value:
        return []
    parts = re.split(r"[,\s]+", raw_value)
    return [part.strip() for part in parts if part.strip()]


def _slugify(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    return value or "page"


def _clamp_max_pages(raw_value: Optional[int]) -> int:
    pages = raw_value or 1
    return max(1, min(pages, 500))


@app.post("/generate")
async def generate_knowledgebase(
    url: str = Form(...),
    max_pages: Optional[int] = Form(10),
    allowed_hosts: Optional[str] = Form(None),
    path_prefixes: Optional[str] = Form(None),
):
    if not url:
        raise HTTPException(status_code=400, detail="URL is required.")

    pages_to_crawl = _clamp_max_pages(max_pages)
    allowed = _parse_list_field(allowed_hosts)
    prefixes = _parse_list_field(path_prefixes)

    crawler = AsyncCrawler(
        start_url=url,
        max_pages=pages_to_crawl,
        include_subdomains=True,
        allowed_hosts=allowed,
        path_prefixes=prefixes,
    )

    markdown_content = await crawler.crawl()
    if not markdown_content:
        raise HTTPException(
            status_code=400,
            detail="No content could be extracted from the provided URL.",
        )

    parsed = urlparse(url)
    hostname = _slugify(parsed.hostname or "output")
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    outputs_dir = Path("outputs")
    outputs_dir.mkdir(parents=True, exist_ok=True)
    output_filename = outputs_dir / f"{hostname}__{timestamp}.md"
    output_filename.write_text(markdown_content, encoding="utf-8")

    headers = {"Content-Disposition": "attachment; filename=knowledgebase.md"}
    return Response(
        content=markdown_content,
        media_type="text/markdown; charset=utf-8",
        headers=headers,
    )


@app.post("/crawl-preview")
async def crawl_preview(
    url: str = Form(...),
    max_pages: Optional[int] = Form(10),
    allowed_hosts: Optional[str] = Form(None),
    path_prefixes: Optional[str] = Form(None),
):
    if not url:
        raise HTTPException(status_code=400, detail="URL is required.")

    pages_to_crawl = _clamp_max_pages(max_pages)
    allowed = _parse_list_field(allowed_hosts)
    prefixes = _parse_list_field(path_prefixes)

    crawler = AsyncCrawler(
        start_url=url,
        max_pages=pages_to_crawl,
        include_subdomains=True,
        allowed_hosts=allowed,
        path_prefixes=prefixes,
    )

    pages = await crawler.crawl_with_pages()
    if not pages:
        raise HTTPException(
            status_code=400,
            detail="No pages found for this configuration.",
        )

    preview = []
    for idx, page in enumerate(pages):
        slug_source = page.title or page.path or page.url
        filename = f"{page.host}__{_slugify(slug_source)}.md"
        preview.append(
            {
                "id": idx,
                "url": page.url,
                "host": page.host,
                "path": page.path,
                "title": page.title,
                "suggested_filename": filename,
            }
        )

    return preview


@app.post("/download-selected")
async def download_selected(payload=Body(...)):
    url = payload.get("url")
    max_pages = payload.get("max_pages", 10)
    allowed_hosts = payload.get("allowed_hosts") or []
    path_prefixes = payload.get("path_prefixes") or []
    pages = payload.get("pages", [])

    if not url:
        raise HTTPException(status_code=400, detail="URL is required.")
    if not pages:
        raise HTTPException(status_code=400, detail="No pages selected.")

    crawler = AsyncCrawler(
        start_url=url,
        max_pages=_clamp_max_pages(max_pages),
        include_subdomains=True,
        allowed_hosts=allowed_hosts,
        path_prefixes=path_prefixes,
    )

    buffer = io.BytesIO()
    added_files = 0

    async with httpx.AsyncClient() as client:
        with zipfile.ZipFile(
            buffer, mode="w", compression=zipfile.ZIP_DEFLATED
        ) as zip_file:
            for page in pages:
                page_url = page.get("url")
                filename = page.get("filename") or f"page-{added_files}.md"
                if not page_url or not crawler._is_allowed_url(page_url):
                    continue

                content = await crawler._fetch_content(client, page_url)
                if not content:
                    continue

                title, markdown = crawler._clean_html(content, page_url)
                host = page.get("host") or (urlparse(page_url).hostname or "")
                heading = page.get("title") or page.get("path") or title

                body = f"# {host}\n## {heading}\n\n{markdown}"
                normalized_body = crawler._normalize_markdown(body)
                zip_file.writestr(filename, normalized_body)
                added_files += 1

    if added_files == 0:
        raise HTTPException(
            status_code=400,
            detail="No pages could be downloaded with the provided selection.",
        )

    buffer.seek(0)
    headers = {"Content-Disposition": 'attachment; filename="knowledgebase_pages.zip"'}
    return StreamingResponse(buffer, media_type="application/zip", headers=headers)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
