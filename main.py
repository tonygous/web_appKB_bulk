from typing import Optional

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import Response
from fastapi.templating import Jinja2Templates

from crawler import AsyncCrawler

app = FastAPI(title="Web-to-KnowledgeBase")
templates = Jinja2Templates(directory="templates")


@app.get("/")
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/generate")
async def generate_knowledgebase(
    url: str = Form(...),
    max_pages: Optional[int] = Form(10),
):
    pages_to_crawl = max(1, max_pages or 1)
    crawler = AsyncCrawler(start_url=url, max_pages=pages_to_crawl, include_subdomains=True)
    markdown_content = await crawler.crawl()

    if not markdown_content:
        raise HTTPException(status_code=400, detail="No content could be extracted from the provided URL.")

    headers = {"Content-Disposition": "attachment; filename=knowledgebase.md"}
    return Response(
        content=markdown_content,
        media_type="text/markdown; charset=utf-8",
        headers=headers,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
