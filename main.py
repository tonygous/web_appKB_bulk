from typing import Optional

from fastapi import FastAPI, Form, Request
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
    start_url: str = Form(...),
    max_pages: Optional[int] = Form(10),
):
    pages_to_crawl = max(1, max_pages or 1)
    crawler = AsyncCrawler(start_url=start_url, max_pages=pages_to_crawl)
    markdown_content = await crawler.crawl()
    headers = {
        "Content-Disposition": "attachment; filename=knowledgebase.md"
    }
    return Response(content=markdown_content, media_type="text/markdown", headers=headers)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
