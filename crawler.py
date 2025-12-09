import asyncio
from typing import List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
import html2text


IGNORED_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".ico",
    ".css",
    ".js",
    ".pdf",
    ".zip",
    ".tar",
    ".gz",
    ".rar",
    ".mp4",
    ".mp3",
    ".wav",
}


class AsyncCrawler:
    def __init__(self, start_url: str, max_pages: int = 10) -> None:
        self.start_url = start_url
        self.max_pages = max_pages
        self.parsed_start = urlparse(start_url)
        self.visited: Set[str] = set()
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.queue.put_nowait(start_url)

    def _is_internal_link(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in {"", "http", "https"}:
            return False
        return parsed.netloc == "" or parsed.netloc == self.parsed_start.netloc

    def _has_ignored_extension(self, url: str) -> bool:
        path = urlparse(url).path.lower()
        return any(path.endswith(ext) for ext in IGNORED_EXTENSIONS)

    async def _fetch_content(self, client: httpx.AsyncClient, url: str) -> Optional[httpx.Response]:
        try:
            response = await client.get(url, timeout=10.0, follow_redirects=True)
            if response.status_code == httpx.codes.OK and "text/html" in response.headers.get("content-type", ""):
                return response
        except httpx.HTTPError:
            return None
        return None

    def _clean_html(self, html: str) -> Tuple[str, str]:
        soup = BeautifulSoup(html, "html.parser")
        for tag_name in ["script", "style", "nav", "footer", "header", "aside"]:
            for tag in soup.find_all(tag_name):
                tag.decompose()

        page_title = soup.title.string.strip() if soup.title and soup.title.string else "Untitled"
        markdown_converter = html2text.HTML2Text()
        markdown_converter.ignore_links = False
        markdown_converter.body_width = 0
        markdown_content = markdown_converter.handle(str(soup))
        return page_title, markdown_content.strip()

    def _extract_links(self, url: str, soup: BeautifulSoup) -> List[str]:
        links = []
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href")
            absolute_url = urljoin(url, href)
            if not self._is_internal_link(absolute_url):
                continue
            if self._has_ignored_extension(absolute_url):
                continue
            normalized_url = absolute_url.split("#")[0].rstrip("/")
            if normalized_url and normalized_url not in self.visited:
                links.append(normalized_url)
        return links

    async def _process_url(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        url: str,
    ) -> Optional[Tuple[str, str]]:
        async with semaphore:
            response = await self._fetch_content(client, url)
            if not response:
                return None
            soup = BeautifulSoup(response.text, "html.parser")
            for link in self._extract_links(url, soup):
                if link not in self.visited and self.queue.qsize() + len(self.visited) < self.max_pages:
                    self.queue.put_nowait(link)

            title, markdown = self._clean_html(response.text)
            return title, markdown

    async def crawl(self) -> str:
        pages: List[Tuple[str, str]] = []
        concurrency_limit = 5
        semaphore = asyncio.Semaphore(concurrency_limit)
        async with httpx.AsyncClient() as client:
            while len(pages) < self.max_pages and not self.queue.empty():
                tasks: List[asyncio.Task[Optional[Tuple[str, str]]]] = []
                while not self.queue.empty() and len(tasks) + len(pages) < self.max_pages:
                    current_url = await self.queue.get()
                    if current_url in self.visited:
                        continue
                    if self._has_ignored_extension(current_url):
                        continue
                    if not self._is_internal_link(current_url):
                        continue
                    self.visited.add(current_url)
                    tasks.append(asyncio.create_task(self._process_url(client, semaphore, current_url)))

                if not tasks:
                    break

                results = await asyncio.gather(*tasks)
                for result in results:
                    if result:
                        pages.append(result)

        return self._combine_pages(pages)

    def _combine_pages(self, pages: List[Tuple[str, str]]) -> str:
        markdown_parts = []
        for title, markdown in pages:
            section = f"# {title}\n\n{markdown.strip()}"
            markdown_parts.append(section)
        return "\n\n---\n\n".join(markdown_parts)
