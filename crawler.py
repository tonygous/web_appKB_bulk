import asyncio
from collections import deque
from typing import Deque, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

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
    def __init__(
        self,
        start_url: str,
        max_pages: int = 50,
        timeout: float = 10.0,
        include_subdomains: bool = True,
    ) -> None:
        self.start_url = self._normalize_url(start_url)
        self.max_pages = max_pages
        self.timeout = timeout
        self.include_subdomains = include_subdomains

        parsed_start = urlparse(self.start_url)
        self.root_domain = self._extract_root_domain(parsed_start.hostname)

        self.visited: Set[str] = set()
        self.enqueued: Set[str] = {self.start_url}
        self.queue: Deque[str] = deque([self.start_url])

    def _normalize_url(self, url: str) -> str:
        if not url:
            return ""
        parsed = urlparse(url)
        if not parsed.scheme:
            parsed = urlparse(f"https://{url}")
        parsed = parsed._replace(fragment="")
        normalized = urlunparse(parsed)
        return normalized.rstrip("/") or normalized

    def _extract_root_domain(self, hostname: Optional[str]) -> str:
        if not hostname:
            return ""
        parts = hostname.lower().split(".")
        if len(parts) >= 2:
            return ".".join(parts[-2:])
        return hostname.lower()

    def _is_internal_link(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in {"", "http", "https"}:
            return False

        hostname = (parsed.hostname or "").lower()
        if hostname == "":
            return True

        if hostname == self.root_domain:
            return True

        if self.include_subdomains and hostname.endswith(f".{self.root_domain}"):
            return True

        return False

    def _has_ignored_extension(self, url: str) -> bool:
        path = urlparse(url).path.lower()
        return any(path.endswith(ext) for ext in IGNORED_EXTENSIONS)

    async def _fetch_content(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
        try:
            response = await client.get(url, timeout=self.timeout, follow_redirects=True)
            content_type = response.headers.get("content-type", "")
            if response.status_code == httpx.codes.OK and "text/html" in content_type:
                return response.text
        except httpx.HTTPError:
            return None
        return None

    def _clean_html(self, html: str, url: str) -> Tuple[str, str]:
        soup = BeautifulSoup(html, "html.parser")
        for tag_name in ["script", "style", "nav", "footer", "header", "aside"]:
            for tag in soup.find_all(tag_name):
                tag.decompose()

        content_area = soup.find("main") or soup.find("article") or soup.body or soup
        title_text = ""
        if soup.title and soup.title.string:
            title_text = soup.title.string.strip()
        title = title_text or url

        markdown_converter = html2text.HTML2Text()
        markdown_converter.body_width = 0
        markdown_converter.ignore_links = False
        markdown_content = markdown_converter.handle(str(content_area))
        return title, markdown_content.strip()

    def _extract_links(self, url: str, soup: BeautifulSoup) -> List[str]:
        links: List[str] = []
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href")
            absolute_url = self._normalize_url(urljoin(url, href))
            if not absolute_url:
                continue
            if not self._is_internal_link(absolute_url):
                continue
            if self._has_ignored_extension(absolute_url):
                continue
            if absolute_url not in self.visited and absolute_url not in self.enqueued:
                links.append(absolute_url)
        return links

    async def _process_url(
        self, client: httpx.AsyncClient, url: str
    ) -> Optional[Tuple[str, str, List[str]]]:
        content = await self._fetch_content(client, url)
        if not content:
            return None

        soup = BeautifulSoup(content, "html.parser")
        links = self._extract_links(url, soup)
        title, markdown = self._clean_html(content, url)
        return title, markdown, links

    async def crawl(self) -> str:
        pages: List[Tuple[str, str]] = []
        concurrency_limit = 5

        async with httpx.AsyncClient() as client:
            while self.queue and len(pages) < self.max_pages:
                tasks = []
                while (
                    self.queue
                    and len(tasks) < concurrency_limit
                    and len(pages) + len(tasks) < self.max_pages
                ):
                    current_url = self.queue.popleft()
                    if current_url in self.visited:
                        continue
                    if self._has_ignored_extension(current_url):
                        continue
                    if not self._is_internal_link(current_url):
                        continue
                    self.visited.add(current_url)
                    tasks.append(asyncio.create_task(self._process_url(client, current_url)))

                if not tasks:
                    break

                results = await asyncio.gather(*tasks)
                for result in results:
                    if not result:
                        continue
                    title, markdown, links = result
                    pages.append((title, markdown))
                    for link in links:
                        if len(self.visited) + len(self.queue) >= self.max_pages:
                            break
                        self.enqueued.add(link)
                        self.queue.append(link)

        return self._combine_pages(pages)

    def _combine_pages(self, pages: List[Tuple[str, str]]) -> str:
        markdown_parts = []
        for title, markdown in pages:
            section = f"# {title}\n\n{markdown.strip()}"
            markdown_parts.append(section)
        return "\n\n---\n\n".join(markdown_parts)
