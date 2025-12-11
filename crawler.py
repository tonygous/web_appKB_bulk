import asyncio
import logging
import re
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Sequence, Set, Tuple
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


@dataclass
class PageRecord:
    url: str
    host: str
    path: str
    title: str
    markdown: str


class AsyncCrawler:
    def __init__(
        self,
        start_url: str,
        max_pages: int = 50,
        timeout: float = 10.0,
        crawl_timeout: float = 90.0,
        include_subdomains: bool = True,
        allowed_hosts: Optional[Sequence[str]] = None,
        path_prefixes: Optional[Sequence[str]] = None,
        max_depth: int = 4,
        max_concurrent_requests: int = 5,
        respect_robots: bool = False,
    ) -> None:
        self.start_url = self._normalize_url(start_url)
        self.max_pages = max(1, min(max_pages, 500))
        self.timeout = timeout
        self.crawl_timeout = crawl_timeout
        self.include_subdomains = include_subdomains
        self.max_depth = max(0, max_depth)
        self.max_concurrent_requests = max(1, max_concurrent_requests)
        self.respect_robots = respect_robots  # Placeholder for future robots.txt handling

        self.allowed_hosts = self._normalize_hosts(allowed_hosts)
        self.path_prefixes = self._normalize_prefixes(path_prefixes)

        parsed_start = urlparse(self.start_url)
        self.root_domain = self._extract_root_domain(parsed_start.hostname)

        self.visited: Set[str] = set()
        self.enqueued: Set[str] = {self.start_url}
        self.queue: Deque[Tuple[str, int]] = deque([(self.start_url, 0)])

        self.errors: List[Dict[str, str]] = []
        self.timed_out: bool = False
        self.logger = logging.getLogger(__name__)

    def _normalize_hosts(self, hosts: Optional[Sequence[str]]) -> List[str]:
        if not hosts:
            return []
        normalized: List[str] = []
        for host in hosts:
            if not host:
                continue
            normalized.append(host.strip().lower())
        return normalized

    def _normalize_prefixes(self, prefixes: Optional[Sequence[str]]) -> List[str]:
        if not prefixes:
            return []
        normalized: List[str] = []
        for prefix in prefixes:
            if not prefix:
                continue
            normalized.append(prefix.strip())
        return normalized

    def _normalize_url(self, url: str) -> str:
        if not url:
            return ""
        parsed = urlparse(url)
        if not parsed.scheme:
            parsed = urlparse(f"https://{url}")
        parsed = parsed._replace(fragment="")
        normalized = urlunparse(parsed)
        return normalized.rstrip("/") or normalized

    @staticmethod
    def _normalize_markdown(content: str) -> str:
        if not content:
            return ""
        collapsed = re.sub(r"\n{3,}", "\n\n", content)
        trimmed_lines = [line.rstrip() for line in collapsed.splitlines()]
        return "\n".join(trimmed_lines).strip()

    def _extract_root_domain(self, hostname: Optional[str]) -> str:
        if not hostname:
            return ""
        parts = hostname.lower().split(".")
        if len(parts) >= 2:
            return ".".join(parts[-2:])
        return hostname.lower()

    def _record_error(self, url: str, reason: str, status: str = "") -> None:
        self.errors.append({"url": url, "reason": reason, "status": status})
        self.logger.warning("Error fetching %s: %s %s", url, reason, status)

    def _is_internal_link(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in {"", "http", "https"}:
            return False

        hostname = (parsed.hostname or "").lower()
        if hostname == "":
            return True

        if self.allowed_hosts:
            for allowed in self.allowed_hosts:
                if hostname == allowed or hostname.endswith(f".{allowed}"):
                    return True
            return False

        if hostname == self.root_domain:
            return True

        if self.include_subdomains and hostname.endswith(f".{self.root_domain}"):
            return True

        return False

    def _matches_path_prefix(self, url: str) -> bool:
        if not self.path_prefixes:
            return True
        parsed = urlparse(url)
        path_with_query = parsed.path or "/"
        if parsed.query:
            path_with_query += f"?{parsed.query}"
        return any(path_with_query.startswith(prefix) for prefix in self.path_prefixes)

    def _is_allowed_url(self, url: str) -> bool:
        if self._has_ignored_extension(url):
            return False
        if not self._is_internal_link(url):
            return False
        if not self._matches_path_prefix(url):
            return False
        return True

    def _has_ignored_extension(self, url: str) -> bool:
        path = urlparse(url).path.lower()
        return any(path.endswith(ext) for ext in IGNORED_EXTENSIONS)

    async def _fetch_content(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
        try:
            response = await client.get(url, timeout=self.timeout, follow_redirects=True)
            content_type = response.headers.get("content-type", "")
            if response.status_code == httpx.codes.OK and "text/html" in content_type:
                return response.text
            self._record_error(url, "non-200 or non-html", str(response.status_code))
        except httpx.TimeoutException:
            self._record_error(url, "timeout")
        except httpx.HTTPError as exc:  # pragma: no cover - safety net
            self._record_error(url, "http_error", str(getattr(exc.response, "status_code", "")))
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
            if not self._is_allowed_url(absolute_url):
                continue
            if absolute_url not in self.visited and absolute_url not in self.enqueued:
                links.append(absolute_url)
        return links

    async def _process_url(
        self, client: httpx.AsyncClient, url: str
    ) -> Optional[Tuple[PageRecord, List[str]]]:
        content = await self._fetch_content(client, url)
        if not content:
            return None

        soup = BeautifulSoup(content, "html.parser")
        links = self._extract_links(url, soup)
        title, markdown = self._clean_html(content, url)
        parsed_url = urlparse(url)
        host = parsed_url.hostname or self.root_domain or ""
        path = parsed_url.path or "/"
        if parsed_url.query:
            path = f"{path}?{parsed_url.query}"
        page_record = PageRecord(
            url=url,
            host=host,
            path=path or "/",
            title=title or "",
            markdown=markdown,
        )
        return page_record, links

    async def crawl_with_pages(self) -> List[PageRecord]:
        pages: List[PageRecord] = []
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        start_time = time.monotonic()

        async with httpx.AsyncClient(
            headers={"User-Agent": "Web-to-KnowledgeBase/1.0 (+https://example.com)"}
        ) as client:
            while self.queue and len(pages) < self.max_pages:
                if time.monotonic() - start_time > self.crawl_timeout:
                    self.timed_out = True
                    self.logger.warning("Crawl timed out after %.2f seconds", self.crawl_timeout)
                    break

                tasks = []
                task_urls: List[Tuple[str, int]] = []
                while (
                    self.queue
                    and len(tasks) < self.max_concurrent_requests
                    and len(pages) + len(tasks) < self.max_pages
                ):
                    current_url, depth = self.queue.popleft()
                    if current_url in self.visited:
                        continue
                    if not self._is_allowed_url(current_url):
                        continue
                    self.visited.add(current_url)
                    task_urls.append((current_url, depth))

                    async def _task(url=current_url):
                        async with semaphore:
                            return await self._process_url(client, url)

                    tasks.append(asyncio.create_task(_task()))

                if not tasks:
                    break

                results = await asyncio.gather(*tasks)
                for (current_url, depth), result in zip(task_urls, results):
                    if not result:
                        continue
                    page_record, links = result
                    pages.append(page_record)
                    for link in links:
                        next_depth = depth + 1
                        if next_depth > self.max_depth:
                            continue
                        if len(self.visited) + len(self.queue) >= self.max_pages:
                            break
                        if not self._is_allowed_url(link):
                            continue
                        if link in self.visited or link in self.enqueued:
                            continue
                        self.enqueued.add(link)
                        self.queue.append((link, next_depth))

        return pages

    async def crawl(self) -> str:
        pages = await self.crawl_with_pages()
        return self._combine_pages(pages)

    def _combine_pages(self, pages: List[PageRecord]) -> str:
        """Собираем финальный markdown: summary + контент по хостам."""
        # summary (в HTML-комментарии, чтобы не мешать рендеру)
        summary_lines: List[str] = ["<!-- Crawl summary:"]
        summary_lines.append(f"Total pages: {len(pages)}")
        summary_lines.append(f"Errors: {len(self.errors)}")
        if self.errors:
            for err in self.errors[:20]:
                url = err.get("url", "")
                reason = err.get("reason", "")
                status = err.get("status", "")
                if status:
                    summary_lines.append(f"  - {url} ({reason} {status})")
                else:
                    summary_lines.append(f"  - {url} ({reason})")
        if self.timed_out:
            summary_lines.append(
                f"Crawl timed out after {self.crawl_timeout:.0f} seconds."
            )
        summary_lines.append("-->")

        # группируем страницы по host
        pages_by_host: Dict[str, List[PageRecord]] = {}
        for page in pages:
            host = page.host or self.root_domain or "unknown host"
            pages_by_host.setdefault(host, []).append(page)

        markdown_parts: List[str] = []
        for host in sorted(pages_by_host.keys()):
            host_section: List[str] = [f"# {host}"]
            for page in pages_by_host[host]:
                title = page.title or page.path or page.url
                body = page.markdown.strip()
                host_section.append(f"## {title}\n\n{body}")
            markdown_parts.append("\n\n".join(host_section))

        body = "\n\n---\n\n".join(markdown_parts)
        combined = "\n".join(summary_lines) + "\n\n" + body
        return self._normalize_markdown(combined)

