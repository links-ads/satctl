import logging
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter

from satctl.auth.base import Authenticator
from satctl.downloaders.base import Downloader
from satctl.progress import ProgressReporter

log = logging.getLogger(__name__)


class HTTPDownloader(Downloader):
    """HTTP downloader with authentication, retries, and progress reporting."""

    def __init__(
        self,
        authenticator: Authenticator,
        max_retries: int = 3,
        chunk_size: int = 8192,
        timeout: int = 30,
        pool_connections: int = 10,
        pool_maxsize: int = 2,
    ):
        super().__init__(authenticator)
        self.max_retries = max_retries
        self.chunk_size = chunk_size
        self.timeout = timeout
        self.pool_conns = pool_connections
        self.pool_size = pool_maxsize

    def init(self) -> None:
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=self.pool_conns, pool_maxsize=self.pool_size)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def download(
        self,
        uri: str,
        destination: Path,
        item_id: str,
        progress: ProgressReporter,
    ) -> bool:
        """Download file from HTTP URL with retries and progress reporting."""
        for attempt in range(self.max_retries):
            try:
                # Ensure we have authentication
                if not self.auth.ensure_authenticated():
                    log.error(f"Authentication failed on attempt {attempt + 1}")
                    continue

                headers = self.auth.auth_headers
                log.debug(f"Downloading {uri} (attempt {attempt + 1}/{self.max_retries})")
                response = self.session.get(uri, headers=headers, stream=True, timeout=self.timeout)

                if response.status_code == 401:
                    log.warning("Authentication failed (401), attempting to refresh token")
                    if not self.auth.ensure_authenticated(refresh=True):
                        log.error("Failed to refresh token")
                        continue
                response.raise_for_status()

                # Set total size for progress tracking if available
                total_size = None
                if "Content-Length" in response.headers:
                    total_size = int(response.headers["Content-Length"])
                    progress.set_task_duration(item_id, total_size)

                # Download file in chunks with progress reporting
                downloaded_bytes = 0
                with open(destination, "wb") as f:
                    for chunk in response.iter_content(chunk_size=self.chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded_bytes += len(chunk)
                            progress.update_progress(item_id, advance=len(chunk))

                log.debug(f"Successfully downloaded {uri} ({downloaded_bytes} bytes)")
                return True

            except requests.exceptions.Timeout:
                log.warning(f"Timeout downloading {uri} on attempt {attempt + 1}")
            except requests.exceptions.RequestException as e:
                log.error(f"Request error downloading {uri} on attempt {attempt + 1}: {e}")
            except Exception as e:
                log.exception(e)
                log.error(f"Unexpected error downloading {uri} on attempt {attempt + 1}: {type(e)} - {e}")

        log.error(f"Failed to download {uri} after {self.max_retries} attempts")
        return False

    def close(self) -> None:
        if self.session:
            self.session.close()
