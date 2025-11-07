import logging
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter

from satctl.auth import Authenticator
from satctl.downloaders.base import Downloader
from satctl.model import ProgressEventType
from satctl.progress.events import emit_event

log = logging.getLogger(__name__)

# HTTP downloader configuration defaults
DEFAULT_MAX_RETRIES = 3
DEFAULT_CHUNK_SIZE = 8192  # 8KB
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_POOL_CONNECTIONS = 10
DEFAULT_POOL_MAX_SIZE = 2


class HTTPDownloader(Downloader):
    """HTTP downloader with authentication, retries, and progress reporting."""

    def __init__(
        self,
        max_retries: int = DEFAULT_MAX_RETRIES,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        pool_connections: int = DEFAULT_POOL_CONNECTIONS,
        pool_maxsize: int = DEFAULT_POOL_MAX_SIZE,
    ):
        """Initialize HTTP downloader.

        Args:
            max_retries (int): Maximum download retry attempts. Defaults to 3.
            chunk_size (int): Download chunk size in bytes. Defaults to 8192.
            timeout (int): Request timeout in seconds. Defaults to 30.
            pool_connections (int): Connection pool size. Defaults to 10.
            pool_maxsize (int): Maximum pool size. Defaults to 2.
        """
        self.max_retries = max_retries
        self.chunk_size = chunk_size
        self.timeout = timeout
        self.pool_conns = pool_connections
        self.pool_size = pool_maxsize
        self.auth = None

    def init(self, authenticator: Authenticator, **kwargs: dict) -> None:
        """Initialize HTTP session.

        Args:
            authenticator (Authenticator): auth object to retrieve the session from.
            **kwargs (dict): Additional keyword arguments (unused)
        """
        self.auth = authenticator
        if authenticator.auth_session is not None:
            self.session = authenticator.auth_session
        else:
            session = requests.Session()
            adapter = HTTPAdapter(pool_connections=self.pool_conns, pool_maxsize=self.pool_size)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            self.session = session

    def download(
        self,
        uri: str,
        destination: Path,
        item_id: str,
    ) -> bool:
        """Download file from HTTP URL with retries and progress reporting.

        Args:
            uri (str): HTTP URL to download from
            destination (Path): Local file path to save to
            item_id (str): Identifier used for progress tracking events

        Returns:
            bool: True if download succeeded, False otherwise
        """
        if not self.auth:
            raise ValueError("Authenticator not set, did you call `init` on this downloader?")
        error = ""
        task_id = f"download_{item_id}"

        log.debug("Downloading resource %s into: %s", uri, destination)
        emit_event(ProgressEventType.TASK_CREATED, task_id=task_id, description="download")
        for attempt in range(self.max_retries):
            try:
                # Ensure we have authentication
                if not self.auth.ensure_authenticated():
                    log.error("Authentication failed on attempt %s", attempt + 1)
                    continue

                headers = self.auth.auth_headers
                log.debug("Downloading %s (attempt %s/%s)", uri, attempt + 1, self.max_retries)
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
                    emit_event(ProgressEventType.TASK_DURATION, task_id=task_id, duration=total_size)

                # Download file in chunks with progress reporting
                downloaded_bytes = 0
                with open(destination, "wb") as f:
                    for chunk in response.iter_content(chunk_size=self.chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded_bytes += len(chunk)
                            emit_event(ProgressEventType.TASK_PROGRESS, task_id=task_id, advance=len(chunk))

                log.debug("Successfully downloaded %s (%s bytes)", uri, downloaded_bytes)
                emit_event(ProgressEventType.TASK_COMPLETED, task_id=task_id, success=True)
                return True

            except requests.exceptions.Timeout:
                log.debug("Timeout downloading %s on attempt %s", uri, attempt + 1)
                error = "timed out"
            except requests.exceptions.RequestException as e:
                log.debug("Request error downloading %s on attempt %s: %s", uri, attempt + 1, e)
                error = "exception request"
            except Exception as e:
                log.warning("Unexpected error downloading %s on attempt %s: %s - %s", uri, attempt + 1, type(e), e)
                error = str(e)
        emit_event(
            ProgressEventType.TASK_COMPLETED,
            task_id=task_id,
            success=False,
            description=f"failed: {error}",
        )
        return False

    def close(self) -> None:
        """Close HTTP session and release resources."""
        if self.session:
            self.session.close()
