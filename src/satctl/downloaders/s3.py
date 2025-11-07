import logging
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

from satctl.auth import Authenticator
from satctl.downloaders.base import Downloader
from satctl.model import ProgressEventType
from satctl.progress.events import emit_event

log = logging.getLogger(__name__)

# S3 downloader configuration defaults
DEFAULT_MAX_RETRIES = 3
DEFAULT_CHUNK_SIZE = 8192  # 8KB


class S3Downloader(Downloader):
    """S3 downloader with authentication, retries, and progress reporting."""

    def __init__(
        self,
        max_retries: int = DEFAULT_MAX_RETRIES,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        endpoint_url: str | None = None,
        region_name: str | None = None,
    ):
        """Initialize S3 downloader.

        Args:
            authenticator (Authenticator): Authenticator instance for S3 credentials
            max_retries (int): Maximum number of download attempts. Defaults to 3.
            chunk_size (int): Size of chunks to read when downloading. Defaults to 8192.
            endpoint_url (str | None): Optional custom S3 endpoint URL. Defaults to None.
            region_name (str | None): AWS region name. Defaults to None.
        """
        super().__init__()
        self.max_retries = max_retries
        self.chunk_size = chunk_size
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.s3_client = None
        self.auth = None

    def init(self, authenticator: Authenticator, **kwargs) -> None:
        """Initialize S3 client with authentication.

        Raises:
            RuntimeError: If authentication fails
        """
        # ensure authentication is valid
        if not authenticator.ensure_authenticated():
            raise RuntimeError("Failed to initialize S3 downloader: authentication failed")
        session = authenticator.auth_session if authenticator else None
        # determine endpoint URL (prefer authenticator's endpoint if available)
        endpoint_url = getattr(authenticator, "endpoint_url", self.endpoint_url)

        # if authenticator provides a session (e.g., boto3 session), use it
        if session:
            try:
                kwargs = {}
                if endpoint_url:
                    kwargs["endpoint_url"] = endpoint_url
                self.s3_client = session.client("s3", **kwargs)
                log.debug(
                    f"Initialized S3 client from authenticator session with endpoint: {endpoint_url or 'default'}"
                )
            except Exception as e:
                log.warning("Failed to create S3 client from session: %s", e)
                self.s3_client = None

        # fallback: create client directly with optional endpoint
        if not self.s3_client:
            kwargs = {}
            if endpoint_url:
                kwargs["endpoint_url"] = endpoint_url
            if self.region_name:
                kwargs["region_name"] = self.region_name

            self.s3_client = boto3.client("s3", **kwargs)
            log.debug("Initialized S3 client with endpoint: %s", endpoint_url or "default")
        # last, set the auth object for futher checks down the line
        self.auth = authenticator

    def _parse_s3_uri(self, uri: str) -> tuple[str, str]:
        """Parse S3 URI into bucket and key.

        Args:
            uri (str): S3 URI in format s3://bucket/key/path

        Returns:
            tuple[str, str]: Tuple of (bucket_name, object_key)

        Raises:
            ValueError: If URI format is invalid
        """
        if not uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI: '{uri}' (expected format: s3://bucket/key/path)")

        path = uri[5:]
        parts = path.split("/", 1)

        if len(parts) != 2:
            raise ValueError(f"Invalid S3 URI: '{uri}' (must include both bucket and key)")

        bucket = parts[0]
        key = parts[1]

        return bucket, key

    def download(
        self,
        uri: str,
        destination: Path,
        item_id: str,
    ) -> bool:
        """Download file from S3 URI with retries and progress reporting.

        Args:
            uri (str): S3 URI (e.g., s3://bucket/path/to/file)
            destination (Path): Local path to save the downloaded file
            item_id (str): Identifier for progress tracking

        Returns:
            bool: True if download succeeded, False otherwise
        """
        if not self.s3_client or not self.auth:
            log.error("S3 client not initialized. Call init() first.")
            return False

        error = ""
        task_id = f"download_{item_id}"

        log.debug("Downloading S3 resource %s to: %s", uri, destination)
        emit_event(ProgressEventType.TASK_CREATED, task_id=task_id, description="s3_download")

        try:
            bucket, key = self._parse_s3_uri(uri)
        except ValueError as e:
            log.error("Invalid S3 URI: %s", e)
            emit_event(
                ProgressEventType.TASK_COMPLETED,
                task_id=task_id,
                success=False,
                description=f"invalid URI: {e}",
            )
            return False

        for attempt in range(self.max_retries):
            try:
                # Ensure we have authentication
                if not self.auth.ensure_authenticated():
                    log.error("Authentication failed on attempt %s", attempt + 1)
                    continue

                log.debug("Downloading s3://%s/%s (attempt %s/%s)", bucket, key, attempt + 1, self.max_retries)

                # Get object metadata to get file size
                try:
                    head_response = self.s3_client.head_object(Bucket=bucket, Key=key)
                    total_size = head_response.get("ContentLength")
                    if total_size:
                        emit_event(ProgressEventType.TASK_DURATION, task_id=task_id, duration=total_size)
                except Exception as e:
                    log.debug("Could not get object metadata: %s", e)
                    total_size = None

                # Download file in chunks with progress reporting
                downloaded_bytes = 0
                destination.parent.mkdir(parents=True, exist_ok=True)

                with open(destination, "wb") as f:
                    # Stream the object in chunks
                    response = self.s3_client.get_object(Bucket=bucket, Key=key)
                    body = response["Body"]

                    for chunk in body.iter_chunks(chunk_size=self.chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded_bytes += len(chunk)
                            emit_event(ProgressEventType.TASK_PROGRESS, task_id=task_id, advance=len(chunk))

                log.debug("Successfully downloaded s3://%s/%s (%s bytes)", bucket, key, downloaded_bytes)
                emit_event(ProgressEventType.TASK_COMPLETED, task_id=task_id, success=True)
                return True

            except NoCredentialsError:
                log.error("No AWS credentials found on attempt %s", attempt + 1)
                error = "no credentials"
                # Try to refresh authentication
                if not self.auth.ensure_authenticated(refresh=True):
                    log.error("Failed to refresh credentials")
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                log.debug("S3 client error on attempt %s: %s - %s", attempt + 1, error_code, e)
                error = f"client error: {error_code}"

                # Handle specific error cases
                if error_code == "403" or error_code == "Forbidden":
                    log.warning("Access forbidden, attempting to refresh credentials")
                    if not self.auth.ensure_authenticated(refresh=True):
                        log.error("Failed to refresh credentials")
                elif error_code == "404" or error_code == "NoSuchKey":
                    log.error("Object not found: s3://%s/%s", bucket, key)
                    break  # No point retrying for 404
            except BotoCoreError as e:
                log.debug("BotoCore error on attempt %s: %s", attempt + 1, e)
                error = f"botocore error: {e}"
            except Exception as e:
                log.warning(
                    "Unexpected error downloading %s on attempt %s: %s - %s", uri, attempt + 1, type(e).__name__, e
                )
                error = str(e)

        emit_event(
            ProgressEventType.TASK_COMPLETED,
            task_id=task_id,
            success=False,
            description=f"failed: {error}",
        )
        return False

    def close(self) -> None:
        """Close S3 client connection and clean up resources."""
        if self.s3_client:
            # boto3 clients don't need explicit closing, but we can clean up the reference
            self.s3_client = None
            log.debug("S3 client closed")
