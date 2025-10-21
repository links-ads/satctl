import logging
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

from satctl.auth import Authenticator
from satctl.downloaders.base import Downloader
from satctl.model import ProgressEventType
from satctl.progress.events import emit_event

log = logging.getLogger(__name__)


class S3Downloader(Downloader):
    """S3 downloader with authentication, retries, and progress reporting."""

    def __init__(
        self,
        authenticator: Authenticator,
        max_retries: int = 3,
        chunk_size: int = 8192,
        endpoint_url: str | None = None,
        region_name: str | None = None,
    ):
        """
        Initialize S3 downloader.

        Args:
            authenticator: Authenticator instance for S3 credentials
            max_retries: Maximum number of download attempts
            chunk_size: Size of chunks to read when downloading
            endpoint_url: Optional custom S3 endpoint URL (e.g., for Copernicus Data Space)
            region_name: AWS region name (defaults to None for custom endpoints)
        """
        super().__init__(authenticator)
        self.max_retries = max_retries
        self.chunk_size = chunk_size
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.s3_client = None

    def init(self) -> None:
        """Initialize S3 client with authentication."""
        # Ensure authentication is valid
        if not self.auth.ensure_authenticated():
            raise RuntimeError("Failed to authenticate for S3 access")

        # Get session from authenticator if available
        session = self.auth.auth_session if hasattr(self.auth, "auth_session") else None

        # Determine endpoint URL (prefer authenticator's endpoint if available)
        endpoint_url = self.endpoint_url
        if hasattr(self.auth, "endpoint_url") and self.auth.endpoint_url:
            endpoint_url = self.auth.endpoint_url

        if session:
            # If authenticator provides a session (e.g., boto3 session), use it
            try:
                kwargs = {}
                if endpoint_url:
                    kwargs["endpoint_url"] = endpoint_url
                self.s3_client = session.client("s3", **kwargs)
                log.debug(
                    f"Initialized S3 client from authenticator session with endpoint: {endpoint_url or 'default'}"
                )
            except Exception as e:
                log.warning(f"Failed to create S3 client from session: {e}")
                self.s3_client = None

        if not self.s3_client:
            # Fallback: create client directly with optional endpoint
            kwargs = {}
            if endpoint_url:
                kwargs["endpoint_url"] = endpoint_url
            if self.region_name:
                kwargs["region_name"] = self.region_name

            self.s3_client = boto3.client("s3", **kwargs)
            log.debug(f"Initialized S3 client with endpoint: {endpoint_url or 'default'}")

    def _parse_s3_uri(self, uri: str) -> tuple[str, str]:
        """
        Parse S3 URI into bucket and key.

        Args:
            uri: S3 URI in format s3://bucket/key/path

        Returns:
            Tuple of (bucket_name, object_key)
        """
        if not uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI format: {uri}")

        # Remove s3:// prefix
        path = uri[5:]
        parts = path.split("/", 1)

        if len(parts) != 2:
            raise ValueError(f"Invalid S3 URI format: {uri}")

        bucket = parts[0]
        key = parts[1]

        return bucket, key

    def download(
        self,
        uri: str,
        destination: Path,
        item_id: str,
    ) -> bool:
        """
        Download file from S3 URI with retries and progress reporting.

        Args:
            uri: S3 URI (e.g., s3://bucket/path/to/file)
            destination: Local path to save the downloaded file
            item_id: Identifier for progress tracking

        Returns:
            True if download succeeded, False otherwise
        """
        if not self.s3_client:
            log.error("S3 client not initialized. Call init() first.")
            return False

        error = ""
        task_id = f"download_{item_id}"

        log.debug("Downloading S3 resource %s to: %s", uri, destination)
        emit_event(ProgressEventType.TASK_CREATED, task_id=task_id, description="s3_download")

        try:
            bucket, key = self._parse_s3_uri(uri)
        except ValueError as e:
            log.error(f"Invalid S3 URI: {e}")
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
                    log.error(f"Authentication failed on attempt {attempt + 1}")
                    continue

                log.debug(f"Downloading s3://{bucket}/{key} (attempt {attempt + 1}/{self.max_retries})")

                # Get object metadata to get file size
                try:
                    head_response = self.s3_client.head_object(Bucket=bucket, Key=key)
                    total_size = head_response.get("ContentLength")
                    if total_size:
                        emit_event(ProgressEventType.TASK_DURATION, task_id=task_id, duration=total_size)
                except Exception as e:
                    log.debug(f"Could not get object metadata: {e}")
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

                log.debug(f"Successfully downloaded s3://{bucket}/{key} ({downloaded_bytes} bytes)")
                emit_event(ProgressEventType.TASK_COMPLETED, task_id=task_id, success=True)
                return True

            except NoCredentialsError:
                log.error(f"No AWS credentials found on attempt {attempt + 1}")
                error = "no credentials"
                # Try to refresh authentication
                if not self.auth.ensure_authenticated(refresh=True):
                    log.error("Failed to refresh credentials")
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                log.debug(f"S3 client error on attempt {attempt + 1}: {error_code} - {e}")
                error = f"client error: {error_code}"

                # Handle specific error cases
                if error_code == "403" or error_code == "Forbidden":
                    log.warning("Access forbidden, attempting to refresh credentials")
                    if not self.auth.ensure_authenticated(refresh=True):
                        log.error("Failed to refresh credentials")
                elif error_code == "404" or error_code == "NoSuchKey":
                    log.error(f"Object not found: s3://{bucket}/{key}")
                    break  # No point retrying for 404
            except BotoCoreError as e:
                log.debug(f"BotoCore error on attempt {attempt + 1}: {e}")
                error = f"botocore error: {e}"
            except Exception as e:
                log.warning(f"Unexpected error downloading {uri} on attempt {attempt + 1}: {type(e).__name__} - {e}")
                error = str(e)

        emit_event(
            ProgressEventType.TASK_COMPLETED,
            task_id=task_id,
            success=False,
            description=f"failed: {error}",
        )
        return False

    def close(self) -> None:
        """Close S3 client connection."""
        if self.s3_client:
            # boto3 clients don't need explicit closing, but we can clean up the reference
            self.s3_client = None
            log.debug("S3 client closed")
