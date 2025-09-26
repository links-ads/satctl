import logging
from pathlib import Path

import earthaccess

from satctl.auth import Authenticator, EarthDataAuthenticator
from satctl.downloaders.base import Downloader
from satctl.progress import ProgressReporter

log = logging.getLogger(__name__)


class EarthDataDownloader(Downloader):
    """Downloader for NASA Earthdata using earthaccess library."""

    def __init__(
        self,
        authenticator: Authenticator,
        max_retries: int = 3,
        temp_dir: str | None = None,
    ):
        """
        Initialize EarthData downloader.

        Args:
            authenticator: Authenticator instance (should be EarthDataAuthenticator)
            max_retries: Maximum number of download attempts
            temp_dir: Temporary directory for intermediate files
        """
        super().__init__(authenticator)
        self.max_retries = max_retries
        self.temp_dir = temp_dir
        self._temp_path: Path | None = None

        if not isinstance(authenticator, EarthDataAuthenticator):
            log.warning("EarthDataDownloader expects EarthDataAuthenticator")

    def init(self) -> None:
        """Initialize the downloader and ensure authentication."""
        # Ensure we have valid authentication
        if not self.auth.ensure_authenticated():
            raise RuntimeError("Failed to authenticate with NASA Earthdata")

        # Set up temporary directory if needed
        if self.temp_dir:
            self._temp_path = Path(self.temp_dir)
            self._temp_path.mkdir(parents=True, exist_ok=True)
        else:
            self._temp_path = None

        log.debug("EarthDataDownloader initialized successfully")

    def download(
        self,
        uri: str,
        destination: Path,
        progress: ProgressReporter | None = None,
        task_id: str | None = None,
    ) -> bool:
        """
        Download file using earthaccess.

        Note: This method expects uri to be an earthaccess granule object
        or list of granules, not a simple URL string.

        Args:
            uri: Should be an earthaccess granule object or granule list
            destination: Directory to save downloaded files
            progress: Progress reporter instance
            task_id: Task ID for progress tracking

        Returns:
            True if download successful, False otherwise
        """
        try:
            # Ensure destination directory exists
            destination.mkdir(parents=True, exist_ok=True)

            # Handle single granule vs list of granules
            if isinstance(uri, list):
                granules = uri
            else:
                # Assume it's a single granule object
                granules = [uri]

            log.debug(f"Downloading {len(granules)} granules to {destination}")

            # Update progress if available
            if progress and task_id:
                progress.set_task_duration(task_id, len(granules))

            # Use earthaccess to download
            for attempt in range(self.max_retries):
                try:
                    downloaded_files = earthaccess.download(granules, str(destination))

                    if downloaded_files:
                        log.debug(f"Successfully downloaded {len(downloaded_files)} files")

                        # Update progress for successful download
                        if progress and task_id:
                            progress.update_progress(task_id, advance=len(downloaded_files))

                        return True
                    else:
                        log.warning(f"No files downloaded on attempt {attempt + 1}")

                except Exception as e:
                    log.warning(f"Download attempt {attempt + 1} failed: {e}")
                    if attempt == self.max_retries - 1:
                        raise

            log.error(f"Failed to download granules after {self.max_retries} attempts")
            return False

        except Exception as e:
            log.error(f"Error downloading granules: {e}")
            return False

    def download_granule_pairs(
        self,
        vnp02_granules: list,
        vnp03_granules: list,
        destination: Path,
        progress: ProgressReporter | None = None,
        task_id: str | None = None,
    ) -> dict[str, list[Path]]:
        """
        Download VIIRS radiance/geolocation granule pairs in organized subdirectories.

        This is a specialized method for VIIRS data that requires paired downloads.

        Args:
            vnp02_granules: List of VNP02 granules
            vnp03_granules: List of VNP03 granules
            destination: Base directory for downloads
            progress: Progress reporter
            task_id: Task ID for progress

        Returns:
            Dictionary with 'vnp02_files' and 'vnp03_files' keys
        """
        import re

        def get_granule_filename(granule) -> str | None:
            """Extract filename from granule (adapted from VIIRS script)."""
            try:
                # Method 1: From data links
                links = granule.data_links()
                if links:
                    filename = links[0].split("/")[-1]
                    if filename and not filename.startswith("LAADS:"):
                        return filename
            except Exception:
                pass

            try:
                # Method 2: From UMM metadata
                filename = granule["umm"]["DataGranule"]["Identifiers"][0]["Identifier"]
                if filename and not filename.startswith("LAADS:"):
                    return filename
            except (KeyError, IndexError, TypeError):
                pass

            # Method 3: From native-id
            native_id = granule.get("meta", {}).get("native-id", "")
            if native_id and not native_id.startswith("LAADS:"):
                return native_id

            return None

        def extract_granule_identifier(granule) -> str:
            """Extract unique identifier for folder naming."""
            try:
                granule_filename = get_granule_filename(granule)
                if granule_filename:
                    filename_match = re.search(
                        r"(VNP|VJ1|VJ2)0[23](?:MOD|IMG)\.(A\d{7})\.(\d{4})\.(\d{3})\.", granule_filename
                    )
                    if filename_match:
                        satellite_prefix = filename_match.group(1)
                        date_part = filename_match.group(2)
                        time_part = filename_match.group(3)
                        version_part = filename_match.group(4)
                        return f"{satellite_prefix}_{date_part}_{time_part}_{version_part}"

                granule_id = granule.get("meta", {}).get("native-id", "unknown")
                # Extract satellite prefix from granule ID if possible
                satellite_prefix = "VNP"  # default
                if granule_id and any(granule_id.startswith(prefix) for prefix in ["VNP", "VJ1", "VJ2"]):
                    satellite_prefix = granule_id[:3]
                return f"{satellite_prefix}_{granule_id}".replace(".", "_")
            except Exception:
                return f"VIIRS_unknown_{hash(str(granule)) % 10000}"

        # Create mapping of VNP02 granules by identifier
        vnp02_map = {}
        for granule in vnp02_granules:
            identifier = extract_granule_identifier(granule)
            vnp02_map[identifier] = granule

        vnp02_files = []
        vnp03_files = []
        total_pairs = len(vnp03_granules)

        if progress and task_id:
            progress.set_task_duration(task_id, total_pairs)

        # Process VNP03 granules and match with VNP02
        for i, vnp03_granule in enumerate(vnp03_granules):
            try:
                identifier = extract_granule_identifier(vnp03_granule)
                pair_dir = destination / identifier
                pair_dir.mkdir(parents=True, exist_ok=True)

                log.debug(f"Processing pair {i + 1}/{total_pairs}: {identifier}")

                # Download VNP03 file
                vnp03_downloaded = earthaccess.download([vnp03_granule], str(pair_dir))
                if vnp03_downloaded:
                    vnp03_path = Path(vnp03_downloaded[0])
                    if vnp03_path.exists():
                        vnp03_files.append(vnp03_path)
                        log.debug(f"Downloaded VNP03: {vnp03_path.name}")

                # Download corresponding VNP02 if available
                if identifier in vnp02_map:
                    vnp02_granule = vnp02_map[identifier]
                    vnp02_downloaded = earthaccess.download([vnp02_granule], str(pair_dir))
                    if vnp02_downloaded:
                        vnp02_path = Path(vnp02_downloaded[0])
                        if vnp02_path.exists():
                            vnp02_files.append(vnp02_path)
                            log.debug(f"Downloaded VNP02: {vnp02_path.name}")
                else:
                    log.warning(f"No matching VNP02 granule found for {identifier}")

                if progress and task_id:
                    progress.update_progress(task_id, advance=1)

            except Exception as e:
                log.error(f"Error processing granule pair {i + 1}: {e}")

        log.info(f"Downloaded {len(vnp02_files)} VNP02 and {len(vnp03_files)} VNP03 files")
        return {"vnp02_files": vnp02_files, "vnp03_files": vnp03_files}

    def close(self) -> None:
        """Clean up downloader resources."""
        # earthaccess doesn't require explicit cleanup, but we can clean temp directories
        if self._temp_path and self._temp_path.exists():
            try:
                import shutil

                shutil.rmtree(self._temp_path, ignore_errors=True)
                log.debug(f"Cleaned up temporary directory: {self._temp_path}")
            except Exception as e:
                log.warning(f"Failed to clean up temporary directory: {e}")

        log.debug("EarthDataDownloader closed")
