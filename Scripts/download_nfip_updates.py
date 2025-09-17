"""Download NFIP policy files from FEMA with checksum validation and safe writes."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import logging
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Final
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


_LOGGER = logging.getLogger(__name__)
_DEFAULT_USER_AGENT: Final[str] = "fema-risks nfip-downloader/0.1"
_DEFAULT_TIMEOUT: Final[float] = 60.0
_DEFAULT_CHUNK_SIZE: Final[int] = 1 << 20  # 1 MiB


@dataclasses.dataclass(slots=True)
class DownloadTarget:
    """Describe a single file to download from FEMA."""

    name: str
    url: str
    filename: str
    checksum: str | None = None
    checksum_algorithm: str = "sha256"

    def destination_path(self, output_dir: Path) -> Path:
        """Return the resolved destination path for this download."""

        return output_dir / self.filename

    def needs_download(self, output_dir: Path) -> bool:
        """Return True when the target file is absent or fails checksum validation."""

        destination = self.destination_path(output_dir)
        if not destination.exists():
            return True
        if not self.checksum:
            return False
        current_digest = compute_checksum(destination, self.checksum_algorithm)
        return not current_digest or current_digest != self.checksum.lower()


@dataclasses.dataclass(slots=True)
class DownloadResult:
    """Capture metadata about a completed download."""

    target: DownloadTarget
    path: Path
    duration_seconds: float
    from_cache: bool


def compute_checksum(path: Path, algorithm: str = "sha256") -> str | None:
    """Compute the hex digest for *path* using the provided algorithm."""

    try:
        digest = hashlib.new(algorithm)
    except ValueError:  # pragma: no cover - defensive guard
        _LOGGER.error("Unsupported checksum algorithm %s", algorithm)
        return None
    try:
        with path.open("rb") as file_obj:
            for chunk in iter(lambda: file_obj.read(_DEFAULT_CHUNK_SIZE), b""):
                digest.update(chunk)
    except FileNotFoundError:
        return None
    return digest.hexdigest()


def read_download_plan(config_path: Path) -> list[DownloadTarget]:
    """Load download targets from a JSON configuration file."""

    with config_path.open("r", encoding="utf-8") as source:
        payload: Any = json.load(source)
    if not isinstance(payload, list):
        raise ValueError("Download configuration must be a list of targets")
    targets: list[DownloadTarget] = []
    for entry in payload:
        if not isinstance(entry, dict):
            msg = "Download target entries must be objects"
            raise ValueError(msg)
        targets.append(
            DownloadTarget(
                name=str(entry["name"]),
                url=str(entry["url"]),
                filename=str(entry.get("filename", entry["name"])),
                checksum=(entry.get("checksum") or None),
                checksum_algorithm=str(entry.get("checksum_algorithm", "sha256")),
            ),
        )
    return targets


def fetch_url(target: DownloadTarget, *, timeout: float = _DEFAULT_TIMEOUT) -> bytes:
    """Retrieve the content for *target* from FEMA."""

    headers = {"User-Agent": _DEFAULT_USER_AGENT}
    request = Request(target.url, headers=headers)
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def persist_bytes(content: bytes, destination: Path) -> None:
    """Write *content* to *destination* atomically."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(delete=False, dir=destination.parent) as tmp_file:
        tmp_path = Path(tmp_file.name)
        tmp_file.write(content)
    tmp_path.replace(destination)


def download_target(
    target: DownloadTarget,
    output_dir: Path,
    *,
    timeout: float = _DEFAULT_TIMEOUT,
) -> DownloadResult:
    """Download *target* into *output_dir* and return metadata about the attempt."""

    destination = target.destination_path(output_dir)
    start = time.perf_counter()
    if not target.needs_download(output_dir):
        return DownloadResult(
            target=target, path=destination, duration_seconds=0.0, from_cache=True
        )

    try:
        content = fetch_url(target, timeout=timeout)
    except (HTTPError, URLError) as error:
        _LOGGER.error(
            "Failed to download %s from %s", target.name, target.url, exc_info=error
        )
        raise

    persist_bytes(content, destination)
    duration = time.perf_counter() - start
    if target.checksum:
        downloaded_digest = compute_checksum(destination, target.checksum_algorithm)
        if downloaded_digest and downloaded_digest != target.checksum.lower():
            raise ValueError(
                "Checksum mismatch for %s: expected %s but received %s"
                % (target.name, target.checksum.lower(), downloaded_digest),
            )
    return DownloadResult(
        target=target, path=destination, duration_seconds=duration, from_cache=False
    )


def configure_logging(verbosity: int) -> None:
    """Configure the logging level based on the provided verbosity count."""

    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments for the downloader."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a JSON file describing download targets.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("Data/raw"),
        help="Destination directory for downloaded files (default: Data/raw).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=_DEFAULT_TIMEOUT,
        help="Timeout for individual requests in seconds (default: %(default)s).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity (can be used multiple times).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Download configured NFIP files and report the outcome."""

    args = parse_args(argv)
    configure_logging(args.verbose)
    try:
        targets = read_download_plan(args.config)
    except Exception as error:  # pragma: no cover - CLI entry point
        _LOGGER.error("Invalid download configuration", exc_info=error)
        return 2

    output_dir: Path = args.output_dir
    results: list[DownloadResult] = []
    for target in targets:
        try:
            result = download_target(target, output_dir, timeout=args.timeout)
        except Exception as error:  # pragma: no cover - CLI entry point
            _LOGGER.error("Download failed for %s", target.name, exc_info=error)
            return 1
        results.append(result)
        status = "cached" if result.from_cache else "downloaded"
        _LOGGER.info(
            "%s: %s -> %s (%.2fs)",
            target.name,
            status,
            result.path,
            result.duration_seconds,
        )
    if not results:
        _LOGGER.warning("No download targets specified in configuration")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())
