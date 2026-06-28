"""
InetIntel AS-to-Organization mapping downloader.

Given a target date (YYYY-MM-DD), this script:
  1) Walks the InetIntel Dataset-AS-to-Organization-Mapping GitHub repo under /data
     using the GitHub Contents API (recursively).
  2) Extracts a date from each file using either:
       - YYYY-MM-DD or YYYYMMDD
       - YYYY-MM or YYYYMM  (month-granularity; assumed day=1)
     IMPORTANT: InetIntel is typically month-granularity (e.g., data/2025-05/...).
  3) Chooses the best match:
       - Prefer same (year, month) as target date
       - If multiple within same month: prefer highest version ".vXX." then name
       - Otherwise: choose the closest by absolute date difference
  4) Downloads the chosen file and caches it locally.

Usage:
  python inetintel_downloader.py --date 2025-05-25
  python inetintel_downloader.py --date 2025-05-25 --cache-dir src/hermes/enrichment/cache/as_org_inetintel
"""

from __future__ import annotations

import argparse
import datetime as _dt
import os
import re
from dataclasses import dataclass
from pathlib import Path

import requests

GITHUB_API_URL = (
    "https://api.github.com/repos/InetIntel/Dataset-AS-to-Organization-Mapping/contents/data"
)
RAW_BASE_URL = (
    "https://raw.githubusercontent.com/InetIntel/Dataset-AS-to-Organization-Mapping/master/data"
)

# Match either:
#   - 2025-05-25 or 20250525
#   - 2025-05    or 202505
# We will treat month-granularity entries as day=1.
_DATE_RE = re.compile(r"(20\d{2})[-_]?(\d{2})(?:[-_]?(\d{2}))?")

# Optional: version embedded as ".v02." in filenames (common in InetIntel)
_VERSION_RE = re.compile(r"\.v(\d{2})\.")


def _extract_date_from_text(text: str) -> _dt.date | None:
    """
    Extract YYYY-MM[-DD] or YYYYMM[DD] from a filename/path.
    If day is missing, assume day=1 (month-granularity datasets).
    """
    m = _DATE_RE.search(text)
    if not m:
        return None
    y = int(m.group(1))
    mo = int(m.group(2))
    d = int(m.group(3)) if m.group(3) else 1
    try:
        return _dt.date(y, mo, d)
    except ValueError:
        return None


def _version(name: str) -> int:
    """
    Extract version from ".vXX." pattern; return 0 if missing.
    """
    m = _VERSION_RE.search(name)
    return int(m.group(1)) if m else 0


@dataclass
class InetIntelFileInfo:
    name: str
    path: str
    download_url: str
    date: _dt.date | None


class InetIntelASOrgDownloader:
    """
    Helper for downloading InetIntel AS→Organization mapping data
    for a given date (or the closest available date).
    """

    def __init__(
        self,
        cache_dir: Path | str | None = None,
        session: requests.Session | None = None,
        github_token: str | None = None,
    ) -> None:
        if cache_dir is None:
            base = os.environ.get(
                "HERMES_CACHE_DIR",
                os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "cache"),
            )
            cache_dir = Path(base) / "as_org_inetintel"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.session = session or requests.Session()

        # Optional: use a GitHub token to avoid rate limits
        token = github_token or os.environ.get("GITHUB_TOKEN")
        if token:
            self.session.headers.update({"Authorization": f"token {token}"})

    def _list_remote_files(self) -> list[InetIntelFileInfo]:
        """
        Recursively list files in the InetIntel data directory via the GitHub
        Contents API. Walk subdirectories under /data.
        """

        def _walk(url: str) -> list[InetIntelFileInfo]:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            items = resp.json()
            if not isinstance(items, list):
                return []

            collected: list[InetIntelFileInfo] = []
            for item in items:
                if not isinstance(item, dict):
                    continue

                item_type = item.get("type")
                if item_type == "file":
                    name = item.get("name", "")
                    path = item.get("path", name)  # e.g., data/2025-05/IIL-AS2Org.v02.2025-05.json

                    # IMPORTANT: derive date from path (directory encodes YYYY-MM)
                    dt = _extract_date_from_text(path)

                    download_url = item.get("download_url")
                    if not download_url:
                        download_url = (
                            f"{RAW_BASE_URL}/{path.replace('data/', '', 1)}"
                            if path.startswith("data/")
                            else f"{RAW_BASE_URL}/{path}"
                        )

                    collected.append(
                        InetIntelFileInfo(
                            name=name,
                            path=path,
                            download_url=download_url,
                            date=dt,
                        )
                    )
                elif item_type == "dir":
                    dir_url = item.get("url")
                    if dir_url:
                        collected.extend(_walk(dir_url))

            return collected

        files = _walk(GITHUB_API_URL)
        if not files:
            raise RuntimeError(
                "No files discovered in InetIntel data directory via GitHub API. "
                "Check network access or the repository structure."
            )
        return files

    def _choose_best_file(
        self, target_date: _dt.date, files: list[InetIntelFileInfo]
    ) -> InetIntelFileInfo:
        """
        Choose best file for target_date:

          1) Prefer files in the same (year, month) as target_date.
             If multiple, prefer highest version ".vXX." then name.
          2) Otherwise choose closest by absolute date difference.
          3) If no dated files, fallback to first file.
        """
        dated = [f for f in files if f.date is not None]
        if not dated:
            return files[0]

        # Prefer same month (InetIntel is typically month-granularity)
        same_month = [
            f for f in dated if (f.date.year, f.date.month) == (target_date.year, target_date.month)
        ]
        if same_month:
            same_month.sort(key=lambda f: (_version(f.name), f.name), reverse=True)
            return same_month[0]

        # Otherwise, closest date
        return min(dated, key=lambda f: abs(f.date - target_date))

    def download_for_date(self, target_date: str) -> Path:
        """
        Download the InetIntel AS→Org mapping file for the given date (YYYY-MM-DD),
        or the closest available date (preferring same month).

        Returns the path to the cached local file.
        """
        tgt = _dt.datetime.strptime(target_date, "%Y-%m-%d").date()

        files = self._list_remote_files()

        # Optional: filter to just AS2Org mapping files to avoid accidental picks
        # (safe default for this repo)
        files = [f for f in files if "AS2Org" in f.name]

        if not files:
            raise RuntimeError("No AS2Org files found under InetIntel /data directory.")

        chosen = self._choose_best_file(tgt, files)

        local_path = self.cache_dir / chosen.name
        if local_path.exists():
            return local_path

        resp = self.session.get(chosen.download_url, timeout=60, stream=True)
        resp.raise_for_status()

        tmp_path = local_path.with_suffix(local_path.suffix + ".tmp")
        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 16):
                if chunk:
                    f.write(chunk)

        os.replace(tmp_path, local_path)
        return local_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download InetIntel AS→Org mapping closest to a target date."
    )
    parser.add_argument(
        "--date", required=True, help="Target date in YYYY-MM-DD format (e.g., 2025-05-25)."
    )
    parser.add_argument(
        "--cache-dir",
        default="src/hermes/enrichment/cache/as_org_inetintel",
        help=(
            "Cache directory for downloaded files "
            "(overridden by the HERMES_CACHE_DIR env var when using the library directly)."
        ),
    )
    parser.add_argument(
        "--github-token",
        default=None,
        help="Optional GitHub token (or set env var GITHUB_TOKEN) to avoid API rate limits.",
    )
    args = parser.parse_args()

    dl = InetIntelASOrgDownloader(cache_dir=args.cache_dir, github_token=args.github_token)
    out = dl.download_for_date(args.date)

    print(out)
    print(f"Wrote: {out}")


if __name__ == "__main__":
    main()
