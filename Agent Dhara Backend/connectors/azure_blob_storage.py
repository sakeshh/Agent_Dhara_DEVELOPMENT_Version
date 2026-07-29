"""
azure_blob_storage.py - Azure Blob Storage Connector

Reads supported files (CSV, TSV, JSON, JSONL, XML, Parquet, XLSX) from Azure Blob Storage.
Uploads with retries (exponential backoff). Single upload_file(..., blob_name=..., container=...) API.
Requires: azure-storage-blob
"""

import os
import json
import io
import time
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any
import pandas as pd

try:
    from azure.storage.blob import BlobServiceClient  # type: ignore[import]
except ImportError:
    BlobServiceClient = None  # type: ignore

try:
    from azure.identity import DefaultAzureCredential  # type: ignore[import]
except Exception:
    DefaultAzureCredential = None

# Retry settings for transient errors
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1  # seconds


def _retry_on_transient(fn, *args, **kwargs):
    """Run fn with exponential backoff on transient errors."""
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            msg = str(e).lower()
            if attempt == MAX_RETRIES - 1:
                raise
            if "429" in msg or "503" in msg or "500" in msg or "timeout" in msg or "connection" in msg:
                time.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
                continue
            raise
    raise last_exc


def _clean_env_value(v: Optional[str]) -> Optional[str]:
    """
    Be resilient to Windows env var quoting/pasting issues:
    - trim whitespace/newlines
    - remove surrounding quotes
    """
    if v is None:
        return None
    s = str(v).strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        s = s[1:-1].strip()
    return s or None


def _clean_account_key(v: Optional[str]) -> Optional[str]:
    """
    Storage account keys are base64. Remove whitespace/newlines that can break decoding
    (e.g. copied with line wraps or accidental spaces).
    """
    s = _clean_env_value(v)
    if not s:
        return None
    return "".join(s.split())


class AzureBlobStorageConnector:
    """
    Reads files from Azure Blob Storage and converts them to pandas DataFrames.
    Supports: CSV, TSV, JSON, JSONL, XML, Parquet, XLSX
    Upload with upload_file(local_path, blob_name=None, container=None) or upload_blob(blob_name, data, container=None).
    """

    def __init__(self, conn_cfg: Dict[str, str]):
        """
        Initialize with connection config:
        {
            "account_name": "...",
            "container": "...",
            "account_key": "..."
        }
        """
        if BlobServiceClient is None:
            raise ImportError(
                "azure-storage-blob is required. Install with: pip install azure-storage-blob"
            )
        self.container = _clean_env_value(conn_cfg.get("container")) or _clean_env_value(os.environ.get("AZURE_STORAGE_CONTAINER"))
        connection_string = _clean_env_value(conn_cfg.get("connection_string")) or _clean_env_value(os.environ.get("AZURE_STORAGE_CONNECTION_STRING"))
        account_name = _clean_env_value(conn_cfg.get("account_name")) or _clean_env_value(os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"))
        account_key = _clean_account_key(conn_cfg.get("account_key")) or _clean_account_key(os.environ.get("AZURE_STORAGE_ACCOUNT_KEY"))

        if not self.container:
            raise ValueError("Missing required connection key: 'container' (or AZURE_STORAGE_CONTAINER env)")

        # Configure high-performance connection pool session for parallel chunk downloads
        try:
            import requests
            from requests.adapters import HTTPAdapter
            session = requests.Session()
            adapter = HTTPAdapter(pool_maxsize=32)
            session.mount('https://', adapter)
            session.mount('http://', adapter)
        except Exception:
            session = None

        try:
            if connection_string:
                self.client = BlobServiceClient.from_connection_string(connection_string, session=session)
            elif account_name and account_key:
                cs = (
                    f"DefaultEndpointsProtocol=https;"
                    f"AccountName={account_name};"
                    f"AccountKey={account_key};"
                    f"EndpointSuffix=core.windows.net"
                )
                self.client = BlobServiceClient.from_connection_string(cs, session=session)
            else:
                env_account = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
                if DefaultAzureCredential is not None and env_account:
                    account_url = f"https://{env_account}.blob.core.windows.net"
                    cred = DefaultAzureCredential()
                    self.client = BlobServiceClient(account_url=account_url, credential=cred, session=session)
                else:
                    raise ValueError(
                        "No Azure storage credentials found; set connection_string or account_name/account_key, or enable managed identity and set AZURE_STORAGE_ACCOUNT_NAME."
                    )
            self.container_client = self.client.get_container_client(self.container)
        except Exception as e:
            raise RuntimeError(f"Failed to create BlobServiceClient: {e}") from e

    def get_blob_properties(self, blob_name: str) -> Dict[str, Any]:
        """Fetch etag and last_modified for a specific blob (with retry)."""
        if self.container_client is None:
            return {"etag": None, "last_modified": None}
        blob_client = self.container_client.get_blob_client(blob_name)
        def _get_props():
            props = blob_client.get_blob_properties()
            return {
                "etag": props.etag,
                "last_modified": props.last_modified.timestamp() if props.last_modified else None
            }
        try:
            return _retry_on_transient(_get_props)
        except Exception:
            return {"etag": None, "last_modified": None}

    def list_blobs(self) -> List[str]:
        """List all blob names in the container (with retry)."""
        if self.container_client is None:
            return []
        try:
            def _list():
                return [b.name for b in self.container_client.list_blobs()]

            return _retry_on_transient(_list)
        except Exception as e:
            msg = str(e)
            if "Failed to resolve" in msg or "Name or service not known" in msg:
                print(f"[INFO] Could not reach Azure Blob endpoint; skipping blob listing ({msg})")
            else:
                print(f"[ERROR] Failed to list blobs: {e}")
            return []

    def _download_blob_bytes_cached(self, blob_name: str, max_bytes: Optional[int] = None) -> bytes:
        """
        Download a blob from Azure Storage with local disk caching and parallel download.
        """
        import hashlib
        
        here = os.path.dirname(os.path.abspath(__file__))
        backend_root = os.path.dirname(here)
        cache_dir = os.path.join(backend_root, "agent", "data", "cache")
        os.makedirs(cache_dir, exist_ok=True)
        
        # Safe cache file name using hash of path
        safe_name = hashlib.md5(blob_name.encode('utf-8')).hexdigest()
        cache_file_path = os.path.join(cache_dir, f"{safe_name}_{os.path.basename(blob_name)}")
        meta_file_path = cache_file_path + ".meta"
        
        blob_client = self.container_client.get_blob_client(blob_name)
        
        try:
            properties = blob_client.get_blob_properties()
            server_etag = properties.etag
            server_size = properties.size
        except Exception as e:
            print(f"[WARNING] Failed to fetch blob properties for {blob_name}: {e}")
            if max_bytes:
                return blob_client.download_blob(offset=0, length=max_bytes, max_concurrency=16).readall()
            return blob_client.download_blob(max_concurrency=16).readall()

        # Validate cache
        cache_valid = False
        if os.path.exists(cache_file_path) and os.path.exists(meta_file_path):
            try:
                with open(meta_file_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                if meta.get("etag") == server_etag and meta.get("size") == server_size:
                    cache_valid = True
            except Exception:
                pass
                
        if cache_valid:
            print(f"[CACHE HIT] Loading {blob_name} from local disk cache: {cache_file_path}")
            with open(cache_file_path, "rb") as f:
                if max_bytes:
                    return f.read(max_bytes)
                return f.read()
                
        # Cache miss or invalid - range request if requested segment is small compared to server file size
        if max_bytes is not None and max_bytes < server_size:
            print(f"[DOWNLOAD RANGE] Downloading first {max_bytes} bytes of {blob_name} (concurrency=16, no cache)...")
            def _download_range():
                return blob_client.download_blob(offset=0, length=max_bytes, max_concurrency=16).readall()
            return _retry_on_transient(_download_range)
            
        # Full download and save to cache
        print(f"[DOWNLOAD FULL] Downloading entire {blob_name} ({server_size} bytes) with concurrency=16...")
        def _download_full_to_cache():
            with open(cache_file_path, "wb") as f:
                blob_client.download_blob(max_concurrency=16).readinto(f)
            with open(meta_file_path, "w", encoding="utf-8") as f:
                json.dump({"etag": server_etag, "size": server_size}, f)
                
        try:
            _retry_on_transient(_download_full_to_cache)
            print(f"[CACHE WRITE] Successfully cached {blob_name} locally.")
            with open(cache_file_path, "rb") as f:
                return f.read()
        except Exception as e:
            if os.path.exists(cache_file_path):
                try: os.remove(cache_file_path)
                except: pass
            if os.path.exists(meta_file_path):
                try: os.remove(meta_file_path)
                except: pass
            raise e

    def _download_blob_bytes(self, blob_name: str) -> bytes:
        """Download a blob as bytes (with retry)."""
        return self._download_blob_bytes_cached(blob_name)

    def _download_blob_bytes_limit(self, blob_name: str, *, max_bytes: int) -> bytes:
        """Download at most `max_bytes` from the start of a blob."""
        max_bytes = int(max_bytes or 0)
        if max_bytes <= 0:
            return self._download_blob_bytes(blob_name)
        return self._download_blob_bytes_cached(blob_name, max_bytes=max_bytes)

    def download_blob_to_file(self, blob_name: str, output_path: str) -> str:
        """
        Download a blob to a local file path.

        This is intended for "raw extraction" use cases where the full blob content
        should be available on disk for downstream processing.
        """
        blob_client = self.container_client.get_blob_client(blob_name)
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

        def _download_to_file():
            stream = blob_client.download_blob()
            with open(output_path, "wb") as f:
                # readinto streams without buffering the entire blob in memory
                stream.readinto(f)

        _retry_on_transient(_download_to_file)
        return os.path.abspath(output_path)

    def load_blob(self, blob_name: str, *, max_rows: Optional[int] = None, max_bytes: Optional[int] = None) -> pd.DataFrame:
        """
        Load a blob and convert to pandas DataFrame.
        Auto-detects format based on file extension.
        """
        try:
            # Enforce env limits if configured
            env_max_rows = os.environ.get("ASSESS_MAX_ROWS_PER_BLOB")
            if env_max_rows:
                try:
                    env_row_limit = int(env_max_rows)
                    if env_row_limit > 0:
                        if max_rows is None or max_rows > env_row_limit:
                            max_rows = env_row_limit
                except Exception:
                    pass

            env_max_bytes = os.environ.get("ASSESS_MAX_BLOB_BYTES")
            if env_max_bytes:
                try:
                    env_byte_limit = int(env_max_bytes)
                    if env_byte_limit > 0:
                        if max_bytes is None or max_bytes > env_byte_limit:
                            max_bytes = env_byte_limit
                except Exception:
                    pass

            max_rows_i = int(max_rows) if max_rows is not None else None
            if max_rows_i is not None:
                max_rows_i = max(1, min(max_rows_i, 10_000_000))
            max_bytes_i = int(max_bytes) if max_bytes is not None else None
            if max_bytes_i is not None:
                max_bytes_i = max(1, min(max_bytes_i, 1536 * 1024 * 1024)) # 1.5GB cap
            else:
                max_bytes_i = 50 * 1024 * 1024 # 50MB safe default cap

            low = blob_name.lower()

            # For text-based formats, if there is a row limit, we can download only a prefix to save time and bandwidth
            if low.endswith((".csv", ".tsv", ".jsonl")):
                if max_rows_i is not None and max_rows_i < 100_000:
                    # Estimate row size of 2KB; download min 1MB, max max_bytes_i
                    prefix_bytes = max(1024 * 1024, max_rows_i * 2048)
                    prefix_bytes = min(prefix_bytes, max_bytes_i)
                else:
                    prefix_bytes = max_bytes_i

                blob_data = self._download_blob_bytes_limit(blob_name, max_bytes=prefix_bytes)
            else:
                blob_data = self._download_blob_bytes_limit(blob_name, max_bytes=max_bytes_i)

            if low.endswith(".csv"):
                return pd.read_csv(io.BytesIO(blob_data), low_memory=False, nrows=max_rows_i)

            if low.endswith(".tsv"):
                return pd.read_csv(io.BytesIO(blob_data), sep="\t", low_memory=False, nrows=max_rows_i)

            if low.endswith(".json"):
                data = json.loads(blob_data.decode("utf-8"))
                if isinstance(data, list):
                    if data and isinstance(data[0], dict):
                        return pd.DataFrame(data[: max_rows_i or len(data)])
                    return pd.DataFrame({"value": data})
                if isinstance(data, dict):
                    return pd.json_normalize(data, max_level=1)
                return pd.DataFrame([{"value": data}])

            if low.endswith(".jsonl"):
                rows = []
                text = blob_data.decode("utf-8", errors="replace")
                lines = text.splitlines()
                # If truncated, drop the last line if it doesn't end with newline
                if text and not text.endswith(("\n", "\r")) and len(lines) > 1:
                    lines.pop()
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        rows.append({"value": line})
                    if max_rows_i is not None and len(rows) >= max_rows_i:
                        break
                if not rows:
                    return pd.DataFrame()
                return pd.json_normalize(rows, max_level=1)

            if low.endswith(".xml"):
                root = ET.fromstring(blob_data)
                nodes = list(root)
                if not nodes:
                    return pd.DataFrame()
                records = []
                for node in nodes:
                    record = {}
                    for child in node:
                        record[child.tag] = child.text
                    records.append(record)
                    if max_rows_i is not None and len(records) >= max_rows_i:
                        break
                return pd.DataFrame(records)

            if low.endswith(".parquet") or low.endswith(".parq"):
                # Parquet needs full footer; limit-bytes may fail. If caller requested a byte limit,
                # fall back to full download for correctness.
                if max_bytes_i is not None:
                    blob_data = self._download_blob_bytes(blob_name)
                return pd.read_parquet(io.BytesIO(blob_data))

            if low.endswith(".xlsx") or low.endswith(".xls"):
                return pd.read_excel(io.BytesIO(blob_data))

            if low.endswith(".html") or low.endswith(".htm"):
                tables = pd.read_html(io.BytesIO(blob_data))
                return tables[0] if tables else pd.DataFrame()

            return pd.read_csv(io.BytesIO(blob_data), low_memory=False, nrows=max_rows_i)

        except Exception as e:
            print(f"[ERROR] Failed to load blob {blob_name}: {e}")
            return pd.DataFrame()

    def load_all_blobs(
        self,
        folder_prefix: str = "",
        blobs: Optional[List[str]] = None,
        *,
        max_rows: Optional[int] = None,
        max_bytes: Optional[int] = None,
    ) -> Dict[str, pd.DataFrame]:
        """Load all supported blobs in the container, optionally filtered by folder prefix."""
        data: Dict[str, pd.DataFrame] = {}
        if blobs is None:
            blobs = self.list_blobs()

        for blob_name in blobs:
            if folder_prefix and not blob_name.startswith(folder_prefix):
                continue
            low = blob_name.lower()
            if low.endswith((".csv", ".tsv", ".json", ".jsonl", ".xml", ".html", ".htm", ".parquet", ".parq", ".xlsx", ".xls")):
                df = self.load_blob(blob_name, max_rows=max_rows, max_bytes=max_bytes)
                if not df.empty:
                    data[blob_name] = df
                else:
                    print(f"[INFO] Blob {blob_name} yielded no data (empty or unreadable)")

        return data

    def upload_blob(
        self,
        blob_name: str,
        data: bytes,
        container: Optional[str] = None,
        content_type: str = "application/octet-stream",
    ) -> bool:
        """Upload raw bytes to a blob; optionally specify container (with retry)."""
        target = container or self.container
        container_client = self.client.get_container_client(target)
        blob_client = container_client.get_blob_client(blob_name)

        def _upload():
            blob_client.upload_blob(data, overwrite=True)

        try:
            _retry_on_transient(_upload)
            return True
        except Exception as e:
            print(f"[ERROR] Failed to upload blob {blob_name}: {e}")
            return False

    def upload_file(
        self,
        local_path: str,
        blob_name: Optional[str] = None,
        container: Optional[str] = None,
    ) -> bool:
        """
        Upload a local file to the container.
        blob_name defaults to basename(local_path); container defaults to self.container.
        """
        if not os.path.isfile(local_path):
            print(f"[ERROR] File not found: {local_path}")
            return False
        dest = blob_name or os.path.basename(local_path)
        target = container or self.container
        container_client = self.client.get_container_client(target)
        blob_client = container_client.get_blob_client(dest)

        def _upload():
            with open(local_path, "rb") as f:
                blob_client.upload_blob(f, overwrite=True)

        try:
            _retry_on_transient(_upload)
            return True
        except Exception as e:
            print(f"[ERROR] Failed to upload file {local_path} -> {dest}: {e}")
            return False
