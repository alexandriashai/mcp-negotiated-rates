"""Transparency in Coverage MRF streaming parser.

Streams insurer MRF files to find negotiated rates for specific billing codes and NPIs.
Does NOT download full files (they can be 100GB+). Streams through a decompressor
and searches with a rolling text buffer, using only ~10MB of RAM regardless of file size.
"""

import gzip
import io
import re
import httpx

# Long timeout — MRF files are huge, initial connection can be slow
_client = httpx.AsyncClient(timeout=httpx.Timeout(connect=30.0, read=270.0, write=30.0, pool=30.0), follow_redirects=True)

# Known insurer MRF Table of Contents URLs
INSURER_TOC_URLS = {
    "anthem": "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-12-01_anthem_index.json.gz",
    "aetna": "https://health1.aetna.com/app/public/toc/2024-10_toc.json",
    "cigna": "https://www.cigna.com/legal/compliance/machine-readable-files",
    "united": "https://transparency-in-coverage.uhc.com",
    "bcbs_nc": "https://www.bcbsnc.com/assets/toc/2024-10-01_BlueCross-and-BlueShield-of-North-Carolina_index.json.gz",
}

# Max bytes to stream through (4GB compressed ≈ 20-40GB decompressed)
# Claude.ai MCP timeout is 5 min. At ~50MB/s CDN speed, 4GB = ~80s download.
# With decompression + search overhead, ~3.5 min total. Leaves ~90s margin.
MAX_STREAM_BYTES = 4 * 1024 * 1024 * 1024  # 4GB of compressed data
# Rolling buffer size for text search
BUFFER_SIZE = 64 * 1024  # 64KB chunks
OVERLAP = 4096  # Overlap between chunks to catch matches at boundaries


async def find_mrf_url(insurer: str, billing_code: str, billing_code_type: str = "CPT") -> dict:
    """Find the MRF file URL for a specific billing code from an insurer's TOC."""
    toc_url = INSURER_TOC_URLS.get(insurer.lower())
    if not toc_url:
        return {
            "error": f"Unknown insurer '{insurer}'. Known: {', '.join(INSURER_TOC_URLS.keys())}",
            "available_insurers": list(INSURER_TOC_URLS.keys()),
        }

    if not toc_url.endswith((".json", ".json.gz")):
        return {
            "insurer": insurer,
            "toc_url": toc_url,
            "note": f"This insurer's MRF index is at {toc_url}. Navigate there to find the specific file for {billing_code_type} {billing_code}.",
        }

    return {
        "insurer": insurer,
        "toc_url": toc_url,
        "billing_code": billing_code,
        "billing_code_type": billing_code_type,
        "note": "TOC file identified. Use lookup_negotiated_rate to query specific rates from the MRF files.",
    }


async def stream_rates_from_url(
    mrf_url: str,
    billing_code: str,
    billing_code_type: str = "CPT",
    npi: str = "",
    limit: int = 20,
) -> list[dict]:
    """Stream an MRF file and extract rates for a specific billing code.

    Uses a rolling buffer approach — streams through gzip decompressor chunk by chunk,
    searching each chunk for the billing code. Memory usage stays at ~10MB regardless
    of file size. Can scan through gigabytes of MRF data.
    """
    results = []
    bytes_streamed = 0
    search_patterns = [
        f'"billing_code":"{billing_code}"',
        f'"billing_code": "{billing_code}"',
    ]

    try:
        async with _client.stream("GET", mrf_url) as resp:
            if resp.status_code != 200:
                return [{"error": f"HTTP {resp.status_code} fetching MRF", "url": mrf_url}]

            is_gzip = mrf_url.endswith(".gz") or resp.headers.get("content-encoding") == "gzip"

            # Set up decompression pipeline
            if is_gzip:
                decompressor = gzip.GzipFile(fileobj=_AsyncStreamWrapper(resp))
            else:
                decompressor = None

            # Rolling buffer for text search
            carry = ""  # Leftover from previous chunk for boundary matching

            async for compressed_chunk in resp.aiter_bytes(chunk_size=256 * 1024):
                bytes_streamed += len(compressed_chunk)

                # Decompress if needed
                if is_gzip:
                    # For gzip, we accumulate and decompress in batches
                    try:
                        text_chunk = _decompress_chunk(compressed_chunk, is_gzip)
                    except Exception:
                        continue
                else:
                    text_chunk = compressed_chunk.decode("utf-8", errors="ignore")

                # Prepend carry from previous chunk
                searchable = carry + text_chunk

                # Search for billing code in this chunk
                for pattern in search_patterns:
                    start = 0
                    while True:
                        idx = searchable.find(pattern, start)
                        if idx == -1:
                            break

                        # Extract window around match
                        window_start = max(0, idx - 1000)
                        window_end = min(len(searchable), idx + 3000)
                        window = searchable[window_start:window_end]

                        rate_info = _extract_rate_from_window(window, billing_code, npi)
                        if rate_info:
                            rate_info["bytes_scanned"] = bytes_streamed
                            results.append(rate_info)
                            if len(results) >= limit:
                                return results

                        start = idx + len(pattern)

                # Keep tail as carry for next iteration (catches boundary matches)
                carry = searchable[-OVERLAP:] if len(searchable) > OVERLAP else searchable

                # Check limits
                if bytes_streamed >= MAX_STREAM_BYTES:
                    if not results:
                        return [{"note": f"Billing code {billing_code} not found after streaming {bytes_streamed // (1024*1024)}MB. The code may not be in this MRF file."}]
                    return results

    except httpx.ReadTimeout:
        if results:
            return results
        return [{"note": f"Stream timed out after {bytes_streamed // (1024*1024)}MB. Billing code {billing_code} not found in data scanned so far."}]
    except Exception as e:
        if results:
            return results
        return [{"error": f"Error streaming MRF: {str(e)}", "bytes_scanned": bytes_streamed, "url": mrf_url}]

    if not results:
        return [{"note": f"Billing code {billing_code} not found after scanning entire file ({bytes_streamed // (1024*1024)}MB)."}]

    return results


# Gzip decompression state for streaming
_decompress_buffer = b""
_decompress_obj = None

def _decompress_chunk(compressed: bytes, is_gzip: bool) -> str:
    """Decompress a chunk of gzipped data. Maintains state across calls."""
    global _decompress_buffer, _decompress_obj
    import zlib

    if _decompress_obj is None:
        _decompress_obj = zlib.decompressobj(zlib.MAX_WBITS | 16)  # gzip mode

    try:
        decompressed = _decompress_obj.decompress(compressed)
        return decompressed.decode("utf-8", errors="ignore")
    except zlib.error:
        # Reset decompressor on error
        _decompress_obj = zlib.decompressobj(zlib.MAX_WBITS | 16)
        return ""


class _AsyncStreamWrapper:
    """Wrapper to make httpx async stream look like a file object for gzip."""
    def __init__(self, resp):
        self.resp = resp
        self.buffer = b""

    def read(self, size=-1):
        return self.buffer[:size] if size > 0 else self.buffer


def _extract_rate_from_window(window: str, billing_code: str, npi_filter: str) -> dict | None:
    """Extract rate information from a JSON window around a billing code match."""
    rate = {"billing_code": billing_code}

    # Extract negotiated rate
    rate_match = re.search(r'"negotiated_rate":\s*([\d.]+)', window)
    if rate_match:
        rate["negotiated_rate"] = float(rate_match.group(1))

    # Extract NPI(s)
    npi_matches = re.findall(r'"npi":\s*\[?\s*(\d{10})', window)
    if npi_matches:
        rate["npis"] = list(set(npi_matches))[:5]
        if npi_filter and npi_filter not in npi_matches:
            return None

    # Extract negotiation arrangement
    arr_match = re.search(r'"negotiation_arrangement":\s*"([^"]+)"', window)
    if arr_match:
        rate["arrangement"] = arr_match.group(1)

    # Extract service code
    svc_match = re.search(r'"service_code":\s*\[?"?(\d+)"?\]?', window)
    if svc_match:
        rate["service_code"] = svc_match.group(1)

    # Extract billing class
    cls_match = re.search(r'"billing_class":\s*"([^"]+)"', window)
    if cls_match:
        rate["billing_class"] = cls_match.group(1)

    # Extract expiration date
    exp_match = re.search(r'"expiration_date":\s*"([^"]+)"', window)
    if exp_match:
        rate["expiration_date"] = exp_match.group(1)

    if "negotiated_rate" in rate or "npis" in rate:
        return rate
    return None
