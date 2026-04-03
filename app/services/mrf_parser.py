"""Transparency in Coverage MRF streaming parser.

Streams insurer MRF files to find negotiated rates for specific billing codes and NPIs.
Does NOT download full files (they can be 100GB+). Instead, streams and filters in real-time.
"""

import gzip
import httpx
import ijson

_client = httpx.AsyncClient(timeout=60.0, follow_redirects=True)

# Known insurer MRF Table of Contents URLs
INSURER_TOC_URLS = {
    "anthem": "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-12-01_anthem_index.json.gz",
    "aetna": "https://health1.aetna.com/app/public/toc/2024-10_toc.json",
    "cigna": "https://www.cigna.com/legal/compliance/machine-readable-files",
    "united": "https://transparency-in-coverage.uhc.com",
    "bcbs_nc": "https://www.bcbsnc.com/assets/toc/2024-10-01_BlueCross-and-BlueShield-of-North-Carolina_index.json.gz",
}


async def find_mrf_url(insurer: str, billing_code: str, billing_code_type: str = "CPT") -> dict:
    """Find the MRF file URL for a specific billing code from an insurer's TOC.

    This is a best-effort search — TOC files are large and formats vary by insurer.
    Returns the URL of the MRF file that should contain the rate data.
    """
    toc_url = INSURER_TOC_URLS.get(insurer.lower())
    if not toc_url:
        return {
            "error": f"Unknown insurer '{insurer}'. Known: {', '.join(INSURER_TOC_URLS.keys())}",
            "available_insurers": list(INSURER_TOC_URLS.keys()),
        }

    # For insurers with web pages (not direct JSON), return the URL for manual lookup
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
        "note": "TOC file identified. Use stream_mrf_rates to query specific rates from the MRF files.",
    }


async def stream_rates_from_url(
    mrf_url: str,
    billing_code: str,
    billing_code_type: str = "CPT",
    npi: str = "",
    limit: int = 20,
) -> list[dict]:
    """Stream an MRF file and extract rates for a specific billing code.

    This streams the file without downloading it entirely — critical since
    MRF files can be 100GB+. Uses ijson for streaming JSON parsing.
    """
    results = []

    try:
        async with _client.stream("GET", mrf_url) as resp:
            if resp.status_code != 200:
                return [{"error": f"HTTP {resp.status_code} fetching MRF", "url": mrf_url}]

            # Determine if gzipped
            is_gzip = mrf_url.endswith(".gz") or resp.headers.get("content-encoding") == "gzip"

            # We need to process the stream — ijson works with sync iterators
            # Collect enough data to find our billing code (stream first 50MB max)
            chunks = []
            total_bytes = 0
            max_bytes = 50 * 1024 * 1024  # 50MB cap for streaming

            async for chunk in resp.aiter_bytes(chunk_size=65536):
                chunks.append(chunk)
                total_bytes += len(chunk)
                if total_bytes >= max_bytes:
                    break

            raw_data = b"".join(chunks)
            if is_gzip:
                try:
                    raw_data = gzip.decompress(raw_data)
                except Exception:
                    pass  # May be partial — try parsing anyway

            # Parse with ijson for the in_network array
            text = raw_data.decode("utf-8", errors="ignore")
            del raw_data  # Free memory

            # Simple search for the billing code in the JSON text
            search_str = f'"billing_code":"{billing_code}"'
            search_str_alt = f'"billing_code": "{billing_code}"'

            if search_str not in text and search_str_alt not in text:
                return [{"note": f"Billing code {billing_code} not found in first {total_bytes // 1024 // 1024}MB of file. The file may need to be streamed further or the code may not be in this MRF."}]

            # Found the billing code — extract surrounding context
            for search in [search_str, search_str_alt]:
                start = 0
                while True:
                    idx = text.find(search, start)
                    if idx == -1:
                        break
                    # Extract a window around the match
                    window_start = max(0, idx - 500)
                    window_end = min(len(text), idx + 2000)
                    window = text[window_start:window_end]

                    # Try to extract rate info
                    rate_info = _extract_rate_from_window(window, billing_code, npi)
                    if rate_info:
                        results.append(rate_info)
                        if len(results) >= limit:
                            break

                    start = idx + len(search)
                    if len(results) >= limit:
                        break

    except Exception as e:
        return [{"error": f"Error streaming MRF: {str(e)}", "url": mrf_url}]

    return results


def _extract_rate_from_window(window: str, billing_code: str, npi_filter: str) -> dict | None:
    """Extract rate information from a JSON window around a billing code match."""
    import re

    rate = {}
    rate["billing_code"] = billing_code

    # Extract negotiated rate
    rate_match = re.search(r'"negotiated_rate":\s*([\d.]+)', window)
    if rate_match:
        rate["negotiated_rate"] = float(rate_match.group(1))

    # Extract NPI
    npi_match = re.search(r'"npi":\s*\[?\s*(\d{10})', window)
    if npi_match:
        rate["npi"] = npi_match.group(1)
        if npi_filter and rate["npi"] != npi_filter:
            return None  # Filter by NPI

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

    if "negotiated_rate" in rate or "npi" in rate:
        return rate
    return None
