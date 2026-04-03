"""MCP Server for Transparency in Coverage MRF rate lookup."""

from dotenv import load_dotenv
load_dotenv()

from mcp.server.fastmcp import FastMCP
from .services import mrf_parser

mcp = FastMCP(
    "Negotiated Rates",
    instructions="Look up insurer negotiated rates from Transparency in Coverage machine-readable files. "
                 "Find what an insurer pays for a specific CPT/HCPCS code to a specific provider. "
                 "Use for rate-comparison arguments in insurance litigation.",
    host="0.0.0.0",
    port=8171,
)


@mcp.tool()
async def find_rate_file(
    insurer: str,
    billing_code: str,
    billing_code_type: str = "CPT",
) -> str:
    """Find the MRF file URL for a specific billing code from an insurer.
    First step: identifies which file contains the rate data.

    Args:
        insurer: Insurer name — "anthem", "aetna", "cigna", "united", "bcbs_nc"
        billing_code: CPT or HCPCS code (e.g., "57335" for vaginoplasty, "99213" for office visit)
        billing_code_type: "CPT" or "HCPCS" (default "CPT")
    """
    result = await mrf_parser.find_mrf_url(insurer, billing_code, billing_code_type)

    if "error" in result:
        return result["error"]

    lines = [
        f"**MRF File Lookup:** {insurer} — {billing_code_type} {billing_code}",
        f"**TOC URL:** {result['toc_url']}",
    ]
    if result.get("note"):
        lines.append(f"\n{result['note']}")

    return "\n".join(lines)


@mcp.tool()
async def lookup_negotiated_rate(
    mrf_url: str,
    billing_code: str,
    billing_code_type: str = "CPT",
    npi: str = "",
    limit: int = 10,
) -> str:
    """Stream an MRF file to find negotiated rates for a specific billing code.
    WARNING: MRF files can be very large. This streams up to 50MB looking for matches.

    Args:
        mrf_url: Direct URL to the MRF JSON file (from find_rate_file or insurer's TOC)
        billing_code: CPT or HCPCS code (e.g., "57335")
        billing_code_type: "CPT" or "HCPCS"
        npi: Optional NPI filter — only show rates for this specific provider
        limit: Max results (default 10)
    """
    results = await mrf_parser.stream_rates_from_url(
        mrf_url, billing_code, billing_code_type, npi, min(limit, 50)
    )

    if not results:
        return f"No rates found for {billing_code_type} {billing_code} in the MRF file."

    if "error" in results[0]:
        return results[0]["error"]
    if "note" in results[0] and "not found" in results[0].get("note", ""):
        return results[0]["note"]

    lines = [f"**Negotiated Rates for {billing_code_type} {billing_code}:**\n"]
    for r in results:
        rate_str = f"${r['negotiated_rate']:,.2f}" if "negotiated_rate" in r else "N/A"
        npi_str = f"NPI: {r.get('npi', 'N/A')}"
        arrangement = r.get("arrangement", "")
        billing_class = r.get("billing_class", "")
        lines.append(f"- **{rate_str}** | {npi_str} | {arrangement} | {billing_class}")

    lines.append("")
    lines.append("*Source: Insurer Transparency in Coverage machine-readable file.*")
    return "\n".join(lines)


@mcp.tool()
async def list_available_insurers() -> str:
    """List insurers with known MRF Table of Contents URLs."""
    lines = ["**Available Insurers for Rate Lookup:**\n"]
    for name, url in mrf_parser.INSURER_TOC_URLS.items():
        lines.append(f"- **{name}**: {url[:80]}...")
    lines.append("")
    lines.append("*Use find_rate_file with an insurer name to locate rate data for a specific billing code.*")
    return "\n".join(lines)
