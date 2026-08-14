"""Negotiated Rates MCP Server — FastAPI application."""
from dotenv import load_dotenv
load_dotenv()
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from .services import extracted, mrf_parser

app = FastAPI(title="Negotiated Rates MCP Server", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
async def health():
    return {"status": "ok", "source": "transparency_in_coverage_mrfs", "insurers": list(mrf_parser.INSURER_TOC_URLS.keys())}

@app.get("/")
def root():
    return {"name": "Negotiated Rates MCP Server", "version": "1.0.0", "docs": "/docs", "mcp": "https://rates.wyldfyre.ai/mcp"}

@app.get("/rates/find")
async def find_file(insurer: str = Query(...), billing_code: str = Query(...), type: str = Query("CPT")):
    return await mrf_parser.find_mrf_url(insurer, billing_code, type)

@app.get("/rates/providers")
async def rate_providers(billing_code: str = Query(..., description="CPT/HCPCS code, e.g. 55970")):
    """Named providers with an Anthem Colorado negotiated rate for one code.

    Serves the two-pass extraction already sitting in data/. Until 2026-08-14
    nothing did: /rates/find returned a pointer to the 7.5GB source file and a
    note to "use lookup_negotiated_rate", which does not exist. So the answer to
    "who is in network for this surgery" was on disk and unreachable.

    This is the one dataset on this box that can name providers for
    gender-affirmation codes. Medicare's public billing file cannot — measured
    2026-08-14, it returns zero rows for 55970, 55980, 57335, 57291, 57292,
    54520, 54125 and 56805, because CMS suppresses any provider-code row under
    eleven beneficiaries.
    """
    return extracted.providers_for_code(billing_code)


@app.get("/rates/codes")
async def rate_codes():
    """Which codes the local extraction actually covers, so a caller can tell
    an empty answer from an unasked question."""
    return {"codes": extracted.codes(), "insurer": "anthem", "state": "CO"}


@app.get("/insurers")
async def insurers():
    return {"insurers": list(mrf_parser.INSURER_TOC_URLS.keys())}
