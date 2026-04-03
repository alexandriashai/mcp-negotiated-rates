"""Negotiated Rates MCP Server — FastAPI application."""
from dotenv import load_dotenv
load_dotenv()
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from .services import mrf_parser

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

@app.get("/insurers")
async def insurers():
    return {"insurers": list(mrf_parser.INSURER_TOC_URLS.keys())}
