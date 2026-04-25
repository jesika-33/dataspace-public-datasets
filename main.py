import os
import csv
import io
import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="CSV Proxy Join Service", version="1.0")

# Configuration from environment variables
PROXY_TABLE_URL = os.getenv("PROXY_TABLE_URL", "https://pub-d6fe39b08661488e81a2159f5c153c29.r2.dev/drugsComTrain_raw.csv")

# Helper: fetch proxy table as dict (mapping)
async def fetch_proxy_table(url: str) -> Dict[str, str]:
    logger.info(f"Fetching proxy table from: {url}")
    mapping = {}

    async with httpx.AsyncClient(timeout=30.0) as client:
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()

            buffer = ""
            async for chunk in resp.aiter_text():
                buffer += chunk

                # process complete lines only
                lines = buffer.split("\n")
                buffer = lines.pop()  # keep last partial line

                reader = csv.DictReader(lines)
                for row in reader:
                    keys = list(row.keys())
                    if len(keys) >= 2:
                        mapping[row[keys[0]]] = row[keys[1]]

    logger.info(f"Loaded {len(mapping)} proxy mappings")
    return mapping

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/analyze")
async def analyze_relationship(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...)
):
    """
    Because of the different standards in pharmaceutical company's and hospital's way of documentation, This service is used to join two CSV datasets using proxy table:
    - file 1: hospital file uses column 'drugName'
    - file 2: pharmacy file uses column 'NAME' 

    Note: In this sample service, proxy table is already defined (cannot be changed) key-value pair 
    """

    try:
        # 1. Validate
        logger.info("==== NEW /analyze REQUEST ====")
        logger.info(f"Received files: {file1.filename}, {file2.filename}")

        if not (file1.filename.endswith('.csv') and file2.filename.endswith('.csv')):
            raise HTTPException(status_code=400, detail="Both files must be CSV.")

        # 1. Load proxy mapping (small enough to fit in memory)
        mapping = await fetch_proxy_table(PROXY_TABLE_URL)

        # 2. Build index for file2 (pharmacy) using streaming
        logger.info("Indexing file2 (pharmacy)...")
        file2_index = {}

        reader2 = csv.DictReader(io.TextIOWrapper(file2.file, encoding="utf-8", errors="ignore"))
        if "NAME" not in reader2.fieldnames:
            raise HTTPException(status_code=400, detail="file2 must contain column 'NAME'")

        for i, row in enumerate(reader2):
            key = row.get("NAME")
            if key:
                file2_index.setdefault(key, []).append(row)

            if i % 10000 == 0:
                logger.info(f"Indexed {i} rows from file2")

        logger.info(f"Finished indexing file2: {len(file2_index)} unique keys")

        # 3. Stream file1 and join on the fly
        logger.info("Processing file1 (hospital)...")
        matches = []

        reader1 = csv.DictReader(io.TextIOWrapper(file1.file, encoding="utf-8", errors="ignore"))
        if "drugName" not in reader1.fieldnames:
            raise HTTPException(status_code=400, detail="file1 must contain column 'drugName'")

        for i, row1 in enumerate(reader1):
            drug_name = row1.get("drugName")
            if not drug_name:
                continue

            for proxy_key, proxy_value in mapping.items():
                if proxy_key in drug_name:
                    for key2, rows in file2_index.items():
                        if proxy_value in key2:
                            for row2 in rows:
                                matches.append({
                                    "hospital": row1,
                                    "pharmacy": row2,
                                    "matched_on": f"{proxy_key} -> {proxy_value}"
                                })

            if i % 1000 == 0:
                logger.info(f"Processed {i} rows from file1, matches so far: {len(matches)}")

            # prevent huge response
            if len(matches) >= 50:
                break

        logger.info(f"Final matches: {len(matches)}")

        return JSONResponse(content={
            "status": "success",
            "matches_found": len(matches),
            "matches": matches[:50],
            "note": "Streaming join (memory-safe)"
        })

    except Exception as e:
        logger.exception("Error during join")
        raise HTTPException(status_code=500, detail=str(e))
