from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError

from pathlib import Path

import os

app = FastAPI(title="GCS File Uploader")


# --- Path ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
SERVICE_ACCOUNT = BASE_DIR / "include" / "gcp" / "sales-upload-bucket-service-account-key.json"

BUCKET_NAME = 'sales-dw-bucket'

# ── Credentials ──────────────────────────────────────────────────────────────────
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(SERVICE_ACCOUNT)



def get_bucket():
    client = storage.Client()
    return client.bucket(BUCKET_NAME)


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("index.html") as f:
        return f.read()


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided.")
    
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only Excel files allowed.")

    try:
        bucket = get_bucket()
        blob = bucket.blob(file.filename)

        blob.upload_from_file(file.file, content_type=file.content_type or "application/octet-stream" )

        return JSONResponse({
            "success": True,
            "filename": file.filename,
            "bucket": BUCKET_NAME
        })

    except GoogleAPIError as e:
        raise HTTPException(status_code=502, detail=f"GCS error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    return {"status": "ok", "bucket": BUCKET_NAME}
