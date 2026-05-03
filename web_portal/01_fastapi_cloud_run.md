# Deploying FastAPI Upload Server to Cloud Run

## Overview

This document covers migrating the FastAPI file upload server from a locally hosted
uvicorn process to a fully serverless **Cloud Run** deployment on GCP. After completing
this section, the upload UI is permanently available at a public HTTPS URL — no local
server, no open terminals, no ngrok required.

This is **Part 1** of the full serverless migration:

| Part | Component | Status |
|------|-----------|--------|
| 1 | FastAPI upload server → Cloud Run | ✅ Complete |
| 2 | dbt jobs → Cloud Run Jobs (x3) | Upcoming |
| 3 | Airflow DAGs → Cloud Composer | Upcoming |
| 4 | Data quality → Dataplex | Upcoming |

---

## Architecture Before & After

**Before**
```
Browser → FastAPI (localhost:8000) → GCS bucket
              ↑
         uvicorn running locally
         terminal must stay open
```

**After**
```
Browser → FastAPI (Cloud Run HTTPS URL) → GCS bucket
              ↑
         fully managed, serverless
         scales to zero when idle
```

---

## Prerequisites

### 1. Google Cloud CLI

The `gcloud` CLI is required to deploy from your local machine.

**Install:** https://cloud.google.com/sdk/docs/install

- **Windows:** Download and run the `.exe` installer
- **macOS:** `brew install google-cloud-sdk` or download the tar archive
- **Linux:** Follow the apt/rpm instructions on the install page

**After installing, initialise:**

```bash
# Log in to your Google account
gcloud auth login

# Set your GCP project
gcloud config set project sales-datawarehouse

# Confirm it is working
gcloud projects list
```

### 2. Enable required GCP APIs

These APIs are needed for `gcloud run deploy --source .` to work. Run once per project:

```bash
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com
```

- **Cloud Run** — hosts the FastAPI service
- **Cloud Build** — builds the Docker image from source
- **Artifact Registry** — stores the built container image

---

## How `gcloud run deploy --source .` Works

This single command triggers an automated chain on GCP — you never touch Docker or
Cloud Build directly:

```
gcloud run deploy --source .
        │
        ▼
Cloud Build reads your Dockerfile
        │
        ▼
Builds the container image
        │
        ▼
Pushes image to Artifact Registry
        │
        ▼
Deploys image to Cloud Run
        │
        ▼
Returns a live HTTPS URL
```

Everything happens on GCP's infrastructure. Your local machine just sends the source
files and waits for the URL.

---

## Project Structure

Place all files inside your `web_portal/` folder before deploying:

```
web_portal/
├── main.py           ← existing, modified (see below)
├── index.html        ← existing, no changes
├── Dockerfile        ← new
├── requirements.txt  ← new
└── .dockerignore     ← new
```

---

## File Changes

### 1. Remove hardcoded values from `main.py`

Cloud Run uses **Application Default Credentials (ADC)** via an attached service
account — no JSON key file is needed inside the container. Remove all references to
the service account file path and replace the hardcoded bucket name with an
environment variable.

**Remove this pattern wherever it appears:**
```python
# DELETE these lines
SERVICE_ACCOUNT = "/usr/local/airflow/include/gcp/sales-upload-sa.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account
```

**Replace hardcoded bucket name:**
```python
# Before
GCS_BUCKET_NAME = "sales-dw-bucket"

# After
import os
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
```

**Update the upload function signature:**
```python
# Before — accepted service_account as a parameter
async def upload_file(file: UploadFile = File(...)):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT
    client = storage.Client()
    ...

# After — ADC handles credentials automatically
async def upload_file(file: UploadFile = File(...)):
    client = storage.Client()   # picks up the attached service account automatically
    ...
```

### 2. `Dockerfile`

```dockerfile
FROM python:3.12-slim

# Don't buffer stdout/stderr — important for Cloud Run logs
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY main.py .
COPY index.html .

# Cloud Run injects PORT env var — default 8080
EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
```

> **Why `PYTHONUNBUFFERED=1`?** Without it, Python buffers stdout and your logs
> won't appear in Cloud Run's log viewer in real time.

> **Why `--host 0.0.0.0`?** By default uvicorn only listens on localhost (127.0.0.1),
> which is unreachable inside a container. `0.0.0.0` makes it listen on all interfaces
> so Cloud Run can route traffic to it.

### 3. `requirements.txt`

```text
fastapi>=0.110.0
uvicorn[standard]>=0.29.0
google-cloud-storage>=2.16.0
python-multipart>=0.0.9
```

### 4. `.dockerignore`

Prevents unnecessary files from being sent to Cloud Build:

```
__pycache__/
*.pyc
*.pyo
.env
.venv
venv/
*.egg-info/
.git/
.gitignore
```

---

## GCP Setup

### Step 1 — Create a dedicated service account

Rather than using the default Compute service account, create one scoped specifically
to this service:

```bash
gcloud iam service-accounts create fastapi-upload-sa \
  --display-name="FastAPI Upload Service Account"
```

### Step 2 — Grant it GCS write access

```bash
gcloud projects add-iam-policy-binding sales-datawarehouse \
  --member="serviceAccount:fastapi-upload-sa@sales-datawarehouse.iam.gserviceaccount.com" \
  --role="roles/storage.objectCreator"
```

`roles/storage.objectCreator` allows writing new objects to GCS but not reading or
deleting existing ones — principle of least privilege.

---

## Deployment

From inside your `web_portal/` folder:

```bash
cd web_portal

gcloud run deploy fastapi-upload \
  --source . \
  --region asia-south2 \
  --platform managed \
  --allow-unauthenticated \
  --service-account=fastapi-upload-sa@sales-datawarehouse.iam.gserviceaccount.com \
  --set-env-vars GCS_BUCKET_NAME=your-actual-bucket-name
```

**Flag breakdown:**

| Flag | Purpose |
|------|---------|
| `--source .` | Send current directory to Cloud Build |
| `--region asia-south2` | GCP region to deploy to |
| `--platform managed` | Fully managed Cloud Run (not GKE) |
| `--allow-unauthenticated` | Public access — anyone can open the upload UI |
| `--service-account` | Attach the SA we created above |
| `--set-env-vars` | Pass `GCS_BUCKET_NAME` into the container |

### What happens during deploy

Cloud Build streams its progress to your terminal:

```
Building and deploying from source...
Building Container...
✓ Building Container... Done.
✓ Pushing Container to Registry... Done.
✓ Creating Revision... Done.
✓ Routing traffic... Done.

Service URL: https://fastapi-upload-xxxx-as.a.run.app
```

The final URL is your permanent upload endpoint.

---

## Verifying the Deployment

### 1. Open the UI

Navigate to the Service URL in your browser. The drag-and-drop upload interface
should load exactly as it did locally.

### 2. Test the health endpoint

```bash
curl https://fastapi-upload-xxxx-as.a.run.app/health
# Expected: {"status":"ok","bucket":"your-bucket-name"}
```

### 3. Upload a test file

Use the UI or curl:

```bash
curl -X POST \
  "https://fastapi-upload-xxxx-as.a.run.app/upload" \
  -F "file=@test.xlsx"
```

### 4. Confirm the file landed in GCS

```bash
gcloud storage ls gs://your-bucket-name/
```

### 5. Check logs in GCP Console

Cloud Run → your service → Logs tab. Every upload request, GCS write, and any
errors appear here in real time.

---

## Environment Variable Management

The `GCS_BUCKET_NAME` env var is stored inside the Cloud Run service revision. To
view or update it without redeploying from source:

**Via GCP Console:**
Cloud Run → `fastapi-upload` → Edit & Deploy New Revision → Variables & Secrets

**Via CLI:**
```bash
# Update env var and deploy a new revision
gcloud run services update fastapi-upload \
  --region asia-south2 \
  --set-env-vars GCS_BUCKET_NAME=new-bucket-name
```

---

## Scaling & Cost

Cloud Run scales to **zero** when there are no requests — meaning you pay nothing
when the upload UI is idle. For a monthly file upload workflow, the cost is
effectively zero (well within the free tier of 2 million requests/month).

The default scaling config is `Min: 0, Max: 1` which is appropriate here. A file
upload UI does not need multiple concurrent instances.

---

## Key Concepts Learned

**Application Default Credentials (ADC)**
On Cloud Run, you attach a service account to the service instead of using a JSON
key file. The `google-cloud-storage` client automatically detects and uses these
credentials — no code change needed beyond removing the `GOOGLE_APPLICATION_CREDENTIALS`
line.

**Environment variables on Cloud Run**
Secrets and config values (bucket names, API URLs, etc.) are passed in at deploy
time via `--set-env-vars` and stored as part of the service revision. Never hardcode
these values in `main.py`.

**`gcloud run deploy --source .` vs `gcloud builds submit`**
`--source .` is an all-in-one command — build, push, and deploy in one step. Use
`gcloud builds submit` separately when you need to decouple the build from the
deploy, for example in a CI/CD pipeline where tests run between the two steps.

**Port binding**
Cloud Run injects a `PORT` environment variable and expects the container to listen
on it. The Dockerfile uses `--port 8080` which matches Cloud Run's default. The
`--host 0.0.0.0` flag is required so uvicorn accepts connections from outside the
container's loopback interface.

---

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `403 Forbidden` on upload | SA missing GCS permission | Re-run the `add-iam-policy-binding` command |
| `500 Internal Server Error` | `GCS_BUCKET_NAME` not set or wrong | Check env vars in Cloud Run console |
| Container fails to start | Port mismatch | Ensure `--port 8080` in CMD and `EXPOSE 8080` in Dockerfile |
| `google.auth.exceptions.DefaultCredentialsError` | SA not attached | Confirm `--service-account` flag was passed at deploy |
| Build fails | API not enabled | Run `gcloud services enable cloudbuild.googleapis.com` |

---

## Next Step

With the upload server live on Cloud Run, the next part of the serverless migration
is packaging the three dbt transformation jobs into Docker containers and deploying
them as **Cloud Run Jobs** — the GCP primitive designed for run-to-completion
workloads rather than persistent HTTP servers.
