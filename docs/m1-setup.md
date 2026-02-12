# M1 — Local Infrastructure Setup

## Prerequisites

- Docker Desktop (with Docker Compose v2)
- At least 4 GB RAM allocated to Docker (8 GB recommended)

## Quick Start

1. **Clone the repo** (if you haven't already):
   ```bash
   git clone <repo-url>
   cd Data_eng_mvp
   ```

2. **Configure environment variables**:
   ```bash
   # Copy the example file
   cp .env.example .env
   
   # Edit .env and configure:
   # - AIRFLOW__CORE__FERNET_KEY (generate a secure key)
   # - AIRFLOW_ADMIN_PASSWORD (change from default)
   # - POSTGRES_PASSWORD (change from default)
   # - MINIO_ROOT_PASSWORD (change from default)
   ```
   
   **CRITICAL SECURITY NOTE**: 
   - Never commit the `.env` file to version control
   - Always generate a unique Fernet key using:
     - Python: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
     - PowerShell: `$bytes = New-Object byte[] 32; (New-Object Random).NextBytes($bytes); [Convert]::ToBase64String($bytes)`
   - Use strong, unique passwords for all credentials

3. **Start everything**:
   ```bash
   docker compose up -d
   ```

4. **Wait for init services to finish** (takes ~30–60 seconds on first run):
   ```bash
   docker compose ps
   ```
   You should see `airflow-init` and `minio-init` with status `exited (0)`.
   The other services should be `running` or `healthy`.

5. **Access the UIs**:
   - Airflow: [http://localhost:8080](http://localhost:8080) — login using credentials from your `.env` file
   - MinIO Console: [http://localhost:9001](http://localhost:9001) — login using credentials from your `.env` file

6. **Verify MinIO buckets**:
   Open the MinIO Console — you should see three buckets: `bronze`, `silver`, `gold`.

7. **Run the verification DAG**:
   - In the Airflow UI, find the `hello_world` DAG
   - Unpause it (toggle the slider)
   - Click the play button to trigger a manual run
   - Both tasks (`print_hello` → `test_minio_connection`) should turn green
   - Check the logs of `test_minio_connection` to see the bucket list

## Stopping

```bash
docker compose down
```

To also remove volumes (wipes all data):
```bash
docker compose down -v
```

## Troubleshooting

### Airflow webserver not starting
- Check that port 8080 isn't already in use
- Check logs: `docker compose logs airflow-webserver`

### MinIO buckets not created
- Check the init container: `docker compose logs minio-init`
- You can create them manually via the MinIO Console

### "Permission denied" errors in Airflow logs
- On Linux, make sure `AIRFLOW_UID` in `.env` matches your user ID (`id -u`)
- On Windows/WSL2, the default `50000` should work fine

## What's Running

| Service | Purpose | Port |
|---------|---------|------|
| postgres | Airflow metadata DB | internal only |
| minio | S3-compatible object storage | 9000 (API), 9001 (Console) |
| minio-init | Creates bronze/silver/gold buckets | exits after setup |
| airflow-init | DB migration + admin user | exits after setup |
| airflow-webserver | Airflow UI | 8080 |
| airflow-scheduler | DAG scheduling + task execution | none |
