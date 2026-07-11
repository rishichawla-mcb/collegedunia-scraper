FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    CD_DB_PATH=/data/data.db

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Persist the SQLite DB outside the image. Do NOT use a Docker VOLUME here —
# Railway rejects it. Instead attach a Railway Volume mounted at /data (Render
# uses a Persistent Disk at /data). CD_DB_PATH already points at /data/data.db.
RUN mkdir -p /data

EXPOSE 8501

# Hosts like Render/Railway inject a $PORT; fall back to 8501 locally.
CMD streamlit run app.py --server.port=${PORT:-8501} --server.address=0.0.0.0 \
    --server.headless=true --server.enableCORS=false --server.enableXsrfProtection=false
