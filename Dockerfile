FROM python:3.12-slim-bookworm
RUN pip install --no-cache-dir duckdb==1.5.6
COPY pipeline/pipeline.py /app/pipeline.py
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 TMPDIR=/scratch
USER 1000:1000
CMD ["python3", "/app/pipeline.py"]
