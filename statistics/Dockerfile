FROM python:3.10-slim

WORKDIR /app
COPY compute_stats.py .
COPY config/ ./config/
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8080

CMD ["python", "compute_stats.py"]