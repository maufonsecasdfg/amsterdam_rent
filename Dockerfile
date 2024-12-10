FROM python:3.10-slim

WORKDIR /app
COPY scraper.py .
COPY run_scraper.py .
COPY config/ ./config/
COPY requirements.txt .

RUN pip install -r requirements.txt

EXPOSE 8080

CMD ["python", "run_scraper.py"]