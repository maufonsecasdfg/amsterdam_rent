# Use a lightweight Python base image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy application code and configuration
COPY scraper.py .
COPY run_scraper.py .
COPY config/ ./config/
COPY requirements.txt .

# Install system dependencies required for Chrome and ChromeDriver
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libxss1 \
    libappindicator1 \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libx11-xcb1 \
    libxcomposite1 \
    libgbm1 \
    libgtk-3-0 \
    libxdamage1 \
    libxtst6 \
    fonts-liberation \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Install Google Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb

# Install ChromeDriver
RUN wget -O /tmp/chromedriver.zip https://chromedriver.storage.googleapis.com/131.0.6778.204/chromedriver_linux64.zip && \
    unzip /tmp/chromedriver.zip -d /usr/local/bin/ && \
    rm /tmp/chromedriver.zip && \
    chmod +x /usr/local/bin/chromedriver

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the required port
EXPOSE 8080

# Command to run the scraper
CMD ["python", "run_scraper.py"]
