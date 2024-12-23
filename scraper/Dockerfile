# Use a lightweight Python image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Install required dependencies for Chrome and ChromeDriver
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    unzip \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libxss1 \
    libxtst6 \
    xdg-utils \
    libcurl4 \
    libvulkan1 \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Install Google Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt install -y ./google-chrome-stable_current_amd64.deb || apt-get -f install -y && \
    rm google-chrome-stable_current_amd64.deb

# Dynamically fetch and install compatible ChromeDriver
RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d. -f1) && \
    DRIVER_VERSION=$(wget -qO- https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_${CHROME_VERSION}) && \
    wget -O /tmp/chromedriver.zip https://storage.googleapis.com/chrome-for-testing-public/${DRIVER_VERSION}/linux64/chromedriver-linux64.zip && \
    unzip /tmp/chromedriver.zip -d /usr/local/bin/ && \
    rm /tmp/chromedriver.zip && \
    chmod +x /usr/local/bin/chromedriver

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY scraper.py .
COPY run_scraper.py .
COPY config/ ./config/

# Expose the required port
EXPOSE 8080

# Command to run the scraper
CMD ["python", "run_scraper.py"]
