FROM python:3.12-slim

WORKDIR /app

# OS依存パッケージ
RUN apt-get update && apt-get install -y \
    wget \
    libnss3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxrandr2 \
    libfontconfig1 \
    libxdamage1 \
    libgbm1 \
    libasound2 \
    fonts-ipafont-gothic \
    fonts-ipafont-mincho \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwright のブラウザをインストール
RUN playwright install chromium

COPY . .

CMD ["python", "unified_pixiv_search.py"]
