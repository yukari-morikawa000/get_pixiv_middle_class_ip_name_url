# Dockerfile

FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libnss3 \
       libfontconfig1 \
       libharfbuzz0b \
       libicu-dev \
       wget \
    && rm -rf /var/lib/apt/lists/*

# Playwrightのシステム設定
ENV PLAYWRIGHT_BROWSERS_PATH=/usr/lib/pwuser
ENV NODE_OPTIONS="--max-old-space-size=4096"

# Playwrightのインストール
RUN pip install playwright \
    && playwright install chromium

# 必要なPythonライブラリ
RUN pip install \
    google-cloud-bigquery \
    pandas \
    google-auth

# 作業ディレクトリの設定
WORKDIR /app

# コードのコピー
COPY unified_pixiv_search.py .
COPY clean_name.py .
