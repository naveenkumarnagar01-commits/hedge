FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY straddle_trader.py dashboard.py straddle_panel.html ./

ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8

# DB lives in /data (mounted as a named volume shared between bot + dashboard)
ENV DB_PATH=/data/straddle_paper.db

# Default: run the bot. Override with `command:` in docker-compose for dashboard.
CMD ["python", "straddle_trader.py"]
