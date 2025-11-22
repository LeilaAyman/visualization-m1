# ---- Base Image ----
FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ---- Set working directory ----
WORKDIR /app

# ---- Install Python dependencies ----
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# ---- Copy project files ----
COPY . .

# ---- Expose the Dash port ----
EXPOSE 8080

# ---- Run the app ----
CMD ["python", "main.py"]
