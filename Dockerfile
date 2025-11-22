# -------- BASE IMAGE --------
FROM python:3.10-slim

# -------- SYSTEM DEPENDENCIES --------
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# -------- WORK DIRECTORY --------
WORKDIR /app

# -------- INSTALL PYTHON DEPENDENCIES --------
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# -------- COPY APP CODE --------
COPY . .

# -------- EXPOSE PORT 8080 (Fly.io) --------
EXPOSE 8080

# -------- START DASH APP --------
CMD ["python", "main.py"]
