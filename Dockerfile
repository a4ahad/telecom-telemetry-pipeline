# Use a slim, stable Python baseline
FROM python:3.12-slim

# Install system dependencies required for Java runtime (needed for PySpark core)
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency mappings and install configurations
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application scripts into the container
COPY . .

# Expose the standard Streamlit networking port
EXPOSE 8501

# Run the system frontend dashboard on container spin-up
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]