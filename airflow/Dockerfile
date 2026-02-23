FROM apache/airflow:2.8.1

# # Switch to root to install any system-level dependencies for scrapers
# USER root
# RUN apt-get update && apt-get install -y gcc build-essential && rm -rf /var/lib/apt/lists/*

# # # Switch back to airflow user to install Python packages
# USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt