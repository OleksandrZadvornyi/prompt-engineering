# Start from the official lightweight Python image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Install a set of common and useful Python libraries
RUN pip install --no-cache-dir numpy pandas psycopg2-binary cachetools openpyxl redis scikit-learn requests pymysql sqlalchemy flask_sqlalchemy fastapi flask newrelic

# Set a default command
CMD ["python", "main.py"]