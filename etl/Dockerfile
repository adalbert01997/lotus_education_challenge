FROM python:3.8-slim

# Set the working directory to /app
WORKDIR /app

# Get requirements
COPY requirements.txt /app
COPY src/ /app/src

# Switch to a non-root users to increase security
RUN pip install --no-cache-dir -r /app/requirements.txt && \
    groupadd -r appuser && \
    useradd -r -g appuser -d /app appuser && \
    chown -R appuser /app 

USER appuser