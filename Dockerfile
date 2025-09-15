# Use official Python base image
FROM python:3.12-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app

# Install dependencies (install package from top-level directory)
RUN pip install --upgrade pip \
    && pip install .

# Set default command
CMD ["python", "nomad_bluesky", "zmq"]
