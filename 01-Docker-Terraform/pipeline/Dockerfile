# base Docker image with Python 3.14.3-slim
FROM python:3.14.3-slim

# Copy uv binary from official uv image to this (multi-stage biuld pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set up working directory inside the container
WORKDIR /app

# Add virtual environment to PATH so we can use installed pakeges:
ENV PATH="/app/.venv/bin:$PATH"

# Copy dependency files into the container (better layer caching)
COPY 'pyproject.toml' 'uv.lock' '.python-version' ./

# Install dependencies from lock file (ensures reproducibility)
RUN uv sync --locked

# Copy the pipeline script into the container. First name is source (local), second is destination (inside container)
COPY pipeline.py pipeline.py

# Define what to do when the container starts; run the pipeline script. In this example we will just run the script
ENTRYPOINT ["uv", "run", "python", "pipeline.py"]