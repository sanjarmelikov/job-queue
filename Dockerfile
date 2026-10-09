FROM python:3.13-slim
WORKDIR /app
COPY pyproject.toml .
COPY jobqueue jobqueue
RUN pip install --no-cache-dir .
ENTRYPOINT ["python", "-m", "jobqueue"]
CMD ["worker", "--handler", "jobqueue.examples:record_effect", "--batch", "10"]
