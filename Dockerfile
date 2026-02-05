FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p uploads bulk_outputs
EXPOSE 5000
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--timeout", "300", "--workers", "4", "app:create_app()"]
