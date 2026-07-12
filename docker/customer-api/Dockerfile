FROM python:3.13-slim

WORKDIR /app
COPY docker/customer-api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY docker/customer-api/app.py .

ENV CUSTOMER_DATA_PATH=/data/customers.json
EXPOSE 8080

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
