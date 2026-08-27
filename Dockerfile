FROM python:3.12-slim

WORKDIR /app

COPY . .

EXPOSE 10000

CMD ["python", "-m", "http.server", "10000", "--bind", "0.0.0.0"]
