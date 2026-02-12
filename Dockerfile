FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY . ./

RUN pip install --no-cache-dir -r requirements.txt
RUN chmod +x start.sh

EXPOSE 8501
CMD ["./start.sh"]
