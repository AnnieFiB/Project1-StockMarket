FROM python:3.13

WORKDIR /app

RUN pip install --no-cache -r requirements.txt

COPY . .

CMD ["python", "main.py"]
