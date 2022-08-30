FROM python:3.10

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN pybabel compile -d translations

COPY . .

CMD [ "python", "./app.py" ]