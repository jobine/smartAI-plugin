FROM python:3.7

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY forecast/requirements.txt ./forecast/requirements.txt
RUN pip install --no-cache-dir -r ./forecast/requirements.txt

COPY . /usr/src/app/smartAI-plugin
WORKDIR /usr/src/app/smartAI-plugin

EXPOSE 56789

ENTRYPOINT ["gunicorn","-c","gunicorn_config.py","forecast.run_server:app"]
#ENTRYPOINT ["python","maga/run_server.py"]