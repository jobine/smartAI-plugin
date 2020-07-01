FROM python:3.7

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . /usr/src/app/smartAI-plugin
WORKDIR /usr/src/app/smartAI-plugin

EXPOSE 56789

ENTRYPOINT ["gunicorn","-c","gunicorn_config.py","dummy.run_server:app"]