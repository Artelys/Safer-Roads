FROM python:3.7-buster
RUN adduser flask --disabled-password
USER flask
WORKDIR /home/flask
COPY . /home/flask
RUN pip install --user -r requirements.txt
ENTRYPOINT ["python","server_flask.py"]