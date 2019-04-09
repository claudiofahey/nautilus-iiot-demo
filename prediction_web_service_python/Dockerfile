FROM continuumio/anaconda3:2018.12

COPY *.py /home/

WORKDIR /home

ENV PYTHONUNBUFFERED=1

CMD ./prediction_web_service.py
