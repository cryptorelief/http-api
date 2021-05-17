FROM alpine:3.13
LABEL maintainer="Mayank <mayank.pro@gmail.com>"

EXPOSE 4444

WORKDIR /app/http-api
RUN apk add --no-cache --update \
        postgresql-dev \
        uwsgi-python3 \
        python3-dev \
        alpine-sdk \
        py-pip

COPY . /app/http-api

RUN mkdir -p /run/uwsgi/ \
        && pip install uwsgitop \
        && pip install --no-cache-dir -r requirements.txt \
        && adduser -DHs /sbin/nologin rauser \
        && chown -R rauser.rauser /run/uwsgi/ /app/

CMD [ "uwsgi", "--ini", "/app/http-api/conf/uwsgi.ini" ]