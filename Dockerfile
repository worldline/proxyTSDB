FROM python:2.7

MAINTAINER Scrapinghub "root@scrapinghub.com"

RUN apt-get update && apt-get -fy install python-pip

COPY . /app

RUN pip install -r /app/requirements.txt

CMD ["-H", "172.17.42.1:4242"]
ENTRYPOINT ["/app/bin/entrypoint.sh"]
