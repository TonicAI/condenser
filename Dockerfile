FROM python:3.9-slim-bullseye


RUN apt-get update \
  && apt-get install -y default-mysql-client unzip\
  && rm -rf /var/lib/apt/lists/*

RUN pip install toposort \
  && pip install psycopg2-binary \
  && pip install mysql-connector-python

# COPY config.json /root/condenser/config.json

COPY ./ /root/condenser/

WORKDIR "/root/condenser"

CMD python direct_subset.py -v