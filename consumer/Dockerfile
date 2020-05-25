FROM ubuntu

WORKDIR /usr/src/app

COPY . .


RUN apt-get update && apt-get install -y \
  vim\
  python3 \
  python3-pip



RUN pip3 install --upgrade pip
RUN pip3 install requests
RUN pip3 install confluent-kafka
RUN pip3 install pandas

CMD ["python3", "./consumer.py"]