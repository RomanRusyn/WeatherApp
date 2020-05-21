import logging

from confluent_kafka import Consumer

c = Consumer({
    # 'bootstrap.servers': 'kafka:29092', # for docker-compose
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['weatherForToday'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    logging.basicConfig(filename='consumer_results.log', filemode='w',
                        datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s '
                               '- %(message)s')

    logging.info('Received message: {}'.format(msg))
    # print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()
