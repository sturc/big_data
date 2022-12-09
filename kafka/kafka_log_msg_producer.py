import sys
sys.path.append("..")
from confluent_kafka import Producer
import logging

filename= "data/nasa_http_log_1995.csv"

class KafkaLogMegProducer: 
    """Kafka Log Message Producer"""

    logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='kafka_log_msg_producer.log',
                    filemode='w')


    def __init__(self):
        self.topicName = 'NasaLogMsg'
        self.producer = Producer({'bootstrap.servers':'localhost:9092'})    
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

    def receipt(self, err, msg):
        if err is not None:
            print('Error: {}'.format(err))
        else:
            message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
            self.logger.info(message)
            print(message)


    def send(self, message: str):
        self.producer.poll(1)
        self.producer.produce(self.topicName, message.encode("utf-8"),callback=self.receipt)
        self.producer.flush()


if __name__ == "__main__":
    my_prod = KafkaLogMegProducer()
    with open (filename,"r") as f:
        header = f.readline()
        for line in f:
            my_prod.send(line)

