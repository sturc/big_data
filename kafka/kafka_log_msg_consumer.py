import sys
sys.path.append("..")
from confluent_kafka import Consumer

class KafkaLogMegConsumer: 
    """Kafka Log Message Consumer"""

    def __init__(self):
        self.consumer=Consumer({'bootstrap.servers':'localhost:9092','group.id':'kafka_log_msg_consumer','auto.offset.reset':'earliest'})
        self.consumer.subscribe(['NasaLogMsg'])

    def poll(self) -> str:
        msg=self.consumer.poll(1.0) #timeout
        if msg is None:
            return
        if msg.error():
            print('Error: {}'.format(msg.error()))
            return
        return msg.value().decode('utf-8')
    
    def __del__(self):
        self.consumer.close()

if __name__ == '__main__':
    my_consumer = KafkaLogMegConsumer()
    while True:
        msg = my_consumer.poll()
        if msg is not None:
            print(msg)


