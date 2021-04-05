import sys
import os
import pika

class Balancer:

    def __init__(self, queue_name):
        print("Creating Balancer")
        self.connection = 0
        self.channel = 0
        self.queue_name = str(queue_name)
    
    def listenClient(self):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
            print(' [*] Waiting for messages. To exit press CTRL+C')
            self.channel.start_consuming()
        except KeyboardInterrupt:
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
    
    def callback(self, ch, method, properties, body):
        print(f" Balancer received: {body}")