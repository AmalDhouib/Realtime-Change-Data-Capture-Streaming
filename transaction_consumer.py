from confluent_kafka import Consumer
import psycopg2
import json
class TransactionConsumer:
    def __init__(self):
        self.topic="transactions"
        self.config={
           "bootstrap.servers":"localhost:9092",
            "group.id":"amal-consumer",
            "auto.offset.reset": "earliest",
        }
        self.consumer=Consumer(self.config)
        self.consumer.subscribe([self.topic])
        self.conn = psycopg2.connect(
            host="localhost",
            database="destinationdb",
            user="postgres",
            password="amal",
        )
        self.cursor = self.conn.cursor()
        self.create_table()
    def create_table(self):
        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS transactions_sink
                            (
                                transaction_id
                                VARCHAR
                            (
                                255
                            ) PRIMARY KEY,
                                user_id VARCHAR
                            (
                                255
                            ),
                                timestamp TIMESTAMP,
                                amount DECIMAL,
                                currency VARCHAR
                            (
                                10
                            ),
                                city VARCHAR
                            (
                                255
                            ),
                                country VARCHAR
                            (
                                255
                            ),
                                merchant_name VARCHAR
                            (
                                255
                            ),
                                payment_method VARCHAR
                            (
                                255
                            ),
                                ip_address VARCHAR
                            (
                                255
                            ),
                                voucher_code VARCHAR
                            (
                                255
                            ),
                                affiliate_id VARCHAR
                            (
                                255
                            )
                                );
                            """)
        self.conn.commit()
    def start(self):
        print("Consumer started ...")
        while True:
            #poll(1.0) = “donne-moi un message s’il y en a, et attends max 1 seconde.”
            msg=self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            tx=json.loads(msg.value().decode('utf-8'))
            print("consumer", tx["transaction_id"])
            #on conflict : Si un enregistrement avec le même transaction_id existe déjà → on ignore l’insert.
            self.cursor.execute("""
                                INSERT INTO transactions_sink
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s) ON CONFLICT (transaction_id) DO NOTHING
                                """, (
                                    tx["transaction_id"],
                                    tx["user_id"],
                                    tx["timestamp"],
                                    tx["amount"],
                                    tx["currency"],
                                    tx["city"],
                                    tx["country"],
                                    tx["merchant_name"],
                                    tx["payment_method"],
                                    tx["ip_address"],
                                    tx["voucher_code"],
                                    tx["affiliate_id"]
                                ))
            self.conn.commit()

if __name__ == "__main__":
        TransactionConsumer().start()
            
