from confluent_kafka import Producer
import psycopg2
import time
import json

class TrasactionProducer:
    def __init__ (self):
        self.topic ="transactions"
        self.conf ={
            "bootstrap.servers":"localhost:9092",
            "client.id":"amal-producer"
        }
        self.producer =Producer(self.conf)
        self.conn=psycopg2.connect(
            host='localhost',
            database="financialDB",
            user="postgres",
            password="amal",
            port="5432"
        )
        self.cursor=self.conn.cursor()

    def delivery_callback(self,err,msg):
        if err :
            print("Failed to deliver message: {}".format(err))
        else:
            key=msg.key().decode('utf-8')
            # json.load() est une fonction Python du module standard json qui sert à lire un fichier JSON et le convertir en objet Python.
            tx_id = json.loads(msg.value().decode('utf-8'))['transaction_id']

            print(f"Producer ->key={key} tx_id = {tx_id}")

    def fetch_transactions(self,limit=50):
            # utiliser des paramètres SQL (éviter format pour la sécurité)
            self.cursor.execute("select * from transactions limit %s", (limit,))
            rows=self.cursor.fetchall()
            columns=[des[0] for des in self.cursor.description]
            data=[dict(zip(columns,row)) for row in rows]
            return data

    def start(self):
        transactions = self.fetch_transactions()
        for tx in transactions:
             key = str(tx['transaction_id'])

             ## Convert Python dict to JSON string; fallback to str() for non-serializable types (e.g. datetime, Decimal)
             value=json.dumps(tx,default=str)
             self.producer.produce(self.topic,key=key,value=value,callback=self.delivery_callback)
             ## Process Kafka internal events and trigger delivery callbacks in 1 seconds
             self.producer.poll(1)
             time.sleep(0.5)

        # Wait up to 10s for all buffered messages to be delivered
        self.producer.flush(10)

if __name__ == "__main__":
    TrasactionProducer().start()
