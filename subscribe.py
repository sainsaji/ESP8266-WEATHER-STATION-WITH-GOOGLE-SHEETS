# python3.6

import random
import pygsheets
from paho.mqtt import client as mqtt_client
from datetime import date

gc = pygsheets.authorize(service_file='key.json')

broker = 'broker.emqx.io'
port = 1883
topic = "weatherstatus"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
username = 'emqxalpha'
password = 'public'


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        incoming = msg.payload.decode() + msg.topic
        print(incoming)
       
        def write_weather(level):
            sh = gc.open('test_sheet')
            wks = sh[3]
            tempdata = ""
            humiddata = ""
            tempdata = level[0:4]
            humiddata = level[5:8]
            now_date = str(date.today())
            data1=[now_date,tempdata,humiddata]
            wks.append_table(data1,start='1')
        write_weather(incoming)
    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()
