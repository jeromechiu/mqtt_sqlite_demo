import paho.mqtt.client as mqtt
import time
import json

host = "broker.mqttdashboard.com"
port = 1883
clientId = "pub01"
keepalive = 60
reqTopic = 'jerome/req'
respTopic = 'jerome/resp'
qos = 1
cmds = ['add']


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe(respTopic, qos=qos)

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(host, port, keepalive)

client.loop_start()

while True:
    print('Input your command and username. Currently only support "add"\nUsage: command username')
    time.sleep(2)
    content = input('command: ')
    command, name = content.split(' ')
    if command not in cmds:
        print('Not yet support your command')
        continue

    payload = {"cmd":command, "name":name}
    client.publish(reqTopic, payload=json.dumps(payload), qos=qos)
    
