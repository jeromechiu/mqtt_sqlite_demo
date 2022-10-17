import sqlite3
import paho.mqtt.client as mqtt
import json
from hashlib import blake2b
import time


con = sqlite3.connect('user.db')
cursorObj = con.cursor()

cursorObj.execute('DROP TABLE IF EXISTS USERS')
sql = ''' CREATE TABLE USERS(
          NAME CHAR(20) NOT NULL,
          COUNT INT)
    '''
cursorObj.execute(sql)
con.commit()

def gendigest():
    k = str(time.time()).encode('utf-8')
    h = blake2b(key=k, digest_size=5)
    return h.hexdigest()


host = 'broker.mqttdashboard.com'
port = 1883
clientId = 'sub'+gendigest()
keepalive = 60
reqTopic = 'jerome/req'
respTopic = 'jerome/resp'
qos = 1


def add2SQL(name):
    cursorObj.execute(f'SELECT count(*) FROM USERS WHERE NAME = \'{name}\'')
    data=cursorObj.fetchone()[-1]
    if data==0:
        print('insert data')
        count = 1
        cursorObj.execute(f'INSERT INTO USERS (NAME, COUNT) VALUES (\'{name}\', \'{count}\')')
        con.commit()
        
    else:
        print('update data')
        count = cursorObj.execute(f'SELECT COUNT from USERS where NAME=\'{name}\'')
        row = cursorObj.fetchone()
        count = int(row[-1])
        count += 1
        cursorObj.execute(f'UPDATE USERS set COUNT = \'{count}\' where NAME=\'{name}\'')
        con.commit()
    return count

def on_connect(client, userdata, flags, rc):
    print('Connected with result code '+str(rc))
    client.subscribe(reqTopic, qos=qos)


def on_message(client, userdata, msg):
    print(f'Got msg {str(msg.payload)} from {msg.topic}')
    payload = json.loads(msg.payload.decode('utf-8'))
    
    if payload['cmd'] == 'add':
        count = add2SQL(payload['name'])
        payload = dict()
        payload['cmd_reply'] = f'All ready to operate DB, count = {count}'
        client.publish(respTopic, payload=json.dumps(payload), qos=qos)

    

client = mqtt.Client(clientId)
client.on_connect = on_connect
client.on_message = on_message
client.connect(host, port, keepalive)


client.loop_forever()

