from kafka import KafkaClient, SimpleProducer, SimpleConsumer
from snakebite.client import Client
import sys
import json
import pydoop.hdfs as hdfs
from natsort import natsorted

kafka = KafkaClient("130.89.171.23:9092")
producer = SimpleProducer(kafka)
listdir = hdfs.ls("/user/djuri/worldcup")
for f in natsorted(listdir):
    with hdfs.open(f) as fi:
        print f
        while 1:
            line = fi.readline()
            if line == "":
                break
                producer.send_messages("worldcup_real",  line)
