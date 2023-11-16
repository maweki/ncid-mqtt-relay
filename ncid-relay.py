#!/usr/bin/python3
#-*-coding: utf-8-*-

import socket
import re
import paho.mqtt.client as mqtt
import json
import datetime
#from itertools import batched # python3.12

def parse_optional(ip_string, default_port, cast=(lambda x: x)):
    ip, _, port = ip_string.partition(':')
    port = port or default_port
    return ip, cast(port)

def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description='Relay ncid data to mqtt server.')
    parser.add_argument('ncid_server')
    parser.add_argument('mqtt_server')
    parser.add_argument('mqtt_topic')
    parser.add_argument('--mqtt_auth', type=str, default=None)
    return parser.parse_args()

def incoming_call(client, topic, incoming_string):
    print("call", incoming_string)
    data_array = incoming_string.split("*")
    # data = dict(batched(data_array, 2)) # This only works in python3.12
    data = dict(zip(data_array[::2],data_array[1::2]))
    d = datetime.datetime(int(data["DATE"][4:8]), int(data["DATE"][0:2]), int(data["DATE"][2:4]),
                        int(data["TIME"][0:2]), int(data["TIME"][2:4]))
    data['datetime'] = d
    now = datetime.datetime.now()
    print(data)
    delta = abs(now - d)
    if delta.seconds < 300:
        client.publish(topic, json.dumps(data))
        print("published")
    else:
        print("delta too large")

def info(client, topic, info_string):
    print("info", info_string)
    data_array = info_string.split("*")
    data = dict(zip(data_array[::2],data_array[1::2]))
    client.publish(topic, json.dumps(data))
    print("published")

incoming_regex = re.compile(r'^[PC]ID: \*(.*)\*$')
info_regex = re.compile(r'^[PC]IDINFO: \*(.*)\*$')
def main(ncid_server, mqtt_server, mqtt_topic, mqtt_auth):
    ncid_host, ncid_port = parse_optional(ncid_server, 3333, int)
    mqtt_host, mqtt_port = parse_optional(mqtt_server, 1883, int)

    while True:
        client = mqtt.Client()
        if mqtt_auth:
            mqtt_username, mqtt_password = parse_optional(mqtt_auth, None)
            client.username_pw_set(mqtt_username, mqtt_password)
        client.connect(mqtt_host,
                       mqtt_port,
                       60)
        client.loop_start()
        s = socket.socket()
        try:
            s.connect((ncid_host, ncid_port))
            print("connected")
            while True:
                data = s.recv(1024).decode().strip()
                print(data)
                if (match := incoming_regex.match(data)):
                  incoming_call(client, mqtt_topic, match.group(1))
                if (match := info_regex.match(data)):
                  info(client, mqtt_topic, match.group(1))
        except Exception as E:
            raise E
        finally:
            s.close()
            client.disconnect()

if __name__ == "__main__":
    args = parse_args()
    main(args.ncid_server, args.mqtt_server, args.mqtt_topic, args.mqtt_auth)
