#!/usr/bin/python
# -*- coding: UTF-8 -*-

import re
import os
import json
import logging
import time
import uuid
from configparser import ConfigParser
from collections import deque
import paho.mqtt.client as mqtt
from subinfluxdb import Worker

config = ConfigParser()
config.read('config.ini')
mqtt_host = config.get('mqtt', 'host') or 'ioe.thingsroot.com'
mqtt_port = config.getint('mqtt', 'port') or 1883
mqtt_keepalive = config.getint('mqtt', 'keepalive') or 60
mqtt_user = config.get('mqtt', 'user') or 'root'
mqtt_pwd = config.get('mqtt', 'pwd') or 'bXF0dF9pb3RfYWRtaW4K'
influx_host = config.get('influxdb', 'host') or '127.0.0.1'
influx_port = config.getint('influxdb', 'port') or 8086
influx_username = config.get('influxdb', 'user') or 'root'
influx_password = config.get('influxdb', 'pwd') or 'root'
influx_db_name = config.get('influxdb', 'database') or "gates_trace"
freeioe_gates = ['0E35A4C2-E3C8-11E8-B740-00163E06DD4A']

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')
logging.debug(os.getpid())

data_queue = deque()
match_topic = re.compile(r'^([^/]+)/(.+)$')
match_data_path = re.compile(r'^([^/]+)/([^/]+)/(.+)$')
match_stat_path = re.compile(r'^([^/]+)/([^/]+)/(.+)$')

workers = {}
device_map = {}
inputs_map = {}


def create_worker(db):
	worker = workers.get(db)
	if not worker:
		worker = Worker(db, influx_host, influx_port, influx_username, influx_password)
		worker.start()
		workers[db] = worker
	return worker


def get_worker(iot_device):
	worker = device_map.get(iot_device)
	if not worker:
		worker = create_worker(influx_db_name)
		device_map[iot_device] = worker
	return worker


def get_input_type(val):
	if isinstance(val, int):
		return "int", val
	elif isinstance(val, float):
		return "float", val
	else:
		return "string", str(val)


def get_input_vt(iot_device, device, input, val):
	t, val = get_input_type(val)
	if t == "string":
		return "string", val

	key = iot_device + "/" + device + "/" + input
	vt = inputs_map.get(key)

	if vt:
		return vt, int(val)

	return None, float(val)


def make_input_map(iot_device, cfg):
	for dev in cfg:
		inputs = cfg[dev].get("inputs")
		if not inputs:
			return
		for it in inputs:
			vt = it.get("vt")
			if vt:
				key = iot_device + "/" + dev + "/" + it.get("name")
				inputs_map[key] = vt


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	logging.info("Connected with result code " + str(rc))

	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	# client.subscribe("$SYS/#")
	# client.subscribe("+/data")
	# client.subscribe("+/apps")
	# client.subscribe("+/devices")
	# client.subscribe("+/stat")
	client.subscribe("+/status")


def on_disconnect(client, userdata, rc):
	logging.info("Disconnect with result code " + str(rc))


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	g = match_topic.match(msg.topic)
	if not g:
		return
	g = g.groups()
	if len(g) < 2:
		return

	devid = g[0]
	topic = g[1]
	if devid:
		if topic == 'status':
			# TODO: Update Quality of All Inputs when gate is offline.
			worker = get_worker(devid)
			# redis_sts.set(devid, msg.payload.decode('utf-8'))
			status = msg.payload.decode('utf-8')
			# print(devid, topic, status)
			if status == "ONLINE" or status == "OFFLINE":
				val = status == "ONLINE"
				now_time = time.time()
				worker.append_data(name="gate_status", property="ipaddr", device=devid, iot=devid,
				                   timestamp=now_time, value=val, quality=0)
			return


client = mqtt.Client(client_id="MQTT_TO_MYDB." + str(uuid.uuid1()))
# client.username_pw_set("root", "bXF0dF9pb3RfYWRtaW4K")
client.username_pw_set(mqtt_user, mqtt_pwd)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

try:
	logging.debug('MQTT Connect to %s:%d', mqtt_host, mqtt_port)
	client.connect_async(mqtt_host, mqtt_port, mqtt_keepalive)
	client.loop_forever(retry_first_connection=True)
except Exception as ex:
	logging.exception('MQTT Exeption', ex)
	os._exit(1)
