#!/usr/bin/python
# -*- coding: UTF-8 -*-

import threading
import queue
import time
import logging
import influxdb
import pickledb
import requests
import json
from influxdb.exceptions import InfluxDBClientError


class Client:
	def __init__(self, host, port, username, password, database):
		self.host = host
		self.port = port
		self.username = username
		self.password = password
		self.database = database
		self._client = None
		self._db = pickledb.load('cache.db', False)

	def connect(self):
		self._client = influxdb.InfluxDBClient(host=self.host, port=self.port, username=self.username,
		                                       password=self.password, database=self.database)

	def write_data(self, data_list):
		points = []
		s = requests.Session()
		s.auth = ("api", "Pa88word")
		for data in data_list:
			value = data['value']
			if value:
				r = s.get('http://127.0.0.1:18083/api/v2/nodes/emq@127.0.0.1/clients/' + data['device'])
				value = "[]"
				if r:
					rdict = json.loads(r.text)
					if rdict['result']:
						objects = rdict['result']['objects']
						if (len(objects) > 0):
							value = "['" + rdict['result']['objects'][0]['ipaddress'] + "','" + str(rdict['result']['objects'][0]['port']) + "','" + rdict['result']['objects'][0]['connected_at'] + "']"
					points.append(
						{"measurement": "gate_wanip", "tags": {"iot": data['iot']},
						 "time": int(data['timestamp'] * 1000),
						 "fields": {"ipaddr": value,
						 "quality": data['quality'], }})
				if self._db.exists(data['device']):
					lastvalue = self._db.get(data['device'])
					if lastvalue != value:
						lastvalue_list = eval(lastvalue)
						value_list = eval(value)
						if len(value_list) > 0:
							self._db.set(data['device'], value)
							if lastvalue_list[0] != value_list[0]:
								# print(data['device'], "上网IP变化，上次IP：", lastvalue, "现在IP：", value)
								points.append(
									{"measurement": "gate_ipchange",
									"tags": {"iot": data['iot']},
									"time": int(data['timestamp'] * 1000),
									"fields": {"ipaddr": json.dumps({"last": lastvalue, "now": value}), "quality": data['quality'], }})
							elif lastvalue_list[0] == value_list[0] and lastvalue_list[1] != value_list[1]:
								# print(data['device'], "网关发生重启故障，上次IP：", lastvalue, "现在IP：", value)
								points.append(
									{"measurement": "gate_fault",
									"tags": {"iot": data['iot']},
									"time": int(data['timestamp'] * 1000),
									"fields": {"ipaddr": json.dumps({"last": lastvalue, "now": value}), "quality": data['quality'], }})
								pass
				else:
					value_list = eval(value)
					if len(value_list) > 0:
						self._db.set(data['device'], value)
			else:
				points.append({"measurement": "gate_wanip", "tags": { "iot": data['iot']},
				               "time": int(data['timestamp'] * 1000),
				               "fields": {"ipaddr": "[]", "quality": data['quality'], }})
			points.append({
				"measurement": data['name'],
				"tags": {"iot": data['iot']},
				"time": int(data['timestamp'] * 1000),
				"fields": {
					data['property']: data['value'],
					"quality": data['quality'], }
				})
		try:
			self._client.write_points(points, time_precision='ms')
		except InfluxDBClientError as ex:
			if ex.code == 400:
				logging.exception('Catch an exception.')
				return

	def create_database(self):
		try:
			self._client.create_database(self.database)
		except Exception as ex:
			logging.exception('Catch an exception.')


class Worker(threading.Thread):
	def __init__(self, db, influx_host, influx_port, influx_username, influx_password):
		threading.Thread.__init__(self)
		client = Client(database=db, host=influx_host, port=influx_port, username=influx_username,
		                password=influx_password)
		client.connect()
		client.create_database()
		self.client = client
		self.data_queue = queue.Queue(10240)
		self.task_queue = queue.Queue(1024)

	def run(self):
		dq = self.data_queue
		tq = self.task_queue
		client = self.client
		while True:
			time.sleep(0.5)
			# Get data points from data queue
			points = []
			while not dq.empty():
				points.append(dq.get())
				dq.task_done()

			# append points into task queue
			if len(points) > 0:
				if tq.full():
					tq.get()
					tq.task_done()
				tq.put(points)

			# process tasks queue
			while not tq.empty():
				points = tq.get()
				try:
					client.write_data(points)
					tq.task_done()
				except Exception as ex:
					logging.exception(
						'Catch an exception.')  # tq.queue.appendleft(points) #TODO: Keep the points writing to influxdb continuely.

	def append_data(self, name, property, device, iot, timestamp, value, quality):
		self.data_queue.put(
			{"name": name,
			 "property": property,
			 "device": device,
			 "iot": iot,
			 "timestamp": timestamp,
			 "value": value,
			"quality": quality, })
