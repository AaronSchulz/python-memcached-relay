import argparse
import json
import time
import socket

import yaml
import redis

def main():
	parser = argparse.ArgumentParser(description='Process memcached commands from a channel')
	parser.add_argument('--config-file', required=True, help='YAML configuration file')
	args = parser.parse_args()

	# Load the config file
	print("Loading YAML config...")
	f = open(args.config_file)
	config = yaml.safe_load(f)
	f.close()

	# Connect to memcached server
	print("Connecting to memcached server %s:%s..." % (config['memcached_host'],config['memcached_port']))
	mcSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	mcSocket.connect((config['memcached_host'], config['memcached_port']))

	retryTTL = 5

	# Connect to the redis PubSub servers
	redisMap = {}
	pubSubMap = {}
	failedMap = {}
	for redis_host in config['redis_hosts']:
		# Construct the redis handle (connection is deferred)
		redisMap[redis_host] = redis.StrictRedis(
			host=redis_host, port=config['redis_port'],
			password=config['redis_password'],
			socket_connect_timeout=2,
			socket_timeout=2)
		pubSubMap[redis_host] = redisMap[redis_host].pubsub()
		print("Subscribing to channel %s on %s:%s" % (config['redis_channel'],redis_host, config['redis_port']))
		# Actually connect and subscribe (connection is on first command)
		try:
			pubSubMap[redis_host].subscribe(config['redis_channel'])
		except Exception as e:
			failedMap[redis_host] = time.time()
			print("Error connected to redis server %s on %s..." % (redis_host, config['redis_port']))

	# Stream in updates from the channel on all servers indefinetely
	print("Listening for channel events...")
	while True:
		wait = True
		# Iterate through each host serving the channel
		for redis_host in pubSubMap:
			try:
				# Avoid down servers but re-connect periodically if possible
				if redis_host in failedMap:
					if (time.time() - failedMap[redis_host]) >= retryTTL:
						print("Re-subscribing to channel %s on %s:%s" % (config['redis_channel'],redis_host, config['redis_port']))
						pubSubMap[redis_host].subscribe(config['redis_channel'])
						del failedMap[redis_host]
					else:
						continue
				# Process the next message if one is ready
				event = pubSubMap[redis_host].get_message()
				if event and event['type'] == 'message':
					relayCommand(mcSocket, event['data'])
					wait = False
			except Exception as e:
				failedMap[redis_host] = time.time()
				print("Error contacting redis server %s on %s..." % (redis_host, config['redis_port']))
		# Avoid high CPU usage
		if wait:
			time.sleep(0.005)

def relayCommand(mcSocket, message):
	command = json.loads(message)

	cmd = str(command['cmd']) # commands are always ASCII
	key = str(command['key']) # keys are always ASCII

	if cmd == 'set' or cmd == 'add':
		if 'sbt' in command and command['sbt']:
			value = command['val'].replace('$UNIXTIME$', time.time())
		else
			value = command['val']
		mcCommand = "%s %s %s %s %s\r\n%s\r\n" % (cmd,key,command['flg'],command['ttl'],len(value),value)
	elif cmd == 'delete':
		mcCommand = "delete %s\r\n" % key
	else:
		print('Got unrecognized command "%s"' % cmd)
		return

	print('Applying command: %s' % mcCommand)

	# Issue the full command
	mcSocket.sendall(mcCommand)
	# Get the response status (terminated with \r\n)
	result = ''
	while True:
		c = mcSocket.recv(1)
		if c == '\r' or c == '':
			break
		else:
			result += c
	mcSocket.recv(1) # consume \n

	# Check if the response was OK
	if result in ['STORED','NOT_STORED','DELETED','NOT_FOUND']:
		print('Got OK result: %s' % result)
	else:
		raise Exception('BadMemcachedResult', 'Got bad result: %s' % result)


if __name__ == '__main__':
	main()
