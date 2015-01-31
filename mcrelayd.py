import argparse
import yaml
import json
import time
import socket

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
	#mcSocket.connect(('127.0.0.1', config['memcached_port']))
	mcSocket.connect((config['memcached_host'], config['memcached_port']))

	# Connect to the redis PubSub servers
	# @fixme: disconnects stop the daemon
	redisMap = {}
	pubSubMap = {}
	for redis_host in config['redis_hosts']:
		print("Connecting to redis server %s on %s..." % (redis_host, config['redis_port']))
		redisMap[redis_host] = redis.StrictRedis(host=redis_host, port=config['redis_port'], password=config['redis_password'])
		pubSubMap[redis_host] = redisMap[redis_host].pubsub()
		print("Subscribing to channel %s" % config['redis_channel'])
		pubSubMap[redis_host].subscribe(config['redis_channel'])

	# Stream in updates from the channel on all servers indefinetely
	print("Listening for channel events...")
	while True:
		wait = True
		# Iterate through each host serving the channel
		for redis_host in pubSubMap:
			# Process the next message if one is ready
			event = pubSubMap[redis_host].get_message()
			if event and event['type'] == 'message':
				relayCommand(mcSocket, event['data'])
				wait = False
		# Avoid high CPU usage
		if wait:
			time.sleep(0.005)

def relayCommand(mcSocket, message):
	command = json.loads(message)

	cmd = str(command['cmd']) # commands are always ASCII
	key = str(command['key']) # keys are always ASCII

	if cmd == 'set' or cmd == 'add':
		mcCommand = "%s %s %s %s %s\r\n%s\r\n" % (cmd,key,command['flg'],command['ttl'],len(command['val']),command['val'])
	elif cmd == 'delete':
		mcCommand = "delete %s\r\n" % key
	else:
		print('Got unrecognized command "%s"' % cmd)
		return

	print('Applying command: %s' % mcCommand)

	# Issue the full command
	mcSocket.sendall(mcCommand)
	# Get the response status
	result = ''
	while True:
		c = mcSocket.recv(1)
		if c == '\n' or c == '':
			break
		else:
			result += c

	print('Get result: %s' % result)

if __name__ == '__main__':
	main()
