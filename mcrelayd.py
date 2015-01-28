import argparse
import yaml
import json
import redis
import memcache
import time

def main()
	parser = argparse.ArgumentParser(description='Process memcached commands from a channel')
	parser.add_argument('--config-file', help='YAML configuration file')
	args = parser.parse_args()
	# Load the config file
	f = open(args.config_file)
	config = yaml.safe_load(f)
	f.close()
	# Connect to memcached server
	mc = memcache.Client([config.memcached_server])
	# Connect to the redis PubSub servers
	redisMap = {}
	pubSubMap = {}
	for redis_host in config.redis_hosts:
		redisMap[redis_host] = redis.StrictRedis(host=redis_host, port=config.redis_port, password=config.redis_password)
		pubSubMap[redis_host] = redisMap[redis_host].pubsub()
		pubSubMap[redis_host].subscribe(config.redis_channel)
	# Stream in updates from the channel on all servers indefinetely
	while True:
		wait = True
		# Iterate through each host serving the channel (HA)
		for redis_host in pubSubMap:
			event = pubSubMap[redis_host].get_message()
			if event and event['type'] == 'message':
				doUpdate(mc, event['data'])
				wait = False
		# Slow down if nothing was found this round
		if wait:
			time.sleep(0.001)

def doUpdate(mc, message):
	command = json.loads(message)
	if command.cmd == 'set':
		mc.set(command.key, command.value, command.ttl)
		print 'Applied %s command key "%s".' % (command.cmd, command.key)
	elif command.cmd == 'add':
		mc.add(command.key, command.value, command.ttl)
		print 'Applied %s command key "%s".' % (command.cmd, command.key)
	elif command.cmd == 'delete':
		mc.delete(command.key)
		print 'Applied %s command key "%s".' % (command.cmd, command.key)
	else:
		print 'Got unrecognized command "%s".' % command.cmd

if __name__ == '__main__':
	main()
