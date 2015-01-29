import argparse
import yaml
import json
import redis
import memcache
import time

def main():
	parser = argparse.ArgumentParser(description='Process memcached commands from a channel')
	parser.add_argument('--config-file', required=True, help='YAML configuration file')
	args = parser.parse_args()

	# Load the config file
	print "Loading YAML config..."
	f = open(args.config_file)
	config = yaml.safe_load(f)
	f.close()

	# Connect to memcached server
	# @note: the client library already handle re-connection logic
	print "Connecting to memcached server %s..." % config['memcached_server']
	mc = memcache.Client([config['memcached_server']])

	# Connect to the redis PubSub servers
	# @fixme: disconnects stop the daemon
	redisMap = {}
	pubSubMap = {}
	for redis_host in config['redis_hosts']:
		print "Connecting to redis server %s on %s..." % (redis_host, config['redis_port'])
		redisMap[redis_host] = redis.StrictRedis(host=redis_host, port=config['redis_port'], password=config['redis_password'])
		pubSubMap[redis_host] = redisMap[redis_host].pubsub()
		print "Subscribing to channel %s" % config['redis_channel']
		pubSubMap[redis_host].subscribe(config['redis_channel'])

	# Stream in updates from the channel on all servers indefinetely
	print "Listening for channel events..."
	while True:
		wait = True
		# Iterate through each host serving the channel
		for redis_host in pubSubMap:
			# Process the next message if one is ready
			event = pubSubMap[redis_host].get_message()
			if event and event['type'] == 'message':
				doUpdate(mc, event['data'])
				wait = False
		# Avoid high CPU usage
		if wait:
			time.sleep(0.005)

def doUpdate(mc, message):
	command = json.loads(message)
	# @fixme: memcached flag handling
	ok = True
	cmd = str(command['cmd']) # commands are always ASCII
	key = str(command['key']) # keys are always ASCII
	if cmd == 'set':
		mc.set(key, command['value'], command['ttl'])
	elif cmd == 'add':
		mc.add(key, command['value'], command['ttl'])
	elif cmd == 'delete':
		mc.delete(key)
	else:
		ok = False
		print 'Got unrecognized command "%s"' % cmd

	if ok:
		print 'Applied %s command on key "%s" to memcached' % (cmd, key)

if __name__ == '__main__':
	main()
