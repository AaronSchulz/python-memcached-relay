import argparse
import json
import time
import socket
import os

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

	config['retry_timeout'] = 5 # time to treat servers as down
	config['pos_write_delay'] = 1 # write positions this often

	# Connect to the memcached server
	print("Connecting to memcached server %s:%s..." % (
		config['memcached_host'],config['memcached_port']))
	mc_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	mc_sock.connect((config['memcached_host'], config['memcached_port']))
	print("Connected")

	rd_handles = {}
	rd_ps_handles = {}
	rd_fail_times = {}
	last_pos_write = {}

	# Connect to all the redis PubSub servers...
	for rd_host in config['redis_hosts']:
		# Construct the redis handle (connection is deferred)
		rd_handles[rd_host] = redis.StrictRedis(
			host=rd_host, port=config['redis_port'],
			password=config['redis_password'],
			socket_connect_timeout=2,
			socket_timeout=2)
		# Create the PubSub object (connection is deferred)
		rd_ps_handles[rd_host] = rd_handles[rd_host].pubsub()
		# Actually connect and subscribe (connection is on first command)
		try:
			# Sync from the reliable stream to the bulk of events
			resyncViaRedisStream(mc_sock, rd_host, rd_handles, time.time(), config)
			# Subscribe to channel to avoid polling overhead
			print("Subscribing to channel %s on %s" % (config['redis_channel'],rd_host))
			rd_ps_handles[rd_host].subscribe(config['redis_channel'])
			print("Subscribed")
			# Quikly resync to avoid any stream gaps (replay a few things twice)
			resyncViaRedisStream(mc_sock, rd_host, rd_handles, time.time(), config)
		except redis.RedisError as e:
			rd_fail_times[rd_host] = time.time()
			print("Error contacting redis server %s" % rd_host)
		# Track the last time the position file was updated for this server
		last_pos_write[rd_host] = time.time()

	# Stream in updates from the channel on all servers indefinitely...
	print("Listening for channel events...")
	while True:
		wait = True
		# Iterate through each host serving the channel
		for rd_host in rd_ps_handles:
			try:
				# Avoid down servers but re-connect periodically if possible
				periodicRedisPing(
					mc_sock, rd_host, rd_handles, rd_ps_handles, rd_fail_times, config)
				# Process the next message if one is ready
				event = rd_ps_handles[rd_host].get_message()
				# @note: events are of the format <UNIX timetamp>:<JSON>
				if event and event['type'] == 'message':
					wait = False
					try:
						eTime, eMsg = event['data'].split(":",1)
						command = json.loads(eMsg)
					except ValueError as e:
						print("Cannot relay command; invalid JSON")
						continue
					# Replicate the update to the memcached server
					relayMemcacheCommand(mc_sock, command)
					# Periodically update the position
					cur_timestamp = time.time();
					if (cur_timestamp - last_pos_write[rd_host]) > config['pos_write_delay']:
						info = {'pos': float(eTime)}
						setCurrentPos(rd_host, info, config)
						last_pos_write[rd_host] = cur_timestamp
			except redis.RedisError as e:
				rd_fail_times[rd_host] = time.time()
				print("Error contacting redis server %s" % rd_host)
		# Avoid high CPU usage
		if wait:
			time.sleep(0.005)

def periodicRedisPing(mc_sock, rd_host, rd_handles, rd_ps_handles, rd_fail_times, config):
	if not rd_host in rd_fail_times:
		return
	if (time.time() - rd_fail_times[rd_host]) >= config['retry_timeout']:
		# Resubscribe before resync to avoid stream gaps
		print("Re-subscribing to channel %s on %s" % (config['redis_channel'],rd_host))
		rd_ps_handles[rd_host].subscribe(config['redis_channel'])
		del rd_fail_times[rd_host]
		print("Subscribed")
		# Resync from the reliable stream (replay a few things twice)
		resyncViaRedisStream(mc_sock, rd_host, rd_handles, time.time(), config)

def resyncViaRedisStream(mc_sock, rd_host, rd_handles, stopPos, config):
	# Prefix the channel to get the stream key
	key = "z-stream:%s" % config['redis_channel']

	print("Applying updates from redis server %s" % rd_host);

	# Get the current position time
	info = getCurrentPos(rd_host, config)
	# Adjust time range to handle any clock skew
	clockSkewFuzz = 5
	info['pos'] = max( 0, info['pos'] - clockSkewFuzz )
	stopPos = stopPos + clockSkewFuzz

	batchSize = 100
	print("Covering position range [%.6f,%.6f]" % (info['pos'],stopPos))
	# Replicate from the log in batches...
	while True:
		events = rd_handles[rd_host].zrangebyscore(
			key, info['pos'], stopPos, start=0, num=batchSize)
		# @note: events are of the format <UNIX timetamp>:<JSON>
		for event in events:
			try:
				eTime, eMsg = event.split(":", 1)
				command = json.loads(eMsg)
			except ValueError as e:
				print("Cannot relay command; invalid JSON")
				continue
			relayMemcacheCommand(mc_sock, command)
			info['pos'] = float(eTime)
		# Update the position after each batch
		print("Updating position to %.6f" % info['pos'])
		setCurrentPos(rd_host, info, config)
		# Stop when there are no batches left
		if len(events) < batchSize:
			break

	print("Done applying updates from redis server %s" % rd_host)

def relayMemcacheCommand(mc_sock, command):
	cmd = str(command['cmd']) # commands are always ASCII
	key = str(command['key']) # keys are always ASCII

	if cmd == 'set' or cmd == 'add':
		value = command['val']
		# Apply value substitutions if requested
		if 'sbt' in command and command['sbt']:
			value = value.replace('$UNIXTIME$', '%.6f' % time.time())

		mcCommand = "%s %s %s %s %s\r\n%s\r\n" % (
			cmd,key,command['flg'],command['ttl'],len(value),value)
	elif cmd == 'delete':
		mcCommand = "delete %s\r\n" % key
	else:
		raise Exception('BadMemcachedCommand', 'Got unrecognized command "%s"' % cmd)

	print('Sending command: %s' % mcCommand)

	# Issue the full command
	mc_sock.sendall(mcCommand)
	# Get the response status (terminated with \r\n)
	result = ''
	while True:
		c = mc_sock.recv(1)
		if c == '\r' or c == '':
			break
		result += c
	mc_sock.recv(1) # consume \n

	# Check if the response was OK
	if result in ['STORED','NOT_STORED','DELETED','NOT_FOUND']:
		print('Got OK result: %s' % result)
	else:
		raise Exception('BadMemcachedResult', 'Got bad result: %s' % result)

	return result

def getCurrentPos(rd_host, config):
	log_path = os.path.join(
		config['data_directory'], '%s:%s.pos' % (rd_host, config['redis_port']))

	try:
		f = open(log_path)
		info = json.load(f)
		f.close()
	except IOError as e:
		info = {'pos': 0.0}
	except ValueError as e:
		info = {'pos': 0.0}
		print("Position file is not valid JSON")

	return info

def setCurrentPos(rd_host, info, config):
	log_path = os.path.join(
		config['data_directory'], '%s:%s.pos' % (rd_host, config['redis_port']))

	f = open(log_path, 'w')
	f.write(json.dumps(info))
	f.close()

if __name__ == '__main__':
	main()
