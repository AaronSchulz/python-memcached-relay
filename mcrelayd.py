import argparse
import json
import time
import socket
import os
import yaml
import redis

incr_script = """
if redis.call('EXISTS', KEYS[1]) == 1 then
	return redis.call('INCR', KEYS[1], ARGV[1])
end
return false"""

decr_script = """
if redis.call('EXISTS', KEYS[1]) == 1 then
	return redis.call('DECR', KEYS[1], ARGV[1])
end
return false"""

def main():
	parser = argparse.ArgumentParser(description='Process memcached commands from a channel')
	parser.add_argument('--config-file', required=True, help='YAML configuration file')
	args = parser.parse_args()

	print("Loading YAML config...")
	config = loadConfig(args.config_file)

	# Connect to the memcached/redis cache server...
	print("Connecting to local %s server..." % config['cache_type'])
	target = None # either StrictRedis or Socket
	if config['cache_type'] == 'memcached':
		target = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		target.connect((config['memcached_host'], config['memcached_port']))
	elif config['cache_type'] == 'redis':
		target = redis.StrictRedis(
			host=config['redis_server'],
			port=config['redis_port'],
			password=config['redis_password'],
			socket_connect_timeout=2,
			socket_timeout=2)
		target.ping()
	else:
		raise Exception('InvalidConfig', 'Invalid "cache_type" config')
	print("Connected")

	rd_handles = {} # map of (redis host => StrictRedis)
	rd_ps_handles = {} # map of (redis host => RedisPubSub)
	rd_fail_times = {} # map of (redis host => UNIX timestamp)
	last_pos_write = {} # map of (redis host => UNIX timestamp)

	# Connect to all the redis PubSub servers...
	for rd_host in config['redis_stream_hosts']:
		# Construct the redis handle (connection is deferred)
		rd_handles[rd_host] = redis.StrictRedis(
			host=rd_host,
			port=config['redis_stream_port'],
			password=config['redis_password'],
			socket_connect_timeout=2,
			socket_timeout=2)
		# Create the PubSub object (connection is deferred)
		rd_ps_handles[rd_host] = rd_handles[rd_host].pubsub()
		# Actually connect and subscribe (connection is on first command)
		try:
			# Sync from the reliable stream to the bulk of events
			resyncViaRedisStream(target, rd_host, rd_handles, time.time(), config)
			# Subscribe to channel to avoid polling overhead
			print("Subscribing to channel %s on %s" % (config['redis_channel'],rd_host))
			rd_ps_handles[rd_host].subscribe(config['redis_channel'])
			print("Subscribed")
			# Quikly resync to avoid any stream gaps (replay a few things twice)
			resyncViaRedisStream(target, rd_host, rd_handles, time.time(), config)
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
				streamPing(target, rd_host, rd_handles, rd_ps_handles, rd_fail_times, config)
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
					# Replicate the update to the memcached/redis server
					if isinstance(target,StrictRedis):
						relayRedisCommand(target, command)
					else:
						relayMemcacheCommand(target, command)
					# Periodically update the position
					cur_time = time.time();
					if (cur_time - last_pos_write[rd_host]) > config['pos_write_delay']:
						info = {'pos': float(eTime)}
						set_current_pos(rd_host, info, config)
						last_pos_write[rd_host] = cur_time
			except redis.RedisError as e:
				rd_fail_times[rd_host] = time.time()
				print("Error contacting redis server %s" % rd_host)
		# Avoid high CPU usage
		if wait:
			time.sleep(0.005)

def loadConfig(config_file):
	f = open(config_file)
	config = yaml.safe_load(f)
	f.close()

	config['retry_timeout'] = 5 # time to treat servers as down
	config['pos_write_delay'] = 1 # write positions this often

	return config

def streamPing(target, rd_host, rd_handles, rd_ps_handles, rd_fail_times, config):
	if not rd_host in rd_fail_times:
		return
	if (time.time() - rd_fail_times[rd_host]) >= config['retry_timeout']:
		# Resubscribe before resync to avoid stream gaps
		print("Re-subscribing to channel %s on %s" % (config['redis_channel'],rd_host))
		rd_ps_handles[rd_host].subscribe(config['redis_channel'])
		del rd_fail_times[rd_host]
		print("Subscribed")
		# Resync from the reliable stream (replay a few things twice)
		resyncViaRedisStream(target, rd_host, rd_handles, time.time(), config)

def resyncViaRedisStream(target, rd_host, rd_handles, stopPos, config):
	# Prefix the channel to get the stream key
	key = "z-stream:%s" % config['redis_channel']

	print("Applying updates from redis server %s" % rd_host);

	# Get the current position time
	info = get_current_pos(rd_host, config)
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
			# Replicate the update to the memcached/redis server
			if isinstance(target,StrictRedis):
				relayRedisCommand(target, command)
			else:
				relayMemcacheCommand(target, command)
			info['pos'] = float(eTime)
		# Update the position after each batch
		print("Updating position to %.6f" % info['pos'])
		set_current_pos(rd_host, info, config)
		# Stop when there are no batches left
		if len(events) < batchSize:
			break

	print("Done applying updates from redis server %s" % rd_host)

def relayMemcacheCommand(mc_sock, command):
	# Try to prevent bogus commands from crashing the daemon
	try:
		cmd = str(command['cmd']) # commands are always ASCII
		key = str(command['key']) # keys are always ASCII
		if ' ' in key:
			print('Got bad memcached key "%s" in command' % key)
			return None

		print("Got %s relay command to key %s" % (cmd,key))

		# Apply value substitutions if requested
		if 'val' in command and 'sbt' in command and command['sbt']:
			command['val'] = command['val'].replace('$UNIXTIME$', '%.6f' % time.time())

		if cmd == 'set' or cmd == 'add':
			mcCommand = "%s %s %s %s %s\r\n%s\r\n" % (cmd, key,
				command['flg'], command['ttl'], len(command['val']), command['val'])
		elif cmd == 'incr' or cmd == 'decr':
			value = long(command['val'])
			mcCommand = "%s %s %s\r\n" % (cmd,key,value)
		elif cmd == 'delete':
			mcCommand = "delete %s\r\n" % key
		else:
			print('Got unrecognized memcached command "%s"' % cmd)
			return None
	except (KeyError,ValueError) as e:
		print('Got incomplete or invalid relay command')
		return None

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
		raise Exception('MemcacheCommandError', 'Got bad result: %s' % result)

	return result

def relayRedisCommand(rd_handle, command):
	# Try to prevent bogus commands from crashing the daemon
	try:
		cmd = str(command['cmd']) # commands are always ASCII
		key = str(command['key']) # keys are always ASCII

		print("Got %s relay command to key %s" % (cmd,key))

		# Apply value substitutions if requested
		if 'val' in command and 'sbt' in command and command['sbt']:
			command['val'] = command['val'].replace('$UNIXTIME$', '%.6f' % time.time())

		if cmd == 'set' and command['ttl'] == 0:
			return rd_handle.set(key, command['val'])
		elif cmd == 'set':
			return rd_handle.setex(key, command['ttl'], command['val'])
		elif cmd == 'add':
			if not rd_handle.exists(key):
				if command['ttl'] == 0:
					return rd_handle.set(key, command['val'])
				else
					return rd_handle.setex(key, command['ttl'], command['val'])
			else
				return False
		elif cmd == 'incr':
			return rd_handle.eval(incr_script, 1, key, command['val'])
		elif cmd == 'decr':
			return rd_handle.eval(decr_script, 1, key, command['val'])
		elif cmd == 'delete':
			return rd_handle.delete(key)
		else:
			print('Got unrecognized redis command "%s"' % cmd)
			return None
	except (KeyError,ValueError) as e:
		print('Got incomplete or invalid relay command')
		return None
	except redis.RedisError as e:
		raise Exception('RedisCommandError', 'Failed to issue redis command')

def get_current_pos(rd_host, config):
	try:
		f = open(get_pos_path(rd_host, config))
		info = json.load(f)
		f.close()
	except IOError as e:
		info = {'pos': 0.0}
	except ValueError as e:
		info = {'pos': 0.0}
		print("Position file is not valid JSON")

	return info

def set_current_pos(rd_host, info, config):
	f = open(get_pos_path(rd_host, config), 'w')
	f.write(json.dumps(info))
	f.close()

def get_pos_path(rd_host, config):
	return os.path.join(config['data_directory'],
		'%s:%s.pos' % (rd_host, config['redis_stream_port']))

if __name__ == '__main__':
	main()
