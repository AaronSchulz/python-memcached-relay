#!/bin/python
import argparse
import json
import time
import socket
import os
import yaml
import redis
import httplib2

parser = argparse.ArgumentParser(description='Process cache relay commands from a channel')
parser.add_argument('--config-file', required=True, help='YAML configuration file')
script_args = parser.parse_args()


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

env = AttrDict()  # global state
env.config = {}
env.rd_handles = {}  # map of (redis host => StrictRedis)
env.rd_ps_handles = {}  # map of (redis host => RedisPubSub)
env.rd_fail_times = {}  # map of (redis host => UNIX timestamp)


def main():
    print("Loading YAML config...")
    env.config = load_config(script_args.config_file)
    print("Done")

    # Connect to the memcached/redis cache server...
    print("Connecting to local %s server..." % env.config['cache_type'])
    target = get_target_cache()  # one of (StrictRedis,Socket,Http)
    print("Connected")

    last_pos_write = {}  # map of (redis host => UNIX timestamp)

    # Connect to all the redis PubSub servers...
    for rd_host in env.config['redis_stream_hosts']:
        # Construct the redis handle (connection is deferred)
        env.rd_handles[rd_host] = redis.StrictRedis(
            host=rd_host,
            port=env.config['redis_stream_port'],
            password=env.config['redis_password'],
            socket_connect_timeout=env.config['redis_connect_timeout'],
            socket_timeout=env.config['redis_timeout'])
        # Create the PubSub object (connection is deferred)
        env.rd_ps_handles[rd_host] = env.rd_handles[rd_host].pubsub()
        # Actually connect and subscribe (connection is on first command)
        try:
            # Sync from the reliable stream to the bulk of events
            resync_via_redis_stream(target, rd_host, time.time())
            # Subscribe to channel to avoid polling overhead
            print("Subscribing to channel %s on %s..." % (env.config['redis_channel'], rd_host))
            env.rd_ps_handles[rd_host].subscribe(env.config['redis_channel'])
            print("Subscribed")
            # Quickly resync to avoid any stream gaps (replay a few things twice)
            resync_via_redis_stream(target, rd_host, time.time())
        except redis.RedisError as e:
            env.rd_fail_times[rd_host] = time.time()
            print("Error contacting redis server %s" % rd_host)
        # Track the last time the position file was updated for this server
        last_pos_write[rd_host] = time.time()

    # Stream in updates from the channel on all servers indefinitely...
    print("Listening for channel events...")
    while True:
        found_any = False
        # Iterate through each host serving the channel
        for rd_host in env.rd_ps_handles:
            try:
                # If a relay command is ready then run it on the cache
                got_cmd = relay_next_command(target, rd_host, last_pos_write)
                found_any = got_cmd or found_any
            except redis.RedisError as e:
                env.rd_fail_times[rd_host] = time.time()
                print("Error contacting redis server %s" % rd_host)
        # Avoid high CPU usage when no commands were found
        if not found_any:
            time.sleep(0.005)


def load_config(config_file):
    f = open(config_file)
    config = yaml.safe_load(f)
    f.close()

    config['retry_timeout'] = 5  # time to treat servers as down
    config['pos_write_delay'] = 1  # write positions this often
    config['redis_connect_timeout'] = 1
    config['redis_timeout'] = 1

    return config


def get_target_cache():
    if env.config['cache_type'] == 'memcached':
        target = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        target.connect((env.config['memcached_host'], env.config['memcached_port']))
    elif env.config['cache_type'] == 'redis':
        target = redis.StrictRedis(
            host=env.config['redis_host'],
            port=env.config['redis_port'],
            password=env.config['redis_password'],
            socket_connect_timeout=env.config['redis_connect_timeout'],
            socket_timeout=env.config['redis_timeout'])
        target.ping()
    elif env.config['cache_type'] == 'cdn':
        target = httplib2.Http()
    else:
        raise Exception('InvalidConfig', 'Invalid "cache_type" config')

    return target


def relay_next_command(target, rd_host, last_pos_write):
    # Avoid down servers but re-connect periodically if possible
    redis_stream_ping(target, rd_host)
    # Process the next message if one is ready
    event = env.rd_ps_handles[rd_host].get_message()
    # @note: events are of the format <UNIX timestamp>:<JSON>
    if event and event['type'] == 'message':
        try:
            e_time, e_msg = event['data'].split(":", 1)
            command = json.loads(e_msg)
            e_time = float(e_time)
        except ValueError as e:
            print("Cannot relay command; invalid JSON")
            return True
        # Replicate the update to the cache server
        relay_cache_command(target, command, e_time)
        # Periodically update the position file
        cur_time = time.time()
        if (cur_time - last_pos_write[rd_host]) > env.config['pos_write_delay']:
            info = {'pos': e_time}
            set_current_position(rd_host, info)
            last_pos_write[rd_host] = cur_time
        return True
    else:
        return False


def redis_stream_ping(target, rd_host):
    if rd_host not in env.rd_fail_times:
        return
    if (time.time() - env.rd_fail_times[rd_host]) >= env.config['retry_timeout']:
        # Resubscribe before resync to avoid stream gaps
        print("Re-subscribing to channel %s on %s" % (env.config['redis_channel'], rd_host))
        env.rd_ps_handles[rd_host].subscribe(env.config['redis_channel'])
        del env.rd_fail_times[rd_host]
        print("Subscribed")
        # Resync from the reliable stream (replay a few things twice)
        resync_via_redis_stream(target, rd_host, time.time())


def resync_via_redis_stream(target, rd_host, stop_pos):
    # Prefix the channel to get the stream key
    key = "z-channel-stream:%s" % env.config['redis_channel']

    print("Applying updates from redis server %s" % rd_host)

    # Get the current position time
    info = get_current_position(rd_host)
    # Adjust time range to handle any clock skew
    clock_skew_fuzz = 5
    info['pos'] = max(0, info['pos'] - clock_skew_fuzz)
    stop_pos += clock_skew_fuzz

    batch_size = 100
    print("Covering position range [%.6f,%.6f]" % (info['pos'], stop_pos))
    # Replicate from the log in batches...
    while True:
        events = env.rd_handles[rd_host].zrangebyscore(
            key, info['pos'], stop_pos, start=0, num=batch_size)
        # @note: events are of the format <UNIX timestamp>:<JSON>
        for event in events:
            try:
                e_time, e_msg = event.split(":", 1)
                command = json.loads(e_msg)
                e_time = float(e_time)
            except ValueError as e:
                print("Cannot relay command; invalid JSON")
                continue
            # Replicate the update to the cache server
            relay_cache_command(target, command, e_time)
            info['pos'] = e_time
        # Update the position after each batch
        print("Updating position to %.6f" % info['pos'])
        set_current_position(rd_host, info)
        # Stop when there are no batches left
        if len(events) < batch_size:
            break

    print("Done applying updates from redis server %s" % rd_host)


def relay_cache_command(target, command, e_time):
    if env.config['cache_type'] == 'memcached':
        return relay_memcache_command(target, command, e_time)
    elif env.config['cache_type'] == 'redis':
        return relay_redis_command(target, command, e_time)
    elif env.config['cache_type'] == 'cdn':
        return relay_cdn_command(target, command, e_time)

    return None


def relay_memcache_command(mc_sock, command, e_time):
    try:
        cmd = str(command['cmd'])  # commands are always ASCII
        key = str(command['key'])  # keys are always ASCII
        if ' ' in key:
            print('Got bad memcached key "%s" in command' % key)
            return None

        print("Got '%s' relay command to key %s" % (cmd, key))

        # Apply value substitutions if requested
        if command.get('sbt', None):
            purge_time = time.time() + command.get('uto', 0)
            command['val'] = command['val'].replace('$UNIXTIME$', '%.6f' % purge_time)

        if cmd == 'set':
            is_stale = False
            if command['ttl'] != 0:
                command['ttl'] -= int(time.time() - e_time)
                is_stale = (command['ttl'] <= 0)
            if is_stale:
                print("Command 'set' for key %s is stale" % key)
                cmd_buffer = "delete %s\r\n" % key
            else:
                cmd_buffer = "set %s %s %s %s\r\n%s\r\n" % (
                    key, command['flg'], command['ttl'], len(command['val']), command['val'])
        elif cmd == 'delete':
            cmd_buffer = "delete %s\r\n" % key
        else:
            print('Got unrecognized memcached command "%s"' % cmd)
            return None
    except (KeyError, ValueError, TypeError) as e:
        print('Got incomplete or invalid relay command')
        print(command)
        return None

    # Issue the full command
    mc_sock.sendall(cmd_buffer)
    # Get the response status (terminated with \r\n)
    result = ''
    while True:
        c = mc_sock.recv(1)
        if c == '\r' or c == '':
            break
        result += c
    mc_sock.recv(1)  # consume \n

    # Check if the response was OK
    if result in ['STORED', 'NOT_STORED', 'DELETED', 'NOT_FOUND']:
        print('Got OK result: %s' % result)
    else:
        raise Exception('MemcacheCommandError', 'Got bad result: %s' % result)

    return result


def relay_redis_command(rd_handle, command, e_time):
    try:
        cmd = str(command['cmd'])  # commands are always ASCII
        key = str(command['key'])  # keys are always ASCII

        print("Got '%s' relay command to key %s" % (cmd, key))

        # Apply value substitutions if requested
        if command.get('sbt', None):
            purge_time = time.time() + command.get('uto', 0)
            command['val'] = command['val'].replace('$UNIXTIME$', '%.6f' % purge_time)

        if cmd == 'set':
            if command['ttl'] == 0:
                return rd_handle.set(key, command['val'])
            else:
                command['ttl'] -= int(time.time() - e_time)
                if command['ttl'] <= 0:
                    print("Command 'set' for key %s is stale" % key)
                    return rd_handle.delete(key)
                else:
                    return rd_handle.setex(key, command['ttl'], command['val'])
        elif cmd == 'delete':
            return rd_handle.delete(key)
        else:
            print('Got unrecognized redis command "%s"' % cmd)
            return None
    except (KeyError, ValueError, TypeError) as e:
        print('Got incomplete or invalid relay command')
        print(command)
        return None
    except redis.RedisError as e:
        raise Exception('RedisCommandError', 'Failed to issue redis command')


def relay_cdn_command(http, command, e_time):
    try:
        cmd = str(command['cmd'])  # HTTP verbs are always ASCII

        print("Got '%s' relay command to URL '%s'" % (cmd, command['path']))

        headers = {'Host': command['host']}
        if cmd == 'PURGE':
            url = env.config['cdn_host'] + '/' + command['path']
            response, content = http.request(url, 'PURGE', headers=headers)
        else:
            print('Got unrecognized CDN command "%s"' % cmd)
            return None
    except (KeyError, ValueError) as e:
        print('Got incomplete or invalid relay command')
        return None

    return response.status


def get_current_position(rd_host):
    try:
        f = open(get_position_path(rd_host))
        info = json.load(f)
        f.close()
    except IOError as e:
        info = {'pos': 0.0}
    except ValueError as e:
        info = {'pos': 0.0}
        print("Position file is not valid JSON")

    return info


def set_current_position(rd_host, info):
    f = open(get_position_path(rd_host), 'w')
    f.write(json.dumps(info))
    f.close()


def get_position_path(rd_host):
    return os.path.join(env.config['data_directory'],
                        '%s:%s.pos' % (rd_host, env.config['redis_stream_port']))


if __name__ == '__main__':
    main()
