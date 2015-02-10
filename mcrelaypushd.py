#!flask/bin/python
from flask import Flask
from flask import request, jsonify, abort
import argparse
import time
import random
import json
import yaml
import redis

parser = argparse.ArgumentParser(description='Enqueue relay cache commands into redis')
parser.add_argument('--config-file', required=True, help='YAML configuration file')
args = parser.parse_args()


def load_config(config_file):
    f = open(config_file)
    config = yaml.safe_load(f)
    f.close()

    config['event_ttl'] = 86400
    config['retry_timeout'] = 5  # time to treat servers as down
    config['redis_connect_timeout'] = 1
    config['redis_timeout'] = 1

    return config

app = Flask(__name__)
app.config.update(load_config(args.config_file))

enqueue_script = """
local kEvents = unpack(KEYS)
local curTime = 1*ARGV[1]
local purgeTTL = 1*ARGV[2]
local channel = ARGV[3]
-- Append the events (the remaining arguments) to the set
local pushed = 0
for i = 4,#ARGV,1 do
local event = ARGV[i]
-- Prepend the timestamp to allow duplicate events
redis.call('zAdd',kEvents,curTime,curTime .. ':' .. event)
-- Also publish the event for the fast common case
redis.call('publish',channel,curTime .. ':' .. event)
    pushed = pushed + 1
end
-- Prune out any stale events
    redis.call('zRemRangeByScore',kEvents,0,curTime - purgeTTL)
return pushed"""

env = {  # global state
    'rd_handles': {},
    'rd_enqueue_handles': {},
    'rd_fail_times': {}
}


def init_redis():
    # Construct the redis handles (connection is deferred)
    for rd_host in app.config['redis_stream_hosts']:
        env['rd_handles'][rd_host] = redis.StrictRedis(
            host=rd_host,
            port=app.config['redis_stream_port'],
            password=app.config['redis_password'],
            socket_connect_timeout=app.config['redis_connect_timeout'],
            socket_timeout=app.config['redis_timeout'])
        env['rd_enqueue_handles'][rd_host] = env['rd_handles'][rd_host].register_script(enqueue_script)


@app.route('/relayer/api/v1.0/<string:channel>', methods=['POST'])
def enqueue_command(channel):
    # Randomize the servers used
    host_candidates = env['rd_handles'].keys()
    random.shuffle(host_candidates)

    # Convert the objects back to JSON
    event_blobs = []
    for event in request.json['events']:
        event_blobs.append(json.dumps(event))

    # Get the messages pushed into a random redis server...
    published = 0
    for rd_host in host_candidates:
        # Avoid down servers but re-connect periodically if possible
        if (time.time() - env['rd_fail_times'].get(rd_host, 0)) < app.config['retry_timeout']:
            continue
        # Insert the event into the channel and the reliable stream
        try:
            keys = ["z-channel-stream:%s" % channel]
            lua_args = [time.time(), app.config['event_ttl'], channel]
            lua_args.extend(event_blobs)
            published += env['rd_enqueue_handles'][rd_host](keys=keys, args=lua_args)
            env['rd_fail_times'].pop(rd_host, None)
            break
        except (KeyError, ValueError) as e:
            print("Got invalid queue event")
        except redis.RedisError as e:
            env['rd_fail_times'][rd_host] = time.time()
            print("Error contacting redis server %s" % rd_host)

    if published != len(request.json['events']):
        status = 500
    else:
        status = 201

    return jsonify({'published': published}), status

if __name__ == '__main__':
    init_redis()
    app.run(threaded=True)