#!flask/bin/python
from flask import Flask

import argparse
import time
import random
import json
import yaml
import redis

app = Flask(__name__)

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

env = {}  # global state


def setup():
    parser = argparse.ArgumentParser(description='Enqueue relay cache commands into redis')
    parser.add_argument('--config-file', required=True, help='YAML configuration file')
    args = parser.parse_args()

    print("Loading YAML config...")
    env.config = load_config(args.config_file)
    print("Done")

    # Construct the redis handles (connection is deferred)
    for rd_host in env.config['redis_stream_hosts']:
        env.rd_handles[rd_host] = redis.StrictRedis(
            host=rd_host,
            port=env.config['redis_stream_port'],
            password=env.config['redis_password'],
            socket_connect_timeout=env.config['redis_connect_timeout'],
            socket_timeout=env.config['socket_timeout'])
        env.rd_enqueue_handle[rd_host] = env.rd_handles[rd_host].register_script(enqueue_script)


def load_config(config_file):
    f = open(config_file)
    config = yaml.safe_load(f)
    f.close()

    config['event_ttl'] = 86400
    config['redis_connect_timeout'] = 1
    config['redis_timeout'] = 1

    return config


@app.route('/relayer/api/v1.0/<string:channel>', methods=['POST'])
def enqueue_command(channel):
    if not request.json or 'events' not in request.json:
        abort(400)
    if not isinstance(request.json.events, list):
        abort(400)

    # Randomize the servers used
    host_candidates = env.rd_handles.keys()
    random.shuffle(host_candidates)

    # Get the messages pushed into a random redis server...
    published = 0
    for rd_host in host_candidates:
        try:
            keys = ["z-channel-stream:%s" % channel]
            args = [time.time(), env.config['event_ttl'], channel]
            args.extend(request.json.events)
            published += env.rd_enqueue_handle[rd_host](keys=keys, args=args)
            break
        except (KeyError, ValueError) as e:
            print("Got invalid queue event")
        except redis.RedisError as e:
            print("Error contacting redis server %s" % rd_host)

    if published != request.json.events:
        status = 500
    else:
        status = 201

    return jsonify({'published': published}), status

if __name__ == '__main__':
    setup()
    app.run(debug=True)