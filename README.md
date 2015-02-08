# python-memcached-relay
Relay memcached updates from a Redis PubSub channel

This includes a "push" HTTP daemon for pushing JSON events into a cluster of redis servers. Writes are randomly striped amongst the redis servers. There is also a "pull" daemon that subscribes to the redis servers and applies the updates to cache. The cache can be memcached, redis, varnish, or squid. The following commands can be relayed:

To push a command, issue a POST request to the "push" daemon to a URL of the form ```"/relayer/api/v1.0/<channel name>"```. A JSON blob should be the POST body, of the form ```"{events:[<event1>,<event2>,...]}"```. Each event should be a JSON object.

memcached/redis:
- set    : set a key to a value. The event requires {cmd='set',key,val,ttl}
- delete : delete a key. The event requires {cmd='delete',key}
  
varnish/squid:
- purge : purge a URL. The event requires {cmd='purge',path,host}
