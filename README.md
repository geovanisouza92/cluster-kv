# cluster-kv

This demonstrates how to use [serf](https://github.com/hashicorp/serf/serf), [mdns](https://github.com/hashicorp/mdns) and [badger](https://github.com/dgraph-io/badger/v4) to build a distributed key-value store.

Serf is used to keep track of the nodes in the cluster and to dispatch events between them.

mDNS is used to discover the nodes in the network without having to know their IP addresses. It's used only during startup of a new node.

Badger is a high-performance key-value store. It's used to store the key-value pairs locally.

## `GET /{key}`

Returns the value for the given key.

If the key is not found locally, it will ask the other nodes in the cluster for the value. If the key is found, it will store it locally and return it to the client.

If none of the nodes have a value, it will return a 404.

There is a default timeout calculated by the serf library, but it can be overridden by setting the `Timeout` query parameter.

### Read example

For unknown keys:

```bash
$ curl http://localhost:9876/foo -v
> GET /foo HTTP/1.1
>
< HTTP/1.1 404 Not Found
```

You can also read from any node in the cluster. If the key is not found locally, it will ask the other nodes in the cluster for the value. If a reply is given, it will store it locally and return it to the client.

For known keys:

```bash
$ curl http://localhost:9876/foo -v
> GET /foo HTTP/1.1
>
< HTTP/1.1 200 OK
<
bar
```

## `POST /{key}`

Sets the value for the given key.

It will dispatch an event to the other nodes in the cluster to remove the key from their local stores (a.k.a. passive replication).

### Write example

```bash
$ curl http://localhost:9876/foo -d bar -v
> POST /foo HTTP/1.1
>
< HTTP/1.1 200 OK
<
```

You can also write to any node in the cluster. The other nodes will receive the event and remove the key from their local stores.
