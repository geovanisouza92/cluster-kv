# cluster-kv

This demonstrates how to use [serf](https://github.com/hashicorp/serf/serf), [mdns](https://github.com/hashicorp/mdns) and [badger](https://github.com/dgraph-io/badger/v4) to build a distributed key-value store.

Serf is used to keep track of the nodes in the cluster and to dispatch events between them.

mDNS is used to discover the nodes in the network without having to know their IP addresses. It's used only during startup of a new node.

Badger is a high-performance key-value store. It's used to store the key-value pairs locally.

## `GET /{key}`

Returns the value for the given key.

If the key is not found locally, it will ask the other nodes in the cluster for the value. If the key is found, it will store it locally and return it to the client.

If none of the nodes have a value, it will return a 404. This not found response can take a while to be returned, because there's a default timeout calculated by the serf library as an estimate of how long it would take to receive a reply from all the nodes in the cluster. That can be overridden by setting the `Timeout` query parameter inside the code.

### Read example

For unknown keys:

```bash
$ curl http://localhost:9876/foo -v
> GET /foo HTTP/1.1
>
< HTTP/1.1 404 Not Found
```

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

You can write to any node in the cluster. It will dispatch an event to the other nodes in the cluster to remove the key from their local stores (a.k.a. passive replication).

### Write example

```bash
$ curl http://localhost:9876/foo -d bar -v
> POST /foo HTTP/1.1
>
< HTTP/1.1 200 OK
<
```

```bash
$ curl http://localhost:9877/foo -d baz -v
> POST /foo HTTP/1.1
>
< HTTP/1.1 200 OK
<
```

If you query the value, you'll see that it's the same on both nodes: `baz`.
