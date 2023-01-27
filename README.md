
# AfloatDB

This project demonstrates how to build very simple distributed key-value store  on top of [MicroRaft](https://github.com/MicroRaft/MicroRaft). Please note that this is not production-ready
code.

## Code pointers

[`afloatdb-server`](https://github.com/MicroRaft/AfloatDB/tree/master/afloatdb-server) directory contains the server code and [`afloatdb-client`](https://github.com/MicroRaft/AfloatDB/tree/master/afloatdb-client)
directory contains the client code.

Each AfloatDB server runs a [`RaftNode`](https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/RaftNode.java) and the communication between servers
and the client-server communication happens through gRPC.

[`afloatdb-server/src/main/proto/AfloatDBRaft.proto`](https://github.com/MicroRaft/AfloatDB/blob/master/afloatdb-server/src/main/proto/AfloatDBRaft.proto) contains the Protobufs definitions
for [MicroRaft's model and network abstractions](https://microraft.io/docs/main-abstractions/), and the key-value operations of
AfloatDB's key-value API. AfloatDB servers talk to each other via
the `RaftService` defined in this file. This service is
implemented at [`afloatdb-server/src/main/java/io/afloatdb/internal/rpc/impl/RaftRpcServiceImpl.java`](https://github.com/MicroRaft/AfloatDB/blob/master/afloatdb-server/src/main/java/io/microraft/afloatdb/internal/rpc/impl/RaftRpcServiceImpl.java).

[`afloatdb-server/src/main/java/io/afloatdb/internal/raft/model`](https://github.com/MicroRaft/AfloatDB/tree/master/afloatdb-server/src/main/java/io/microraft/afloatdb/internal/raft/model) package contains
implementing MicroRaft's model abstractions using the Protobufs definitions
defined in the above file.

[`afloatdb-server/src/main/java/io/afloatdb/internal/rpc/RaftRpcService.java`](https://github.com/MicroRaft/AfloatDB/blob/master/afloatdb-server/src/main/java/io/microraft/afloatdb/internal/rpc/RaftRpcService.java) implements
MicroRaft's [`Transport`](https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/transport/Transport.java) abstraction and makes AfloatDB servers talk to each
other with gRPC.

[`afloatdb-server/src/main/java/io/afloatdb/internal/raft/impl/KVStoreStateMachine.java`](https://github.com/MicroRaft/AfloatDB/blob/master/afloatdb-server/src/main/java/io/microraft/afloatdb/internal/raft/KVStoreStateMachine.java)
contains the state machine implementation of AfloatDB's key-value API and it
implements MicroRaft's [`StateMachine`](https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/statemachine/StateMachine.java) interface.

[`afloatdb-commons/src/main/proto/KV.proto`](https://github.com/MicroRaft/AfloatDB/blob/master/afloatdb-commons/src/main/proto/KV.proto) defines the protocol between
AfloatDB clients and servers. The server side counterpart is at
[`afloatdb-server/src/main/java/io/afloatdb/internal/rpc/impl/KVService.java`](https://github.com/MicroRaft/AfloatDB/blob/master/afloatdb-server/src/main/java/io/microraft/afloatdb/internal/rpc/impl/KVService.java).
It handles requests sent by clients and passes them to `RaftNode`.

[`afloatdb-server/src/main/proto/AfloatDBAdmin.proto`](https://github.com/MicroRaft/AfloatDB/blob/master/afloatdb-server/src/main/proto/AfloatDBAdmin.proto) contains the Protobufs
definitions for management operations on AfloatDB clusters, such as
adding / removing servers, querying RaftNode reports. Operators can manage
AfloatDB clusters by making gRPC calls to the `AdminService`
service defined in this file. Its server side counter part is at
[`afloatdb-server/src/main/java/io/afloatdb/internal/rpc/impl/AdminService.java`](https://github.com/MicroRaft/AfloatDB/blob/master/afloatdb-server/src/main/java/io/microraft/afloatdb/internal/rpc/impl/AdminService.java).
It handles requests sent by clients and passes them to `RaftNode`.

That is enough pointers for curious readers.

MicroRaft provides a [SQLite-based `RaftStore` implementation](https://github.com/MicroRaft/MicroRaft/blob/master/microraft-store-sqlite/src/main/java/io/microraft/store/sqlite/RaftSqliteStore.java). AfloatDB injects its [SerDe class](https://github.com/MicroRaft/AfloatDB/blob/master/afloatdb-server/src/main/java/io/microraft/afloatdb/internal/raft/model/ProtoStateStoreSerializer.java) to persist [its Protocol Buffers based implementations](https://github.com/MicroRaft/AfloatDB/tree/master/afloatdb-server/src/main/java/io/microraft/afloatdb/internal/raft/model) of [the RaftModel interfaces](https://github.com/MicroRaft/MicroRaft/tree/master/microraft/src/main/java/io/microraft/model).

## How to start a 3-node AfloatDB cluster

Configuration is built with the [Typesafe Config](https://github.com/lightbend/config)
library. You can also create config programmatically. Please see
[`afloatdb-server/src/main/java/io/afloatdb/config/AfloatDBConfig.java`](https://github.com/MicroRaft/AfloatDB/blob/master/afloatdb-server/src/main/java/io/microraft/afloatdb/config/AfloatDBConfig.java).

You need to pass a config to start an AfloatDB server. The config should
provide the Raft endpoint (id and address) and the initial member list of
the cluster. [`afloatdb-server/src/test/resources`](https://github.com/MicroRaft/AfloatDB/tree/master/afloatdb-server/src/test/resources) contains example config files to
start a 3 node AfloatDB cluster.

`mvn clean package`

`java -jar  afloatdb-server/target/afloatdb-server-fat.jar afloatdb-server/src/test/resources/node1.conf &`

`java -jar  afloatdb-server/target/afloatdb-server-fat.jar afloatdb-server/src/test/resources/node2.conf &`

`java -jar  afloatdb-server/target/afloatdb-server-fat.jar afloatdb-server/src/test/resources/node3.conf &`

## Adding a new server to a running cluster

Once you start your AfloatDB cluster, you can add new servers at runtime.
For this, you need to provide address of one of the running servers via
the "join-to" config field for the new server.

`java -jar  afloatdb-server/target/afloatdb-server-fat.jar afloatdb-server/src/test/resources/node4.conf &`

## Key-value API

[`afloatdb-client/src/main/java/io/afloatdb/client/kv/KV.java`](https://github.com/MicroRaft/AfloatDB/blob/master/afloatdb-client/src/main/java/io/microraft/afloatdb/client/kv/KV.java) contains the
key-value interface. Keys have `String` type and values can be one of `String`,
`long`, or `byte[]`.

## Client API

You can start a client with [`AfloatDBClient.newInstance(config)`](https://github.com/MicroRaft/AfloatDB/blob/master/afloatdb-client/src/main/java/io/microraft/afloatdb/client/AfloatDBClient.java#L29) to
write and read key-value pairs on the cluster. Please see
[`afloatdb-client/src/main/java/io/afloatdb/client/AfloatDBClientConfig.java`](https://github.com/MicroRaft/AfloatDB/blob/master/afloatdb-client/src/main/java/io/microraft/afloatdb/client/config/AfloatDBClientConfig.java)
for configuration options. You need to pass a server address.
If `singleConnection` is `false`, which is the default value, the client will
discover all servers and connect to them.

## Key-value CLI for experimentation

`java  -jar afloatdb-client-cli/target/afloatdb-client-cli-fat.jar --server localhost:6701 --key name --value basri set`

Output: `Ordered{commitIndex=2, value=null}`

`java  -jar afloatdb-client-cli/target/afloatdb-client-cli-fat.jar --server localhost:6701 --key name get`

Output: `Ordered{commitIndex=2, value=basri}`

See [`afloatdb-client-cli/src/main/java/io/afloatdb/client/AfloatDBClientCliRunner.java`](https://github.com/MicroRaft/AfloatDB/blob/master/afloatdb-client-cli/src/main/java/io/microraft/afloatdb/client/AfloatDBClientCliRunner.java) for more options.
