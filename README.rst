goupr is Go implementation of couchbase UPR protocol for cluster replication
and failover, and right now it implements the client (consumer) side of UPR
protocol.

Basic design:

Applications Connect() with producer (server) and initiates a session with the
producer. Connect() returns a reference to `Client` instance back to the
application.

Using the client instance, application can create one or more `Stream` on the
same connection by calling client.UprStream(). Every stream is uniquely
identified by the opaque-id supplied during stream creation. A map of
opaque-id and Stream reference is maintained by the `Client` instance.

Every connection also spawns a goroutine to handle all incoming messages
from the server. If the messages are mapped to stream instance, then those
messages are sent to the corresponding stream's channel, else they are sent to
a common response channel (maintained per connection).
