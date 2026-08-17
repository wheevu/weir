# Wire Protocol

Each event is one binary frame in network byte order:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | ASCII magic `WR01` |
| 4 | 8 | Event id, unsigned 64-bit big-endian |
| 12 | 4 | Payload length, unsigned 32-bit big-endian |
| 16 | N | Payload bytes |
| 16 + N | 4 | FNV-1a 32-bit checksum of payload |

The maximum accepted payload is 1 MiB.
TCP boundaries have no meaning: a frame may be split across reads or multiple frames may share one read.
The server ignores the frame's transmitted id, assigns the next id during startup recovery or receipt, and returns `OK <id>\n` only after a successful flushed append.
Persistence failure returns `ERR persistence\n`.
When the bounded admission queue is full, the server keeps the connection open and returns `ERR queue\n` for that event.
If a client leaves more than 65,536 response slots unresolved, or queues more than 1 MiB of outbound responses, the server closes that connection to bound memory.
An invalid checksum is ignored and an oversized frame closes the connection without bringing down the server.

`weir-producer` is a small example client, not a production delivery client.
