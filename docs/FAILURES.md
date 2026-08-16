# Failure Behavior

| Failure | Behavior | Metric/log signal |
| --- | --- | --- |
| Bad checksum | Frame is dropped; parser continues | no accepted event |
| Unknown bytes before magic | Bytes are discarded until a candidate frame | no accepted event |
| Payload over 1 MiB | Connection is closed | warning log |
| Log open/write failure | Event is not processed; client receives `ERR persistence` | no durable counter |
| Full bounded queue | Submit blocks until space or shutdown | backpressure |
| Queue close | Blocked producers fail; consumers drain existing items then return empty | clean joins |
| Process crash during append | Complete prior frames replay; partial tail ignored | inspect with `weir-inspect-log` |

There is no automatic retry or replication.
Callers must reconnect and resend when delivery matters.
