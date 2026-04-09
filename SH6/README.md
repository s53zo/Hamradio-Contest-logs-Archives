# SH6 SQLite Shards

This folder contains the SQLite shard indexes used by the SH6 client.
Each `logs_XX.sqlite` file is a shard keyed by a stable hash of the callsign,
so the browser can download only one small shard via HTTP Range requests and
run fast lookups locally.

Callsigns are stored in uppercase. Clients should normalize user input to
uppercase before computing the shard bucket and querying.

The data is used by the SH6 web app:
https://s53zo.github.io/SH6/index.html
