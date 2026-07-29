# Key-Value Storage Rest Integration

This set of components allows using Key-Value Storage remotely.

### Quick Design Explanation

The `client` crate contains an implementation of the KVS API, but instead of handling calls by itself, the client makes RPC calls to the `server` that calls the same KVS API on the server-side. They share common logic via `common` crate.
