# pulsar-ws

This is a node.js library for Apache Pulsar with communication over WebSocket.

# Motivation

Current Apache Pulsar libraries are wrappers over C++ client library, which adds difficulties
for cross-platform deployment. Using WebSocket, this library becomes pure JavaScript
and need no C++ client library installation anymore.

# Examples

See examples folder.

# Secure Connection

Since npm ws module allows custom header on WebSocket request, it is possible to send JWT access token in header.
But you might need to set environment variables as following:
```
export NODE_EXTRA_CA_CERTS=[Your CA cert path];
export NODE_TLS_REJECT_UNAUTHORIZED=0;
```
This disable TLS authorization so it's unsafe, so use in production on your own risk.
