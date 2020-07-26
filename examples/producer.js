'use strict';
const { Producer } = require('../index');
const { createLogger, format, transports } = require('winston');
const logger = createLogger({
    level: 'debug',
    format: format.simple(),
    transports: [new transports.Console()]
});

// example opts
// {
//     persistent: true,
//     reconnect: true,
//     host: "localhost",
//     port: 8080,
//     secure: false,
//     tenant: "my-tenant",
//     namespace: "my-ns",
//     topic: "my-topic",
//     token: "JWT token", // optional, only supports JWT token authentication and authorization for now
//     params: {
//         ...other Producer endpoint query params
//     }
// }
const producer = new Producer({
    persistent: true,
    reconnect: true,
    host: "localhost",
    port: 8080,
    secure: false,
    tenant: "my-tenant",
    namespace: "my-ns",
    topic: "my-topic",
    token: "JWT token", // optional, only supports JWT token authentication and authorization for now
    params: {
        maxPendingMessages: 65535
    }
}, logger)

producer.start(() => {
    var message = {
        "payload" : Buffer.from("Hello World!").toString('hex'), // Pulsar requires payload to be hex or base64 encoding
        "properties": {
            "key1" : "value1",
        },
        "context" : "1"
    };
    producer.send(message);
});
