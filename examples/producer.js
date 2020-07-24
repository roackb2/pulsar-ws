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
//     host: "localhost",
//     port: 8080,
//     tenant: "my-tenant",
//     cluster: "us-east-1",
//     namespace: "my-ns",
//     topic: "my-topic",
//     reconnect: true,
//     params: {
//         ...other Producer endpoint query params
//     }
// }
const producer = new Producer({
    persistent: true,
    host: "localhost",
    port: 8080,
    tenant: "my-tenant",
    cluster: "us-east-1",
    namespace: "my-namespace",
    topic: "my-topic",
    reconnect: true,
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
