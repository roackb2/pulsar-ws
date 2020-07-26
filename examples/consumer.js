'use strict';
const { Consumer } = require('../index');
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
//     subscription: "1",
//     token: "JWT token", // optional, only supports JWT token authentication and authorization for now
//     params: {
//         ...other Consumer endpoint query params
//     }
// }
const consumer = new Consumer({
    persistent: true,
    reconnect: true,
    host: "localhost",
    port: 8080,
    secure: false,
    tenant: "my-tenant",
    namespace: "my-ns",
    topic: "my-topic",
    subscription: "1",
    token: "JWT token", // optional, only supports JWT token authentication and authorization for now
    params: {
        receiverQueueSize: 65535
    }
}, logger)

consumer.listen(message => {
    console.log(`recevied message: ${JSON.stringify(message, null, '\t')}`);
})
