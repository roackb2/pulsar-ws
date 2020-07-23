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
//     host: "localhost",
//     port: 8080,
//     tenant: "my-tenant",
//     cluster: "us-east-1",
//     namespace: "my-ns",
//     topic: "my-topic"
//     subscription: "1",
//     reconnect: true,
//     params: {
//         ...other Consumer endpoint query params
//     }
// }
const consumer = new Consumer({
    host: "localhost",
    port: 8080,
    tenant: "my-tenant",
    cluster: "us-east-1",
    namespace: "my-namespace",
    topic: "my-topic",
    subscription: "sub-1",
    reconnect: true,
    params: {
        receiverQueueSize: 65535
    }
}, logger)

consumer.listen(message => {
    console.log(`recevied message: ${JSON.stringify(message, null, '\t')}`);
})
