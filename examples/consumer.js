'use strict';
const { Consumer } = require('../index');
const { createLogger, format, transports } = require('winston');
const logger = createLogger({
    level: 'debug',
    format: format.simple(),
    transports: [new transports.Console()]
});

const consumer = new Consumer({
    host: "localhost",
    port: 8080,
    tenant: "my-tenant",
    cluster: "us-east-1",
    namespace: "my-namespace",
    topic: "my-topic",
    subscription: "sub-1",
    reconnect: true,
}, logger)

consumer.listen(message => {
    console.log(`recevied message: ${JSON.stringify(message, null, '\t')}`);
})
