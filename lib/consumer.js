'use strict'
const WebSocket = require('ws');
const emptyLogger = require('./logging');

// example opts
// {
//     host: "localhost",
//     port: 8080,
//     tenant: "my-tenant",
//     cluster: "us-east-1",
//     namespace: "my-ns",
//     topic: "my-topic"
//     subscription: "1"
// }
class Consumer {
    constructor(opts, logger) {
        this.opts = opts;
        this.wsUrl = `ws://${opts.host}:${opts.port}/ws/consumer/persistent/${opts.tenant}/${opts.cluster || 'default'}/${opts.namespace}/${opts.topic}/${opts.subscription}`;
        this.logger = logger != null ? logger : emptyLogger;
        this.websocket = new WebSocket(this.wsUrl);
        this.logger.info(`WebSocket consumer initialized with url: ${this.wsUrl}`);
    }

    listen(cb) {
        const inst = this;
        const topic = this.opts.topic
        this.websocket.on('open', function() {
            inst.logger.info(`WebSocket consumer opened for topic: ${topic}`);
        });

        this.websocket.on('message', function(message) {
            const messageObj = JSON.parse(message);
            inst.logger.debug(`WebSocket consumer received message: ${JSON.stringify(messageObj, null, '\t')}`);
            inst._ack(messageObj);
            cb(messageObj);
        });
    }

    stop() {
        this.websocket.terminate();
    }

    _ack(msgObj) {
        const ackMsg = {"messageId" : msgObj.messageId};
        this.logger.debug(`consumer sending ack message to topic: ${JSON.stringify(ackMsg, null, '\t')}`);
        this.websocket.send(JSON.stringify(ackMsg));
    }
}

module.exports = Consumer;
