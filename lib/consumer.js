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
//     subscription: "1",
//     reconnect: true,
// }
class Consumer {
    constructor(opts, logger) {
        this.opts = opts;
        this.wsUrl = `ws://${opts.host}:${opts.port}/ws/consumer/persistent/${opts.tenant}/${opts.cluster || 'default'}/${opts.namespace}/${opts.topic}/${opts.subscription}`;
        this.logger = logger != null ? logger : emptyLogger;
    }

    listen(cb) {
        const inst = this;
        const {topic, reconnect} = this.opts;

        this.websocket = new WebSocket(this.wsUrl);
        this.logger.info(`WebSocket consumer initialized with url: ${this.wsUrl}`);

        this.websocket.on('open', function() {
            inst.logger.info(`WebSocket consumer opened for topic: ${topic}`);
        });

        this.websocket.on('close', function() {
            inst.logger.info(`WebSocket consumer closed`);
            if (reconnect) {
                inst.reconnect(cb);
            }
        })

        this.websocket.on('error', function(err) {
            inst.logger.warn(`WebSocket consumer failed: ${err.message}`);
            if (reconnect) {
                inst.reconnect(cb);
            }
        })

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

    reconnect(cb) {
        let inst = this;
        if (this.timer != null) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        this.timer = setTimeout(() => {
            inst.logger.info(`WebSocket consumer trying to reconnect`);
            inst.stop();
            inst.listen(cb);
        }, 1000)
    }

    _ack(msgObj) {
        const ackMsg = {"messageId" : msgObj.messageId};
        this.logger.debug(`consumer sending ack message to topic: ${JSON.stringify(ackMsg, null, '\t')}`);
        this.websocket.send(JSON.stringify(ackMsg));
    }
}

module.exports = Consumer;
