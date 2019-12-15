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

    _setupListeners(cb) {
        const inst = this;
        const {topic, reconnect} = this.opts;
        this.onOpen = function() {
            inst.logger.info(`WebSocket consumer opened for topic: ${topic}`);
        };

        this.onClose = function() {
            inst.logger.info(`WebSocket consumer closed`);
            if (reconnect) {
                inst.reconnect(cb);
            }
        };

        this.onError = function(err) {
            inst.logger.warn(`WebSocket consumer failed: ${err.message}`);
            if (reconnect) {
                inst.reconnect(cb);
            }
        };

        this.onMessage = function(message) {
            const messageObj = JSON.parse(message);
            inst.logger.debug(`WebSocket consumer received message: ${JSON.stringify(messageObj, null, '\t')}`);
            inst._ack(messageObj);
            cb(messageObj);
        };
    }

    listen(cb) {
        this._setupListeners(cb);
        this.websocket = new WebSocket(this.wsUrl);
        this.logger.info(`WebSocket consumer initialized with url: ${this.wsUrl}`);

        this.websocket.on('open', this.onOpen);
        this.websocket.on('close', this.onClose);
        this.websocket.on('error', this.onError);
        this.websocket.on('message', this.onMessage);
    }

    stop() {
        this.websocket.removeListener('open', this.onOpen);
        this.websocket.removeListener('close', this.onClose);
        this.websocket.removeListener('error', this.onError);
        this.websocket.removeListener('message', this.onMessage);
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
