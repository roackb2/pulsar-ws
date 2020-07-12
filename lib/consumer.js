'use strict'
const WebSocket = require('ws');
const querystring = require('querystring');
const emptyLogger = require('./logging');

// example opts
// {
//     persistent: true,
//     host: "localhost",
//     port: 8080,
//     secure: false,
//     tenant: "my-tenant",
//     namespace: "my-ns",
//     topic: "my-topic"
//     subscription: "1",
//     token: "JWT token", // optional, only supports JWT token authentication and authorization for now
//     reconnect: true,
//     params: {
//         ...other query params
//     }
// }
class Consumer {
    constructor(opts, logger) {
        this.opts = opts;
        let params = querystring.stringify(opts.params || {});
        this.wsUrl = `${otps.secure ? 'wss' : 'ws'}://${opts.host}:${opts.port}/ws/consumer/${opts.persistent ? "persistent" : "non-persistent"}/${opts.tenant}/${opts.cluster || 'default'}/${opts.namespace}/${opts.topic}/${opts.subscription}?${params}`;
        this.logger = logger != null ? logger : emptyLogger;
        this.wsOpts = null;
        if (opts.token) {
            this.wsOpts = {
                headers: {
                    Authorization: `Bearer ${opts.token}`
                }
            }
        }
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
            console.log(err)
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
        this.websocket = new WebSocket(this.wsUrl, this.wsOpts);
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
