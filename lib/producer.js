'use strict'
const WebSocket = require('ws');
const querystring = require('querystring');
const emptyLogger = require('./logging');

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
//         ...other query params
//     }
// }
class Producer {
    constructor(opts, logger) {
        this.opts = opts;
        let params = querystring.stringify(opts.params || {});
        this.wsUrl = `ws://${opts.host}:${opts.port}/ws/producer/${opts.persistent ? "persistent" : "non-persistent"}/${opts.tenant}/${opts.cluster || 'default'}/${opts.namespace}/${opts.topic}?${params}`;
        this.logger = logger != null ? logger : emptyLogger;
    }

    _setupListeners(cb) {
        const inst = this;
        const {topic, reconnect} = this.opts;

        this.onOpen = function() {
            inst.logger.info(`WebSocket producer opened for topic: ${topic}`);
            if (cb) {
                cb();
            }
        };

        this.onClose = function() {
            inst.logger.info(`WebSocket producer closed`);
            if (reconnect) {
                inst.reconnect(cb);
            }
        };

        this.onError = function(err) {
            inst.logger.warn(`WebSocket producer failed: ${err.message}`);
            if (reconnect) {
                inst.reconnect(cb);
            }
        };

        this.onMessage = function(message) {
            inst.logger.debug(`WebSocket producer recevied ack message on topic: ${message}`);
        };
    }

    start(cb) {
        this._setupListeners(cb);
        this.websocket = new WebSocket(this.wsUrl);
        this.logger.info(`WebSocket producer initialized with url: ${this.wsUrl}`);

        this.websocket.on('open', this.onOpen);
        this.websocket.on('close', this.onClose);
        this.websocket.on('error', this.onError);
        this.websocket.on('message', this.onMessage);
    }

    reconnect(cb) {
        let inst = this;
        if (this.timer != null) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        this.timer = setTimeout(() => {
            inst.logger.info(`WebSocket producer trying to reconnect`);
            inst.stop();
            inst.start(cb);
        }, 1000)
    }

    stop() {
        this.websocket.removeListener('open', this.onOpen);
        this.websocket.removeListener('close', this.onClose);
        this.websocket.removeListener('error', this.onError);
        this.websocket.removeListener('message', this.onMessage);
        this.websocket.terminate();
    }

    send(message) {
        const topic = this.opts.topic
        this.logger.debug(`WebSocket producer sending message to topic ${topic}: ${JSON.stringify(message, null, '\t')}`)
        if (this.websocket.readyState === WebSocket.OPEN) {
            this.websocket.send(JSON.stringify(message));
        } else {
            this.logger.warn(`WebSocket not ready`)
        }
    }
}

module.exports = Producer;
