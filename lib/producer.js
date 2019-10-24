'use strict'
const WebSocket = require('ws');

// example opts
// {
//     host: "localhost",
//     port: 8080,
//     tenant: "my-tenant",
//     cluster: "us-east-1",
//     namespace: "my-ns",
//     topic: "my-topic"
// }
class Producer {
    constructor(opts, logger) {
        this.opts = opts;
        this.wsUrl = `ws://${opts.host}:${opts.port}/ws/producer/persistent/${opts.tenant}/${opts.cluster || 'default'}/${opts.namespace}/${opts.topic}`;
        this.logger = logger;
        this.websocket = new WebSocket(this.wsUrl);
        logger.info(`WebSocket producer initialized with url: ${this.wsUrl}`);
    }

    start(cb) {
        const inst = this;
        const topic = this.opts.topic
        this.websocket.on('open', function() {
            inst.logger.info(`WebSocket producer opened for topic: ${topic}`);
            cb();
        });

        this.websocket.on('message', function(message) {
            inst.logger.debug(`WebSocket producer recevied ack message on topic: ${message}`);
        });
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
