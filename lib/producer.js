'use strict'
const Promise = require('bluebird');
const WebSocket = require('isomorphic-ws');
const queryString = require('query-string');

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
//     token: "JWT token", // optional, only supports JWT token authentication and authorization for now
//     params: {
//         ...other query params
//     }
// }
class Producer {
    constructor(opts, logger) {
        this.opts = opts;
        let params = queryString.stringify(opts.params || {});
        this.wsUrl = `${opts.secure ? 'wss' : 'ws'}://${opts.host}:${opts.port}/ws/v2/producer/${opts.persistent ? "persistent" : "non-persistent"}/${opts.tenant}/${opts.namespace}/${opts.topic}?${params}`;
        this.logger = logger != null ? logger : console;
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
            inst.logger.info(`WebSocket producer opened for topic: ${topic}`);
            inst.openResolve()
            if (cb) {
                cb();
            }
        };

        this.onClose = function() {
            inst.logger.info(`WebSocket producer closed`);
            if (inst.realClose == true) {
                inst.websocket.onclose = null;
                inst.closeResolve()
            } else if (reconnect) {
                inst.reconnect(cb).catch(err => {
                    inst.logger.info(`WebSocket producer fail on reconnect: ${err}`)
                });
            }
        };

        this.onError = function(err) {
            inst.logger.warn(`WebSocket producer failed: ${err.message}`);
            if (reconnect) {
                inst.reconnect(cb).catch(err => {
                    inst.logger.info(`WebSocket producer fail on reconnect: ${err}`)
                });
            }
        };

        this.onMessage = function(message) {
            inst.logger.debug(`WebSocket producer recevied ack message on topic ${topic}: ${message}`);
            if (inst.sendResolve) {
                inst.sendResolve(message)
            }
        };
    }

    start(cb) {
        let inst = this;
        let openResolver = (resolve, reject) => {
            inst.openResolve = resolve;
            inst.openReject = reject;
        }
        let closeResolver = (resolve, reject) => {
            inst.closeResolve = resolve;
            inst.closeReject = reject;
        }
        this.realClose = false;
        this.openPromise = new Promise(openResolver);
        this.closePromise = new Promise(closeResolver)
        this._setupListeners(cb);
        this.websocket = new WebSocket(this.wsUrl, this.wsOpts);

        this.logger.info(`WebSocket producer initialized with url: ${this.wsUrl}`);

        this.websocket.onopen = this.onOpen;
        this.websocket.onclose = this.onClose;
        this.websocket.onerror = this.onError
        this.websocket.onmessage = this.onMessage;
        return this.openPromise;
    }

     reconnect(cb) {
        let inst = this;
        if (this.timer != null) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        return new Promise((resolve, reject) => {
            this.timer = setTimeout(async () => {
                inst.logger.info(`WebSocket producer trying to reconnect`);
                inst.stop().then(inst.start(cb)).then(resolve).catch(reject)
            }, 1000)
        })
    }

    stop() {
        this.realClose = true;
        this.websocket.onopen = null;
        this.websocket.onerror = null;
        this.websocket.onmessage = null;
        this.websocket.close();
        return this.closePromise;
    }

    send(message, ignoreResult = false) {
        let inst = this;
        let promise = new Promise((resolve, reject) => {
            inst.sendResolve = resolve,
            inst.sendReject = reject;
        })
        const topic = this.opts.topic
        this.logger.debug(`WebSocket producer sending message to topic ${topic}: ${JSON.stringify(message, null, '\t')}`)
        if (this.websocket.readyState === WebSocket.OPEN) {
            this.websocket.send(JSON.stringify(message));
        } else {
            this.logger.warn(`WebSocket not ready`)
        }
        if (ignoreResult) {
            return null
        } else {
            return promise
        }
    }
}

module.exports = Producer;
