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
//     subscription: "1",
//     token: "JWT token", // optional, only supports JWT token authentication and authorization for now
//     params: {
//         ...other query params
//     }
// }
class Consumer {
    constructor(opts, logger) {
        this.opts = opts;
        let params = queryString.stringify(opts.params || {});
        this.wsUrl = `${opts.secure ? 'wss' : 'ws'}://${opts.host}:${opts.port}/ws/v2/consumer/${opts.persistent ? "persistent" : "non-persistent"}/${opts.tenant}/${opts.namespace}/${opts.topic}/${opts.subscription}?${params}`;
        this.logger = logger != null ? logger : console;
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
            inst.openResolve()
        };

        this.onClose = function() {
            inst.logger.info(`WebSocket consumer closed`);
            if (inst.realClose == true) {
                inst.websocket.onclose = null;
                inst.closeResolve()
            } else if (reconnect) {
                inst.reconnect(cb).catch(err => {
                    inst.logger.info(`WebSocket consumer fail on reconnect: ${err}`)
                });
            }
        };

        this.onError = function(err) {
            inst.logger.warn(`WebSocket consumer failed: ${err.message}`);
            if (reconnect) {
                inst.reconnect(cb).catch(err => {
                    inst.logger.info(`WebSocket consumer fail on reconnect: ${err}`)
                });
            }
        };

        this.onMessage = function(message) {
            const messageObj = JSON.parse(message.data);
            inst.logger.debug(`WebSocket consumer received message: ${JSON.stringify(messageObj, null, '\t')}`);
            inst._ack(messageObj);
            if (inst.readResolve) {
                inst.readResolve(messageObj)
            }
            if (cb) {
                cb(messageObj);
            }
        };
    }

    read() {
        this.logger.debug('waiting for message...')
        let inst = this;
        return new Promise((resolve, reject) => {
            inst.readResolve = resolve
        })
    }


    listen(cb) {
        let inst = this;
        let openResolver = (resolve, reject) => {
            inst.openResolve = resolve;
            inst.openReject = reject;
        }
        let closeResolver = (resolve, reject) => {
            inst.closeResolve = resolve;
            inst.closeReject = reject;
        }
        this.openPromise = new Promise(openResolver);
        this.closePromise = new Promise(closeResolver)
        this._setupListeners(cb);
        this.websocket = new WebSocket(this.wsUrl, '', this.wsOpts);
        this.logger.info(`WebSocket consumer initialized with url: ${this.wsUrl}`);

        this.websocket.onopen = this.onOpen;
        this.websocket.onclose = this.onClose;
        this.websocket.onerror = this.onError;
        this.websocket.onmessage = this.onMessage;
        return this.openPromise;
    }

    stop() {
        this.realClose = true;
        this.websocket.onopen = null
        this.websocket.onerror = null
        this.websocket.onmessage = null
        this.websocket.close();
        return this.closePromise;
    }

    reconnect(cb) {
        let inst = this;
        if (this.timer != null) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        return new Promise((resolve, reject) => {
            this.timer = setTimeout(async () => {
                inst.logger.info(`WebSocket consumer trying to reconnect`);
                inst.stop().then(inst.listen(cb)).then(resolve).catch(reject)
            }, 1000)
        })
    }

    _ack(msgObj) {
        const ackMsg = {"messageId" : msgObj.messageId};
        this.logger.debug(`consumer sending ack message to topic: ${JSON.stringify(ackMsg, null, '\t')}`);
        this.websocket.send(JSON.stringify(ackMsg));
    }
}

module.exports = Consumer;
