const { createLogger, format, transports } = require('winston');
const Transport = require('winston-transport');

class emptyTransport extends Transport {
    constructor(opts) {
        super(opts);
    }

    log(info, callback) {

    }
};
const emptyLogger = createLogger({
    level: 'debug',
    format: format.simple(),
    transports: [new emptyTransport()]
});

module.exports = emptyLogger;
