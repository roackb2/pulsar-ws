'use strict';
const { Producer } = require('../index');

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
//     useNative: false, // set to true if you want to use native WebSocket in browser
//     params: {
//         ...other Producer endpoint query params
//     }
// }
const producer = new Producer({
    persistent: true,
    reconnect: true,
    host: "localhost",
    port: 8080,
    secure: false,
    tenant: "my-tenant",
    namespace: "my-ns",
    topic: "my-topic",
    token: "JWT token", // optional, only supports JWT token authentication and authorization for now
    params: {
        maxPendingMessages: 65535
    }
}, console)

// Creates a message that Pulsar WebSocket could accept
function createMessage(msg) {
    return {
        "payload" : Buffer.from(msg).toString('base64'), // Pulsar requires payload to be hex or base64 encoding
        "properties": {
            "key1" : "value1",
        },
        "context" : "1"
    };
}

async function run() {
    await producer.start();
    console.log("producer started")
    // send a hellow world message
    let msg = createMessage("Hello World!")
    let res = await producer.send(msg);
    // send an end message
    msg = createMessage("end");
    res = await producer.send(msg)
    await producer.stop()
    console.log("producer stopped")
}

async function main() {
    try {
        run()
    } catch (err) {
        console.log(err)
    }
}

main()
