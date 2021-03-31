'use strict';
const { Consumer } = require('../index');

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
//         ...other Consumer endpoint query params
//     }
// }
const consumer = new Consumer({
    persistent: true,
    reconnect: true,
    host: "localhost",
    port: 8080,
    secure: false,
    tenant: "my-tenant",
    namespace: "my-ns",
    topic: "my-topic",
    subscription: "1",
    token: "JWT token", // optional, only supports JWT token authentication and authorization for now
    params: {
        receiverQueueSize: 65535
    }
}, console)

function handleMessage(message) {
    let payload = Buffer.from(message.payload, 'base64')
    return payload
}

async function run() {
    // you could either use callback to receive message (fired with every new incoming message)
    await consumer.listen(message => {
        let content = handleMessage(message)
        console.log(`recevied message in callback: ${content}`);
    })
    let content = null;
    while (content != 'end') {
        // or read message with await (single read, you might want to put this in a while loop to keep reading message)
        let message = await consumer.read();
        content = handleMessage(message)
        console.log(`recevied message in single read: ${content}`);
    }
    await consumer.stop()
    console.log("consumer stopped")
}

async function main() {
    try {
        run()
    } catch (err) {
        console.log(err)
    }
}

main()
