const { Kafka } = require('kafkajs')
const fs = require('fs')
const assert = require('assert')

function delay(time) {
    return new Promise(resolve => setTimeout(resolve, time));
}

const run = async () => {
    const args = process.argv.slice(2);
    const config = args[0];

    const kafka = new Kafka(eval(config))

    // Producing
    const producer = kafka.producer()
    await producer.connect()
    await producer.send({
        topic: 'test',
        messages: [
            { value: 'foo' },
        ],
    })
    await producer.send({
        topic: 'test',
        messages: [
            { value: 'a longer string' },
        ],
    })
    await producer.disconnect()

    // Consuming
    const consumer = kafka.consumer({ groupId: 'test-group' })
    await consumer.connect()
    await consumer.subscribe({ topic: 'test', fromBeginning: true })

    messages = []
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            messages.push({
                topic,
                partition,
                offset: message.offset,
                value: message.value.toString(),
            })
        },
    })

    // Use a very primitive sleep loop since nodejs doesnt seem to have any kind of mpsc or channel functionality :/
    while (messages.length < 2) {
        await delay(10);
    }
    assert.deepStrictEqual(messages, [
        {
            topic: 'test',
            partition: 0,
            offset: '0',
            value: 'foo',
        },
        {
            topic: 'test',
            partition: 0,
            offset: '1',
            value: 'a longer string',
        }
    ])
    await consumer.disconnect()
}

run()