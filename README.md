# Kafka Node.js Handler 🚀

A lightweight and easy-to-use Kafka handler for Node.js applications. This package provides a simple interface for setting up Kafka consumers and producers, making it easy to integrate Kafka messaging into your Node.js projects.

## ✨ Features

- 🔧 Easy setup with a single line of code
- 🔄 Automatic reconnection handling
- 📡 Event-based consumer with customizable message handling
- 📤 Simple producer interface for sending messages
- 🛑 Graceful shutdown handling

## 📦 Installation

```bash
npm install kafka-node-handler
```

## 🚀 Usage

### Initializing Kafka

To set up the Kafka handler, use the `initializeKafka` function:

```javascript
import { initializeKafka } from 'kafka-node-handler';

initializeKafka({
  topic: process.env.TOPIC,
  groupId: process.env.GROUP_ID,
  brokers: [process.env.KAFKA_BROKER_ADDRESS],
  onNewMessage: handleNewMessage,
});

function handleNewMessage({ routingKey, event }) {
  console.log(`Received message with key ${routingKey}:`, event);
  // Process the message here
}
```

### Sending Messages 📨

To send messages, use the `sendMessage` function:

```javascript
import { sendMessage } from 'kafka-node-handler';

await sendMessage('messageKey', { your: 'message', data: 'here' });
```

## ⚙️ Configuration

The `initializeKafka` function accepts a configuration object with the following properties:

- `topic`: The Kafka topic to subscribe to (required)
- `groupId`: The consumer group ID (required)
- `brokers`: An array of Kafka broker addresses (required)
- `onNewMessage`: A callback function to handle new messages (required)

## 🐛 Error Handling

The package includes built-in error handling and logging. Check your console for any error messages or warnings.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License.