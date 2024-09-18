import { Kafka, logLevel } from "kafkajs";
import ConsumerService from "./consumerService.js";
import ProducerService from "./producerService.js";

export class SetupKafkaService {
  constructor() {}

  async initialize(config) {
    const kafka = new Kafka({
      brokers: config.brokers,
      logLevel: logLevel.ERROR,
    });
    SetupKafkaService.producer = new ProducerService(kafka, config);
    new ConsumerService(kafka, config).initialize();
  }

  async sendMessage(key, message) {
    if (!SetupKafkaService.producer) {
      throw new Error("[kafka] Producer is not initialized");
    }

    try {
      await SetupKafkaService.producer.sendMessage(key, message);
      console.log("[kafka] Message sent successfully:", key);
    } catch (error) {
      console.error("[kafka] Failed to send message:", error);
    }
  }
}

const setupKafkaService = new SetupKafkaService();

export const initializeKafka = (config) => setupKafkaService.initialize(config);
export const sendMessage = (key, message) => setupKafkaService.sendMessage(key, message);
