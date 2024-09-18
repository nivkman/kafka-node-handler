import { setTimeout } from "timers/promises";
export default class ConsumerService {
  constructor(kafka, config) {
    this._config = config;
    this._kafka = kafka;
    this._consumer = null;
  }

  async initialize() {
    this._consumer = this._kafka.consumer({
      groupId: this._config.groupId,
      sessionTimeout: 7000,
      heartbeatInterval: 3000,
    });
    await this.connect();
    this.setupEventListeners();
    await this.onNewMessage();
    this.closeConnection();
  }

  async connect() {
    try {
      await this._consumer.connect();
      await this._consumer.subscribe({
        topic: this._config.topic,
      });
    } catch (error) {
      console.error("[kafka] Failed to connect consumer:", error);
      throw error;
    }
  }

  async onNewMessage() {
    try {
      await this._consumer.run({
        eachMessage: async ({ message }) => {
          try {
            await this._config.onNewMessage({
              routingKey: message.key.toString(),
              event: JSON.parse(message.value.toString()),
            });
          } catch (error) {
            console.error("[kafka] Error processing message:", error);
          }
        },
        onRevoke: async (assignments) => {
          console.log("[kafka] Partitions revoked:", assignments);
        },
      });
      console.log("[kafka] Consumer started running");
    } catch (error) {
      console.error("[kafka] Failed to start consumer:", error);
      throw error;
    }
  }

  setupEventListeners() {
    if (this._consumer?.events) {
      this._consumer.on(this._consumer.events.GROUP_JOIN, () => {
        console.log(`[kafka] Consumer joined group`);
      });

      this._consumer.on(
        this._consumer.events.CRASH,
        ({ payload: { error } }) => {
          console.error(`[kafka] Consumer crashed:`, error);
          this.reconnect();
        }
      );

      this._consumer.on(this._consumer.events.CONNECT, () => {
        console.log("[kafka] Consumer connected successfully");
      });

      this._consumer.on(this._consumer.events.DISCONNECT, () => {
        console.log("[kafka] Consumer disconnected");
      });

      this._consumer.on(
        this._consumer.events.RECEIVED_UNSUBSCRIBED_TOPICS,
        ({ payload }) => {
          console.log("[kafka] Received unsubscribed topics:", payload);
        }
      );
    }
  }

  async reconnect(attempt = 1) {
    const maxAttempts = 5;
    const delay = Math.min(100 * Math.pow(2, attempt), 30000);

    try {
      await this._consumer.disconnect();
      console.log("[kafka] Consumer disconnected for reconnection");
      await setTimeout(delay);
      await this.connect();
      console.log("[kafka] Consumer reconnected successfully");
    } catch (error) {
      console.error(
        `[kafka] Failed to reconnect consumer (attempt ${attempt}):`,
        error
      );
      if (attempt < maxAttempts) {
        await this.reconnect(attempt + 1);
      } else {
        console.error("[kafka] Max reconnection attempts reached. Exiting.");
        process.exit(1);
      }
    }
  }

  closeConnection() {
    const shutdown = async () => {
      try {
        console.log("[kafka] Shutting down consumer...");
        await this._consumer.stop();
        await this._consumer.disconnect();
        console.log("[kafka] Consumer disconnected gracefully");
      } catch (error) {
        console.error("[kafka] Error disconnecting consumer:", error);
      } finally {
        process.exit(0);
      }
    };

    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
  }
}
