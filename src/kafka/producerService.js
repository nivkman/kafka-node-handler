export default class ProducerService {
  constructor(kafka, config) {
    this._config = config;
    this._producer = kafka.producer();
    this.connect();
    this.closeConnection();
  }

  async connect() {
    await this._producer.connect();
    console.log("[kafka] producer connected successfully");
  }

  async sendMessage(key, msg) {
    try {
      return this._producer
        .send({
          topic: this._config.topic,
          messages: [
            {
              key: key,
              value: JSON.stringify(msg),
            },
          ],
        })

        .catch((e) => {
          console.error(`[kafka] Unable to send message: ${e.message}`, e);
        });
    } catch (e) {
      console.error(`[kafka] Unable to send message: ${e.message}`, e);
    }
  }

  async closeConnection() {
    process.once("SIGINT", async () => {
      try {
        await this._producer.disconnect();
        console.log("[kafka] Producer disconnected");
      } finally {
        process.kill(process.pid, "SIGINT");
      }
    });
  }
}
