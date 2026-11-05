import { Admin, Producer } from "@platformatic/kafka";
import type { Logger } from "winston";

import type { AppConfig } from "./config.js";
import type { ScanEvent, ScanPayload } from "./types.js";

/** Retained delivery failure. Replay originalEvent only after fixing its callback. */
export interface FailedCallback {
  schemaVersion: 1;
  id: string;
  failedAt: string;
  source: { topic: string; partition: number; offset: string };
  originalEvent: ScanEvent;
  result: ScanPayload;
  attempts: number;
}

/** Resolves only after Kafka acknowledges durable storage of a failed delivery. */
export interface DeadLetterPublisher {
  publish(record: FailedCallback): Promise<void>;
}

type ProducerClient = Pick<Producer, "send" | "close" | "on">;
type AdminClient = Pick<Admin, "listTopics" | "createTopics" | "close" | "on">;

/** Dedicated topic and all-replica acknowledgements keep failures replayable. */
export class KafkaDeadLetterPublisher implements DeadLetterPublisher {
  private readonly producer: ProducerClient;

  constructor(
    private readonly config: AppConfig,
    private readonly logger: Logger,
    producer?: ProducerClient,
    private readonly adminFactory: (
      options: ConstructorParameters<typeof Admin>[0],
    ) => AdminClient = (options) => new Admin(options),
  ) {
    this.producer =
      producer ??
      new Producer({
        ...this.connectionOptions(),
        acks: -1,
        autocreateTopics: false,
      });
    // Request promises carry delivery failures. Never log request objects or records.
    this.producer.on("error", () =>
      this.logger.error("Kafka dead-letter producer error"),
    );
  }

  private connectionOptions() {
    return {
      bootstrapBrokers: this.config.kafka.brokers,
      clientId: `${this.config.kafka.clientId}-callback-failures`,
      timeout: 15_000,
      retries: 3,
      ...(this.config.kafka.tls ? { tls: this.config.kafka.tls } : {}),
    };
  }

  /** Provisions the isolated topic before intake; failures prevent readiness. */
  async initialize(): Promise<void> {
    const admin = this.adminFactory(this.connectionOptions());
    admin.on("error", () =>
      this.logger.error("Kafka dead-letter topic initialization error"),
    );
    try {
      if (!(await admin.listTopics()).includes(this.config.deadLetter.topic)) {
        try {
          await admin.createTopics({
            topics: [this.config.deadLetter.topic],
            partitions: 1,
            replicas: this.config.deadLetter.replicas,
            configs: [
              { name: "cleanup.policy", value: "delete" },
              {
                name: "retention.ms",
                value: String(this.config.deadLetter.retentionMs),
              },
              {
                name: "min.insync.replicas",
                value: String(Math.min(2, this.config.deadLetter.replicas)),
              },
            ],
          });
        } catch (error) {
          // Another replica may have created the same topic during a rollout.
          if (
            !(await admin.listTopics()).includes(this.config.deadLetter.topic)
          ) {
            throw error;
          }
        }
      }
    } finally {
      await admin.close();
    }
  }

  async publish(record: FailedCallback): Promise<void> {
    const result = await this.producer.send({
      acks: -1,
      autocreateTopics: false,
      messages: [
        {
          topic: this.config.deadLetter.topic,
          key: Buffer.from(record.id),
          value: Buffer.from(JSON.stringify(record)),
        },
      ],
    });
    if (result.unwritableNodes?.length || !result.offsets?.length) {
      throw new Error("Kafka did not acknowledge the failed callback record");
    }
  }

  async close(): Promise<void> {
    await this.producer.close();
  }
}
