import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  Message,
} from "@aws-sdk/client-sqs";
import Redis from "ioredis";
import { prisma } from "@tayog/db";
import path from "path";
import dotenv from "dotenv";

dotenv.config({ path: path.resolve(__dirname, "../../../.env") });

const redis = new Redis(process.env.REDIS_URL!);

const SQS_QUEUE_URL = process.env.SQS_NOTIFICATION_QUEUE_URL!;
const sqs = new SQSClient({
  region: process.env.AWS_REGION! || "ap-south-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  },
});

async function receiveMessages() {
  const response = await sqs.send(
    new ReceiveMessageCommand({
      QueueUrl: SQS_QUEUE_URL,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 20,
    })
  );
  return response.Messages ?? [];
}

async function deleteMessage(receiptHandle: string) {
  await sqs.send(
    new DeleteMessageCommand({
      QueueUrl: SQS_QUEUE_URL,
      ReceiptHandle: receiptHandle,
    })
  );
}

function generateMessage(message: any) {
  const title = message.postTitle || "your post";
  const truncatedTitle =
    title.length > 50 ? title.substring(0, 47) + "..." : title;

  switch (message.type) {
    case "POST_LIKE":
      return `Someone liked your post "${truncatedTitle}"`;
    case "POST_COMMENT":
      return `Someone commented on your post "${truncatedTitle}"`;
    default:
      return "You have a new notification";
  }
}

async function processMessage(message: Message) {
  if (!message.Body) return;

  const outer = JSON.parse(message.Body);
  console.log("Outer SQS message: ", outer)
  const payload = JSON.parse(outer.Message);
  console.log("Inner SQS message: ", payload)
  const { eventId, targetUserId, metadata, createdAt, type, postId, postType } =
    payload;
  const triggeredById = payload.metadata?.triggeredById || payload.userId;
  console.log("SQS payload: ", payload)
  try {
    await prisma.notification.create({
      data: {
        user: { connect: { id: targetUserId } },
        type: "LIKE",
        message: generateMessage(payload),
        entityId: null,
        entityType: payload.triggeredByType ?? null,
        createdAt: new Date(payload.createdAt || Date.now()),
        triggeredBy: triggeredById
          ? { connect: { id: triggeredById } }
          : undefined,
        isRead: false,
      },
    });

    await redis.publish(
      `notification:${targetUserId}`,
      JSON.stringify({
        eventId,
        type,
        metadata,
        createdAt,
        postId,
        postType,
        ...payload,
      })
    );

    if (message.ReceiptHandle) {
      await deleteMessage(message.ReceiptHandle);
    }
  } catch (error) {
    console.error("Worker process error:", error);
  }
}

async function loop() {
  while (true) {
    try {
      const messages = await receiveMessages();
      await Promise.all(messages.map((m) => processMessage(m)));
      console.log(`Processed ${messages.length} messages`);
    } catch (err) {
      console.error("Worker loop error:", err);
      await new Promise((res) => setTimeout(res, 5000));
    }
  }
}

loop();
