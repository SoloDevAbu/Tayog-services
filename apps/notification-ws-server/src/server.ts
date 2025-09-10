import WebSocket, { WebSocketServer } from 'ws';
import Redis from 'ioredis';
import path from "path";
import dotenv from "dotenv";

dotenv.config({ path: path.resolve(__dirname, "../../../.env") });

const redis = new Redis(process.env.REDIS_URL!);
const wss = new WebSocketServer({ port: 8080 });

interface ClientMeta {
  socket: WebSocket;
  userId: string;
}

// Store connected users <userId, WebSocket[]>
const clients = new Map<string, Set<WebSocket>>();

function subscribeUser(userId: string) {
  // Each user gets their own Redis subscription
  const subscriber = new Redis(process.env.REDIS_URL!);
  subscriber.subscribe(`notification:${userId}`);
  subscriber.on('message', (_, message) => {
    const wsSet = clients.get(userId);
    if (wsSet) {
      wsSet.forEach(ws => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(message);
        }
      });
    }
  });
  return subscriber;
}

wss.on('connection', (ws, req) => {
  // Expects frontend to send authentication/userId when opening connection
  ws.on('message', (msg) => {
    try {
      const { type, userId } = JSON.parse(msg.toString());
      if (type === 'INIT' && userId) {
        // Init connection: subscribe to Redis channel
        if (!clients.has(userId)) clients.set(userId, new Set());
        clients.get(userId)!.add(ws);
        ws.subscriber = subscribeUser(userId); // Attach subscriber for cleanup
        ws.userId = userId;
      }
    } catch {}
  });

  ws.on('close', () => {
    if (ws.userId) {
      const wsSet = clients.get(ws.userId);
      wsSet?.delete(ws);
      if (ws.subscriber) ws.subscriber.quit();
    }
  });
});

console.log('WebSocket server started at ws://localhost:8080');
