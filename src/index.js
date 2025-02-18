import { WebSocketServer } from 'ws';
import dotenv from 'dotenv';
import { createClient } from 'redis';
import cluster from 'cluster';
import os from 'os';
import createMessageHandler from './handleMessage';
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'binance-producer',
  brokers: ['localhost:9092'],    // Adjust your Kafka broker address
  fromBeginning: false,
  retry: {
    retries: 0
  }
});

const producer = kafka.producer()

const connectKafka = async () => {
  console.log('connecting to kafka')
  await producer.connect();
};
connectKafka();

dotenv.config();

const numCPUs = os.cpus().length; // Number of CPU cores

if (cluster.isMaster) {
  console.log(`Master process started: PID ${process.pid}`);

  // Fork workers for each CPU core
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  // Restart workers if they die
  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died. Restarting...`);
    cluster.fork();
  });
} else {
(async () => {
  dotenv.config()

  const redisClient = createClient({
    username: process.env.REDIS_USER,
    password: process.env.REDIS_AUTH,
    socket: {
        host: process.env.REDIS_HOST,
        port: process.env.REDIS_PORT
    }
});
redisClient.on('error', (err) => console.log('Redis Client Error', err));

  await redisClient.connect()

  const wss = new WebSocketServer({
    host: '0.0.0.0',
    port: process.env.PORT || 5000,
    maxPayload: 1024 * 1024 // 1 MB max payload size
  });

  connectedUids = []
  wss.on('connection', (ws, req) => {
    const url = new URL(req.url, process.env.BASE_URL)
    const uid = url.searchParams.get('user')
   connectedUids.push(uid)
    const existingClient = Array.from(wss.clients).find(
      (sock) => sock.clientUid === uid
    )

    if (existingClient) {
      ws.send(`error: client with uid ${uid} is already connected`)
      ws.close()
      return
    }
    ws.clientUid = uid
    
    // console.log('clientUids ',clientUids);
    console.log(`connected:${uid}`)
   
    ws.on('message', async (message) => {
      const reply = await createMessageHandler(uid,connectedUids,redisClient)(message.toString())
      if (reply) {
        ws.send(JSON.stringify(reply))

        if (reply.type === 'match') {
          const client = Array.from(wss.clients).find(
            (sock) => sock.clientUid === reply.data.matchedBy
          )
          if (client) {
            client.send(
              JSON.stringify({
                type: 'match',
                message: `Order matched`,
                data: {
                  matchedBy: uid,
                  matchedAt: reply.data.matchedAt,
                  yourOrder: reply.data.matchedOrder,
                  matchedOrder: reply.data.yourOrder,
                },
              })
            )
          } 
          // else {
          //   // console.log(`client lost: ${uid}`)
          // }
        }
      } 
    })

    ws.on('close', () => {
      console.log(`disconnected:${uid}`)
    })
  })
  console.log(`Worker process started: PID ${process.pid}`);
})()
}

const sendExecutionReportToKafka = async (topic, message) => {
  try {
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(message)}],
    });
    console.log(`Sent to Kafka topic: ${JSON.stringify(topic)}`);
  } catch (error) {
    console.error(`Error sending to Kafka topic ${topic}:`, error);
  }
};
export default sendExecutionReportToKafka 





