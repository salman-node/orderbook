// const redis = require("redis");
// const WebSocket = require("ws");

// const redisClient = redis.createClient({
//   username: process.env.REDIS_USER || "default",
//   password: process.env.REDIS_AUTH || "blE32GqYBT9dHDyopO1tiG10AKOuW0C8",
//   socket: {
//     host: process.env.REDIS_HOST || "redis-18514.c322.us-east-1-2.ec2.redns.redis-cloud.com",
//     port: process.env.REDIS_PORT || 18514,
//   },
// });

// const subscriber = redis.createClient({
//   username: process.env.REDIS_USER || "default",
//   password: process.env.REDIS_AUTH || "blE32GqYBT9dHDyopO1tiG10AKOuW0C8",
//   socket: {
//     host: process.env.REDIS_HOST || "redis-18514.c322.us-east-1-2.ec2.redns.redis-cloud.com",
//     port: process.env.REDIS_PORT || 18514,
//   },
// });

// const wss = new WebSocket.Server({ port: 8080 });
// const clientSubscriptions = new Map(); // { pairId: [ws1, ws2] }

// (async () => {
//   await redisClient.connect();
//   await subscriber.connect();
//   console.log("Connected to Redis and listening for pub/sub messages...");
// })();

// // WebSocket Server
// wss.on("connection", (ws) => {
//   console.log("New WebSocket client connected");

//   ws.send(JSON.stringify({ message: "Connected. Send { pairId: <id> } to subscribe." }));

//   ws.on("message", async (message) => {
//     try {
//       const { pairId } = JSON.parse(message);
//       if (!pairId) return;

//       subscribeClientToPair(ws, pairId);
//     } catch (error) {
//       console.error("Error parsing message:", error);
//     }
//   });

//   ws.on("close", () => {
//     console.log("Client disconnected");
//     removeClientFromAllSubscriptions(ws);
//   });
// });

// // Subscribe client to a pairId
// function subscribeClientToPair(ws, pairId) {
//   if (!clientSubscriptions.has(pairId)) {
//     clientSubscriptions.set(pairId, []);
//     subscribeToPairIdChannels(pairId); // 🔹 Start Redis subscription for new pairId
//   }

//   clientSubscriptions.get(pairId).push(ws);
//   console.log(`Client subscribed to pairId: ${pairId}`);
// }

// // Remove disconnected clients
// function removeClientFromAllSubscriptions(ws) {
//   clientSubscriptions.forEach((clients, pairId) => {
//     const updatedClients = clients.filter((client) => client !== ws);
//     if (updatedClients.length === 0) {
//       clientSubscriptions.delete(pairId); // Remove subscription if no clients
//     } else {
//       clientSubscriptions.set(pairId, updatedClients);
//     }
//   });
// }

// // Redis Subscription Logic
// async function subscribeToPairIdChannels(pairId) {
//   console.log(`Subscribing to Redis channels for pairId: ${pairId}`);

//   await subscriber.subscribe(`${pairId}:ASK`, (message) => {
//     console.log(`ASK updated for pairId: ${pairId}`);
//     console.log(`ASK updated for pairId: ${message}`);
//     broadcastUpdate(pairId, { type: "ASK_UPDATE", data: JSON.parse(message) });
//   });

//   await subscriber.subscribe(`${pairId}:BID`, (message) => {
//     console.log(`BID updated for pairId: ${pairId}`);
//     console.log(`BID updated for pairId: ${message}`);
//     broadcastUpdate(pairId, { type: "BID_UPDATE", data: JSON.parse(message) });
//   });

//   await subscriber.subscribe(`${pairId}:trade_history`, (message) => {
//     console.log(`TRADE_HISTORY updated for pairId: ${pairId}`);
//     console.log(`TRADE_HISTORY updated for pairId: ${message}`);
//     broadcastUpdate(pairId, { type: "TRADE_HISTORY", data: JSON.parse(message) });
//   });
// }

// // Broadcast updates to relevant WebSocket clients
// function broadcastUpdate(pairId, update) {
//   if (!clientSubscriptions.has(pairId)) return;

//   clientSubscriptions.get(pairId).forEach((client) => {
//     if (client.readyState === WebSocket.OPEN) {
//       client.send(JSON.stringify(update));
//     }
//   });
// }









import redis from "redis";
import {WebSocketServer } from "ws";
import { raw_query} from './db_query.js';
import Big from 'big.js';

const redisConfig = {
  username: process.env.REDIS_USER || "default",
  password: process.env.REDIS_AUTH || "blE32GqYBT9dHDyopO1tiG10AKOuW0C8",
  socket: {
    host: process.env.REDIS_HOST || "redis-18514.c322.us-east-1-2.ec2.redns.redis-cloud.com",
    port: process.env.REDIS_PORT || 18514,
  },
}

const redisClient = redis.createClient(redisConfig);

const subscriber = redis.createClient(redisConfig);

const wss = new WebSocketServer({ port: 8080 });
const clientSubscriptions = new Map(); // Stores client subscriptions

(async () => {
  await redisClient.connect();
  await subscriber.connect();
  console.log("Connected to Redis and listening for changes...");
})();

// WebSocket Server
wss.on("connection", (ws) => {
  console.log("New WebSocket client connected");

  ws.send(JSON.stringify({ message: "Connected to order book updates. Send { pairId: <id> } to subscribe." }));

  ws.on("message", async (message) => {
    try {
      console.log("Received message:", message.toString());
      const { pairId } = JSON.parse(message);
      if (!pairId) return;

      // Subscribe the client to the given pairId
      console.log(`Client subscribing to pairId: ${pairId}`);
      subscribeClientToPair(ws, pairId);
    } catch (error) {
      console.error("Error parsing message:", error);
    }
  });

  ws.on("close", () => {
    console.log("Client disconnected");
    // Remove client from all subscriptions when they disconnect
    clientSubscriptions.forEach((clients, pairId) => {
      clientSubscriptions.set(
        pairId,
        clients.filter((client) => client !== ws)
      );
    });
  });
});

const activeSubscriptions = new Set(); // Stores active subscriptions

// Subscribe client to a pairId
function subscribeClientToPair(ws, pairId) {
  const askKey = `${pairId}:ASK`;
  const bidKey = `${pairId}:BID`;
  const tradeHistoryKey = `${pairId}:trade_history`;

  // Add client to the subscription list
  if (!clientSubscriptions.has(pairId)) {
    clientSubscriptions.set(pairId, []);
  }
  clientSubscriptions.get(pairId).push(ws);

  // Subscribe to Redis events only if this is the first subscriber
  if (clientSubscriptions.get(pairId).length === 1) {
    subscribeToRedis(pairId, askKey, "ASK_UPDATE");
    subscribeToRedis(pairId, bidKey, "BID_UPDATE");
    subscribeToTradeHistory(pairId, tradeHistoryKey);
  }
  console.log(`Client subscribed to pairId: ${pairId}`);
}

// Subscribe to Redis keyspace events
async function subscribeToRedis(pairId, key, updateType) {

    if (activeSubscriptions.has(key)) return;
    activeSubscriptions.add(key);

    await subscriber.subscribe(`__keyspace@0__:${key}`, async () => {
      // console.log(`${updateType} updated for pairId: ${pairId}`);
      
      let data;
      if (updateType === "ASK_UPDATE") {
        data = (await redisClient.zRange(key, 0, 4, { withScores: true })); // Top 5 ASKs
        
      } else if (updateType === "BID_UPDATE") {
        data = (await redisClient.zRange(key, -5, -1, { withScores: true })).reverse(); // Top 5 BIDs
      }
      
      let formattedData = data.map((item)=>{
        const order = JSON.parse(item);
        return { price: parseFloat(order.price), quantity: parseFloat(order.quantity) };
      })
   
      broadcastUpdate(pairId, { type: updateType, pairId, data: formattedData });
    });
  }


  

  // Subscribe to Redis keyspace events for trade history
  async function subscribeToTradeHistory(pairId, key) {
    if (activeSubscriptions.has(key)) return;
    activeSubscriptions.add(key);

    await subscriber.subscribe(`__keyspace@0__:${key}`, async () => {
        console.log(`TRADE_HISTORY updated for pairId: ${pairId}`);

        // Fetch last 5 trades from sorted set
        const tradeData = await redisClient.zRange(`${pairId}:trade_history`, -5, -1, { WITHSCORES: true });

        const tradeHistory = [];
        for (let i = 0; i < tradeData.length; i ++) {
            const parsedTrade = JSON.parse(tradeData[i]);
            tradeHistory.push({
                price: parsedTrade.price,
                qty: parsedTrade.qty,
                time:parsedTrade.timestamp, // Score (timestamp)
                type: parsedTrade.type
            });
        }

        broadcastUpdate(pairId, { type: "TRADE_HISTORY", pairId, data: tradeHistory });

        const now = Date.now();
        const oneDayAgo = now - 24 * 60 * 60 * 1000;

        // Fetch trades from the last 24 hours
        const marketData = await redisClient.zRangeByScore(`${pairId}:trade_history`, oneDayAgo, now, { WITHSCORES: true });

        const trades = [];
        for (let i = 0; i < marketData.length; i += 2) {
            const parsedTrade = JSON.parse(marketData[i]);
            trades.push({
                price: parsedTrade.price,
                qty: parsedTrade.qty,
                time: parseInt(marketData[i + 1]), // Score (timestamp)
                type: parsedTrade.type
            });
        }

        if (!trades.length) return;

        const v24h = trades.reduce((sum, trade) => sum.plus(new Big(trade.qty)), new Big(0));

        const currentPrice = trades[trades.length - 1].price;
        const low24h = Math.min(...trades.map(trade => trade.price));
        const high24h = Math.max(...trades.map(trade => trade.price));
        const volume24h = v24h.toString();
        const openingPrice = trades[0].price;
        const change24h = openingPrice ? ((currentPrice - openingPrice) / openingPrice * 100).toFixed(2) : "0.00";

        broadcastUpdate(pairId, { type: "MARKET_SUMMARY", pairId, data: { currentPrice, low24h, high24h, volume24h, change24h } });

        // Update database
        await raw_query(`UPDATE crypto_pair SET current_price = ${currentPrice}, change_in_price = ${change24h} WHERE id = ${pairId}`);
    });
}

  

// Broadcast updates to only relevant clients
function broadcastUpdate(pairId, update) {
  if (!clientSubscriptions.has(pairId)) return;

  clientSubscriptions.get(pairId).forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(update));
    }
  });
}


