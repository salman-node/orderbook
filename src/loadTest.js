// const axios = require('axios');

// const NUM_ORDERS = 2; // Total number of orders to place
// const CONCURRENCY = 1; // Number of concurrent requests
// const URL = 'http://localhost:9696/place-order';

// const axiosOptions = {
//   headers: {
//     'Content-Type': 'application/json',
//   },
// };

// // Function to generate random orders
// const generateRandomOrder = (side, symbol) => {
//   const uid = side === 0 ? 'alice' : 'bob'; // Assign Alice to Buy orders and Bob to Sell orders
//   const quantity = side === 0 ? 10 : 5
//   return {
//     uid,
//     side, // 0 for Buy, 1 for Sell
//     symbol,
//     price: 91000, // Random price between 90000 and 92000
//     quantity: quantity, // Random quantity between 1 and 10
//   };
// };

// // Function to place an order
// const placeOrder = async (order) => {
//   try {
//     const response = await axios.post(URL, order, axiosOptions);
//     return response.data;
//   } catch (error) {
//     console.error(`Error placing order: ${JSON.stringify(order)}`, error.message);
//     return null;
//   }
// };

// // Function to run performance test
// const performanceTest = async () => {
//   console.time('Total Execution Time');

//   const orders = [];

//   // Generate random buy and sell orders
//   for (let i = 0; i < NUM_ORDERS / 2; i++) {
//     // orders.push(generateRandomOrder(0, 'BTC/USD')); // Buy order
//     orders.push(generateRandomOrder(1, 'BTC/USD')); // Sell order
//   }

//   let completed = 0;
//   const results = [];

//   // Function to process orders in batches with concurrency
//   const processBatch = async (batch) => {
//     const promises = batch.map((order) =>
//       placeOrder(order).then((result) => {
//         completed++;
//         if (result) results.push(result);
//         process.stdout.write(`\rProcessed: ${completed}/${orders.length}`); // Progress tracking
//       })
//     );
//     await Promise.all(promises);
//   };

//   // Split orders into batches based on concurrency level
//   for (let i = 0; i < orders.length; i += CONCURRENCY) {
//     const batch = orders.slice(i, i + CONCURRENCY);
//     await processBatch(batch);
//   }

//   console.timeEnd('Total Execution Time');

//   // Log the results summary
//   console.log(`\nTotal Orders Placed: ${NUM_ORDERS}`);
//   console.log(`Matched Orders: ${results.length}`);
//   console.log(`Unmatched Orders: ${NUM_ORDERS - results.length}`);
// };

// console.log('Starting load test...');

// // Start the performance test
// performanceTest().catch((err) => console.error('Performance Test Failed:', err));

import axios from 'axios';
import { createClient } from 'redis';

const numOrders = 5; // Number of orders to place
const concurrency = 0; // Number of concurrent requests
const url = 'http://localhost:9696/place-order';

  const redisClient = createClient({
    username: 'default',
    password: 'blE32GqYBT9dHDyopO1tiG10AKOuW0C8',
    socket: {
        host: 'redis-18514.c322.us-east-1-2.ec2.redns.redis-cloud.com',
        port: 18514
    }
});
redisClient.on('error', (err) => console.log('Redis Client Error', err));

  await redisClient.connect()

// const buyOrders  = [
//   {
//     "uid": "bob",
//     "side": 0,
//     "symbol": "BTC/USD",
//     "price": 90080,
//     "quantity": 8.6,
//     "order_type": "limit"
//   }
//   // {
//   //   "uid": "bob",
//   //   "side": 0,
//   //   "symbol": "BTC/USD",
//   //   "price": 90070,
//   //   "quantity": 12,
//   //   "order_type": "limit"
//   // },
//   // {
//   //   "uid": "bob",
//   //   "side": 0,
//   //   "symbol": "BTC/USD",
//   //   "price": 90180,
//   //   "quantity": 3,
//   //   "order_type": "limit"
//   // },
//   // {
//   //   "uid": "bob",
//   //   "side": 0,
//   //   "symbol": "BTC/USD",
//   //   "price": 89997,
//   //   "quantity": 8,
//   //   "order_type": "limit"
//   // },
// //   {
// //     "uid": "bob",
// //     "side": 0,
// //     "symbol": "BTC/USD",
// //     "price": 90055,
// //     "quantity": 9,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "bob",
// //     "side": 0,
// //     "symbol": "BTC/USD",
// //     "price": 90035,
// //     "quantity": 1,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "bob",
// //     "side": 0,
// //     "symbol": "BTC/USD",
// //     "price": 90005,
// //     "quantity": 5,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "bob",
// //     "side": 0,
// //     "symbol": "BTC/USD",
// //     "price": 90080,
// //     "quantity": 7,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "bob",
// //     "side": 0,
// //     "symbol": "BTC/USD",
// //     "price": 90105,
// //     "quantity": 12,
// //     "order_type": "limit"
// //   }, 
// //   {
// //     "uid": "bob",
// //     "side": 0,
// //     "symbol": "BTC/USD",
// //     "price": 90119,
// //     "quantity": 18,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "bob",
// //     "side": 0,
// //     "symbol": "BTC/USD",
// //     "price": 90220,
// //     "quantity": 6,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "bob",
// //     "side": 0,
// //     "symbol": "BTC/USD",
// //     "price": 90009,
// //     "quantity": 5,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "bob",
// //     "side": 0,
// //     "symbol": "BTC/USD",
// //     "price": 90175,
// //     "quantity": 3,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "bob",
// //     "side": 0,
// //     "symbol": "BTC/USD",
// //     "price": 90162,
// //     "quantity": 2,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "bob",
// //     "side": 0,
// //     "symbol": "BTC/USD",
// //     "price": 90162,
// //     "quantity": 7,
// //     "order_type": "limit"
// //   }
// ];
// const sellOrders = [
//   {
//     "uid": "10",
//     "side": 1,
//     "symbol": "BTC/USD",
//     "price": 90075,
//     "quantity": 8.5,
//     "order_type": "limit"
//   },
//   // {
//   //   "uid": "alice",
//   //   "side": 1,
//   //   "symbol": "BTC/USD",
//   //   "price": 89998,
//   //   "quantity": 50,
//   //   "order_type": "limit"
//   // },
//   // {
//   //   "uid": "alice",
//   //   "side": 1,
//   //   "symbol": "BTC/USD",
//   //   "price": 90082,
//   //   "quantity": 10,
//   //   "order_type": "limit"
//   // },
//   // {
//   //   "uid": "alice",
//   //   "side": 1,
//   //   "symbol": "BTC/USD",
//   //   "price": 90011,
//   //   "quantity": 5,
//   //   "order_type": "limit"
//   // },
//   // {
//   //   "uid": "alice",
//   //   "side": 1,
//   //   "symbol": "BTC/USD",
//   //   "price": 90034,
//   //   "quantity": 8,
//   //   "order_type": "limit"
//   // },
// //   {
// //     "uid": "alice",
// //     "side": 1,
// //     "symbol": "BTC/USD",
// //     "price": 90122,
// //     "quantity": 19,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "alice",
// //     "side": 1,
// //     "symbol": "BTC/USD",
// //     "price": 90000,
// //     "quantity": 1,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "alice",
// //     "side": 1,
// //     "symbol": "BTC/USD",
// //     "price": 90300,
// //     "quantity": 10,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "alice",
// //     "side": 1,
// //     "symbol": "BTC/USD",
// //     "price": 90099,
// //     "quantity": 2,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "alice",
// //     "side": 1,
// //     "symbol": "BTC/USD",
// //     "price": 90198,
// //     "quantity": 6,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "alice",
// //     "side": 1,
// //     "symbol": "BTC/USD",
// //     "price": 90240,
// //     "quantity": 8,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "alice",
// //     "side": 1,
// //     "symbol": "BTC/USD",
// //     "price": 90112,
// //     "quantity": 5,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "alice",
// //     "side": 1,
// //     "symbol": "BTC/USD",
// //     "price": 90011,
// //     "quantity": 7,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "alice",
// //     "side": 1,
// //     "symbol": "BTC/USD",
// //     "price": 90118,
// //     "quantity": 6,
// //     "order_type": "limit"
// //   },
// //   {
// //     "uid": "alice",
// //     "side": 1,
// //     "symbol": "BTC/USD",
// //     "price": 90101,
// //     "quantity": 0,
// //     "order_type": "limit"
// //   }
// ];

// for (let i = 0; i <100; i++) {
//   const buyOrder = {
//     uid: `alice`,
//     side: 0,
//     symbol: 'BTC/INR',
//     price: Math.floor(Math.random() * (910 - 900 + 1)) + 900,
//     quantity: Math.floor(Math.random() * (5 - 1 + 1)) + 1,
//     order_type: 'limit',
//   };
//   buyOrders.push(buyOrder);
// }

// for (let i = 0; i <150; i++) {
//   const sellOrder = {
//     uid: `bob`,
//     side: 1,
//     symbol: 'BTC/INR',
//     price: Math.floor(Math.random() * (920 - 900 + 1)) + 900,
//     quantity: Math.floor(Math.random() * (5 - 1 + 1)) + 1,
//     order_type: 'limit',
//   };
//   sellOrders.push(sellOrder);
// }


const buyOrders = [];
const sellOrders = [];

for (let i = 0; i < numOrders; i++) {
  const buyOrder = {
    uid: `10`, // Assign Alice to Buy orders
    side: 0, // 0 for Buy, 1 for Sell
    symbol: 'BTC/INR',
    // price:  Math.floor(Math.random() * (905 - 900 + 1)) + 900, // Random price between 90000 and 92000
    // quantity: Math.floor(Math.random() * (5 - 1 + 1)) + 1, // Random quantity between 1 and 10
    price:800,
    quantity:5,
    order_type: 'limit',
  };  
  buyOrders.push(buyOrder);
}

for (let i = 0; i < numOrders; i++) {
  const sellOrder = {
    uid: `15`, // Assign Bob to Sell orders
    side: 1, // 0 for Buy, 1 for Sell
    symbol: 'BTC/INR',
    // price: Math.floor(Math.random() * (905 - 900 + 1)) + 900, // Random price between 90000 and 92000
    // quantity: Math.floor(Math.random() * (5 - 1 + 1)) + 1, // Random quantity between 1 and 10
    price:900,
    quantity:5,
    order_type: 'limit',
  };
  sellOrders.push(sellOrder);
}


const axiosOptions = {
  headers: {
    'Content-Type': 'application/json',
  },
  timeout: 10000
};

const placeOrder = async (order) => {
  try {
    await axios.post(url, order, axiosOptions);
    // console.log(`Order placed successfully: ${order.uid}`);
  } catch (error) {
    console.error(`Error placing order: ${order.uid}`, error);
  }
};

const placeBuyOrders = async () => {
  const buyPromises = buyOrders.map((order, i) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        placeOrder(order).then(async() => {
          resolve();
          await redisClient.rPush(`BUY:BTCUSD`,JSON.stringify(order))
        });
      }, 10*i);
    });
  });
  await Promise.all(buyPromises);
  redisClient.quit();
};

const placeSellOrders = async () => {
 const sellPromises = sellOrders.map((order, i) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        placeOrder(order).then(async() => {
          resolve();
          await redisClient.rPush(`SELL:BTCUSD`,JSON.stringify(order))
        });
      }, 10*i);
    });
  });
  await Promise.all(sellPromises);
};
  

console.time('placeOrders');

  async function placebothorders() {
    await placeSellOrders();
    console.log('placed sell orders');
    // await new Promise(resolve => setTimeout(resolve, 0));
    await placeBuyOrders();
    console.log('placed buy orders');
    console.timeEnd('placeOrders');
    return;
  }
  
  placebothorders();


