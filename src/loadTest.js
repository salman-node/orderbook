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

const axios = require('axios');

const numOrders = 10000; // Number of orders to place
const concurrency = 0; // Number of concurrent requests
const url = 'http://localhost:9696/place-order';

const buyOrders  = [];
const sellOrders = [];  

for (let i = 0; i < numOrders; i++) {
  buyOrders.push({
    uid: `alice`,
    side: 0,
    symbol: 'BTC/USD',
    // price:90000,
    // quantity: 10,
    price:  Math.floor(Math.random() * 100000) + 90000,
    quantity: Math.floor(Math.random() * 10) + 1,
  });
}

for (let i = 0; i <numOrders; i++) {
  sellOrders.push({
    uid: `bob`,
    side: 1,
    symbol: 'BTC/USD',
    // price:90000,
    // quantity: 10,
    price:  Math.floor(Math.random() * 100000) + 90000,
    quantity: Math.floor(Math.random() * 10) + 1,
  });
}

const axiosOptions = {
  headers: {
    'Content-Type': 'application/json',
  },
  timeout: 10000
};

const placeOrder = async (order) => {
  try {
    const response = await axios.post(url, order, axiosOptions);
    // console.log(`Order placed successfully: ${order.uid}`);
  } catch (error) {
    console.error(`Error placing order: ${order.uid}`, error);
  }
};

const placeBuyOrders = async () => {
  const buyPromises = buyOrders.map((order, i) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        placeOrder(order).then(() => {
          resolve();
        });
      }, 10*i);
    });
  });
  await Promise.all(buyPromises);
};

const placeSellOrders = async () => {
  const sellPromises = sellOrders.map((order, i) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        placeOrder(order).then(() => {
          resolve();
        });
      }, 10*i);
    });
  });
  await Promise.all(sellPromises);
};

// console.time('placeOrders');
// Promise.all([placeBuyOrders(),placeSellOrders()]).then(() => {
//   console.timeEnd('placeOrders');
// });

console.time('placeOrders');
Promise.all([
  placeBuyOrders(),
  new Promise(resolve => setTimeout(resolve, 500)).then(placeSellOrders)
]).then(() => {
  console.timeEnd('placeOrders');
});