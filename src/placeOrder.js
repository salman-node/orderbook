// const protobuf = require('protobufjs');
// const base64 = require('base64-js');
import { createClient } from 'redis';

async function encodeProtobufMessage() {
  const proto =  protobuf.loadSync('./orderBook.proto');  // Load your .proto file

  const OrderMessage = proto.lookupType('orderbook.Order');  // Lookup your message type

  const message = OrderMessage.create({
    "uid": "alice",
    "side": 1,
    "symbol": "BTC/USD",
    "price": 91200,
    "quantity": 3
  });

  // Verify the message before encoding
  const errMsg = OrderMessage.verify(message);
  if (errMsg) {
    console.error("Error:", errMsg);
    return;
  }

  // Encode the message into a buffer
  const buffer = OrderMessage.encode(message).finish();

  // Convert the binary buffer to base64 string
  const base64Message = base64.fromByteArray(buffer);

  return base64Message;
}

// Call the function
// encodeProtobufMessage();


const redisClient = createClient({
  url: `redis://redis-12875.c301.ap-south-1-1.ec2.redns.redis-cloud.com:12875`,
  password: 'WwvALs6ACyZR73dfmekFVa3LnRctbtg4',
})

await redisClient.connect()
import { randomUUID } from 'crypto'
import { constants } from 'buffer';
let count=-9999
function storeUUIDInRedisList() {
  try{
  for (let i = 0; i < 10000; i++) { // loop 10 times
    console.log('in loop',count++)
    const hash = randomUUID();
    redisClient.RPUSH('UUID', hash, (err, count) => {
      console.log(count)
      if (err) {
        console.error(err);
      } else {
        console.log(`Stored UUID ${hash} in Redis list`);
      }
    });
  }
  }catch(e){
    console.log(e)
  }
}

storeUUIDInRedisList();

// const listKey = 'MATCHED:BTC/USD'; // Replace with your Redis list key
// const searchHash = '2431a67f-ec51-4a93-ad09-7f2e5543c66e'; // Replace with your target hash

// try {
//   const listData = await redisClient.lRange(listKey, 0, -1); // Fetch all data from the list

//   const matchingItem = listData.find((item) => {
//     const parsedItem = JSON.parse(item);
//     return parsedItem.hash === searchHash; // Match by hash
//   });

//   if (matchingItem) {
//     console.log('Data found:', matchingItem);
//   } else {
//     console.log('Data not found');
//   }
// } catch (error) {
//   console.error('Error fetching data:', error);
// } finally {
//   await redisClient.quit();
// }
