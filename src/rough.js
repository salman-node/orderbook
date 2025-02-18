
//  testing script ends here 


// working script-01 with lnit order below.
// const matchOrder = async ({ uid, side, symbol, price, quantity, redisClient }) => {
//   try {
//     const opposingSide = side === 0 ? 1 : 0; // Opposing side: 0 for Buy, 1 for Sell
//     const opposingKey = `${opposingSide}:${symbol}`;
//     const orderKey = `${side}:${symbol}`;

//     const ts = Date.now();
//     const adjustmentFactor = 1e10;
//     const numericPrice = parseFloat(price);

//     const ordersExist = (await redisClient.ZCARD(opposingKey)) > 0;
//     console.log(opposingKey ," ", ordersExist)
//     if (!ordersExist) return null;

//     const hash = randomUUID().substring(0, 25);
//     let remainingQuantity = quantity;
//     const matchedOrders = [];                               

//     // Lua Script for matching orders
//     const luaScript = `
//       local orderRange = redis.call('ZRANGE', KEYS[1], ARGV[1], ARGV[1], 'WITHSCORES')
//       if next(orderRange) == nil then
//           return nil
//       end

//       local orderBookData = cjson.decode(orderRange[1])
//       local orderBookPrice = tonumber(orderBookData.price)
//       local orderBookQuantity = tonumber(orderBookData.quantity)
//       local requestedPrice = tonumber(ARGV[3])
//       local requestedQuantity = tonumber(ARGV[2])
//       local newScore = tonumber(ARGV[4])

//       -- Price check: Only match if the price conditions are met
//       if (ARGV[5] == "BUY" and orderBookPrice > requestedPrice) or (ARGV[5] == "SELL" and orderBookPrice < requestedPrice) then
//           return nil -- Price does not satisfy matching conditions
//       end

//       -- Match or partially match the order
//       if orderBookQuantity <= requestedQuantity then
//           redis.call('ZREM', KEYS[1], orderRange[1])
//           return { orderRange[1], 'MATCHED', orderBookQuantity }
//       else
//           orderBookData.quantity = orderBookQuantity - requestedQuantity
//           redis.call('ZREM', KEYS[1], orderRange[1])
//           redis.call('ZADD', KEYS[1], newScore, cjson.encode(orderBookData))
//           return { orderRange[1], 'PARTIAL', requestedQuantity }
//       end
//     `;
// try{
//     while (remainingQuantity > 0) {
//       const rangeArg = side === 0 ? 0 : -1;
//       const keys = [opposingKey];
//       const args = [
//         rangeArg.toString(),
//         remainingQuantity.toString(),
//         numericPrice.toString(),
//         side === 0
//           ? (numericPrice - ts / adjustmentFactor).toString()
//           : (numericPrice + ts / adjustmentFactor).toString(),
//         side === 0 ? "BUY" : "SELL",
//       ];
//         console.log('Keys:', keys);
//         console.log('Arguments:', args);

//         // Execute Lua script with eval
//         const luaResult = await redisClient.eval(luaScript, {
//           keys,
//           arguments: args,
//         });

//         if (!luaResult) {
//           if (matchedOrders.length > 0) {
//             await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
//               type: 1,
//               uid,
//               side,
//               symbol,
//               price,
//               quantity: remainingQuantity,
//               hash,
//             }));

//             await redisClient.multi().ZADD(orderKey, {
//               score: side ===0 ?numericPrice - ts / adjustmentFactor : numericPrice + ts / adjustmentFactor,
//               value: JSON.stringify({
//                 side,
//                 uid,
//                 ts: Date.now(),
//                 hash,
//                 price,
//                 quantity: remainingQuantity,
//               }),
//             }).exec();

//             return { order: orderKey, ...matchedOrders };
//           }
//           return null;
//         }

//         const [orderBookJson, matchType, matchedQuantity] = luaResult;
//         const orderBookData = JSON.parse(orderBookJson);

//         if (matchType === 'MATCHED') {
//           await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
//             type: 3,
//             side: opposingSide,
//             symbol,
//             hash: orderBookData.hash,
//           }));
//         } else if (matchType === 'PARTIAL') {
//           await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
//             type: 1,
//             uid,
//             side,
//             symbol,
//             price: orderBookData.price,
//             quantity: matchedQuantity,
//             hash: orderBookData.hash,
//           }));
//         }

//         remainingQuantity -= matchedQuantity;
//         matchedOrders.push({ price: orderBookData.price, quantity: matchedQuantity });

//         await redisClient.multi().RPUSH(`MATCHED:${symbol}`, JSON.stringify({
//           side: opposingSide,
//           ...orderBookData,
//           remQty:orderBookData.quantity - matchedQuantity
//         })).RPUSH(`MATCHED:${symbol}`, JSON.stringify({
//           side,
//           uid,
//           ts: Date.now(),
//           hash,
//           price,
//           quantity,
//           remQty:remainingQuantity,
//         })).exec();

//         await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
//           type: 2,
//           uid,
//           side: opposingSide,
//           symbol,
//           price,
//           quantity: matchedQuantity,
//           hash,
//         }));
//         await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
//           type: 2,
//           uid,
//           side,
//           symbol,
//           price,
//           quantity:remainingQuantity,
//           hash,
//         }));
//       }
//     } catch (error) {
//       console.error('Redis Eval Error:', error);
//     }

//     return { order: opposingKey, ...matchedOrders };
//   } catch (err) {
//     console.error(err);
//     return null;
//   }
// };
// script 01 ends here.




// import { randomUUID } from 'crypto'
// import  sendExecutionReportToKafka from './index.js'


// const matchOrder = async ({ uid,side, symbol, price, quantity, redisClient }) => {
//   try{
//   const opposingSide = side === 0 ? 1 : 0; // Opposing side: 0 for Buy, 1 for Sell
//   const opposingKey = `${opposingSide}:${symbol}`;
//   const orderkey = `${side}:${symbol}`
// console.log(1)
//   const ts = Date.now()
//   const adjustmentFactor = 1e10;
//   const numericPrice = parseFloat(price)

//   const ordersExist = (await redisClient.ZCARD(opposingKey)) > 0;
//   console.log(ordersExist)
//   if (!ordersExist) return null;
  
//   const hash = randomUUID().substring(0, 10);
// console.log(2)
//   let remainingQuantity = quantity;
//   const matchedOrders = []; 

//   while (remainingQuantity > 0){
// console.log(3)
//     // Fetch the best match from the order book
//     const orderRange = side === 0
//       ? await redisClient.ZRANGE(opposingKey, 0, 0, 'WITHSCORES') // Buy looks for lowest sell price
//       : await redisClient.ZRANGE(opposingKey, -1, -1, 'WITHSCORES'); // Sell looks for highest buy price
//     console.log(3.5,orderRange)
//      if (!orderRange.length){ 
//       console.log(8) 
//           if(matchedOrders.length > 0){
//             console.log(9)
//             await  sendExecutionReportToKafka('trade-engine-message', JSON.stringify({type:1, uid,side, symbol, price, quantity:remainingQuantity,hash }))
//             console.log('in partial match')
//             side === 0 ? await redisClient.ZADD(orderkey, { score: numericPrice - ts / adjustmentFactor, value: JSON.stringify({ "side": side, "uid": uid, "ts": Date.now(), "hash":hash, "price": price, "quantity": remainingQuantity}) }):
//             await redisClient.ZADD(orderkey, { score: numericPrice + ts / adjustmentFactor, value: JSON.stringify({ "side": side, "uid": uid, "ts": Date.now(), "hash":hash, "price": price, "quantity": remainingQuantity}) });
//             return { order: orderkey, ...matchedOrders}
//           }
//           return null;   
//      } 
// console.log(4)
//     const orderBookData = JSON.parse(orderRange[0]);
//     const orderBookPrice = orderBookData.price;
//     const numericOrderBookPrice = parseFloat(orderBookPrice)
//     const orderBookQuantity = orderBookData.quantity;
//     const orderBookHash = orderBookData.hash

//     // Check price condition
//     if ((side === 0 && price >= orderBookPrice) || (side === 1 && price <= orderBookPrice)) {
//       const matchQuantity = Math.min(remainingQuantity, orderBookQuantity);
// console.log(5)
//       const hash = randomUUID().substring(0, 20);
       
//       if (matchQuantity === orderBookQuantity) {
// console.log(6)
//         // Exact match: Remove order from book
//         await redisClient.ZREM(opposingKey, orderRange[0]);
//         await  sendExecutionReportToKafka('trade-engine-message', JSON.stringify({ type:3,side:opposingSide, symbol, hash:orderBookHash }))
//       } else {
//         console.log(10)
//         // Partial match: Update order book quantity
//         orderBookData.quantity -= matchQuantity;
//        await redisClient.ZREM(opposingKey, orderRange[0]);
//         side === 0 ? await redisClient.ZADD(opposingKey, { score: numericOrderBookPrice - ts / adjustmentFactor, value: JSON.stringify(orderBookData) }):
//         await redisClient.ZADD(opposingKey, { score: numericOrderBookPrice + ts / adjustmentFactor, value: JSON.stringify(orderBookData) })
//         await  sendExecutionReportToKafka('trade-engine-message', JSON.stringify({ type:1,uid,side, symbol, price:orderBookPrice, quantity:orderBookData.quantity,hash:orderBookHash }))
//         await  sendExecutionReportToKafka('trade-engine-message', JSON.stringify({ type:3,side:opposingSide, symbol, hash:orderBookHash }))
       
//       }
// console.log(7)
//       // Update remaining quantity
//       remainingQuantity -= matchQuantity;
      
//       orderBookData.ts = Date.now();
//       await redisClient.RPUSH(`MATCHED:${symbol}`, JSON.stringify({side:opposingSide,...orderBookData}))
//       await redisClient.RPUSH(`MATCHED:${symbol}`, JSON.stringify({ "side": side, "uid": uid, "ts": Date.now(), "hash":hash, "price": price, "quantity": quantity}))
//       await  sendExecutionReportToKafka('trade-engine-message', JSON.stringify({type:2, uid,side:opposingSide, symbol, price, quantity,hash }))
//       await  sendExecutionReportToKafka('trade-engine-message', JSON.stringify({type:2, uid,side, symbol, price, quantity,hash }))
      
//       // Add matched order details
//       matchedOrders.push({
//         price: orderBookPrice,
//         quantity: matchQuantity,
//       });

//     } else {
//       console.log(11)
//       // No more matches possible due to price condition
//       return null;
//     }
//   }
  
//   return { order: opposingKey, ...matchedOrders}
//   }
//   catch(err){
//     console.log(err)
//     return null
//   }
// };

// export default matchOrder;

// |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// const sendExecutionReportToKafka = async (topic, message) => {
//   try {
//     await producer.send({
//       topic,
//       key: JSON.stringify(message.i),
//       messages: [{ value: JSON.stringify(message)}],
//     });
//     console.log(`Sent to Kafka topic: ${JSON.stringify(message.t)}`);
//   } catch (error) {
//     console.error(`Error sending to Kafka topic ${topic}:`, error);
//   }
// };



// const matchOrder = async ({ side, symbol, price,quantity, redisClient }) => {
//   const opposingSide = side === 0 ? 1 : 0
//   const opposingKey = `${opposingSide}:${symbol}`
//   console.log('opposingKey', opposingKey)
//   const keys = await redisClient.KEYS(opposingKey)
//   if (!keys) return null

//   const ordersExist = (await redisClient.ZCARD(opposingKey)) > 0
//   if (!ordersExist) return null
  
//   if(side === 0) {
//     const minScoreData = await redisClient.ZRANGE(opposingKey, 0, 0, 'WITHSCORES');
//     console.log('minScoreData', minScoreData)
//     const orderData = JSON.parse(minScoreData[0]);
//     const orderBookPrice = orderData.price;
//     console.log('orderBookPrice', orderBookPrice)
    
//     if(price >= orderBookPrice){
//       const matchedOrder = await redisClient.ZREM(opposingKey,minScoreData[0]);
    
//      console.log('matchedOrder', matchedOrder);
//      await redisClient.RPUSH(`MATCHED:${symbol}`, JSON.stringify(orderData))
//      return { order: opposingKey, ...JSON.parse(matchedOrder) }
//     }
//     return null
//   }
//   if(side === 1) {
//     const maxScoreData = await redisClient.ZRANGE(opposingKey, -1, -1, 'WITHSCORES');
//     console.log('maxScoreData', maxScoreData)
//     const orderData = JSON.parse(maxScoreData[0]);
//     const orderBookPrice = orderData.price;
//     console.log('orderBookPrice', orderBookPrice)
//     if(price <= orderBookPrice){
//     const matchedOrder = await redisClient.ZREM(opposingKey,maxScoreData[0]);
    
//      console.log('matchedOrder', matchedOrder);
//      await redisClient.RPUSH(`MATCHED:${symbol}`, JSON.stringify(orderData))
//      return { order: opposingKey, ...JSON.parse(matchedOrder) }
//     }
//     return null
//   }
 
// }

// export default matchOrder




// const matchOrder = async ({ side, symbol, price, quantity, redisClient }) => {
//   const opposingSide = side === 0 ? 1 : 0;
//   const opposingKey = `${opposingSide}:${symbol}`;
//   console.log('opposingKey', opposingKey);

//   const ordersExist = (await redisClient.ZCARD(opposingKey)) > 0;
//   if (!ordersExist) return null;

//   let remainingQuantity = quantity;
//   const matchedOrders = [];

//   while (remainingQuantity > 0) {
//     // Fetch the best match from the order book
//     const orderRange = side === 0
//       ? await redisClient.ZRANGE(opposingKey, 0, 0, 'WITHSCORES') // Buy looks for lowest sell price
//       : await redisClient.ZRANGE(opposingKey, -1, -1, 'WITHSCORES'); // Sell looks for highest buy price

//     if (!orderRange.length) return null;

//     const orderBookData = JSON.parse(orderRange[0]);
//     const orderBookPrice = orderBookData.price;
//     const orderBookQuantity = orderBookData.quantity;

//     console.log('orderBookPrice:', orderBookPrice, 'orderBookQuantity:', orderBookQuantity);

//     // Check price condition
//     if ((side === 0 && price >= orderBookPrice) || (side === 1 && price <= orderBookPrice)) {
//       const matchQuantity = Math.min(remainingQuantity, orderBookQuantity);

//       // Update order book
//       if (orderBookQuantity > matchQuantity) {
//         // Decrement the order book quantity
//         orderBookData.quantity -= matchQuantity;
//         await redisClient.ZADD(opposingKey, { score: orderBookPrice, value: JSON.stringify(orderBookData)});
//       } else {
//         // Remove the fully matched order
//         await redisClient.ZREM(opposingKey, orderRange[0]);
//       }

//       // Update remaining quantity
//       remainingQuantity -= matchQuantity;

//       // Add to matched orders
//       matchedOrders.push({
//         price: orderBookPrice,
//         quantity: matchQuantity,
//       });

//       console.log('Matched Order:', { price: orderBookPrice, quantity: matchQuantity });
//     } else {
//       return null; // No more matches possible
//     }
//   }

//   console.log('Remaining Quantity:', remainingQuantity);
//   console.log('Matched Orders:', matchedOrders);

//   return { order: opposingKey, ...matchedOrders }
// };

// export default matchOrder;







// const matchOrder = async ({ side, symbol, price,quantity, redisClient }) => {
//   const opposingSide = side === 0 ? 1 : 0
//   const opposingKey = `${opposingSide}:${symbol}`
//   console.log('opposingKey', opposingKey)
//   const keys = await redisClient.KEYS(opposingKey)
//   if (!keys) return null

//   const ordersExist = (await redisClient.ZCARD(opposingKey)) > 0
//   if (!ordersExist) return null

//   let MatchedOrder=[]
//   let orderQuatity = quantity
  
//   if(side === 0) {
//     const minScoreData = await redisClient.ZRANGE(opposingKey, 0, 0, 'WITHSCORES');
//     console.log('minScoreData', minScoreData)
//     const orderBookPrice = JSON.parse(minScoreData[0]).price
//     const orderBookQuantity = JSON.parse(minScoreData[0]).quantity
//     console.log('orderBookPrice', orderBookPrice)
     
//     if(price >= orderBookPrice){
//       if(orderQuatity >= orderBookQuantity){
//         const matchedOrder = await redisClient.ZREM(opposingKey,minScoreData[0]);
//          MatchedOrder.push(matchedOrder)
//          orderQuatity = orderQuatity - orderBookQuantity
//          return { order: opposingKey, ...JSON.parse(MatchedOrder) }
//       }
//       if(orderQuatity < orderBookQuantity){
//         const matchedOrder = await redisClient.ZREM(opposingKey,minScoreData[0]);
//          MatchedOrder.push(matchedOrder)
//          orderQuatity = orderQuatity - quantity
//          return { order: opposingKey, ...JSON.parse(MatchedOrder) }
//       }
//     }






//       const matchedOrder = await redisClient.ZREM(opposingKey,minScoreData[0]);
    
//      console.log('matchedOrder', matchedOrder);
//      return { order: opposingKey, ...JSON.parse(matchedOrder) }
//     }
//     return null
//   }
//   if(side === 1) {
//     const maxScoreData = await redisClient.ZRANGE(opposingKey, -1, -1, 'WITHSCORES');
//     console.log('maxScoreData', maxScoreData)
//     const orderBookPrice = JSON.parse(maxScoreData[0]).price
//     console.log('orderBookPrice', orderBookPrice)
//     if(price <= orderBookPrice){
//       const matchedOrder = await redisClient.ZREM(opposingKey,maxScoreData[0]);
    
//      console.log('matchedOrder', matchedOrder);
//      return { order: opposingKey, ...JSON.parse(matchedOrder) }
//     }
//     return null
//   }
 


// export default matchOrder




//testing code ends here 


//working handleOrder function    date - 11/2/25   time 5:12
// const handleOrder = async ({ data,clientUids, proto, redisClient }) => {

//   const OrderMessage = proto.lookupType('orderbook.Order')
  
//   const buf = Buffer.from(data, 'base64')
//   const decoded = OrderMessage.decode(buf)
//   const message = OrderMessage.toObject(decoded)

//   if (!clientUids.includes(message.uid)) {
//     // console.log('message rejected : uid mismatch')
//     return {
//       type: 'order',
//       error: 'Connected user ID does not match message user ID.',
//     }
//   }
//   const uid = message.uid
//   await redisClient.INCR('TOTAL_ORDERS')

//   const { side, symbol, price, quantity, order_type } = message
  
//   const ts = Date.now()
//   const adjustmentFactor = 1e10;
//   const orderString = `${side}:${symbol}`
//   const hash = randomUUID().substring(0, 25);

//   const matchedOrder = await matchOrder({ uid,side, symbol, price, quantity,order_type,redisClient })
//   // console.log('ELAPSED TIME:', elapsedTime)
//   // console.log('MATCHED ORDER:',side, matchedOrder)
//   if (matchedOrder) {
//     await redisClient.INCRBY('TOTAL_MATCHED', 2)
//     console.log('Order matched:', matchedOrder)
//     return {
//       type: 'match',
//       message: 'Order matched',
//       data: {
//         matchedBy: matchedOrder.uid,
//         matchedAt: Date.now(),
//         yourOrder: { order: orderString, uid, ts, hash },
//         matchedOrder,
//       },
//     }
//   } else {
//     console.log('order not matched.')
//     // Ensure price is a valid number and not Infinity or NaN
//     const numericPrice = parseFloat(price)
//     const numericQuantity = parseFloat(quantity)

//     if (isNaN(numericPrice) || !isFinite(numericPrice)) {
//       // console.log('Error: Invalid price value:', price)
//       return {
//         type: 'order',
//         error: 'Invalid price value.',
//       }
//     }

//     const transaction = redisClient.multi();
//     const score = side === 0 ? numericPrice - ts / adjustmentFactor : numericPrice + ts / adjustmentFactor;

//     try {
//       await  sendExecutionReportToKafka('trade-engine-message', JSON.stringify({type:1, uid,side, symbol, price, quantity:quantity,hash }))
//       transaction.ZADD(orderString, { score, value: JSON.stringify({ uid, ts, hash, price: numericPrice, quantity: numericQuantity }) });
//       await transaction.exec(); // Execute the transaction
//     } catch (error) {
//       console.error('Error adding to sorted set:', error)
//       return {
//         type: 'order',
//         error: 'Failed to add order to sorted set.',
//       }
//     }

//     return {
//       type: 'order',
//       message: 'Order submitted to queue',
//       data: {
//         order: orderString,
//         uid,
//         ts,
//         hash,
//         price: numericPrice,
//         quantity:numericQuantity
//       },
//     }
//   }
// }
// working handleOrder function ends here


// const handleOrder = async ({ data, clientUids, proto, redisClient }) => {
//   const OrderMessage = proto.lookupType('orderbook.Order');
//   const buf = Buffer.from(data, 'base64');
//   const decoded = OrderMessage.decode(buf);
//   const message = OrderMessage.toObject(decoded);

//   if (!clientUids.includes(message.uid)) {
//     return {
//       type: 'order',
//       error: 'Connected user ID does not match message user ID.',
//     };
//   }

//   const uid = message.uid;
//   await redisClient.INCR('TOTAL_ORDERS');

//   const { side, symbol, price, quantity } = message;
//   const ts = Date.now();
//   const adjustmentFactor = 1e10;
//   const orderString = `${side}:${symbol}`;
//   const hash = randomUUID().substring(0, 25);

//   const numericPrice = parseFloat(price);
//   const numericQuantity = parseFloat(quantity);

//   if (isNaN(numericPrice) || !isFinite(numericPrice)) {
//     return {
//       type: 'order',
//       error: 'Invalid price value.',
//     };
//   }

//   // Calculate the score outside the Lua script
//   const score = side === 0 ? numericPrice - ts / adjustmentFactor : numericPrice + ts / adjustmentFactor;

//   // Prepare order data as a JSON string
//   const orderData = JSON.stringify({ uid, ts, hash, price: numericPrice, quantity: numericQuantity });

//   // Lua script to handle matching and adding the order
//   const luaScript = `
//     local opposingKey = KEYS[1]
//     local orderKey = ARGV[1]
//     local orderData = cjson.decode(ARGV[2])
//     local requestedQuantity = orderData.quantity
//     local requestedPrice = orderData.price
//     local ts = orderData.ts
//     local hash = orderData.hash
//     local score = tonumber(ARGV[4])  -- Get the score from arguments

//     local orderRange = redis.call('ZRANGE', opposingKey, 0, 0, 'WITHSCORES')
//     if next(orderRange) == nil then
//         -- No matching order found, add to order book
//         redis.call('ZADD', orderKey, score, cjson.encode(orderData))
//         return { 'NO_MATCH', requestedQuantity }
//     end

//     local orderBookData = cjson.decode(orderRange[1])
//     local orderBookPrice = tonumber(orderBookData.price)
//     local orderBookQuantity = tonumber(orderBookData.quantity)

//     -- Price check: Only match if the price conditions are met
//     if (ARGV[5] == "BUY" and orderBookPrice > requestedPrice) or (ARGV[5] == "SELL" and orderBookPrice < requestedPrice) then
//         redis.call('ZADD', orderKey, score, cjson.encode(orderData))
//         return { 'NO_MATCH', requestedQuantity }
//     end

//     -- Match or partially match the order
//     if orderBookQuantity <= requestedQuantity then
//         redis.call('ZREM', opposingKey, orderRange[1])
//         // -- Store matched order in the Redis list
//         // redis.call('RPUSH', 'MATCHED_ORDERS', cjson.encode(orderBookData))  -- Store matched order
//         return { orderRange[1], 'MATCHED', orderBookQuantity }
//     else
//         orderBookData.quantity = orderBookQuantity - requestedQuantity
//         redis.call('ZREM', opposingKey, orderRange[1])
//         redis.call('ZADD', opposingKey, orderBookPrice, cjson.encode(orderBookData))
//         return { orderRange[1], 'PARTIAL', requestedQuantity }
//     end
//   `;

//   const opposingKey = `${side === 0 ? 1 : 0}:${symbol}`;
//   const orderKey = `${side}:${symbol}`;

//   try {
//     const luaResult = await redisClient.eval(luaScript, {
//       keys: [opposingKey],
//       arguments: [orderKey, orderData.toString(), numericQuantity.toString(), score.toString(), side === 0 ? "BUY" : "SELL"],
//     });
//    console.log('luaResult:',luaResult)
//     if (luaResult[1] === 'MATCHED') {
//       // Handle matched order
//       const matchedOrder = JSON.parse(luaResult[0]);
//       // Send execution report for matched order
//       await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
//         type: 3,
//         side: side === 0 ? 1 : 0,
//         symbol,
//         hash: matchedOrder.hash,
//       }));
//       console.log('Order matched:', matchedOrder);  
//       return {
//         type: 'match',
//         message: 'Order matched',
//         data: {
//           matchedBy: matchedOrder.uid,
//           matchedAt: Date.now(),
//           yourOrder: { order: orderString, uid, ts, hash },
//           matchedOrder,
//         },
//       };
//     } else {
//       // Handle no match or partial match
//       console.log('Order not matched.');
//       return {
//         type: 'order',
//         message: luaResult[0] === 'NO_MATCH' ? 'Order submitted to queue' : 'Order partially matched',
//         data: {
//           order: orderString,
//           uid,
//           ts,
//           hash,
//           price: numericPrice,
//           quantity: numericQuantity,
//         },
//       };
//     }
//   } catch (error) {
//     console.error('Error executing Lua script:', error);
//     return {
//       type: 'order',
//       error: 'Failed to process order.',
//     };
//   }
// };

