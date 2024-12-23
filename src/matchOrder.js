import { randomUUID } from 'crypto';
import sendExecutionReportToKafka from './index.js';

const matchOrder = async ({ uid, side, symbol, price, quantity, redisClient }) => {
  try {
    const opposingSide = side === 0 ? 1 : 0; // Opposing side: 0 for Buy, 1 for Sell
    const opposingKey = `${opposingSide}:${symbol}`;
    const orderKey = `${side}:${symbol}`;

    const ts = Date.now();
    const adjustmentFactor = 1e10;
    const numericPrice = parseFloat(price);

    const ordersExist = (await redisClient.ZCARD(opposingKey)) > 0;
    console.log(opposingKey ," ", ordersExist)
    if (!ordersExist) return null;

    const hash = randomUUID().substring(0, 25);
    let remainingQuantity = quantity;
    const matchedOrders = [];

    // Lua Script for matching orders
    const luaScript = `
      local orderRange = redis.call('ZRANGE', KEYS[1], ARGV[1], ARGV[1], 'WITHSCORES')
      if next(orderRange) == nil then
          return nil
      end

      local orderBookData = cjson.decode(orderRange[1])
      local orderBookQuantity = orderBookData.quantity

      if tonumber(ARGV[3]) >= tonumber(orderBookData.price) then
          if tonumber(orderBookQuantity) <= tonumber(ARGV[2]) then
              redis.call('ZREM', KEYS[1], orderRange[1])
              return { orderRange[1], 'MATCHED', orderBookQuantity }
          else
              orderBookData.quantity = orderBookData.quantity - ARGV[2]
              redis.call('ZREM', KEYS[1], orderRange[1])
              redis.call('ZADD', KEYS[1], ARGV[4], cjson.encode(orderBookData))
              return { orderRange[1], 'PARTIAL', ARGV[2] }
          end
      end
      return nil
    `;

    try {
      while (remainingQuantity > 0) {
        const rangeArg = side === 0 ? 0 : -1;

        // Convert arguments to strings and log them for debugging
        const keys = [opposingKey];
        const args = [
          rangeArg.toString(),
          remainingQuantity.toString(),
          numericPrice.toString(),
          (numericPrice - ts / adjustmentFactor).toString(),
        ];

        console.log('Keys:', keys);
        console.log('Arguments:', args);

        // Execute Lua script with eval
        const luaResult = await redisClient.eval(luaScript, {
          keys,
          arguments: args,
        });
        console.log('Lua Result:', luaResult);

        if (!luaResult) {
          if (matchedOrders.length > 0) {
            await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
              type: 1,
              uid,
              side,
              symbol,
              price,
              quantity: remainingQuantity,
              hash,
            }));

            await redisClient.ZADD(orderKey, {
              score: numericPrice - ts / adjustmentFactor,
              value: JSON.stringify({
                side,
                uid,
                ts: Date.now(),
                hash,
                price,
                quantity: remainingQuantity,
              }),
            });

            return { order: orderKey, ...matchedOrders };
          }
          return null;
        }

        const [orderBookJson, matchType, matchedQuantity] = luaResult;
        const orderBookData = JSON.parse(orderBookJson);

        if (matchType === 'MATCHED') {
          await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
            type: 3,
            side: opposingSide,
            symbol,
            hash: orderBookData.hash,
          }));
        } else if (matchType === 'PARTIAL') {
          await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
            type: 1,
            uid,
            side,
            symbol,
            price: orderBookData.price,
            quantity: matchedQuantity,
            hash: orderBookData.hash,
          }));
        }

        remainingQuantity -= matchedQuantity;
        matchedOrders.push({ price: orderBookData.price, quantity: matchedQuantity });

        await redisClient.RPUSH(`MATCHED:${symbol}`, JSON.stringify({
          side: opposingSide,
          remQty:matchedQuantity,
          ...orderBookData
        }));
        await redisClient.RPUSH(`MATCHED:${symbol}`, JSON.stringify({
          side,
          uid,
          ts: Date.now(),
          hash,
          price,
          remQty:remainingQuantity,
          quantity,
        }));

        await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
          type: 2,
          uid,
          side: opposingSide,
          symbol,
          price,
          quantity: matchedQuantity,
          hash,
        }));
        await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
          type: 2,
          uid,
          side,
          symbol,
          price,
          quantity:remainingQuantity,
          hash,
        }));
      }
    } catch (error) {
      console.error('Redis Eval Error:', error);
    }
    return { order: opposingKey, ...matchedOrders };
  } catch (err) {
    console.error(err);
    return null;
  }
};

export default matchOrder;




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



