import sendExecutionReportToKafka from './index.js';
const path = require('path');
const logFilePath = path.join('C:/Users/dell/Desktop/salman/orderbook/src', 'logs.txt');
const winston = require('winston');
const Big = require('big.js');

const fileTransport = new winston.transports.File({ filename: logFilePath });

fileTransport.on('error', (err) => {
  console.error('Winston File Transport Error:', err);
});

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.simple(),
  transports: [fileTransport]
});

const matchOrder = async ({ hash, uid, side, symbol, price, quantity, redisClient }) => {
  try {
 
    const opposingSide = side === 0 ? 'ASK_ORDERS' : 'BID_ORDERS'; // Opposing side: 0 for Buy, 1 for Sell
    const orderSide = side === 0 ? 'BID_ORDERS' : 'ASK_ORDERS'; // Order side: 0 for Buy, 1 for Sell
    const opposingKey = `${symbol}:${opposingSide}`;
    const orderKey = `${symbol}:${orderSide}`;

    const ts = new Big(Date.now());
    const numericPrice = new Big(price);

    let remainingQuantity = new Big(quantity);
    console.log('remainingQuantity: ', remainingQuantity.toString());
    const matchedOrders = [];

    // Lua Script for matching orders
    const luaScript = `
    local function recalculateOrderBook(bookKey, orderSetKey)
        redis.call("DEL", bookKey)
    
        local priceLevels = redis.call("ZRANGE", orderSetKey, 0, -1)
        for _, price in ipairs(priceLevels) do
            local listKey = orderSetKey .. ":" .. price
            local orders = redis.call("LRANGE", listKey, 0, -1)
            local totalQty = 0
    
            for _, orderJson in ipairs(orders) do
                local order = cjson.decode(orderJson)
                totalQty = totalQty + tonumber(order.quantity)
            end
    
            if totalQty > 0 then
                redis.call("ZADD", bookKey, tonumber(price), cjson.encode({ price = price, quantity = totalQty }))
            end
        end
    end
    
    local requestedPrice = tonumber(ARGV[3])
    local requestedQuantity = tonumber(ARGV[2])
    local side = ARGV[5]
    local newScore = tonumber(ARGV[4])
    
    -- Determine best price from opposing side
    local bestPriceArr
    if side == "BUY" then
        bestPriceArr = redis.call("ZRANGE", KEYS[1], 0, 0) -- best ask
    else
        bestPriceArr = redis.call("ZREVRANGE", KEYS[1], 0, 0) -- best bid
    end
    
    -- No opposing orders
    if next(bestPriceArr) == nil then
        local newOrder = {
            uid = ARGV[6],
            ts = ARGV[7],
            hash = ARGV[8],
            price = requestedPrice,
            quantity = ARGV[2]
        }
    
        redis.call('ZADD', KEYS[2], requestedPrice, tostring(requestedPrice))
        redis.call("RPUSH", KEYS[2] .. ":" .. requestedPrice, cjson.encode(newOrder))
    
        if side == "SELL" then
            recalculateOrderBook(KEYS[3], KEYS[2])
        else
            recalculateOrderBook(KEYS[4], KEYS[2])
        end
    
        return { 'ADDED', newOrder, 0 }
    end
    
    local bestPrice = tonumber(bestPriceArr[1])
    local listKey = KEYS[1] .. ":" .. bestPrice
    local listOrders = redis.call("LRANGE", listKey, 0, 0)
    
    if next(listOrders) == nil then
        return { 'NO_MATCH', 'Empty list at best price' }
    end
    
    local headOrder = cjson.decode(listOrders[1])
    local orderBookQuantity = tonumber(headOrder.quantity)
    
    -- Check price match condition
    if (side == "BUY" and requestedPrice < bestPrice) or
       (side == "SELL" and requestedPrice > bestPrice) then
        local newOrder = {
            uid = ARGV[6],
            ts = ARGV[7],
            hash = ARGV[8],
            price = requestedPrice,
            quantity = ARGV[2]
        }
    
        redis.call('ZADD', KEYS[2], requestedPrice, tostring(requestedPrice))
        redis.call("RPUSH", KEYS[2] .. ":" .. requestedPrice, cjson.encode(newOrder))
    
        if side == "SELL" then
            recalculateOrderBook(KEYS[3], KEYS[2])
        else
            recalculateOrderBook(KEYS[4], KEYS[2])
        end
    
        return { 'ADDED', newOrder, 1 }
    end
    
    -- MATCH
    if orderBookQuantity <= requestedQuantity then
        redis.call("LPOP", listKey)
        local listLen = redis.call("LLEN", listKey)
        if listLen == 0 then
            redis.call("ZREM", KEYS[1], tostring(bestPrice))
        end
    
        if side == "BUY" then
            recalculateOrderBook(KEYS[3], KEYS[1])
        else
            recalculateOrderBook(KEYS[4], KEYS[1])
        end
    
        return { listOrders[1], 'MATCHED', tostring(orderBookQuantity) }
    else
        headOrder.quantity = orderBookQuantity - requestedQuantity
        redis.call("LSET", listKey, 0, cjson.encode(headOrder))
    
        if side == "BUY" then
            recalculateOrderBook(KEYS[3], KEYS[1])
        else
            recalculateOrderBook(KEYS[4], KEYS[1])
        end
    
        return { listOrders[1], 'PARTIAL', ARGV[2] }
    end

    `;

    try {
      while (remainingQuantity.gt(0)) {
        console.log('remainingQuantity: ', remainingQuantity.toString());
        const rangeArg = side === 0 ? 0 : -1;
        const luaResult = await redisClient.eval(luaScript, {
          keys: [opposingKey, orderKey, `${symbol}:ASK`, `${symbol}:BID`],
          arguments: [
            rangeArg.toString(),
            remainingQuantity.toString(),
            numericPrice.toString(),
            numericPrice.toString(),
            side === 0 ? "BUY" : "SELL",
            uid,
            ts.toString(),
            hash
          ]
        });
        if (!luaResult) {
          return null;
        }
        if (luaResult[0] === 'ADDED') {
          logger.info(JSON.stringify({
            msg: 1,
            type: 1,
            uid,
            side,
            symbol,
            price,
            quantity,
            hash,
            status: 'OPEN',
            script: luaResult[2]
          }));
          await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
            type: 1,
            uid,
            side,
            symbol,
            price,
            quantity,
            hash,
            status: 'OPEN',
            script: luaResult[2],
          }));
          return { order: orderKey, added: true };
        }
  
        const [orderBookJson, matchType, matchedQuantity] = luaResult;
        const matchedQuantityBig = matchedQuantity ? new Big(matchedQuantity) : new Big(0); // Default to 0 if undefined
        console.log('matchedQuantity: ', matchedQuantity, 'matchedQuantityBig: ', matchedQuantityBig.toString()); 
        const orderBookData = JSON.parse(orderBookJson);

        const tradeData = JSON.stringify({
          qty: matchedQuantityBig.toString(),
          buyer_hash: side === 0 ? hash : orderBookData.hash,
          seller_hash: side === 1 ? hash : orderBookData.hash,
          price: orderBookData.price,
          timestamp: Date.now(),
          symbol,
          type: side === 0 ? "SELL" : "BUY",
        });
        
        console.log(`${symbol}:trade_history`);
        console.log(tradeData);
        
        // Ensure ZADD receives correct arguments
        await redisClient.ZADD(
          `${symbol}:trade_history`,
          [{ score: Date.now(), value: tradeData }] // Correct ZADD syntax for Node.js Redis client
        );

        if (matchType === 'MATCHED') {
          logger.info(JSON.stringify({
            msg: 2,
            type: 2,
            side: opposingSide === 'ASK_ORDERS' ? 1 : 0,
            symbol,
            quantity: orderBookData.quantity,
            uid: orderBookData.uid,
            hash: orderBookData.hash,
            price: orderBookData.price,
            execution_price: orderBookData.price,
            execute_qty: matchedQuantityBig.toString(),
            status: 'FILLED',
          }));
          await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
            type: 2,
            side: opposingSide === 'ASK_ORDERS' ? 1 : 0,
            symbol,
            quantity: orderBookData.quantity,
            uid: orderBookData.uid,
            hash: orderBookData.hash,
            price: orderBookData.price,
            execution_price: orderBookData.price,
            execute_qty: matchedQuantityBig.toString(),
            status: 'FILLED',
          }));
          const orderSideStatus = matchedQuantityBig.eq(remainingQuantity) ? 'FILLED' : 'PARTIALLY_FILLED';
          logger.info(JSON.stringify({
            msg: 3,
            type: 2,
            side,
            symbol,
            uid,
            hash,
            price,
            execution_price: orderBookData.price,
            quantity: remainingQuantity.toString(),
            execute_qty: matchedQuantityBig.toString(),
            status: orderSideStatus,
          }));
          await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
            type: 2,
            side,
            symbol,
            uid,
            hash,
            price,
            execution_price: orderBookData.price,
            quantity: remainingQuantity.toString(),
            execute_qty: matchedQuantityBig.toString(),
            status: orderSideStatus,
          }));
        } else if (matchType === 'PARTIAL') {
          const oppsiteSideStatus = matchedQuantityBig.eq(orderBookData.quantity) ? 'FILLED' : 'PARTIALLY_FILLED';
          logger.info(JSON.stringify({
            msg: 4,
            type: 2,
            side: opposingSide === 'ASK_ORDERS' ? 1 : 0,
            symbol,
            quantity: orderBookData.quantity,
            uid: orderBookData.uid,
            hash: orderBookData.hash,
            price: orderBookData.price,
            execution_price: orderBookData.price,
            execute_qty: matchedQuantityBig.toString(),
            status: oppsiteSideStatus,
          }));
          await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
            type: 2,
            side: opposingSide === 'ASK_ORDERS' ? 1 : 0,
            symbol,
            quantity: orderBookData.quantity,
            uid: orderBookData.uid,
            hash: orderBookData.hash,
            price: orderBookData.price,
            execution_price: orderBookData.price,
            execute_qty: matchedQuantityBig.toString(),
            status: oppsiteSideStatus,
          }));
          const orderSideStatus = matchedQuantityBig.eq(remainingQuantity) ? 'FILLED' : 'PARTIALLY_FILLED';
          logger.info(JSON.stringify({
            msg: 5,
            type: 2,
            uid,
            side: orderSide === 'BID_ORDERS' ? 0 : 1,
            symbol,
            price,
            execution_price: orderBookData.price,
            quantity: remainingQuantity.toString(),
            execute_qty: matchedQuantityBig.toString(),
            hash,
            status: orderSideStatus,
          }));
          await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
            type: 2,
            uid,
            side: orderSide === 'BID_ORDERS' ? 0 : 1,
            symbol,
            price,
            execution_price: orderBookData.price,
            quantity: remainingQuantity.toString(),
            execute_qty: matchedQuantityBig.toString(),
            hash,
            status: orderSideStatus,
          }));
        }
        remainingQuantity = remainingQuantity.minus(matchedQuantityBig);
        matchedOrders.push({ price: orderBookData.price, quantity: matchedQuantityBig.toString() });

        await redisClient.multi().RPUSH(`${symbol}:MATCHED`, JSON.stringify({
          side: opposingSide === 'ASK_ORDERS' ? 1 : 0,
          ...orderBookData,
          remQty: new Big(orderBookData.quantity).minus(matchedQuantityBig).toString()
        })).RPUSH(`${symbol}:MATCHED`, JSON.stringify({
          side,
          uid,
          ts: Date.now(),
          hash,
          price,
          quantity: remainingQuantity.toString(),
          remQty: remainingQuantity.toString(),
        })).exec();
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


// import sendExecutionReportToKafka from './index.js';
// const path = require('path');
// const logFilePath = path.join('C:/Users/dell/Desktop/salman/orderbook/src', 'logs.txt');
// const winston = require('winston');
// const Big = require('big.js');

// const fileTransport = new winston.transports.File({ filename: logFilePath });

// fileTransport.on('error', (err) => {
//   console.error('Winston File Transport Error:', err);
// });

// const logger = winston.createLogger({
//   level: 'info',
//   format: winston.format.simple(),
//   transports: [fileTransport]
// });


// const matchOrder = async ({ hash, uid, side, symbol, price, quantity, redisClient }) => {
//   try {
//     console.log('side:', side, 'price:', price, 'quantity:', quantity)
//     const opposingSide = side === 0 ? 'ASK_ORDERS' : 'BID_ORDERS'; // Opposing side: 0 for Buy, 1 for Sell
//     const orderSide = side === 0 ? 'BID_ORDERS' : 'ASK_ORDERS'; // Order side: 0 for Buy, 1 for Sell
//     const opposingKey = `${symbol}:${opposingSide}`;
//     const orderKey = `${symbol}:${orderSide}`;

//     const ts = Date.now();
//     const adjustmentFactor = 1e10;
//     const numericPrice = parseFloat(price);

//     // const hash = randomUUID().substring(0, 25);
//     let remainingQuantity = Number(quantity);
//     console.log('remainingQuantityy: ', remainingQuantity)
//     const matchedOrders = [];

//     // Lua Script for matching orders
//     const luaScript = `
//       local function recalculateOrderBook(bookKey, orderSetKey)
//           redis.call("DEL", bookKey) -- Clear the ASK/BID list before recalculating
      
//           local orders = redis.call("ZRANGE", orderSetKey, 0, -1) -- Get all open orders
//           local priceMap = {}
      
//           for _, orderJson in ipairs(orders) do
//               local order = cjson.decode(orderJson)
//               local price = tostring(order.price)
//               local quantity = tonumber(order.quantity)
      
//               if priceMap[price] then
//                   priceMap[price] = priceMap[price] + quantity  -- Merge quantities for the same price
//               else
//                   priceMap[price] = quantity
//               end
//           end
      
//           -- Store updated ASK/BID list
//           for price, qty in pairs(priceMap) do
//               redis.call("ZADD", bookKey, price, cjson.encode({ price = price, quantity = qty }))
//           end
//       end
      
//       local orderRange = redis.call('ZRANGE', KEYS[1], ARGV[1], ARGV[1], 'WITHSCORES')
//       if next(orderRange) == nil then
//           local newOrder = {
//               uid = ARGV[6],
//               ts = ARGV[7],
//               hash = ARGV[8],
//               price = tonumber(ARGV[3]),
//               quantity = ARGV[2]
//           }
//           redis.call('ZADD', KEYS[2], ARGV[4], cjson.encode(newOrder))
      
//           -- Recalculate ASK/BID from the order books
//           if ARGV[5] == "SELL" then
//               recalculateOrderBook(KEYS[3], KEYS[2]) -- Update ASK list from ASK_ORDERS
//           else
//               recalculateOrderBook(KEYS[4], KEYS[2]) -- Update BID list from BID_ORDERS
//           end
      
//           return { 'ADDED', newOrder, 0 }
//       end
      
//       local orderBookData = cjson.decode(orderRange[1])
//       local orderBookPrice = tonumber(orderBookData.price)
//       local orderBookQuantity = orderBookData.quantity
//       local requestedPrice = tonumber(ARGV[3])
//       local requestedQuantity = ARGV[2]
//       local newScore = tonumber(ARGV[4])
      
//       -- Price check: Only match if the price conditions are met
//       if (ARGV[5] == "BUY" and orderBookPrice > requestedPrice) or (ARGV[5] == "SELL" and orderBookPrice < requestedPrice) then
//           local newOrder = {
//               uid = ARGV[6],
//               ts = ARGV[7],
//               hash = ARGV[8],
//               price = tonumber(ARGV[3]),
//               quantity = ARGV[2]
//           } 
//           redis.call('ZADD', KEYS[2], ARGV[4], cjson.encode(newOrder))
      
//           -- Recalculate ASK/BID from the order books
//           if ARGV[5] == "SELL" then
//               recalculateOrderBook(KEYS[3], KEYS[2])
//           else
//               recalculateOrderBook(KEYS[4], KEYS[2])
//           end
      
//           return { 'ADDED', newOrder, 1 }
//       end
      
//       -- Match or partially match the order
//       if tonumber(orderBookQuantity) <= tonumber(requestedQuantity) then
//           redis.call('ZREM', KEYS[1], orderRange[1])
      
//           -- Recalculate ASK/BID from order books
//           if ARGV[5] == "BUY" then
//               recalculateOrderBook(KEYS[3], KEYS[1])
//           else
//               recalculateOrderBook(KEYS[4], KEYS[1])
//           end
      
//           return { orderRange[1], 'MATCHED', orderBookQuantity }
//       else
//           orderBookData.quantity = tonumber(orderBookQuantity) - tonumber(requestedQuantity)
//           redis.call('ZREM', KEYS[1], orderRange[1])
//           redis.call('ZADD', KEYS[1], newScore, cjson.encode(orderBookData))
      
//           -- Recalculate ASK/BID for partially matched orders
//           if ARGV[5] == "BUY" then
//               recalculateOrderBook(KEYS[3], KEYS[1])
//           else
//               recalculateOrderBook(KEYS[4], KEYS[1])
//           end
      
//           return { orderRange[1], 'PARTIAL', requestedQuantity }
//       end`
    
//     try {
//       while (remainingQuantity > 0) {
//         console.log('remainingQuantity12: ', remainingQuantity)
//         const rangeArg = side === 0 ? 0 : -1;
//         const luaResult = await redisClient.eval(luaScript, {
//           keys: [opposingKey, orderKey,`${symbol}:ASK`, `${symbol}:BID`],
//           arguments: [
//             rangeArg.toString(),
//             remainingQuantity.toString(),
//             numericPrice.toString(),
//             (side === 0 ? (numericPrice - ts / adjustmentFactor) : (numericPrice + ts / adjustmentFactor)).toString(),
//             side === 0 ? "BUY" : "SELL",
//             uid,
//             ts.toString(),
//             hash
//           ]
//         });
//         if (!luaResult) {
//           return null;
//         };
//         if (luaResult[0] === 'ADDED') {
//           logger.info(JSON.stringify({
//             msg:1,
//             type: 1,
//             uid,
//             side,
//             symbol,
//             price,
//             quantity,
//             hash,
//             status: 'OPEN',
//             script: luaResult[2]
//           }));
//           await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
//             type: 1,
//             uid,
//             side,
//             symbol,
//             price,
//             quantity,
//             hash,
//             status: 'OPEN',
//             script: luaResult[2],
//           }));
//           return { order: orderKey, added: true };
//         }

//         const [orderBookJson, matchType, matchedQuantity] = luaResult;
//         const orderBookData = JSON.parse(orderBookJson);
        

//         console.log('matchType: ', matchType, 'matchedQuantity: ', matchedQuantity)

//         // Add trade details to Trade_history list in Redis
//         await redisClient.RPUSH(`${symbol}:trade_history`, JSON.stringify({
//           qty: matchedQuantity,
//           buyer_hash: side === 0 ? hash : orderBookData.hash,
//           seller_hash: side === 1 ? hash : orderBookData.hash,
//           price: orderBookData.price,
//           timestamp: Date.now(),
//           symbol,
//           type: side === 0 ? 'SELL' : 'BUY',
//         }));

//         if (matchType === 'MATCHED') {
//           logger.info(JSON.stringify({
//             msg:2,
//             type: 2,
//             side: opposingSide === 'ASK_ORDERS' ? 1 : 0,
//             symbol,
//             quantity: orderBookData.quantity,
//             uid: orderBookData.uid,
//             hash: orderBookData.hash,
//             price: orderBookData.price,
//             execution_price: orderBookData.price,
//             execute_qty: matchedQuantity,
//             status: 'FILLED',
//           }))
//           await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
//             type: 2,
//             side: opposingSide === 'ASK_ORDERS' ? 1 : 0,
//             symbol,
//             quantity: orderBookData.quantity,
//             uid: orderBookData.uid,
//             hash: orderBookData.hash,
//             price: orderBookData.price,
//             execution_price: orderBookData.price,
//             execute_qty: matchedQuantity,
//             status: 'FILLED',
//           }));
//           const orderSideStatus = matchedQuantity == remainingQuantity ? 'FILLED' : 'PARTIALLY_FILLED';
//           logger.info(JSON.stringify({
//             msg:3,
//             type: 2,
//             side,
//             symbol,
//             uid,
//             hash,
//             price,
//             execution_price: orderBookData.price,
//             quantity:remainingQuantity,
//             execute_qty: matchedQuantity,
//             status: orderSideStatus,
//           }))
//           await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
//             type: 2,
//             side,
//             symbol,
//             uid,
//             hash,
//             price,
//             execution_price: orderBookData.price,
//             quantity:remainingQuantity,
//             execute_qty: matchedQuantity,
//             status: orderSideStatus,
//           }));
//         } else if (matchType === 'PARTIAL') {
//           const oppsiteSideStatus = matchedQuantity == orderBookData.quantity ? 'FILLED' : 'PARTIALLY_FILLED';
//           logger.info(JSON.stringify({
//             msg:4,
//             type: 2,
//             side: opposingSide === 'ASK_ORDERS' ? 1 : 0,
//             symbol,
//             quantity: orderBookData.quantity,
//             uid: orderBookData.uid,
//             hash: orderBookData.hash,
//             price: orderBookData.price,
//             execution_price: orderBookData.price,
//             execute_qty: matchedQuantity,
//             status: oppsiteSideStatus,
//           }))
//           await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
//             type: 2,
//             side: opposingSide === 'ASK_ORDERS' ? 1 : 0,
//             symbol,
//             quantity: orderBookData.quantity,
//             uid: orderBookData.uid,
//             hash: orderBookData.hash,
//             price: orderBookData.price,
//             execution_price: orderBookData.price,
//             execute_qty: matchedQuantity,
//             status: oppsiteSideStatus,
//           }));
//           const orderSideStatus = matchedQuantity == remainingQuantity ? 'FILLED' : 'PARTIALLY_FILLED';
//           logger.info(JSON.stringify({
//             msg:5,
//             type: 2,
//             uid,
//             side: orderSide === 'BID_ORDERS' ? 0 : 1,
//             symbol,
//             price,
//             execution_price: orderBookData.price,
//             quantity: remainingQuantity, 
//             execute_qty: matchedQuantity,
//             hash,
//             status: orderSideStatus,
//           }))
//           await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
//             type: 2,
//             uid,
//             side: orderSide === 'BID_ORDERS' ? 0 : 1,
//             symbol,
//             price,
//             execution_price: orderBookData.price,
//             quantity: remainingQuantity, 
//             execute_qty: matchedQuantity,
//             hash,
//             status: orderSideStatus,
//           }));
//         }

//         console.log('remainingQuantity: ', remainingQuantity, 'matchedQuantity: ', matchedQuantity)
//         remainingQuantity -= matchedQuantity;
//         console.log('remainingQuantityyy: ', remainingQuantity)
//         matchedOrders.push({ price: orderBookData.price, quantity: matchedQuantity });


//         await redisClient.multi().RPUSH(`${symbol}:MATCHED`, JSON.stringify({
//           side: opposingSide === 'ASK_ORDERS' ? 1 : 0,
//           ...orderBookData,
//           remQty: orderBookData.quantity - matchedQuantity
//         })).RPUSH(`${symbol}:MATCHED`, JSON.stringify({
//           side,
//           uid,
//           ts: Date.now(),
//           hash,
//           price,
//           quantity: parseFloat(matchedQuantity) + parseFloat(remainingQuantity),
//           remQty: remainingQuantity,
//         })).exec();

//         // await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
//         //   type: 2,
//         //   uid,
//         //   side: opposingSide,
//         //   symbol,
//         //   price,
//         //   quantity,
//         //   execute_qty: matchedQuantity + remainingQuantity,
//         //   hash,
//         // }));
//         // await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
//         //   type: 2,
//         //   uid,
//         //   side,
//         //   symbol,
//         //   price,
//         //   quantity:remainingQuantity,
//         //   hash,
//         // }));
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


// export default matchOrder;












    // const luaScript = `
    //   local orderRange = redis.call('ZRANGE', KEYS[1], ARGV[1], ARGV[1], 'WITHSCORES')
    //   if next(orderRange) == nil then
    //       local newOrder = {
    //           uid = ARGV[6],
    //           ts = ARGV[7],
    //           hash = ARGV[8],
    //           price = tonumber(ARGV[3]),
    //           quantity = ARGV[2]
    //          }
    //       redis.call('ZADD', KEYS[2], ARGV[4], cjson.encode(newOrder))
    //       return { 'ADDED', newOrder,0 }
    //   end

    //   local orderBookData = cjson.decode(orderRange[1])
    //   local orderBookPrice = tonumber(orderBookData.price)
    //   local orderBookQuantity = orderBookData.quantity
    //   local requestedPrice = tonumber(ARGV[3])
    //   local requestedQuantity = ARGV[2]
    //   local newScore = tonumber(ARGV[4])

    //   -- Price check: Only match if the price conditions are met
    //   if (ARGV[5] == "BUY" and orderBookPrice > requestedPrice) or (ARGV[5] == "SELL" and orderBookPrice < requestedPrice) then
    //       local newOrder = {
    //           uid = ARGV[6],
    //           ts = ARGV[7],
    //           hash = ARGV[8],
    //           price = tonumber(ARGV[3]),
    //           quantity = ARGV[2]
    //          }
    //       redis.call('ZADD', KEYS[2], ARGV[4], cjson.encode(newOrder))
    //       return { 'ADDED', newOrder, 1 }
    //   end

    //   -- Match or partially match the order
    //   if tonumber(orderBookQuantity) <= tonumber(requestedQuantity) then
    //       redis.call('ZREM', KEYS[1], orderRange[1])
    //       return { orderRange[1], 'MATCHED', orderBookQuantity }
    //   else
    //       orderBookData.quantity = tonumber(orderBookQuantity) - tonumber(requestedQuantity)
    //       redis.call('ZREM', KEYS[1], orderRange[1])
    //       redis.call('ZADD', KEYS[1], newScore, cjson.encode(orderBookData))
    //       return { orderRange[1], 'PARTIAL', requestedQuantity }
    //   end
    // `;


