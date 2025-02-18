import { match } from 'assert';
import { randomUUID } from 'crypto';
import { type } from 'os';
import sendExecutionReportToKafka from './index.js';
import { exec } from 'child_process';

const matchOrder = async ({ uid, side, symbol, price, quantity, redisClient }) => {
  try {
    console.log('side:',side, 'price:',price, 'quantity:',quantity)
    const opposingSide = side === 0 ? 'ASK_ORDERS' : 'BID_ORDERS'; // Opposing side: 0 for Buy, 1 for Sell
    const orderSide = side === 0 ? 'BID_ORDERS' : 'ASK_ORDERS'; // Order side: 0 for Buy, 1 for Sell
    const opposingKey = `${symbol}:${opposingSide}`;
    const orderKey = `${symbol}:${orderSide}`;

    const ts = Date.now();
    const adjustmentFactor = 1e10;
    const numericPrice = parseFloat(price);

    const hash = randomUUID().substring(0, 25);
    let remainingQuantity = quantity;
    const matchedOrders = [];                               

    // Lua Script for matching orders
    const luaScript = `
      local orderRange = redis.call('ZRANGE', KEYS[1], ARGV[1], ARGV[1], 'WITHSCORES')
      if next(orderRange) == nil then
          local newOrder = {
              uid = ARGV[6],
              ts = ARGV[7],
              hash = ARGV[8],
              price = tonumber(ARGV[3]),
              quantity = tonumber(ARGV[2])
             }
          redis.call('ZADD', KEYS[2], ARGV[4], cjson.encode(newOrder))
          return { 'ADDED', newOrder,0 }
      end

      local orderBookData = cjson.decode(orderRange[1])
      local orderBookPrice = tonumber(orderBookData.price)
      local orderBookQuantity = tonumber(orderBookData.quantity)
      local requestedPrice = tonumber(ARGV[3])
      local requestedQuantity = tonumber(ARGV[2])
      local newScore = tonumber(ARGV[4])

      -- Price check: Only match if the price conditions are met
      if (ARGV[5] == "BUY" and orderBookPrice > requestedPrice) or (ARGV[5] == "SELL" and orderBookPrice < requestedPrice) then
          local newOrder = {
              uid = ARGV[6],
              ts = ARGV[7],
              hash = ARGV[8],
              price = tonumber(ARGV[3]),
              quantity = tonumber(ARGV[2])
             }
          redis.call('ZADD', KEYS[2], ARGV[4], cjson.encode(newOrder))
          return { 'ADDED', newOrder, 1 }
      end

      -- Match or partially match the order
      if orderBookQuantity <= requestedQuantity then
          redis.call('ZREM', KEYS[1], orderRange[1])
          return { orderRange[1], 'MATCHED', orderBookQuantity }
      else
          orderBookData.quantity = orderBookQuantity - requestedQuantity
          redis.call('ZREM', KEYS[1], orderRange[1])
          redis.call('ZADD', KEYS[1], newScore, cjson.encode(orderBookData))
          return { orderRange[1], 'PARTIAL', requestedQuantity }
      end
    `;
try{
    while (remainingQuantity > 0) {
      const rangeArg = side === 0 ? 0 : -1;
      const luaResult = await redisClient.eval(luaScript, {
        keys: [opposingKey, orderKey],
        arguments: [
          rangeArg.toString(),
          remainingQuantity.toString(),
          numericPrice.toString(),
          (side === 0 ? (numericPrice - ts / adjustmentFactor) : (numericPrice + ts / adjustmentFactor)).toString(),
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
          console.log('in added', luaResult[2])
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
        const orderBookData = JSON.parse(orderBookJson);
        const tradeId = await redisClient.INCR('trade_history:id');

        console.log('matchType: ',matchType, 'matchedQuantity: ',matchedQuantity)

        // Add trade details to Trade_history list in Redis
        await redisClient.RPUSH(`${symbol}:trade_history`, JSON.stringify({
          id: tradeId,
          qty: matchedQuantity,
          buyer_hash: side === 0 ? hash : orderBookData.hash,
          seller_hash: side === 1 ? hash : orderBookData.hash,
          price: orderBookData.price,
          timestamp: Date.now(),
          symbol,
        }));

        if (matchType === 'MATCHED') {
          await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
            type: 2,
            side: opposingSide === 'ASK_ORDERS' ? 1 : 0,
            symbol,
            quantity: orderBookData.quantity,
            uid: orderBookData.uid,
            hash: orderBookData.hash,
            price: orderBookData.price,
            execute_qty: matchedQuantity,
            status: 'FILLED',
          }));
          const orderSideStatus = matchedQuantity == quantity ? 'FILLED' : 'PARTIALLY_FILLED';
          await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
            type: 2,
            side,
            symbol,
            uid,
            hash,
            price,
            quantity,
            execute_qty: matchedQuantity,
            status: orderSideStatus,
          }));
        } else if (matchType === 'PARTIAL') { 

          await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
            type: 2,
            side: opposingSide === 'ASK_ORDERS' ? 1 : 0,
            symbol,
            quantity: orderBookData.quantity,
            uid: orderBookData.uid,
            hash: orderBookData.hash,
            price: orderBookData.price,
            execute_qty: matchedQuantity,
            status: 'PARTIALLY_FILLED',
          }));
          const orderSideStatus = matchedQuantity == quantity ? 'FILLED' : 'PARTIALLY_FILLED';
          await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
            type: 2,
            uid,
            side: orderSide === 'BID_ORDERS' ? 0 : 1,
            symbol,
            price,
            quantity,
            execute_qty: matchedQuantity,
            hash,
            status: orderSideStatus,
          }));
        }

        remainingQuantity -= matchedQuantity;
        matchedOrders.push({ price: orderBookData.price, quantity: matchedQuantity });


        await redisClient.multi().RPUSH(`${symbol}:MATCHED`, JSON.stringify({
          side: opposingSide === 'ASK_ORDERS' ? 1 : 0,
          ...orderBookData,
          remQty:orderBookData.quantity - matchedQuantity
        })).RPUSH(`${symbol}:MATCHED`, JSON.stringify({
          side,
          uid,
          ts: Date.now(),
          hash,
          price,
          quantity: matchedQuantity + remainingQuantity, 
          remQty:remainingQuantity,
        })).exec();

        // await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
        //   type: 2,
        //   uid,
        //   side: opposingSide,
        //   symbol,
        //   price,
        //   quantity,
        //   execute_qty: matchedQuantity + remainingQuantity,
        //   hash,
        // }));
        // await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
        //   type: 2,
        //   uid,
        //   side,
        //   symbol,
        //   price,
        //   quantity:remainingQuantity,
        //   hash,
        // }));
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


