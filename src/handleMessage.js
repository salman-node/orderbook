import Big from 'big.js'
import protobuf from 'protobufjs'
import murmurhash from 'murmurhash'
import matchOrder from './matchOrderMain.js'
import sendExecutionReportToKafka from './index.js'
import { randomUUID } from 'crypto'

export const messageTypes = {
  0: 'order',
  1: 'query',
  2: 'view',
  3: 'cancelOrder',
}

const createMessageHandler = (uid, clientUids, redisClient) => async (message) => {
  const [messageType, data] = message.split('|')

  const proto = await protobuf.load('src/proto/order.proto')

  const type = messageTypes[messageType]

  console.log('messageType:', messageType, 'type:', type)

  try {
    switch (type) {
      case 'order':
        return await handleOrder({ data, clientUids, proto, redisClient })
      case 'query':
        return await handleQuery({ data, uid, proto, redisClient })
      case 'view':
        return await handleView({ data, uid, proto, redisClient })
      case 'cancelOrder':
        return await handleCancelOrder({ data, clientUids, proto, redisClient })
      default:
        return {
          type: 'unknown',
          error: `Message type “${messageType}” unknown`,
        }
    }
  } catch (e) {
    return {
      type,
      error: `Failed to handle message. Actual error: ${e.message}`,
    }
  }
}

const handleOrder = async ({ data, clientUids, proto, redisClient }) => {

  const OrderMessage = proto.lookupType('orderbook.Order')

  const buf = Buffer.from(data, 'base64')
  const decoded = OrderMessage.decode(buf)
  const message = OrderMessage.toObject(decoded)

  if (!clientUids.includes(message.uid)) {
    console.log('message rejected : uid mismatch')
    return {
      type: 'order',
      error: 'Connected user ID does not match message user ID.',
    }
  }
  const uid = message.uid

  const { hash, side, symbol, price, quantity, order_type } = message

  const ts = Date.now()
  const orderString = `${side}:${symbol}`
  // const hash = randomUUID().substring(0, 25);

  // Use Big.js to safely handle precision
  const bigPrice = new Big(price)
  const bigQuantity = new Big(quantity)

  const matchedOrder = await matchOrder({ hash, uid, side, symbol, price: bigPrice.toString(), quantity: bigQuantity.toString(), order_type, redisClient })
  // console.log('ELAPSED TIME:', elapsedTime)
  // console.log('MATCHED ORDER:',side, matchedOrder)
  if (matchedOrder) {
    return {
      type: 'match',
      message: 'Order matched',
      data: {
        matchedBy: matchedOrder.uid,
        matchedAt: Date.now(),
        yourOrder: { order: orderString, uid, ts, hash },
        matchedOrder,
      },
    }
  } else {

    return {
      type: 'order',
      message: 'Order submitted to queue',
      data: {
        order: orderString,
        uid,
        ts,
        hash,
        price: bigPrice.toString(),
        quantity: bigQuantity.toString(),
      },
    }
  }
}

const handleCancelOrder = async ({ data, clientUids, proto, redisClient }) => {
  try {
    const OrderMessage = proto.lookupType('orderbook.CancelOrder')
    const buf = Buffer.from(data, 'base64')
    const decoded = OrderMessage.decode(buf)
    const message = OrderMessage.toObject(decoded)

    if (!clientUids.includes(message.uid)) {
      console.log('message rejected: uid mismatch')
      return { type: 'order', error: 'Connected user ID does not match message user ID.' }
    }
    const { side, symbol, orderId, price } = message
    const orderType = side === 0 ? 'BID_ORDERS' : 'ASK_ORDERS'
    const bookType = side === 0 ? 'BID' : 'ASK'

    const orderString = `${symbol}:${orderType}:${parseFloat(price)}` // Individual user orders
    const priceSet = `${symbol}:${orderType}` // Sorted set for price levels
    const bookString = `${symbol}:${bookType}`   // Aggregated order book

    const orders = await redisClient.LRANGE(orderString, 0, -1); // Get all orders for the user
    
    const orderToRemove = orders.find(order => JSON.parse(order).hash === orderId);

  if (orderToRemove) {
    const removed = await redisClient.LREM(orderString, 0, orderToRemove); 
    if (!removed) {
      console.log(`ERRR: Order with hash ${orderId} not found in ${orderString}`);
      return { type: 'cancelOrder', error: 'Order not found' };
    }
    console.log(`Removed order with hash: ${orderId} from ${orderString}`);
  } else {
    console.log(`No order with hash: ${orderId} found in ${orderString}`);
    return { type: 'cancelOrder', error: 'Order not found' }; // No need to continue if the order doesn't exist
  }

  // Step 2: Check if the list is empty
  const listLength = await redisClient.LLEN(orderString);
   
  if (listLength === 0) {
    const members = await redisClient.ZRANGEBYSCORE(priceSet, parseFloat(price), parseFloat(price));
    // List is empty, so remove the price from the sorted set
    const removed = await redisClient.ZREM(priceSet, members);
    if(!removed) {
      console.log(`ERRR: Price ${price} not found in sorted set ${priceSet}`);
      return { type: 'cancelOrder', error: 'Price not found in sorted set' }
    }
    console.log(`Removed price: ${price} from sorted set ${priceSet}`);
  } else {
    console.log(`List at ${orderString} is not empty. Price ${price} remains in sorted set.`);
  }

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

    recalculateOrderBook(KEYS[1], KEYS[2])
 `;
   console.log(priceSet, bookString)
    await redisClient.eval(luaScript, { numberOfKeys: 2, keys: [bookString, priceSet] });


    const parsedOrder = JSON.parse(orderToRemove)

    console.log('message accepted: order cancelled')
    await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
      type: 4, uid: message.uid, side,price:parsedOrder.price,quantity:parsedOrder.quantity, symbol, hash: orderId, data: orderToRemove
    }))

    return { type: 'cancelOrder', message: 'Order cancelled' }

  } catch (e) {
    console.log('error:', e)
    return { type: 'cancelOrder', error: 'Internal server error' }
  }
}

// const handleCancelOrder = async ({ data, clientUids, proto, redisClient }) => {
//   try {
//     const OrderMessage = proto.lookupType('orderbook.CancelOrder')
//     const buf = Buffer.from(data, 'base64')
//     const decoded = OrderMessage.decode(buf)
//     const message = OrderMessage.toObject(decoded)

//     if (!clientUids.includes(message.uid)) {
//       console.log('message rejected: uid mismatch')
//       return { type: 'order', error: 'Connected user ID does not match message user ID.' }
//     }

//     const { side, symbol, orderId } = message
//     const orderType = side === 0 ? 'BID_ORDERS' : 'ASK_ORDERS'
//     const bookType = side === 0 ? 'BID' : 'ASK'

//     const orderString = `${symbol}:${orderType}` // Individual user orders
//     const bookString = `${symbol}:${bookType}`   // Aggregated order book
//     console.log('orderString', orderString)
    
//     // Get user's orders
//     const orders = await redisClient.ZRANGE(orderString, 0, -1)
//     const order = orders.find((o) => {
//       const { hash, uid } = JSON.parse(o)
//       return hash === orderId && uid === message.uid
//     })
//     console.log('order: ', order)
//     if (!order) {
//       console.log('message rejected: order not found')
//       return { type: 'cancelOrder', error: 'Order not found' }
//     }

//     // Remove order from user's sorted set
//     await redisClient.ZREM(orderString, order)

//     const luaScript = `
//     local function recalculateOrderBook(bookKey, orderSetKey)
//         redis.call("DEL", bookKey) -- Clear the ASK/BID list before recalculating
    
//         local orders = redis.call("ZRANGE", orderSetKey, 0, -1) -- Get all open orders
//         local priceMap = {}
    
//         for _, orderJson in ipairs(orders) do
//             local order = cjson.decode(orderJson)
//             local price = tostring(order.price)
//             local quantity = tonumber(order.quantity)
    
//             if priceMap[price] then
//                 priceMap[price] = priceMap[price] + quantity  -- Merge quantities for the same price
//             else
//                 priceMap[price] = quantity
//             end
//         end
    
//         -- Store updated ASK/BID list
//         for price, qty in pairs(priceMap) do
//             redis.call("ZADD", bookKey, price, cjson.encode({ price = price, quantity = qty }))
//         end
//     end

//     recalculateOrderBook(KEYS[1], KEYS[2])
// `;
//    console.log(orderString, bookString)
//     await redisClient.eval(luaScript, { numberOfKeys: 2, keys: [bookString, orderString] });

//     //     // Parse order data
//     //     const { price, quantity } = JSON.parse(order)
//     //     const bigPrice = new Big(price)
//     //     const bigQuantity = new Big(quantity)

//     //     // Fetch the current aggregated price entry from BID/ASK
//     //     const bookOrders = await redisClient.ZRANGE(bookString, 0, -1)
//     //     const matchingOrder = bookOrders.find((o) => JSON.parse(o).price == bigPrice.toString())
//     // console.log('matchingOrder', matchingOrder)
//     // if (matchingOrder) {
//     //   let bookData = JSON.parse(matchingOrder)

//     //   let updatedQuantity = new Big(bookData.quantity).minus(bigQuantity) // Safe subtraction
//     //   console.log('updatedQuantity', updatedQuantity.toString())

//     //   if (updatedQuantity.lte(0)) { // If quantity is zero or negative, remove the price level
//     //     console.log('Quantity is zero or negative, removing:', updatedQuantity.toString())
//     //     await redisClient.ZREM(bookString, matchingOrder)
//     //   } else {
//     //     console.log('Updating quantity to:', updatedQuantity.toString())

//     //     bookData.quantity = updatedQuantity.toString()
//     //     console.log('bookString', bookString)
//     //     console.log('bigPrice', bigPrice)
//     //     console.log('bookData', bookData)
//     //     await redisClient.ZREM(bookString, matchingOrder)
//     //     // await redisClient.ZADD(bookString, bigPrice.toString(), JSON.stringify(bookData))
//     //   }
//     // }

//     const parsedOrder = JSON.parse(order)

//     console.log('message accepted: order cancelled',{
//       type: 4, uid: message.uid, side,price:parsedOrder.price,quantity:parsedOrder.quantity, symbol, hash: orderId, data: order
//     })
//     await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({
//       type: 4, uid: message.uid, side,price:parsedOrder.price,quantity:parsedOrder.quantity, symbol, hash: orderId, data: order
//     }))

//     return { type: 'cancelOrder', message: 'Order cancelled' }

//   } catch (e) {
//     console.log('error:', e)
//     return { type: 'cancelOrder', error: 'Internal server error' }
//   }
// }



// const handleCancelOrder = async ({ data, clientUids, proto, redisClient }) => {
//  try{ 
//   const OrderMessage = proto.lookupType('orderbook.CancelOrder')
//   const buf = Buffer.from(data, 'base64')
//   const decoded = OrderMessage.decode(buf)
//   const message = OrderMessage.toObject(decoded)

//   if (!clientUids.includes(message.uid)) {
//     console.log('message rejected : uid mismatch')
//     return {
//       type: 'order',
//       error: 'Connected user ID does not match message user ID.',
//     }
//   }

//     const { side, symbol, orderId } = message

//     const orderType = side === 0 ? 'BID_ORDERS' : 'ASK_ORDERS'
//     const orderString = `${symbol}:${orderType}`
//     const orders = await redisClient.ZRANGE(orderString, 0, -1)

//     const order = orders.find((o) => {
//       const { hash, uid } = JSON.parse(o);
//       return hash === orderId && uid === message.uid;
//     });
//     if (!order) {
//       console.log('message rejected: order not found')
//       return { type: 'cancelOrder', error: 'Order not found' }
//     } else {
//       await redisClient.ZREM(orderString,order)
//       console.log('message accepted: order cancelled')
//       await sendExecutionReportToKafka('trade-engine-message', JSON.stringify({type:4,uid: message.uid,side, symbol, hash:orderId,data:order }))
//       return { type: 'cancelOrder', message: 'Order cancelled' }        
//     }
//     }catch(e){
//       console.log('error:', e)
//     }

//   }

const handleView = async ({ data, uid, proto, redisClient }) => {
  const OrderMessage = proto.lookupType('orderbook.View')

  const buf = Buffer.from(data, 'base64')
  const decoded = OrderMessage.decode(buf)
  const message = OrderMessage.toObject(decoded)

  if (uid !== message.uid) {
    // console.log('message rejected: uid mismatch')
    return { type: 'order', error: 'User IDs do not match' }
  }

  const { side, symbol, price, start, stop } = message

  const orders = await redisClient.LRANGE(
    `${side}:${symbol}@${price}`,
    start,
    stop
  )

  return { type: 'query', data: orders.map((o) => JSON.parse(o)) }
}

export default createMessageHandler



