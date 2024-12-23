import protobuf from 'protobufjs'
import murmurhash from 'murmurhash'
import matchOrder from './matchOrder'
import  sendExecutionReportToKafka from './index.js'
import { randomUUID } from 'crypto'

export const messageTypes = {
  0: 'order',
  1: 'query',
  2: 'view',
}

const createMessageHandler = (uid,clientUids,redisClient) => async (message) => {
  const [messageType, data] = message.split('|')

  const proto = await protobuf.load('src/proto/order.proto')

  const type = messageTypes[messageType]



  try {
    switch (type) {
      case 'order':
        return await handleOrder({ data, clientUids, proto, redisClient })
      case 'query':
        return await handleQuery({ data, uid, proto, redisClient })
      case 'view':
        return await handleView({ data, uid, proto, redisClient })
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
//   const hash = randomUUID();

//   const matchedOrder = await matchOrder({ uid, side, symbol, price, quantity, redisClient });

//   if (matchedOrder) {
//     await redisClient.INCRBY('TOTAL_MATCHED', 2);
//     return {
//       type: 'match',
//       message: 'Order matched',
//       data: {
//         matchedBy: matchedOrder.uid,
//         matchedAt: Date.now(),
//         yourOrder: { order: orderString, uid, ts, hash },
//         matchedOrder,
//       },
//     };
//   } else {
//     return {
//       type: 'order',
//       message: 'Order submitted to queue',
//       data: {
//         order: orderString,
//         uid,
//         ts,
//         hash,
//         price: parseFloat(price),
//         quantity: parseFloat(quantity),
//       },
//     };
//   }
// };
const handleOrder = async ({ data,clientUids, proto, redisClient }) => {

  const OrderMessage = proto.lookupType('orderbook.Order')
  
  const buf = Buffer.from(data, 'base64')
  const decoded = OrderMessage.decode(buf)
  const message = OrderMessage.toObject(decoded)

  if (!clientUids.includes(message.uid)) {
    // console.log('message rejected : uid mismatch')
    return {
      type: 'order',
      error: 'Connected user ID does not match message user ID.',
    }
  }
  const uid = message.uid
  await redisClient.INCR('TOTAL_ORDERS')

  const { side, symbol, price, quantity } = message
  
  const ts = Date.now()
  const adjustmentFactor = 1e10;
  const orderString = `${side}:${symbol}`
  const hash = randomUUID().substring(0, 25);

  const matchedOrder = await matchOrder({ uid,side, symbol, price, quantity,redisClient })
  // console.log('ELAPSED TIME:', elapsedTime)
  // console.log('MATCHED ORDER:',side, matchedOrder)
  if (matchedOrder) {
    await redisClient.INCRBY('TOTAL_MATCHED', 2)
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
    // Ensure price is a valid number and not Infinity or NaN
    const numericPrice = parseFloat(price)
    const numericQuantity = parseFloat(quantity)

    if (isNaN(numericPrice) || !isFinite(numericPrice)) {
      // console.log('Error: Invalid price value:', price)
      return {
        type: 'order',
        error: 'Invalid price value.',
      }
    }

    try {
      console.log(orderString, {score:numericPrice - ts / adjustmentFactor,value:JSON.stringify({ uid, ts, hash, price: numericPrice,quantity:numericQuantity })})
      await  sendExecutionReportToKafka('trade-engine-message', JSON.stringify({type:1, uid,side, symbol, price, quantity:quantity,hash }))
      side === 0 ? await redisClient.ZADD(orderString, {score:numericPrice - ts / adjustmentFactor,value:JSON.stringify({ uid, ts, hash, price: numericPrice,quantity:numericQuantity })}) : 
      await redisClient.ZADD(orderString, {score:numericPrice + ts / adjustmentFactor,value:JSON.stringify({ uid, ts, hash, price: numericPrice,quantity:numericQuantity })})
 
    } catch (error) {
      console.error('Error adding to sorted set:', error)
      return {
        type: 'order',
        error: 'Failed to add order to sorted set.',
      }
    }

    return {
      type: 'order',
      message: 'Order submitted to queue',
      data: {
        order: orderString,
        uid,
        ts,
        hash,
        price: numericPrice,
        quantity:numericQuantity
      },
    }
  }
}


// const handleQuery = async ({ data, uid, proto, redisClient }) => {
//   const OrderMessage = proto.lookupType('orderbook.Query')

//   const buf = Buffer.from(data, 'base64')
//   const decoded = OrderMessage.decode(buf)
//   const message = OrderMessage.toObject(decoded)

//   if (uid !== message.uid) {
//     // console.log('message rejected: uid mismatch')
//     return { type: 'order', error: 'User IDs do not match' }
//   }

//   const { side, symbol } = message

//   const keys = await redisClient.KEYS(`${side}:${symbol}*`)
//   const lists = {}

//   for (const key of keys) {
//     const [, price] = key.split('@')
//     lists[price] = await redisClient.LLEN(key)
//   }

//   return { type: 'query', data: lists }
// }

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
