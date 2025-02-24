import protobuf from 'protobufjs'
import murmurhash from 'murmurhash'
import matchOrder from './matchOrder1.js'
// import  sendExecutionReportToKafka from './index.js'
import { randomUUID } from 'crypto'

export const messageTypes = {
  0: 'order',
  1: 'query',
  2: 'view',
  3: 'cancelOrder',
}

const createMessageHandler = (uid,clientUids,redisClient) => async (message) => {
  const [messageType, data] = message.split('|')

  const proto = await protobuf.load('src/proto/order.proto')

  const type = messageTypes[messageType]

  console.log('messageType:', messageType, 'type:', type) 

  try {
    switch (type) {
      case 'order':
        return await handleOrder({ data, clientUids, proto,redisClient })
      case 'query':
        return await handleQuery({ data, uid, proto, redisClient })
      case 'view':
        return await handleView({ data, uid, proto, redisClient })
      case 'cancelOrder':
        return await handleCancelOrder({ data, uid, proto, redisClient })
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

const handleOrder = async ({ data,clientUids, proto, redisClient }) => {

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

  const { hash,side, symbol, price, quantity, order_type } = message
  
  const ts = Date.now()
  const adjustmentFactor = 1e10;
  const orderString = `${side}:${symbol}`
  // const hash = randomUUID().substring(0, 25);

  const matchedOrder = await matchOrder({ uid,side, symbol, price, quantity,order_type,redisClient })
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

    // const transaction = redisClient.multi();
    // const score = side === 0 ? numericPrice - ts / adjustmentFactor : numericPrice + ts / adjustmentFactor;

    try {
      // await  sendExecutionReportToKafka('trade-engine-message', JSON.stringify({type:1, uid,side, symbol, price, quantity:quantity,hash }))
      // transaction.ZADD(orderString, { score, value: JSON.stringify({ uid, ts, hash, price: numericPrice, quantity: numericQuantity }) });
      // await transaction.exec(); // Execute the transaction
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

const handleCancelOrder = async ({ data, uid, proto, redisClient }) => {
 try{ 
  const OrderMessage = proto.lookupType('orderbook.CancelOrder')
  const buf = Buffer.from(data, 'base64')
  const decoded = OrderMessage.decode(buf)
  const message = OrderMessage.toObject(decoded)

  if (uid !== message.uid) {
    console.log('message rejected: uid mismatch')
    return { type: 'cancelOrder', error: 'User IDs do not match' }
  } else {
    const { side, symbol, orderId } = message
    const orderType = side === 0 ? 'BID_ORDERS' : 'ASK_ORDERS'
    const orderString = `${symbol}:${orderType}`
    const orders = await redisClient.ZRANGE(orderString, 0, -1)
    const order = orders.find((o) => JSON.parse(o).hash === orderId)
    if (!order) {
      console.log('message rejected: order not found')
      return { type: 'cancelOrder', error: 'Order not found' }
    } else {
      await redisClient.ZREM(orderString,order)
      console.log('message accepted: order cancelled')
      return { type: 'cancelOrder', message: 'Order cancelled' }        
    }
    }}catch(e){
      console.log('error:', e)
    }

  }

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



