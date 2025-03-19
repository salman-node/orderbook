import dotenv from 'dotenv';
dotenv.config();
import express from 'express';
const app = express();
app.use(express.json({extended: true}));
import protobuf from 'protobufjs';
import base64 from 'base64-js';
import {  Get_Where_Universal_Data} from './db_query.js'; 
import WebSocket from 'ws';
import { randomUUID } from 'crypto';
import connection from './db_connection.js';
import { createClient } from 'redis';
import cors from 'cors';
import RedisClient from 'redis/dist/lib/client/index.js';
import { parse } from 'path';
app.use(cors());
const wsClients = new Map();

const redisClient = createClient({
  username: process.env.REDIS_USER || 'default',
  password: process.env.REDIS_AUTH || 'blE32GqYBT9dHDyopO1tiG10AKOuW0C8',
  socket: {
      host: process.env.REDIS_HOST || 'redis-18514.c322.us-east-1-2.ec2.redns.redis-cloud.com',
      port: process.env.REDIS_PORT || 18514
  }
});
redisClient.on('error', (err) => console.log('Redis Client Error', err));

await redisClient.connect()

// const wsClient = new WebSocket(`ws://0.0.0.0:5000?user=15`);
// const wsClient1 = new WebSocket(`ws://0.0.0.0:5000?user=10`);

// wsClient1.on('open', () => {
//   console.log('Connection established with bob');
//   wsClients.set(15, wsClient1);
// });

// wsClient.on('open', () => {
//     console.log('Connection established with alice');
//     wsClients.set(10, wsClient);
// });

function connectWebSocket(userId) {
  if (!wsClients.has(userId)) {
    const ws = new WebSocket(`ws://0.0.0.0:5000?user=${userId}`);

    ws.on('open', () => console.log(`WebSocket connected for user ${userId}`));
    
    ws.on('close', () => {
      console.log(`WebSocket closed for user ${userId}`);
      wsClients.delete(userId);
    });

    ws.on('error', (err) => {
      console.error(`WebSocket error for user ${userId}:`, err);
      wsClients.delete(userId);
    });

    wsClients.set(userId, ws);
  }
  return wsClients.get(userId);
}


app.post('/connect-websocket', (req, res) => {
  const { userId } = req.body;
  
  if (!userId || !Number.isInteger(userId)) {
    return res.status(400).send({ message: "Invalid user ID." });
  }

  connectWebSocket(userId);
  return res.send({ message: `WebSocket connected for user ${userId}` });
});


app.post('/place-order', async (req, res) => {
  const conn = await connection.getConnection();
  try{
  const { uid, side, pair_id, price, quantity,order_type } = req.body;

 if ([uid, side, pair_id, price, quantity, order_type].some(val => val === undefined || val === null)) {
  return res.status(400).send({ message: 'send all request data.' });
}
if (
  !Number.isInteger(uid) ||
  ![0, 1].includes(side) ||
  !Number.isInteger(pair_id) ||
  !(typeof price === "number" && price > 0) ||
  !(typeof quantity === "number" && quantity > 0) ||
  order_type !== "limit"
) {
  return res.status(400).send({ message: "Invalid request data." });
}

  const wsClient = wsClients.get(uid);
  if (!wsClient || wsClient.readyState !== WebSocket.OPEN) {
    return res.status(400).send({ message: 'WebSocket connection not established. Please refresh the Trade page.' });
  }

  const [pair_data] = await conn.execute('select base_asset_id,quote_asset_id,status,trade_status from crypto_pair where id=?',[pair_id])
  const pairData = pair_data[0]
  const base_asset_id = pairData.base_asset_id
  const quote_asset_id = pairData.quote_asset_id

  if (pairData == undefined) {
    return res.status(400).send({ message: 'Invalid symbol.' });  
  }
 
  if(pairData.status != "Active" ||  pairData.trade_status != 1){
    return res.status(400).send({ message: 'Currently trade is not active for this pair.' }); 
  }
  const check_balance_asset = side === 0 ? quote_asset_id : base_asset_id
 
  const order_amount = side === 0 ? parseFloat(price) * parseFloat(quantity) : quantity
  const order_amount_for_fee = parseFloat(price) * parseFloat(quantity);

  const trade_fee = await Get_Where_Universal_Data('fees_percent_inr,tds_percent_inr','settings_fees',{id : 1})

  const fee_percent = trade_fee[0].fees_percent_inr
  const tds_percent = trade_fee[0].tds_percent_inr

  const fee = (parseFloat(order_amount_for_fee) * parseFloat(fee_percent)) / 100
  const order_amount_after_execution =  parseFloat(order_amount_for_fee) - parseFloat(fee)
  const tds = side === 0 ? 0 : (parseFloat(order_amount_after_execution) * parseFloat(tds_percent)) / 100

  const order_amount_with_fee = side === 0 ? parseFloat(order_amount) + parseFloat(fee) : parseFloat(order_amount)
  const order_amount_estimate_fee = side === 0 ?  parseFloat(order_amount_for_fee) + parseFloat(fee) : (parseFloat(order_amount_for_fee) - parseFloat(fee)) - parseFloat(tds)
  await conn.beginTransaction();
  const [user_balance] = await conn.query('select balance from balances where user_id = ? and coin_id = ? FOR UPDATE',[uid,check_balance_asset])

    if(user_balance.length == 0){
      await conn.rollback();
      return res.status(400).send({ message: 'Insufficient balance.' });
    }
    if(Number(order_amount_with_fee) > Number(user_balance[0].balance)){
      await conn.rollback();
      return res.status(400).send({ message: 'Insufficient balance.' }); 
    }

  const proto = protobuf.loadSync('./orderBook.proto');  // Load your .proto file

  const OrderMessage = proto.lookupType('orderbook.Order');  // Lookup your message type

  const hash = randomUUID().substring(0, 25);

  const message = OrderMessage.create({
    "uid": uid.toString(),
    "side": side,
    "symbol": pair_id.toString(),  
    "price": parseFloat(price),
    "quantity": parseFloat(quantity),
    "hash": hash
  });

  // Verify the message before encoding
  const errMsg = OrderMessage.verify(message);
  if (errMsg){
    console.error("Error::", errMsg);
    return;
  }

  // Encode the message into a buffer
  const buffer = OrderMessage.encode(message).finish();

  // Convert the binary buffer to base64 string
  const base64Message = base64.fromByteArray(buffer);
  const finalMessage = `0|${base64Message}`

  //insert into orderbook_open_orders
 const insert_order = await conn.query('insert into orderbook_open_orders (order_id,pair_id,quantity,execute_qty,user_id,coin_id,coin_base,type,price,amount,tds,fees,final_amount,order_type,status,date_time) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',  
    [hash,pair_id,quantity,0,uid,base_asset_id,quote_asset_id,side === 0 ? 'BUY' : 'SELL',price,parseFloat(price) * parseFloat(quantity),tds,fee,order_amount_estimate_fee,"LIMIT","PENDING",Date.now()]
  )

  if(insert_order.affectedRows == 0){
    await conn.rollback();
    return res.status(400).send({ message: 'Failed to create order.' });
  }


 const insert_transaction = await conn.query('insert into transactions (user_id,coin_id,amount,opening,closing,order_id,type,remarks,txn_id,date_time) values (?,?,?,?,?,?,?,?,?,?)',
    [uid,side == 0 ? quote_asset_id : base_asset_id,order_amount_with_fee,user_balance[0].balance,user_balance[0].balance - order_amount_with_fee,hash,'Dr',side === 0 ? 'Buy' : 'Sale',hash,Date.now()]
  )

  if(insert_transaction.affectedRows == 0){
    await conn.rollback();
    return res.status(400).send({ message: 'Failed to create order.' });
  }

  //debit user balance 
  // const [result] = await conn.query('UPDATE balances SET balance = balance - ? WHERE user_id = ? AND coin_id = ?',[parseFloat(order_amount_with_fee),uid,check_balance_asset])
  const result = await conn.query(
    'INSERT INTO balances (user_id, coin_id, balance) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE balance = balance - VALUES(balance)',
    [uid, check_balance_asset, order_amount_with_fee]
  );

  if(result.affectedRows !== 0){
    wsClient.send(finalMessage);
    await conn.query('INSERT INTO balances_inorder (user_id, coin_id, balance) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance)',[uid,side == 0 ? quote_asset_id : base_asset_id,order_amount_with_fee])
    await conn.commit(); 
    return res.status(200).send({ message: 'Order placed successfully',data:{order_id:hash} });
  }
  await conn.rollback();
  return res.status(400).send({ message: 'Error while placing order.' });

}catch(err){
  await conn.rollback();
  console.log(err)
  return res.status(400).send({ message: 'Error while placing order.' , error:err});
}finally{
  conn.release();
}
});



app.post('/cancel-order', async (req, res) => {
  const { uid, side, pair_id, orderId } = req.body;
  if ([uid, side, pair_id, orderId].some(val => val === undefined || val === null)) {
    return res.status(400).send({ message: 'send all request data.' });
  }
  if (
    !Number.isInteger(uid) ||
    ![0, 1].includes(side) ||
    !Number.isInteger(pair_id) 
  ) {
    return res.status(400).send({ message: "Invalid request data." });
  }
  const proto = protobuf.loadSync('./orderBook.proto');  // Load your .proto file
  // console.log(proto);
  const OrderMessage = proto.lookupType('orderbook.CancelOrder');  // Lookup your message type
  // console.log(OrderMessage);
  const message = OrderMessage.create({
    uid : uid,
    orderId : orderId,
    side : parseInt(side),
    symbol : symbol,
  });
  console.log(message)
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
  const finalMessage = `3|${base64Message}`
  console.log(finalMessage)
  wsClient.send(finalMessage);
  return res.send({ message: 'Order placed successfully' });
});


async function getOrderBookData(pairId) {
  try {
    // Fetch top 5 ASK (lowest prices)
    const askData = await redisClient.zRangeWithScores(`${pairId}:ASK`, 0, 4);
    
    // Fetch top 5 BID (highest prices)
    const bidData = await redisClient.zRangeWithScores(`${pairId}:BID`, -5, -1);

    return {
      asks: askData.map(({ score, value }) => {
        const order = JSON.parse(value);
        return { price: parseFloat(order.price), quantity: parseFloat(order.quantity) };
      }),
      bids: bidData.reverse().map(({ score, value }) => {
        const order = JSON.parse(value);
        return { price: parseFloat(order.price), quantity: parseFloat(order.quantity) };
      }),
    };
  } catch (error) {
    console.error("Error fetching order book data:", error);
    return { asks: [], bids: [] };
  }
}



app.get("/order-book/:pairId", async (req, res) => {
  const pairId = req.params.pairId;
  const data = await getOrderBookData(pairId);
  res.status(200).send(data);
});

app.get('/trade-history/:pairId', async (req, res) => {
  try {
    const { pairId } = req.params;
    const trades = await redisClient.lRange(`${pairId}:trade_history`, -5, -1); // Fetch all trades

    const tradeHistory = trades.map(trade => {
      const parsedTrade = JSON.parse(trade);
      return {
        price: parsedTrade.price,
        qty: parsedTrade.qty,
        time: parsedTrade.timestamp,
        type: parsedTrade.type
      };
    });

    res.json(tradeHistory);
  } catch (error) {
    console.error('Error fetching trade history:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.get("/market-summary/:pairId", async (req, res) => {
  try {
    const { pairId } = req.params;
    const now = Date.now();
    const oneDayAgo = now - 24 * 60 * 60 * 1000;

    // Fetch last 1000 trades (adjust count as needed)
    const tradeHistory = await redisClient.lRange(`${pairId}:trade_history`, -1000, -1);

    if (!tradeHistory.length) {
      return res.json({ currentPrice: 0, low24h: 0, high24h: 0, volume24h: 0 });
    }

    // Parse and filter trades within 24 hours
    const trades = tradeHistory
      .map((trade) => JSON.parse(trade))
      .filter((trade) => trade.timestamp >= oneDayAgo);

    if (!trades.length) {
      return res.json({ currentPrice: 0, low24h: 0, high24h: 0, volume24h: 0 });
    }

    // Extract necessary values
    const currentPrice = trades[trades.length - 1].price; // Last trade price
    const low24h = Math.min(...trades.map((trade) => trade.price));
    const high24h = Math.max(...trades.map((trade) => trade.price));
    const volume24h = trades.reduce((sum, trade) => sum + parseFloat(trade.qty), 0);

    const openingPrice = trades[0].price;

  // Calculate 24h change percentage
   const change24h = openingPrice
    ? ((currentPrice - openingPrice) / openingPrice * 100).toFixed(2)
    : "0.00";

  res.json({ currentPrice, low24h, high24h, volume24h, change24h });


  } catch (error) {
    console.error("Error fetching market summary:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});
const INTERVAL_MAP = {
  '1m': 1,
  '5m': 5,
  '15m': 15,
  '1h': 60,
  '1d': 1440,
  '1w': 10080
};

async function getOHLC(interval, pairId, startTime, endTime, numCandles = 100) {
  try {
    const mappedInterval = INTERVAL_MAP[interval];
    if (!mappedInterval) {
      throw new Error("Invalid interval format");
    }

    // If startTime and endTime are not provided, calculate them
    if (!startTime || !endTime) {
      const now = Date.now();
      endTime = now;
      startTime = endTime - numCandles * mappedInterval * 60 * 1000;
    }

    const query = `
      SELECT price, quantity, date_time 
      FROM buy_sell 
      WHERE pair_id = ? AND date_time BETWEEN ? AND ?
      ORDER BY date_time ASC;
    `;
    
    const [rows] = await connection.execute(query, [pairId, startTime, endTime]);

    const candles = {};
    rows.forEach(({ price, quantity, date_time }) => {
      const timestamp = Math.floor(date_time / (mappedInterval * 60 * 1000)) * (mappedInterval * 60 * 1000);
      if (!candles[timestamp]) {
        candles[timestamp] = {
          open: parseFloat(price),
          high: parseFloat(price),
          low: parseFloat(price),
          close: parseFloat(price),
          volume: 0,
          time: timestamp
        };
      }
      candles[timestamp].high = Math.max(candles[timestamp].high, price);
      candles[timestamp].low = Math.min(candles[timestamp].low, price);
      candles[timestamp].close = parseFloat(price);
      candles[timestamp].volume += parseFloat(quantity);
    });

    // Convert object to sorted array
    let sortedCandles = Object.values(candles).sort((a, b) => a.time - b.time);

    // If no trades exist, return a default candle
    if (sortedCandles.length === 0) {
      const lastPriceQuery = `SELECT price FROM buy_sell WHERE pair_id = ? ORDER BY date_time DESC LIMIT 1;`;
      const [lastPriceRow] = await connection.execute(lastPriceQuery, [pairId]);

      const lastPrice = lastPriceRow.length > 0 ? parseFloat(lastPriceRow[0].price) : 0;
      const lastCandleTime = Math.floor(endTime / (mappedInterval * 60 * 1000)) * (mappedInterval * 60 * 1000);

      sortedCandles = [{
        open: lastPrice,
        high: lastPrice,
        low: lastPrice,
        close: lastPrice,
        volume: 0,
        time: lastCandleTime
      }];
    }

    return sortedCandles.slice(-numCandles);

  } catch (error) {
    console.error("Error fetching OHLC data:", error);
    return [];
  }
}


// API endpoint
app.get("/candlestick/:pairId", async (req, res) => {
  const { pairId } = req.params;
  const { interval, startTime, endTime, numCandles = 100 } = req.query;

  if (!interval) {
    return res.status(400).json({ error: "Missing required parameter: interval" });
  }
  
  const data = await getOHLC(interval, pairId, parseInt(startTime) || null, parseInt(endTime) || null, parseInt(numCandles));
  res.status(200).json(data);
});

// Example request:
// http://localhost:9696/candlestick/:pairId?interval=1m&startTime=1742236200000&endTime=1742238200000&numCandles=100



app.listen(9696, () => {
  console.log('Server started on port 9696');
});




// app.post('/place-order', async (req, res) => {
//   try{
//   const { uid, side, symbol, price, quantity,order_type } = req.body;
 
//   if(!uid && !side && !symbol && !price && !quantity && !order_type) {
//     return res.status(400).send({ message: 'send all request data.' });
//   }
  
//   const [base_asset,quote_asset] = symbol.split('/')

//   const asset_data = await raw_query('select id ,status,trade_status from currencies where symbol IN(?,?)',[base_asset,quote_asset])

//   const base_asset_data = asset_data[0]
//   const quote_asset_data = asset_data[1]

//   if (base_asset_data == undefined || quote_asset_data == undefined) {
//     return res.status(400).send({ message: 'Invalid symbol.' });  
//   }

//   if(base_asset_data.status != "Active" || quote_asset_data.status != 'Active' && base_asset_data.trade_status != 1 || quote_asset_data.trade_status != 1){
//     return res.status(400).send({ message: 'Currently trade is not active for this pair.' }); 
//   }
//   const check_balance_asset = side === 0 ? quote_asset_data.id : base_asset_data.id
//   const order_amount = side === 0 ? parseFloat(price) * parseFloat(quantity) : quantity
//   const order_amount_for_fee = parseFloat(price) * parseFloat(quantity);

//   const trade_fee = await Get_Where_Universal_Data('fees_percent_inr,tds_percent_inr','settings_fees',{id : 1})

//   const fee_percent = trade_fee[0].fees_percent_inr
//   const tds_percent = trade_fee[0].tds_percent_inr

//   const fee = (parseFloat(order_amount_for_fee) * parseFloat(fee_percent)) / 100
//   const order_amount_after_execution =  parseFloat(order_amount_for_fee) - parseFloat(fee)
//   const tds = side === 0 ? 0 : (parseFloat(order_amount_after_execution) * parseFloat(tds_percent)) / 100
//   console.log('fee',fee)

//   const order_amount_with_fee = side === 0 ? parseFloat(order_amount) + parseFloat(fee) : parseFloat(order_amount)
//   const order_amount_estimate_fee = side === 0 ?  parseFloat(order_amount_for_fee) + parseFloat(fee) : (parseFloat(order_amount_for_fee) - parseFloat(fee)) - parseFloat(tds)
//   console.log('order_amount_estimate_fee',order_amount_estimate_fee)

//    const user_balance = await Get_Where_Universal_Data('balance','balances',{user_id : `${uid}` , coin_id : `${check_balance_asset}`})
   
//     if(user_balance.length == 0){
//       return res.status(400).send({ message: 'Invalid user.' });
//     }
//     if(Number(order_amount_with_fee) > Number(user_balance[0].balance)){
//       return res.status(400).send({ message: 'Insufficient balance.' }); 
//     }

//   const proto = protobuf.loadSync('./orderBook.proto');  // Load your .proto file

//   const OrderMessage = proto.lookupType('orderbook.Order');  // Lookup your message type

//   const hash = randomUUID().substring(0, 25);

//   const message = OrderMessage.create({
//     "uid": uid,
//     "side": side,
//     "symbol": symbol,  
//     "price": parseFloat(price),
//     "quantity": parseFloat(quantity),
//     "hash": hash
//   });

//   // Verify the message before encoding
//   const errMsg = OrderMessage.verify(message);
//   if (errMsg){
//     console.error("Error::", errMsg);
//     return;
//   }

//   // Encode the message into a buffer
//   const buffer = OrderMessage.encode(message).finish();

//   // Convert the binary buffer to base64 string
//   const base64Message = base64.fromByteArray(buffer);
//   const finalMessage = `0|${base64Message}`

//   await Create_Universal_Data('orderbook_open_orders',{
//     order_id:hash,
//     quantity:quantity,
//     execute_qty:0,
//     user_id:uid,
//     coin_id: base_asset_data.id,
//     coin_base: quote_asset_data.id,
//     type: side === 0 ? 'BUY' : 'SELL',
//     price: price,
//     amount: parseFloat(price) * parseFloat(quantity),
//     tds:tds,
//     fees:fee,
//     final_amount:order_amount_estimate_fee,
//     order_type:"LIMIT",
//     status:"PENDING",
//     date_time : Date.now(),
//   })

//  await Create_Universal_Data('transactions',{
//     user_id: uid,
//     coin_id: side == 0 ? quote_asset_data.id : base_asset_data.id,
//     amount: order_amount_with_fee,
//     opening: user_balance[0].balance,
//     closing: user_balance[0].balance - order_amount_with_fee,
//     order_id: hash,
//     type: 'Dr',
//     remarks: side === 0 ? 'Buy' : 'Sale',
//     txn_id: hash,
//     date_time: Date.now(),
//   })
 
//   //debit user balance 
//   const result = await raw_query('UPDATE balances SET balance = balance - ? WHERE user_id = ? AND coin_id = ?',[parseFloat(order_amount_with_fee),uid,check_balance_asset])
//   if(result.affectedRows === 1){
//     wsClient.send(finalMessage);
//     await raw_query('INSERT INTO balances_inorder (user_id, coin_id, balance) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance)',[uid,side == 0 ? quote_asset_data.id : base_asset_data.id,order_amount_with_fee])
//     return res.send({ message: 'Order placed successfully',data:{order_id:hash} });
//   }
//   return res.status(400).send({ message: 'Error while placing order.' });

// }catch(err){
//   console.log(err)
//   return res.status(400).send({ message: 'Error while placing order.' });
// }
// });

