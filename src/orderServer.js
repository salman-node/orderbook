const express = require('express');
const app = express();
app.use(express.json({extended: true}));
const protobuf = require('protobufjs');
const base64 = require('base64-js');
const WebSocket = require('ws');
const wsClient = new WebSocket(`ws://0.0.0.0:5000?user=bob`);
const wsClient1 = new WebSocket(`ws://0.0.0.0:5000?user=alice`);

wsClient1.on('open', () => {
  console.log('Connection established with bob');
});
wsClient.on('open', () => {
    console.log('Connection established with alice');
});

let buyOrderCount = 0;
let sellOrderCount = 0;

app.post('/place-order', async (req, res) => {
  const { uid, side, symbol, price, quantity } = req.body;


  const proto = protobuf.loadSync('./orderBook.proto');  // Load your .proto file
  // console.log(proto);
  const OrderMessage = proto.lookupType('orderbook.Order');  // Lookup your message type
  // console.log(OrderMessage);
  const message = OrderMessage.create({
    "uid": uid,
    "side": side,
    "symbol": symbol,
    "price": parseFloat(price),
    "quantity": parseFloat(quantity)
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
  const finalMessage = `0|${base64Message}`
  console.log(finalMessage)
  wsClient.send(finalMessage);
  
  if (side === 0) {
    buyOrderCount++;
    console.log(`Buy order placed. Total buy orders: ${buyOrderCount}`);
  } else if (side === 1) {
    sellOrderCount++;
    console.log(`Sell order placed. Total sell orders: ${sellOrderCount}`);
  }

  return res.send({ message: 'Order placed successfully' });
});

app.post('/cancel-order', async (req, res) => {
  const { uid, side, symbol, orderId } = req.body;
  if(!uid || !side || !symbol || !orderId) {
    return res.status(400).send({ message: 'send all request data.' });
  }
  const proto = protobuf.loadSync('./orderBook.proto');  // Load your .proto file
  // console.log(proto);
  const OrderMessage = proto.lookupType('orderbook.CancelOrder');  // Lookup your message type
  // console.log(OrderMessage);
  const message = OrderMessage.create({
    uid : uid,
    orderId : orderId,
    side : side,
    symbol : symbol,
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
  const finalMessage = `3|${base64Message}`
  console.log(finalMessage)
  wsClient.send(finalMessage);

  return res.send({ message: 'Order placed successfully' });
});





app.listen(9696, () => {
  console.log('Server started on port 9696');
});