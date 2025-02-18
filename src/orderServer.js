import express from 'express';
const app = express();
app.use(express.json({extended: true}));
import protobuf from 'protobufjs';
import base64 from 'base64-js';
import { Create_Universal_Data , Update_Universal_Data , Get_All_Universal_Data , Get_Where_Universal_Data, raw_query} from './db_query.js'; 
import WebSocket from 'ws';
const wsClient = new WebSocket(`ws://0.0.0.0:5000?user=bob`);
const wsClient1 = new WebSocket(`ws://0.0.0.0:5000?user=alice`);

wsClient1.on('open', () => {
  console.log('Connection established with bob');
});
wsClient.on('open', () => {
    console.log('Connection established with alice');
});


app.post('/place-order', async (req, res) => {
  const { uid, side, symbol, price, quantity,order_type } = req.body;
  console.log('req.body : ',req.body)
  if(!uid && !side && !symbol && !price && !quantity && !order_type) {
    return res.status(400).send({ message: 'send all request data.' });
  }
  
  const [base_asset,quote_asset] = symbol.split('/')

  const asset_data = await raw_query('select id ,status,trade_status from currencies where symbol IN(?,?)',[base_asset,quote_asset])
  const base_asset_data = asset_data[0]
  const quote_asset_data = asset_data[1]

  if (base_asset_data.length == 0 || quote_asset_data.length == 0) {
    return res.status(400).send({ message: 'Invalid symbol.' });  
  }

  if(base_asset_data.status == 0 || quote_asset_data.status == 0 && base_asset_data.trade_status == 0 || quote_asset_data.trade_status == 0){
    return res.status(400).send({ message: 'Currently trade is not active for this pair.' }); 
  }


   const user_balance = await Get_Where_Universal_Data('balance','user_balance',{user_id : `${uid}` , coin_id : `${base_asset_data.id}`})
   
    if(user_balance.length == 0){
      return res.status(400).send({ message: 'Invalid user.' });
    }

    const order_amount = price * quantity
    if(order_amount > user_balance[0].balance){
      return res.status(400).send({ message: 'Insufficient balance.' });
    }

  const proto = protobuf.loadSync('./orderBook.proto');  // Load your .proto file

  const OrderMessage = proto.lookupType('orderbook.Order');  // Lookup your message type
  // console.log(OrderMessage);
  const message = OrderMessage.create({
    "uid": uid,
    "side": side,
    "symbol": symbol,    "price": parseFloat(price),
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

  //debit user balance 
  const result = await raw_query('UPDATE user_balance SET balance = balance - ? WHERE user_id = ? AND coin_id = ?',[order_amount,uid,base_asset_data.id])
  if(result.affectedRows === 1){
    console.log('order created')
    return res.send({ message: 'Order placed successfully' });
  }
  return res.status(400).send({ message: 'Error while placing order.' });
  
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