import express from 'express';
const app = express();
app.use(express.json({extended: true}));
import protobuf from 'protobufjs';
import base64 from 'base64-js';
import { Create_Universal_Data , Update_Universal_Data , Get_All_Universal_Data , Get_Where_Universal_Data, raw_query} from './db_query.js'; 
import WebSocket from 'ws';
import { randomUUID } from 'crypto';

const wsClient = new WebSocket(`ws://0.0.0.0:5000?user=15`);
const wsClient1 = new WebSocket(`ws://0.0.0.0:5000?user=10`);

wsClient1.on('open', () => {
  console.log('Connection established with bob');
});
wsClient.on('open', () => {
    console.log('Connection established with alice');
});



app.post('/place-order', async (req, res) => {
  const { uid, side, symbol, price, quantity,order_type } = req.body;
 
  if(!uid && !side && !symbol && !price && !quantity && !order_type) {
    return res.status(400).send({ message: 'send all request data.' });
  }
  
  const [base_asset,quote_asset] = symbol.split('/')

  const asset_data = await raw_query('select id ,status,trade_status from currencies where symbol IN(?,?)',[base_asset,quote_asset])

  const base_asset_data = asset_data[0]
  const quote_asset_data = asset_data[1]

  if (base_asset_data == undefined || quote_asset_data == undefined) {
    return res.status(400).send({ message: 'Invalid symbol.' });  
  }

  if(base_asset_data.status != "Active" || quote_asset_data.status != 'Active' && base_asset_data.trade_status != 1 || quote_asset_data.trade_status != 1){
    return res.status(400).send({ message: 'Currently trade is not active for this pair.' }); 
  }
  const check_balance_asset = side === 0 ? quote_asset_data.id : base_asset_data.id
  const order_amount = side === 0 ? parseFloat(price) * parseFloat(quantity) : quantity

   const user_balance = await Get_Where_Universal_Data('balance','balances',{user_id : `${uid}` , coin_id : `${check_balance_asset}`})
   
    if(user_balance.length == 0){
      return res.status(400).send({ message: 'Invalid user.' });
    }
    if(order_amount > user_balance[0].balance){
      return res.status(400).send({ message: 'Insufficient balance.' }); 
    }

  const proto = protobuf.loadSync('./orderBook.proto');  // Load your .proto file

  const OrderMessage = proto.lookupType('orderbook.Order');  // Lookup your message type

  const hash = randomUUID().substring(0, 25);

  const message = OrderMessage.create({
    "uid": uid,
    "side": side,
    "symbol": symbol,  
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
  console.log(finalMessage)

 await Create_Universal_Data('transactions',{
    user_id: uid,
    coin_id: side == 0 ? quote_asset_data.id : base_asset_data.id,
    amount: order_amount,
    opening: user_balance[0].balance,
    closing: user_balance[0].balance - order_amount,
    order_id: hash,
    type: 'Dr',
    remarks: side === 0 ? 'Buy' : 'Sale',
    txn_id: hash,
    date_time: Date.now(),
  })
 
  //debit user balance 
  if(side === 0){
  const result = await raw_query('UPDATE balances SET balance = balance - ? WHERE user_id = ? AND coin_id = ?',[parseFloat(order_amount),uid,quote_asset_data.id])
  if(result.affectedRows === 1){
    wsClient.send(finalMessage);
    console.log('user_id: ',uid,'assetid: ',quote_asset_data.id,'Quote asset debited: ',order_amount) 
    return res.send({ message: 'Order placed successfully' });
  }
  return res.status(400).send({ message: 'Error while placing order.' });
}
if(side === 1){
  const result = await raw_query('UPDATE balances SET balance = balance - ? WHERE user_id = ? AND coin_id = ?',[parseFloat(quantity),uid,base_asset_data.id])
  if(result.affectedRows === 1){
    wsClient.send(finalMessage);
    console.log('user_id: ',uid,'assetid: ',base_asset_data.id,'Base asset debited: ',quantity)
    return res.send({ message: 'Order placed successfully' });
  }
  return res.status(400).send({ message: 'Error while placing order.' });
}
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