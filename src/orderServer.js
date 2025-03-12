import express from 'express';
const app = express();
app.use(express.json({extended: true}));
import protobuf from 'protobufjs';
import base64 from 'base64-js';
import { Create_Universal_Data , Update_Universal_Data , Get_All_Universal_Data , Get_Where_Universal_Data, raw_query} from './db_query.js'; 
import WebSocket from 'ws';
import { randomUUID } from 'crypto';
import connection from './db_connection.js';
import { error } from 'console';

const wsClient = new WebSocket(`ws://0.0.0.0:5000?user=15`);
const wsClient1 = new WebSocket(`ws://0.0.0.0:5000?user=10`);

wsClient1.on('open', () => {
  console.log('Connection established with bob');
});
wsClient.on('open', () => {
    console.log('Connection established with alice');
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
    return res.send({ message: 'Order placed successfully',data:{order_id:hash} });
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

