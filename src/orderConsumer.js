import { Kafka } from "kafkajs";
import { Create_Universal_Data , Update_Universal_Data , Get_All_Universal_Data , Get_Where_Universal_Data, raw_query} from './db_query.js';
import { parse } from "path";
import Big from 'big.js';
import {WebSocketServer } from "ws";
import { exec } from "child_process";
import { type } from "os";
// import { parse } from "protobufjs";
// import { console } from "inspector";

const wss = new WebSocketServer({ port: 8081 });
const clientSubscriptions = new Map(); // Stores client subscriptions

// WebSocket Server
wss.on("connection", (ws) => {
  console.log("New WebSocket client connected");

  ws.send(JSON.stringify({ message: "Connected to order book updates. Send { pairId: <id> } to subscribe." }));

  ws.on("message", async (message) => {
    try {
      const { userID } = JSON.parse(message);
      const userId = JSON.stringify(userID);
      if (!userId) return;

       // Add client to the subscription list
  if (!clientSubscriptions.has(userId)) {
    clientSubscriptions.set(userId, []);
    console.log(`Client subscribed to userId: ${userId}`);
    ws.send(JSON.stringify({ message: `Subscribed to userId: ${userId}` }));
  }
  clientSubscriptions.get(userId).push(ws);
    } catch (error) {
      console.error("Error parsing message:", error);
    }
  });

  // ws.on("close", () => {
  //   console.log("Client disconnected");
  //   // Remove client from all subscriptions when they disconnect
  //   clientSubscriptions.forEach((clients, userId) => {
  //     clientSubscriptions.set(
  //       userId,
  //       clients.filter((client) => client !== ws)
  //     );
  //   });
  // });
});

// Broadcast updates to only relevant clients
function broadcastUpdate(userId, update) {
  // if (!clientSubscriptions.has(userId)) return;

  // send to ALL clients
  clientSubscriptions.forEach((clients, userID) => {

 // Ensure userId matches
      clients.forEach((client) => { // Iterate over all subscribed clients
        if (client.readyState === WebSocket.OPEN) {
          client.send(JSON.stringify(update));
        }
      });
  });

  // send to SPECIFIED ORDER USER ID
  // clientSubscriptions.get(userId).forEach((client) => {
  //   if (client.readyState === WebSocket.OPEN) {
  //     client.send(JSON.stringify(update));
  //   }
  // });
}


// Kafka client and consumer setup
const kafka = new Kafka({
  clientId: "binance-consumer",
  brokers: ["localhost:9092"], // Adjust your Kafka broker address
  fromBeginning: false,
  retry: {
    retries: 0,
  },
});

const consumer = kafka.consumer({ groupId: "execution-group-new" });

// Connect the Kafka consumer
const connectKafka = async () => {
  try {
    
    consumer.on('consumer.connect', () => {
      console.log('Consumer connected');
    });
    await consumer.connect();
    await consumer.subscribe({ topic: 'trade-engine-message'});
    await consumeMessages();
  } catch (error) {
    console.error("Error connecting to Kafka:", error);
  }
};


// Consume messages from Kafka and process them
const consumeMessages = async () => {
  try {
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        const data = JSON.parse(JSON.parse(message.value));
        console.log(
          `Received message from topic ${topic}: ${JSON.stringify(data)}`
        )
        console.log('data : ',data.price)
        const messageToSend = {
          messageType: data.type,
          user_id: data.uid,
          pair_id: data.symbol,
          type: data.side === 0 ? 'BUY' : 'SELL',
          price:  Big(data.price).toString(),
          quantity: Big(data.quantity).toString(),
          execute_qty: Big(data.execute_qty),
          amount: Big(data.price).times(data.quantity).toString() || 0,
          // final_amount: parseFloat(data.execution_price) * parseFloat(data.execute_qty),
          order_id: data.hash,
          api_order_id: data.hash,
          status: data.status == "PENDING" || "PARTIALLY_FILLED" ? "OPEN" : data.status,
          date_time: Date.now(),
        };
        
        broadcastUpdate(data.uid, messageToSend);
        // data.status = data.status == "PENDING" ? "OPEN" : data.status
        // console.log('data.status : ',data.status)
        // const [base_asset,quote_asset] = data.symbol.split('/')
        const asset_data = await raw_query('select base_asset_id,quote_asset_id from crypto_pair where id=?',[parseInt(data.symbol)])
        const base_asset_id = asset_data[0].base_asset_id
        const quote_asset_id = asset_data[0].quote_asset_id  

    
        if(data.type === 1){
          
          const DataCount = await Get_Where_Universal_Data('id','orderbook_open_orders',{order_id : `${data.hash}`})
         
          if(DataCount.length == 0){
          await Create_Universal_Data('orderbook_open_orders',{
            order_id:data.hash,
            pair_id:data.symbol,
            quantity:Big(data.quantity).toString(),
            execute_qty:0,
            user_id:data.uid,
            coin_id: base_asset_id,
            coin_base: quote_asset_id,
            type: data.side === 0 ? 'BUY' : 'SELL',
            price:  Big(data.price).toString(),
            amount: 0,
            order_type:"LIMIT",
            status:data.status,
            date_time : Date.now(),
          }) 
        }
        else{
          await Update_Universal_Data("orderbook_open_orders",{status:"OPEN",date_time : Date.now()},{order_id : `${data.hash}`})
        }
      }
        if(data.type === 2){
           const update_status = data.status == "PARTIALLY_FILLED" ? "OPEN" : data.status
           const result = await raw_query('UPDATE orderbook_open_orders SET execute_qty = execute_qty + ?,status = ?,date_time=? WHERE order_id = ?',[Big(data.execute_qty).toString(),update_status,Date.now(),data.hash])
  
           if(result.affectedRows === 0){
              await Create_Universal_Data('orderbook_open_orders',{
                order_id:data.hash,
                pair_id:data.symbol,
                quantity: Big(data.quantity).toString(), // Use Big for quantity
                execute_qty: Big(data.execute_qty).toString(),
                user_id:data.uid,
                coin_id: base_asset_id,
                coin_base: quote_asset_id,
                type: data.side === 0 ? 'BUY' : 'SELL',
                price: Big(data.price).toString(),
                amount: Big(data.price).times(data.execute_qty).toString(),
                final_amount: Big(data.execution_price).minus(Big(data.execute_qty)).toString(),
                order_type:"LIMIT",
                status:update_status,
                date_time : Date.now(),
              })
            }

            // Calculating fees and tds
            const trade_fee = await Get_Where_Universal_Data('fees_percent_inr,tds_percent_inr','settings_fees',{id : 1})

            const fee_percent = trade_fee[0].fees_percent_inr
            const tds_percent = trade_fee[0].tds_percent_inr

            const order_amount = data.side === 0 ? Big(data.execute_qty) : Big(data.execution_price).times(data.execute_qty)

            const fee = Big(order_amount).times(new Big(fee_percent)).div(100);
            const after_fees_amount = order_amount.minus(fee);
            const tds = data.side === 0 ? 0 : Big(after_fees_amount).times(new Big(tds_percent)).div(100);

           const order_amount_with_fee = data.side === 0 ?  order_amount.toString() : after_fees_amount.minus(tds).toString();

            // const status = data.status === 'OPEN' || data.status === 'PARTIALLY_FILLED' ? 'PENDING' : 'FILLED'
            
            // this fee to just update in buy_sell table to show the fee in the order history
            const estimate_fee = new Big(data.execution_price).times(new Big(data.execute_qty)).times(new Big(fee_percent)).div(100).toString();
            const order_amount_after_execution =  new Big(data.execution_price).times(data.execute_qty).toString();
            const order_amount_estimate_fee = data.side === 0 ? new Big(order_amount_after_execution).plus(Big(estimate_fee)).toString() : new Big(order_amount_after_execution).minus(new Big(estimate_fee)).minus(new Big(tds)).toString();

             await Create_Universal_Data('buy_sell',{
                user_id:data.uid,
                pair_id:data.symbol,
                pair_id:data.symbol,
                coin_id: data.side === 0 ? quote_asset_id : base_asset_id,  
                type: data.side === 0 ? 'BUY' : 'SELL',
                price: new Big(data.execution_price).toString(),
                current_usdt_price: new Big(data.execution_price).toString(),
                quantity: new Big(data.execute_qty).toString(),
                amount: order_amount_after_execution,
                tds: tds.toString(),
                gst: estimate_fee,
                final_amount: order_amount_estimate_fee,
                order_id: data.hash,
                api_order_id: data.hash,
                status: 'FILLED',  
                api_status: 1,
                api_id: 0,
                response: JSON.stringify(data),
                date_time: Date.now(),
                response_time: Date.now(),
                profit: 0,
                api: 0,
                device: 'WEB',
                tds_usdt: 0,
                fee_usdt: 0,
                usdt_convert_rate: 0,
                base_pair: data.side == 0 ? base_asset_id : quote_asset_id,
                order_type: "LIMIT",
             });
           
            const opening_balance_asset = data.side === 0 ? base_asset_id : quote_asset_id;
          
            const opening_balance = await Get_Where_Universal_Data('balance','balances',{user_id : `${data.uid}` , coin_id : `${opening_balance_asset}`});
     
          // Update balance based on the side of the order
          if (data.side === 0) { // BUY
            await raw_query(
                'INSERT INTO balances (user_id, coin_id, balance) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance)',
                [data.uid, base_asset_id, Big(data.execute_qty).toString()]
            );
        
            const price = new Big(data.price);
            const executeQty = new Big(data.execute_qty);
            const executionPrice = new Big(data.execution_price);
            const feePercent = new Big(fee_percent).div(100); // Convert percentage to decimal
        
            const amount = price.times(executeQty);  // price * execute_qty
            const orderAmount = executionPrice.times(executeQty); // execution_price * execute_qty
        
            const diff = amount.minus(orderAmount); // difference
            const diff_fee = diff.times(feePercent); // fee calculation
            const diff_amount_with_fee = diff.plus(diff_fee); // diff + fee
        
            if (!diff.eq(0)) {
                const get_user_balance = await Get_Where_Universal_Data('balance', 'balances', { user_id: `${data.uid}`, coin_id: `${quote_asset_id}` });
                const opening_balance = new Big(get_user_balance[0].balance);
        
                await raw_query(
                    'INSERT INTO balances (user_id, coin_id, balance) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance)',
                    [data.uid, quote_asset_id, diff_amount_with_fee.toString()]
                );
        
                await Create_Universal_Data('transactions', {
                    user_id: data.uid,
                    coin_id: quote_asset_id,
                    amount: diff_amount_with_fee.toString(),
                    opening: opening_balance.toString(),
                    closing: opening_balance.plus(diff_amount_with_fee).toString(),
                    order_id: data.hash,
                    type: "Cr",
                    remarks: "buy",
                    txn_id: data.hash,
                    date_time: Date.now(),
                });
        
                await raw_query(
                    'UPDATE balances_inorder SET balance = balance - ? WHERE user_id = ? AND coin_id = ?',
                    [diff_amount_with_fee.toString(), data.uid, quote_asset_id]
                );
            }
        
            const order_fee = orderAmount.times(feePercent).toString();
            const order_amount_with_fee = orderAmount.plus(order_fee).toString();
            console.log('order_amount_with_fee : ', order_amount_with_fee);
        
            await raw_query(
                'UPDATE balances_inorder SET balance = balance - ? WHERE user_id = ? AND coin_id = ?',
                [order_amount_with_fee, data.uid, quote_asset_id]
            );
        } else if (data.side === 1) {    // SELL
             await raw_query(
              'INSERT INTO balances (user_id, coin_id, balance) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance)',
              [data.uid, quote_asset_id, order_amount_with_fee]
            );
            await Create_Universal_Data('tds_user_details',{user_id: data.uid, total_amount:after_fees_amount.toString(), amount: tds.toString() ,order_id: data.hash, type:1,description:"TDS"}); 
            await raw_query('UPDATE balances_inorder SET balance = balance - ? WHERE user_id = ? AND coin_id = ?', [data.execute_qty, data.uid, base_asset_id]);        
          }

          const executed_qty = data.execute_qty == null || undefined ? 0 : data.execute_qty
          const closing_balance = data.side === 0 ? new Big(opening_balance[0].balance).plus(executed_qty) : new Big(opening_balance[0].balance).plus(new Big(order_amount_with_fee));
  
          await Create_Universal_Data('transactions',{
            user_id:data.uid,
            coin_id: data.side === 0 ? base_asset_id : quote_asset_id,
            amount: order_amount_with_fee,
            opening: opening_balance[0].balance,
            closing: closing_balance.toString(),
            order_id: data.hash,
            type: "Cr",
            remarks: data.side === 0 ? 'Buy' : 'Sale',
            txn_id: data.hash,
            date_time: Date.now(),
          })

          // update orderbook_trade_history table 
          if(data.status === "FILLED"){
            const get_order_data = await Get_Where_Universal_Data("*",'orderbook_open_orders',{order_id:data.hash})
            if(get_order_data[0].status == "FILLED"){

              const getOrderData = await Get_Where_Universal_Data("*",'buy_sell',{order_id:data.hash})
               console.log('get_order_data : ',getOrderData)
              const avg_price = getOrderData
              .map(item => Big(item.price))
              .reduce((acc, curr) => acc.plus(curr), Big(0))
              .div(getOrderData.length);

              const executed_amount = getOrderData
              .map(item => Big(item.amount))
              .reduce((acc, curr) => acc.plus(curr), Big(0));

              const executed_final_amount = getOrderData
              .map(item => Big(item.final_amount))
              .reduce((acc, curr) => acc.plus(curr), Big(0));
            
             const executedFinalAmount = executed_final_amount.toString();
             const executedAmount = executed_amount.toString();
             const avgPrice = avg_price.toString();

              await Create_Universal_Data('orderbook_trade_history',{
                order_id:data.hash,
                pair_id:get_order_data[0].pair_id,
                quantity:get_order_data[0].quantity,
                user_id:get_order_data[0].user_id,
                coin_id: base_asset_id,
                coin_base: quote_asset_id,
                type: data.side === 0 ? 'BUY' : 'SELL',
                price: get_order_data[0].price,
                avg_price: avgPrice,
                amount: get_order_data[0].amount,
                executed_amount: executedAmount,
                final_amount:get_order_data[0].final_amount,
                executed_final_amount: executedFinalAmount,
                tds:get_order_data[0].tds,
                fees:get_order_data[0].fees,  
                order_type:"LIMIT",
                status:"FILLED",
                date_time : Date.now(),  
                device: 'WEB', 
              });
            }
          }
          
         await raw_query('INSERT INTO users_trade (user_id, trades, amount) VALUES (?, 1, ?) ON DUPLICATE KEY UPDATE trades = trades + 1, amount = amount + VALUES(amount)', [data.uid, order_amount_with_fee]); 
       
        }

        // HANDLING CANCELED ORDER
        if(data.type === 4){

          const get_orderData = await Get_Where_Universal_Data('*','orderbook_open_orders',{order_id : `${data.hash}`})

          const quantity = new Big(get_orderData[0].quantity);
          const executed_qty = new Big(get_orderData[0].execute_qty);
          const remaining_qty = quantity.minus(executed_qty);
          const orderType = get_orderData[0].type;

          const order_asset = orderType === "BUY" ? get_orderData[0].coin_base : get_orderData[0].coin_id

          const opening_balance = await Get_Where_Universal_Data('balance','balances',{user_id : `${data.uid}` , coin_id : `${order_asset}`})
          const opening_bal = new Big(opening_balance[0].balance);
          let amount =Big(0);
          
          if(executed_qty.eq(0)){
            await Update_Universal_Data('orderbook_open_orders',{status:"CANCELLED",date_time : Date.now()},{order_id : `${data.hash}`})

            const orderAmount = Big(orderType === "BUY" ? get_orderData[0].final_amount : get_orderData[0].quantity)
            amount = orderAmount

            // await raw_query('UPDATE balances SET balance = balance + ? WHERE user_id = ? AND coin_id = ?',[orderAmount,data.uid,order_asset])
            await raw_query(
              'INSERT INTO balances (user_id, coin_id, balance) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance)',
              [data.uid, order_asset, orderAmount.toString()]
            );
          }
          else if(!remaining_qty.eq(0)){
            await Update_Universal_Data('orderbook_open_orders',{status:"PARTIALLY_FILLED", date_time : Date.now()},{order_id : `${data.hash}`})

            const trade_fee = await Get_Where_Universal_Data('fees_percent_inr','settings_fees',{id : 1})
            const fee_percent = Big(trade_fee[0].fees_percent_inr)

            const order_amount = orderType === "BUY" ? Big(get_orderData[0].price).times(remaining_qty) : remaining_qty;

            const fees = new Big(get_orderData[0].price).times(remaining_qty).times(fee_percent).div(100);

            const order_amount_with_fee = orderType === "BUY" ? order_amount.plus(fees) : order_amount
            amount = order_amount_with_fee

            // await raw_query('UPDATE balances SET balance = balance + ? WHERE user_id = ? AND coin_id = ?',[order_amount_with_fee,data.uid,order_asset])
            await raw_query(
              'INSERT INTO balances (user_id, coin_id, balance) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance)',
              [data.uid, order_asset, order_amount_with_fee.toString()]
            );
          }
          
          await Create_Universal_Data('transactions',{
            user_id:data.uid,
            coin_id: order_asset,
            amount: amount.toString(),
            opening: opening_bal.toString(),
            closing: opening_bal.plus(amount).toString(),
            order_id: data.hash,
            type: "Cr",
            remarks: orderType === "BUY" ? 'Buy' : 'Sale',
            txn_id: data.hash,
            date_time: Date.now(),
          })

          const get_order_data = await Get_Where_Universal_Data("*",'orderbook_open_orders',{order_id:data.hash})

          const getOrderData = await Get_Where_Universal_Data("*",'buy_sell',{order_id:data.hash})
          
          let avg_price = 0;
          let executed_amount = 0;
          let executed_final_amount = 0;
          if(getOrderData.length > 0){
             avg_price = getOrderData
            .map(item => Big(item.price))
            .reduce((acc, curr) => acc.plus(curr), Big(0))
            .div(get_order_data.length);

             executed_amount = getOrderData
            .map(item => Big(item.amount))
            .reduce((acc, curr) => acc.plus(curr), Big(0));

             executed_final_amount = getOrderData
            .map(item => Big(item.final_amount))
            .reduce((acc, curr) => acc.plus(curr), Big(0));
          }

            
             const executedFinalAmount = executed_final_amount.toString();
             const executedAmount = executed_amount.toString();
             const avgPrice = avg_price.toString();

          await Create_Universal_Data('orderbook_trade_history',{
            order_id:get_order_data[0].order_id,
            pair_id:get_order_data[0].pair_id,
            quantity:get_order_data[0].quantity,
            user_id:get_order_data[0].user_id,
            coin_id: get_orderData[0].id,
            coin_base: get_orderData[0].id,
            type: get_order_data[0].side === 0 ? 'BUY' : 'SELL',
            price: get_order_data[0].price,
            avg_price: avgPrice,
            amount: get_orderData[0].amount,
            final_amount:get_order_data[0].final_amount,
            executed_amount: executedAmount,
            executed_final_amount: executedFinalAmount,
            order_type:"LIMIT",
            status:"CANCELLED",
            date_time : Date.now(), 
            device: 'WEB',
            tds:get_order_data[0].tds,
            fees:get_order_data[0].fees, 
          });

          await raw_query('UPDATE balances_inorder SET balance = balance - ? WHERE user_id = ? AND coin_id = ?', [amount.toString(), data.uid, order_asset]);
        }
      },
    });
  } catch (error) {
    console.error("Error consuming messages:", error);
  }
};

connectKafka();
// consumeMessages();



// import { Kafka } from "kafkajs";
// import { Create_Universal_Data , Update_Universal_Data , Get_All_Universal_Data , Get_Where_Universal_Data, raw_query} from './db_query.js';
// import { parse } from "path";
// import Big from 'big.js';
// import {WebSocketServer } from "ws";
// import { exec } from "child_process";
// import { type } from "os";
// // import { parse } from "protobufjs";
// // import { console } from "inspector";

// const wss = new WebSocketServer({ port: 8081 });
// const clientSubscriptions = new Map(); // Stores client subscriptions

// // WebSocket Server
// wss.on("connection", (ws) => {
//   console.log("New WebSocket client connected");

//   ws.send(JSON.stringify({ message: "Connected to order book updates. Send { pairId: <id> } to subscribe." }));

//   ws.on("message", async (message) => {
//     try {
//       const { userID } = JSON.parse(message);
//       const userId = JSON.stringify(userID);
//       if (!userId) return;

//        // Add client to the subscription list
//   if (!clientSubscriptions.has(userId)) {
//     clientSubscriptions.set(userId, []);
//     console.log(`Client subscribed to userId: ${userId}`);
//     ws.send(JSON.stringify({ message: `Subscribed to userId: ${userId}` }));
//   }
//   clientSubscriptions.get(userId).push(ws);
//     } catch (error) {
//       console.error("Error parsing message:", error);
//     }
//   });

//   // ws.on("close", () => {
//   //   console.log("Client disconnected");
//   //   // Remove client from all subscriptions when they disconnect
//   //   clientSubscriptions.forEach((clients, userId) => {
//   //     clientSubscriptions.set(
//   //       userId,
//   //       clients.filter((client) => client !== ws)
//   //     );
//   //   });
//   // });
// });

// // Broadcast updates to only relevant clients
// function broadcastUpdate(userId, update) {
//   // if (!clientSubscriptions.has(userId)) return;

//   // send to ALL clients
//   clientSubscriptions.forEach((clients, userID) => {

//  // Ensure userId matches
//       clients.forEach((client) => { // Iterate over all subscribed clients
//         if (client.readyState === WebSocket.OPEN) {
//           client.send(JSON.stringify(update));
//         }
//       });
//   });

//   // send to SPECIFIED ORDER USER ID
//   // clientSubscriptions.get(userId).forEach((client) => {
//   //   if (client.readyState === WebSocket.OPEN) {
//   //     client.send(JSON.stringify(update));
//   //   }
//   // });
// }


// // Kafka client and consumer setup
// const kafka = new Kafka({
//   clientId: "binance-consumer",
//   brokers: ["localhost:9092"], // Adjust your Kafka broker address
//   fromBeginning: false,
//   retry: {
//     retries: 0,
//   },
// });

// const consumer = kafka.consumer({ groupId: "execution-group-new" });

// // Connect the Kafka consumer
// const connectKafka = async () => {
//   try {
    
//     consumer.on('consumer.connect', () => {
//       console.log('Consumer connected');
//     });
//     await consumer.connect();
//     await consumer.subscribe({ topic: 'trade-engine-message'});
//     await consumeMessages();
//   } catch (error) {
//     console.error("Error connecting to Kafka:", error);
//   }
// };


// // Consume messages from Kafka and process them
// const consumeMessages = async () => {
//   try {
//     await consumer.run({
//       eachMessage: async ({ topic, message }) => {
//         const data = JSON.parse(JSON.parse(message.value));
//         console.log(
//           `Received message from topic ${topic}: ${JSON.stringify(data)}`
//         )
//         const messageToSend = {
//           messageType: data.type,
//           user_id: data.uid,
//           pair_id: data.symbol,
//           type: data.side === 0 ? 'BUY' : 'SELL',
//           price: data.price,
//           quantity: data.quantity,
//           execute_qty: data.execute_qty || 0,
//           amount: parseFloat(data.price) * parseFloat(data.quantity) || 0,
//           // final_amount: parseFloat(data.execution_price) * parseFloat(data.execute_qty),
//           order_id: data.hash,
//           api_order_id: data.hash,
//           status: data.status == "PENDING" || "PARTIALLY_FILLED" ? "OPEN" : data.status,
//           date_time: Date.now(),
//         };
        
//         broadcastUpdate(data.uid, messageToSend);
//         // data.status = data.status == "PENDING" ? "OPEN" : data.status
//         // console.log('data.status : ',data.status)
//         // const [base_asset,quote_asset] = data.symbol.split('/')
//         const asset_data = await raw_query('select base_asset_id,quote_asset_id from crypto_pair where id=?',[parseInt(data.symbol)])
//         const base_asset_id = asset_data[0].base_asset_id
//         const quote_asset_id = asset_data[0].quote_asset_id  

    
//         if(data.type === 1){
          
//           const DataCount = await Get_Where_Universal_Data('id','orderbook_open_orders',{order_id : `${data.hash}`})
         
//           if(DataCount.length == 0){
//           await Create_Universal_Data('orderbook_open_orders',{
//             order_id:data.hash,
//             pair_id:data.symbol,
//             quantity:data.quantity,
//             execute_qty:0,
//             user_id:data.uid,
//             coin_id: base_asset_id,
//             coin_base: quote_asset_id,
//             type: data.side === 0 ? 'BUY' : 'SELL',
//             price: data.price,
//             amount: 0,
//             order_type:"LIMIT",
//             status:data.status,
//             date_time : Date.now(),
//           }) 
//         }
//         else{
//           await Update_Universal_Data("orderbook_open_orders",{status:"OPEN",date_time : Date.now()},{order_id : `${data.hash}`})
//         }
//       }
//         if(data.type === 2){
//            const update_status = data.status == "PARTIALLY_FILLED" ? "OPEN" : data.status
//            const result = await raw_query('UPDATE orderbook_open_orders SET execute_qty = execute_qty + ?,status = ?,date_time=? WHERE order_id = ?',[data.execute_qty,update_status,Date.now(),data.hash])
  
//            if(result.affectedRows === 0){
//               await Create_Universal_Data('orderbook_open_orders',{
//                 order_id:data.hash,
//                 pair_id:data.symbol,
//                 quantity:data.quantity == null || undefined ? 0 : data.quantity,
//                 execute_qty:data.execute_qty,
//                 user_id:data.uid,
//                 coin_id: base_asset_id,
//                 coin_base: quote_asset_id,
//                 type: data.side === 0 ? 'BUY' : 'SELL',
//                 price: data.price,
//                 amount: parseFloat(data.price) * parseFloat(data.execute_qty),
//                 final_amount: parseFloat(data.execution_price) * parseFloat(data.execute_qty),
//                 order_type:"LIMIT",
//                 status:update_status,
//                 date_time : Date.now(),
//               })
//             }

//             // Calculating fees and tds
//             const trade_fee = await Get_Where_Universal_Data('fees_percent_inr,tds_percent_inr','settings_fees',{id : 1})

//             const fee_percent = trade_fee[0].fees_percent_inr
//             const tds_percent = trade_fee[0].tds_percent_inr

//             const order_amount = data.side === 0 ? parseFloat(data.execute_qty) : parseFloat(data.execution_price) * parseFloat(data.execute_qty)

//             const fee = (parseFloat(order_amount) * parseFloat(fee_percent)) / 100;
//             const after_fees_amount = parseFloat(order_amount) - parseFloat(fee);
//             const tds = data.side === 0 ? 0 : (parseFloat(after_fees_amount) * parseFloat(tds_percent)) / 100;

//            const order_amount_with_fee = data.side === 0 ?  order_amount : (parseFloat(order_amount) - parseFloat(fee)) - parseFloat(tds);

//             // const status = data.status === 'OPEN' || data.status === 'PARTIALLY_FILLED' ? 'PENDING' : 'FILLED'
            
//             // this fee to just update in buy_sell table to show the fee in the order history
//             const estimate_fee = (parseFloat(data.execution_price) * parseFloat(data.execute_qty) * parseFloat(fee_percent)) / 100
//             const order_amount_after_execution =  parseFloat(data.execution_price) * parseFloat(data.execute_qty);
//             const order_amount_estimate_fee = data.side === 0 ? order_amount_after_execution + estimate_fee : (parseFloat(order_amount_after_execution) - parseFloat(estimate_fee)) - parseFloat(tds)

//              await Create_Universal_Data('buy_sell',{
//                 user_id:data.uid,
//                 pair_id:data.symbol,
//                 pair_id:data.symbol,
//                 coin_id: data.side === 0 ? quote_asset_id : base_asset_id,  
//                 type: data.side === 0 ? 'BUY' : 'SELL',
//                 price: data.execution_price,
//                 current_usdt_price: data.execution_price,
//                 quantity: data.execute_qty,
//                 amount: order_amount_after_execution,
//                 tds: parseFloat(tds),
//                 gst: parseFloat(estimate_fee),
//                 final_amount: parseFloat(order_amount_estimate_fee),
//                 order_id: data.hash,
//                 api_order_id: data.hash,
//                 status: 'FILLED',  
//                 api_status: 1,
//                 api_id: 0,
//                 response: JSON.stringify(data),
//                 date_time: Date.now(),
//                 response_time: Date.now(),
//                 profit: 0,
//                 api: 0,
//                 device: 'WEB',
//                 tds_usdt: 0,
//                 fee_usdt: 0,
//                 usdt_convert_rate: 0,
//                 base_pair: data.side == 0 ? base_asset_id : quote_asset_id,
//                 order_type: "LIMIT",
//              });
           
//             const opening_balance_asset = data.side === 0 ? base_asset_id : quote_asset_id;
          
//             const opening_balance = await Get_Where_Universal_Data('balance','balances',{user_id : `${data.uid}` , coin_id : `${opening_balance_asset}`});
     
//           // Update balance based on the side of the order
//           if (data.side === 0) { // BUY
//           //  const result1 =  await raw_query('UPDATE balances SET balance = balance + ? WHERE user_id = ? AND coin_id = ?', [data.execute_qty, data.uid, base_asset_id]);
//           await raw_query(
//             'INSERT INTO balances (user_id, coin_id, balance) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance)',
//             [data.uid, base_asset_id, data.execute_qty]
//           );
//             const price = data.price
//             const amount = parseFloat(price) * parseFloat(data.execute_qty);
//             const orderAmount = parseFloat(data.execution_price) * parseFloat(data.execute_qty);
//             const diff = parseFloat(amount) - parseFloat(orderAmount);
//             const diff_fee = parseFloat(diff) * parseFloat(fee_percent) / 100
//             const diff_amount_with_fee = parseFloat(diff) + parseFloat(diff_fee)

//             if(diff != 0){
//               const get_user_balance = await Get_Where_Universal_Data('balance','balances',{user_id : `${data.uid}` , coin_id : `${quote_asset_id}`})
//               const opening_balance = get_user_balance[0].balance

//               // await raw_query('UPDATE balances SET balance = balance + ? WHERE user_id = ? AND coin_id = ?', [diff_amount_with_fee, data.uid, quote_asset_id]);
//               await raw_query(
//                 'INSERT INTO balances (user_id, coin_id, balance) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance)',
//                 [data.uid, quote_asset_id, diff_amount_with_fee]
//               );
//               await Create_Universal_Data('transactions',{
//                 user_id:data.uid,
//                 coin_id: quote_asset_id,
//                 amount: parseFloat(diff_amount_with_fee),
//                 opening: opening_balance,
//                 closing: parseFloat(opening_balance) + parseFloat(diff_amount_with_fee),
//                 order_id: data.hash,
//                 type: "Cr",
//                 remarks: "buy",
//                 txn_id: data.hash,
//                 date_time: Date.now(),
//               })
//               await raw_query('UPDATE balances_inorder SET balance = balance - ? WHERE user_id = ? AND coin_id = ?', [diff_amount_with_fee, data.uid, quote_asset_id]);
//             }

//             const order_fee = parseFloat(orderAmount) * parseFloat(fee_percent) / 100
//             const order_amount_with_fee = parseFloat(orderAmount) + parseFloat(order_fee)

//             await raw_query('UPDATE balances_inorder SET balance = balance - ? WHERE user_id = ? AND coin_id = ?', [order_amount_with_fee, data.uid, quote_asset_id]);
            
//           } else if (data.side === 1) {    // SELL
//              await raw_query(
//               'INSERT INTO balances (user_id, coin_id, balance) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance)',
//               [data.uid, quote_asset_id, order_amount_with_fee]
//             );
//             await Create_Universal_Data('tds_user_details',{user_id: data.uid, total_amount:after_fees_amount, amount: tds ,order_id: data.hash, type:1,description:"TDS"}); 
//             await raw_query('UPDATE balances_inorder SET balance = balance - ? WHERE user_id = ? AND coin_id = ?', [data.execute_qty, data.uid, base_asset_id]);        
//           }

//           const executed_qty = data.execute_qty == null || undefined ? 0 : data.execute_qty
//           const closing_balance = data.side === 0 ? parseFloat(opening_balance[0].balance) + parseFloat(executed_qty) : parseFloat(opening_balance[0].balance) + parseFloat(order_amount_with_fee);
  
//           await Create_Universal_Data('transactions',{
//             user_id:data.uid,
//             coin_id: data.side === 0 ? base_asset_id : quote_asset_id,
//             amount: order_amount_with_fee,
//             opening: opening_balance[0].balance,
//             closing: closing_balance,
//             order_id: data.hash,
//             type: "Cr",
//             remarks: data.side === 0 ? 'Buy' : 'Sale',
//             txn_id: data.hash,
//             date_time: Date.now(),
//           })

//           // update orderbook_trade_history table 
//           if(data.status === "FILLED"){
//             const get_order_data = await Get_Where_Universal_Data("*",'orderbook_open_orders',{order_id:data.hash})
//             if(get_order_data[0].status == "FILLED"){
//               await Create_Universal_Data('orderbook_trade_history',{
//                 order_id:data.hash,
//                 pair_id:get_order_data[0].pair_id,
//                 quantity:get_order_data[0].quantity,
//                 execute_qty:get_order_data[0].execute_qty,
//                 user_id:get_order_data[0].user_id,
//                 coin_id: base_asset_id,
//                 coin_base: quote_asset_id,
//                 type: data.side === 0 ? 'BUY' : 'SELL',
//                 price: get_order_data[0].price,
//                 amount: get_order_data[0].amount,
//                 final_amount:get_order_data[0].final_amount,
//                 tds:get_order_data[0].tds,
//                 fees:get_order_data[0].fees,  
//                 order_type:"LIMIT",
//                 status:"FILLED",
//                 date_time : Date.now(),  
//                 device: 'WEB', 
//               })
//             }
//           }
          
//          await raw_query('INSERT INTO users_trade (user_id, trades, amount) VALUES (?, 1, ?) ON DUPLICATE KEY UPDATE trades = trades + 1, amount = amount + VALUES(amount)', [data.uid, order_amount_with_fee]); 
       
//         }

//         // HANDLING CANCELED ORDER
//         if(data.type === 4){

//           const get_orderData = await Get_Where_Universal_Data('*','orderbook_open_orders',{order_id : `${data.hash}`})

//           const quantity = get_orderData[0].quantity 
//           const executed_qty = get_orderData[0].execute_qty
//           const remaining_qty = parseFloat(quantity) - parseFloat(executed_qty)
//           const orderType = get_orderData[0].type

//           const order_asset = orderType === "BUY" ? get_orderData[0].coin_base : get_orderData[0].coin_id

//           const opening_balance = await Get_Where_Universal_Data('balance','balances',{user_id : `${data.uid}` , coin_id : `${order_asset}`})
//           const opening_bal = opening_balance[0].balance
//           var amount = 0
          
//           if(executed_qty == 0){
//             await Update_Universal_Data('orderbook_open_orders',{status:"CANCELLED",date_time : Date.now()},{order_id : `${data.hash}`})

//             const orderAmount = orderType === "BUY" ? get_orderData[0].final_amount : get_orderData[0].quantity
//             amount = orderAmount

//             // await raw_query('UPDATE balances SET balance = balance + ? WHERE user_id = ? AND coin_id = ?',[orderAmount,data.uid,order_asset])
//             await raw_query(
//               'INSERT INTO balances (user_id, coin_id, balance) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance)',
//               [data.uid, order_asset, orderAmount]
//             );
//           }
//           else if(remaining_qty != 0){
//             await Update_Universal_Data('orderbook_open_orders',{status:"PARTIALLY_FILLED", date_time : Date.now()},{order_id : `${data.hash}`})

//             const trade_fee = await Get_Where_Universal_Data('fees_percent_inr','settings_fees',{id : 1})
//             const fee_percent = trade_fee[0].fees_percent_inr

//             const order_amount = orderType === "BUY" ? parseFloat(get_orderData[0].price) * parseFloat(remaining_qty) : parseFloat(remaining_qty)

//             const fees = parseFloat(get_orderData[0].price) * parseFloat(remaining_qty) * parseFloat(fee_percent) / 100

//             const order_amount_with_fee = orderType === "BUY" ? order_amount + fees : order_amount
//             amount = order_amount_with_fee

//             // await raw_query('UPDATE balances SET balance = balance + ? WHERE user_id = ? AND coin_id = ?',[order_amount_with_fee,data.uid,order_asset])
//             await raw_query(
//               'INSERT INTO balances (user_id, coin_id, balance) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance)',
//               [data.uid, order_asset, order_amount_with_fee]
//             );
//           }
          
//           await Create_Universal_Data('transactions',{
//             user_id:data.uid,
//             coin_id: order_asset,
//             amount: amount,
//             opening: opening_bal,
//             closing: parseFloat(opening_bal) + parseFloat(amount),
//             order_id: data.hash,
//             type: "Cr",
//             remarks: orderType === "BUY" ? 'Buy' : 'Sale',
//             txn_id: data.hash,
//             date_time: Date.now(),
//           })

//           const get_order_data = await Get_Where_Universal_Data("*",'orderbook_open_orders',{order_id:data.hash})

//           await Create_Universal_Data('orderbook_trade_history',{
//             order_id:get_order_data[0].order_id,
//             pair_id:get_order_data[0].pair_id,
//             quantity:get_order_data[0].quantity,
//             execute_qty:get_order_data[0].execute_qty,
//             user_id:get_order_data[0].user_id,
//             coin_id: get_orderData[0].id,
//             coin_base: get_orderData[0].id,
//             type: get_order_data[0].side === 0 ? 'BUY' : 'SELL',
//             price: get_order_data[0].price,
//             amount: get_orderData[0].amount,
//             final_amount:get_order_data[0].final_amount,
//             order_type:"LIMIT",
//             status:"CANCELLED",
//             date_time : Date.now(), 
//             device: 'WEB',
//             tds:get_order_data[0].tds,
//             fees:get_order_data[0].fees, 
//           });

//           await raw_query('UPDATE balances_inorder SET balance = balance - ? WHERE user_id = ? AND coin_id = ?', [amount, data.uid, order_asset]);
//         }
//       },
//     });
//   } catch (error) {
//     console.error("Error consuming messages:", error);
//   }
// };

// connectKafka();
// // consumeMessages();
