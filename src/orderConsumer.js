import { Kafka } from "kafkajs";
import { Create_Universal_Data , Update_Universal_Data , Get_All_Universal_Data , Get_Where_Universal_Data, raw_query} from './db_query.js';
// import { parse } from "protobufjs";
// import { console } from "inspector";



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

        const [base_asset,quote_asset] = data.symbol.split('/')
        const asset_data = await raw_query('select id,status,trade_status from currencies where symbol IN(?,?)',[base_asset,quote_asset])
        const base_asset_data = asset_data[0]
        const quote_asset_data = asset_data[1]  
    
        if(data.type === 1){
          const DataCount = await Get_Where_Universal_Data('*','orderbook_open_orders',{order_id : `${data.hash}`})

          if(DataCount.length == 0){
          const result = await Create_Universal_Data('orderbook_open_orders',{
            order_id:data.hash,
            quantity:data.quantity,
            execute_qty:0,
            user_id:data.uid,
            coin_id: base_asset_data.id,
            coin_base: 'INR',
            type: data.side === 0 ? 'BUY' : 'SELL',
            price: data.price,
            amount: 0,
            order_type:"LIMIT",
            status:data.status,
            date_time : Date.now(),
          }) 
          if(result.affectedRows === 1){
            console.log('order created')
          }
        }
      }
        if(data.type === 2){
           console.log('data : ',data)  
           const result = await raw_query('UPDATE orderbook_open_orders SET execute_qty = execute_qty + ?,status = ? WHERE order_id = ?',[data.execute_qty,data.status,data.hash])
            console.log('order updated' , result.affectedRows)
           if(result.affectedRows === 0){
              await Create_Universal_Data('orderbook_open_orders',{
                order_id:data.hash,
                quantity:data.quantity == null || undefined ? 0 : data.quantity,
                execute_qty:data.execute_qty,
                user_id:data.uid,
                coin_id: base_asset_data.id,
                coin_base: 'INR',
                type: data.side === 0 ? 'BUY' : 'SELL',
                price: data.price,
                amount: parseFloat(data.price) * parseFloat(data.execute_qty),
                final_amount: parseFloat(data.execution_price) * parseFloat(data.execute_qty),
                order_type:"LIMIT",
                status:data.status,
                date_time : Date.now(),
              })
            }

              const status = data.status === 'OPEN' || data.status === 'PARTIALLY_FILLED' ? 'PENDING' : 'FILLED'

              const insertData = await Create_Universal_Data('buy_sell',{
                user_id:data.uid,
                coin_id: data.side === 0 ? quote_asset_data.id : base_asset_data.id,  
                type: data.side === 0 ? 'BUY' : 'SELL',
                price: data.execution_price,
                current_usdt_price: data.execution_price,
                quantity: data.execute_qty,
                amount: parseFloat(data.price) * parseFloat(data.execute_qty),
                tds: 0,
                gst: 0,
                final_amount: parseFloat(data.execution_price) * parseFloat(data.execute_qty),
                order_id: data.hash,
                api_order_id: data.hash,
                status: status,  
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
                base_pair: data.side == 0 ? base_asset_data.id : quote_asset_data.id,
                order_type: "LIMIT",
              })
              console.log('insertData : ',insertData[0].affectedRows)
           
            
            const opening_balance_asset = data.side === 0 ? base_asset_data.id : quote_asset_data.id
            const opening_balance = await Get_Where_Universal_Data('balance','balances',{user_id : `${data.uid}` , coin_id : `${opening_balance_asset}`})
           
          // Update balance based on the side of the order
          if (data.side === 0) { // BUY
            await raw_query('UPDATE balances SET balance = balance + ? WHERE user_id = ? AND coin_id = ?', [data.execute_qty, data.uid, base_asset_data.id]);
         
            const price = data.price

            console.log('price : ',price)
            const amount = parseFloat(price) * parseFloat(data.execute_qty);
            console.log('amount : ',amount)
            console.log('data.execution_price : ',data.execution_price)
            const orderAmount = parseFloat(data.execution_price) * parseFloat(data.execute_qty);
            console.log('orderAmount : ',orderAmount)
            const diff = parseFloat(amount) - parseFloat(orderAmount);
            console.log('difference : ',diff)
            if(diff != 0){
              const get_user_balance = await Get_Where_Universal_Data('balance','balances',{user_id : `${data.uid}` , coin_id : `${quote_asset_data.id}`})
              const opening_balance = get_user_balance[0].balance
              console.log('opening_balance before adding : ',opening_balance)
              console.log('balance to add : ',diff)
              await raw_query('UPDATE balances SET balance = balance + ? WHERE user_id = ? AND coin_id = ?', [diff, data.uid, quote_asset_data.id]);
              await Create_Universal_Data('transactions',{
                user_id:data.uid,
                coin_id: quote_asset_data.id,
                amount: parseFloat(diff),
                opening: opening_balance,
                closing: parseFloat(opening_balance) + parseFloat(diff),
                order_id: data.hash,
                type: "Cr",
                remarks: "buy",
                txn_id: data.hash,
                date_time: Date.now(),
              })
            }
            
          } else if (data.side === 1) {    // SELL
            const amount = parseFloat(data.execution_price) * parseFloat(data.execute_qty);
            await raw_query('UPDATE balances SET balance = balance + ? WHERE user_id = ? AND coin_id = ?', [amount, data.uid, quote_asset_data.id]);
            
          }

          console.log('price: ',data.price)
          console.log('execute_qty : ',data.execute_qty)
          const executed_qty = data.execute_qty == null || undefined ? 0 : data.execute_qty
          console.log('executed_qty : ',executed_qty)

          const closing_balance = data.side === 0 ? parseFloat(opening_balance[0].balance) + parseFloat(executed_qty) : parseFloat(opening_balance[0].balance) + (parseFloat(executed_qty) * parseFloat(data.execution_price));
          
          console.log('opening_balance : ',opening_balance[0].balance)
          console.log('closing_balance : ',closing_balance)
  
          const transaction_updated = await Create_Universal_Data('transactions',{
            user_id:data.uid,
            coin_id: data.side === 0 ? base_asset_data.id : quote_asset_data.id,
            amount: data.side === 0 ? parseFloat(executed_qty) : parseFloat(executed_qty) * parseFloat(data.execution_price),
            opening: opening_balance[0].balance,
            closing: closing_balance,
            order_id: data.hash,
            type: "Cr",
            remarks: data.side === 0 ? 'Buy' : 'Sale',
            txn_id: data.hash,
            date_time: Date.now(),
          })
          console.log('transaction inserted : ',transaction_updated[0].affectedRows)
        }
      },
    });
  } catch (error) {
    console.error("Error consuming messages:", error);
  }
};

connectKafka();
// consumeMessages();
