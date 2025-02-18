import { Kafka } from "kafkajs";
import { Create_Universal_Data , Update_Universal_Data , Get_All_Universal_Data , Get_Where_Universal_Data, raw_query} from './db_query.js';
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
         console.log('data type : ',data)
        if(data.type === 1){
          const DataCount = await Get_Where_Universal_Data('*','orderbook_open_orders',{order_id : `${data.hash}`})
          console.log('DataCount : ',DataCount.length)
          if(DataCount.length == 0){
          console.log('data type 1')
         const result = await Create_Universal_Data('orderbook_open_orders',{
            order_id:data.hash,
            quantity:data.quantity,
            execute_qty:0,
            user_id:1001,
            coin_id: 101,
            coin_base: 'USDT',
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
          console.log('data type 2')
           const result = await raw_query('UPDATE orderbook_open_orders SET execute_qty = execute_qty + ?,status = ? WHERE order_id = ?',[data.execute_qty,data.status,data.hash])
            console.log('order updated' , result.affectedRows)
           if(result.affectedRows === 0){
              await Create_Universal_Data('orderbook_open_orders',{
                order_id:data.hash,
                quantity:data.quantity == null || undefined ? 0 : data.quantity,
                execute_qty:data.execute_qty,
                user_id:1001,
                coin_id: 101,
                coin_base: 'USDT',
                type: data.side === 0 ? 'BUY' : 'SELL',
                price: data.price,
                amount: 0,
                order_type:"LIMIT",
                status:data.status,
                date_time : Date.now(),
              }) 
            
           }
        }
        console.log(
          `Received message from topic ${topic}: ${JSON.stringify(data)}`
        )
      },
    });
  } catch (error) {
    console.error("Error consuming messages:", error);
  }
};

connectKafka();
// consumeMessages();
