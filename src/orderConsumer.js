import { Kafka } from "kafkajs";
import { Create_Universal_Data , Update_Universal_Data , Get_All_Universal_Data , Get_Where_Universal_Data} from './db_query.js';


// Kafka client and consumer setup
const kafka = new Kafka({
  clientId: "binance-consumer",
  brokers: ["localhost:9092"], // Adjust your Kafka broker address
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
        console.log('side: ',data);
        if(data.side === 0 && data.type === 1){
          console.log('date', Date.now())
          await Create_Universal_Data('buy_open_orders',{
            order_id:data.hash,
            pair_id:data.symbol,
            price:data.price,
            quantity:data.quantity,
            side:data.side,
            executed_quantity:0,
            uid:data.uid,
            timestamp:Date.now(),
            user_id:data.uid
          }) 
        }else if(data.side === 1 && data.type === 1){
          
          await Create_Universal_Data('sell_open_orders',{
            order_id:data.hash,
            pair_id:data.symbol,
            price:data.price,
            quantity:data.quantity,
            side:data.side,
            executed_quantity:0,
            uid:data.uid,
            timestamp:Date.now(),
            user_id:data.uid
          }) 
        }
        if(data.type === 2){
          // order_id	user_id	pair_id	price	quantity	executed_quantity	uid	timestamp	side	updated_at
          await Create_Universal_Data('matched_orders',{
            order_id:data.hash,
            pair_id:data.symbol,
            price:data.price,
            quantity:data.quantity,
            side:data.side,
            executed_quantity:data.quantity,
            uid:data.uid,
            timestamp:Date.now(),
            user_id:data.uid
          })
          if(data.side === 0){
            await Update_Universal_Data('buy_open_orders',{status:'filled',executed_quantity:data.quantity},{order_id:data.hash})
           }else if(data.side === 1){
            await Update_Universal_Data('sell_open_orders',{status:'filled',executed_quantity:data.quantity},{order_id:data.hash})
           }
        }
        if(data.type === 3){
          console.log('in filled')
           if(data.side === 0){
            await Update_Universal_Data('buy_open_orders',{status:'filled',executed_quantity:data.quantity},{order_id:data.hash})
           }else if(data.side === 1){
            await Update_Universal_Data('sell_open_orders',{status:'filled',executed_quantity:data.quantity},{order_id:data.hash})
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
