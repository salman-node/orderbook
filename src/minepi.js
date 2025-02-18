require('dotenv').config();
const axios = require('axios');

const API_KEY = 'clzcsluyfu57lpdibpyozdh60iifvckpt4fhoscyl6w33mgitvibdp5e7ua61ikb'; // Store in .env file
const BASE_URL = 'https://api.minepi.com/v2';

async function getTransaction(txid) {
    try {
        const response = await axios.get(`${BASE_URL}/payments/${txid}`, {
            headers: { Authorization: `Key ${API_KEY}` }
        });
        console.log(response.data);
    } catch (error) {
        console.error('Error fetching transaction:', error.response?.data || error.message);
    }
}

// Example: Listen for a specific transaction
getTransaction('671a75b779be76ccf57ae9cc370c24a4d27e6cc5aac75ab5f27b46413945578d');
