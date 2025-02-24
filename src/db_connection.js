import { createPool } from 'mysql2';

const connection = createPool({
  host: 'localhost',
  database: 'matchengine',
  user: 'root',
  password: 'root',
  port:3307,
  connectTimeout: 10000,
  connectionLimit: 50, // adjust this value to control the number of connections in the pool
});       


connection.getConnection((err, conn) => {
  if (err) {
    console.error('Error connecting to database:', err);
  } else {
    console.log('Connected to database');
    conn.release();
  }
});
export default connection;