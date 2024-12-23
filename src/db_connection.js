import { createConnection } from 'mysql2';

const connection = createConnection({
  host: 'localhost',
  database: 'match_engine_db',
  user: 'root',
  password: '',
});

export default connection;