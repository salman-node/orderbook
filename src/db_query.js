import connection from './db_connection.js';

// ... (rest of the code remains the same)

 const Create_Universal_Data = async function (
  database_table_name,
  obJdata
) {
  const abkey = Object.keys(obJdata);
  const abvalue = Object.values(obJdata);
  const sql = `INSERT INTO ${database_table_name} (${abkey}) VALUES (${abvalue.map(() => '?').join(',')})`;

  try {
    const conn = await connection.promise().getConnection();
    try {
      const result = await conn.query(sql, abvalue);
      return result;
    } finally {
      conn.release();
    }
  } catch (err) {
    throw err;
  }
};

const raw_query = async function (query, values) {
  try {
    const sql = query;
    const conn = await connection.promise().getConnection();
    try {
      const [result] = await conn.query(sql, values);
      return result;
    } finally {
      conn.release();
    }
  } catch (err) {
    throw err;
  }
};

// ... (rest of the code remains the same)

const Get_All_Universal_Data = async function (data, database_table_name) {
  const sql = `SELECT ${data} FROM ${database_table_name};`;
  try {
    const conn = await connection.promise().getConnection();
    try {
      const [result] = await conn.query(sql);
      return result;
    } finally {
      conn.release();
    }
  } catch (err) {
    throw err;
  }
};

// ... (rest of the code remains the same)

const Get_Where_Universal_Data = async function (data, database_table_name, filterquery) {
  const abkey = Object.keys(filterquery);
  const abvalue = Object.values(filterquery);
  const abkeydata = abkey.join("=? and ") + "=?";
  const sql = `SELECT ${data} FROM ${database_table_name} WHERE ${abkeydata}`;
  console.log(sql);
  try {
    const conn = await connection.promise().getConnection();
    try {
      const [result] = await conn.query(sql, abvalue);
      return result;
    } finally {
      conn.release();
    }
  } catch (err) {
    throw err;
  }
};

// ... (rest of the code remains the same)

const Update_Universal_Data = async function (database_table_name, updatedata, filterquery) {
  const abkey = Object.keys(updatedata);
  const abvalue = Object.values(updatedata);
  const abkeydata = abkey.join("= ? , ") + "= ? ";

  const filterkey = Object.keys(filterquery);
  const filtervalue = Object.values(filterquery);
  const filterkeydata = filterkey.join("= ? AND ") + " = ?";

  const values = [...abvalue, ...filtervalue];
  const sql = `UPDATE ${database_table_name} SET ${abkeydata} WHERE ${filterkeydata}`;

  try {
    const conn = await connection.promise().getConnection();
    try {
      const result = await conn.query(sql, values);
      console.log(result.affectedRows + " record(s) updated");
      return result;
    } finally {
      conn.release();
    }
  } catch (err) {
    throw err;
  }
};

export  { Create_Universal_Data , Update_Universal_Data , Get_All_Universal_Data , Get_Where_Universal_Data, raw_query };