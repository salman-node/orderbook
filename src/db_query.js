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
    const result = await connection.promise().query(sql, abvalue);
    return result;
  } catch (err) {
    throw err;
  }
};

const raw_query = async function (query, values) {
  try {
  const sql = query;
    // Use connection.promise() to ensure that the query returns a promise
    const [result] = await connection.promise().query(sql, values);
    return result;
  } catch (err) {
    throw err;
  }
};

// ... (rest of the code remains the same)

 const Get_All_Universal_Data = async function (
  data,
  database_table_name
) {
  return await new Promise((resolve, reject) => {
    const sql = `SELECT ${data} FROM ${database_table_name};`;
    connection.query(sql, (err, result) => {
      if (err) throw err;
      resolve(result);
    });
  });
};

// ... (rest of the code remains the same)

 const Get_Where_Universal_Data = async function (
  data,
  database_table_name,
  filterquery
) {
  return await new Promise((resolve, reject) => {
    const abkey = Object.keys(filterquery);
    const abvalue = Object.values(filterquery);
    const abkeydata = abkey.join("=? and ") + "=?";

    const sql = `SELECT ${data} FROM ${database_table_name} WHERE ${abkeydata}`;
    connection.query(sql, abvalue, (err, result) => {
      if (err) throw err;
      resolve(result);
    });
  });
};

// ... (rest of the code remains the same)

const Update_Universal_Data = async function (
  database_table_name,
  updatedata,
  filterquery
) {
  return await new Promise((resolve, reject) => {
    const abkey = Object.keys(updatedata);
    const abvalue = Object.values(updatedata);
    const abkeydata = abkey.join("= ? , ") + "= ? ";

    const filterkey = Object.keys(filterquery);
    const filtervalue = Object.values(filterquery);
    const filterkeydata = filterkey.join("= ? AND ") + " = ?";

    const result = Object.entries(filterquery);
    const rdata = result.join(" and ");
    const sdata = rdata.split(",");
    const equaldata = sdata.join(" = ");
    const values = [...abvalue, ...filtervalue];

    connection.connect((err) => {
      if (err) throw err;
      const sql = `UPDATE ${database_table_name} SET ${abkeydata} WHERE ${filterkeydata}`;
      connection.query(sql, values, (err, result) => {
        if (err) throw err;
        console.log(result.affectedRows + " record(s) updated");
        resolve(result);
      });
    });
  });
};

export  { Create_Universal_Data , Update_Universal_Data , Get_All_Universal_Data , Get_Where_Universal_Data, raw_query };