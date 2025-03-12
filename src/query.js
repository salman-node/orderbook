import connection from "./db_connection.js";


const raw_query = async function (query, values) {
    try {
      const sql = query;
      const conn = await connection.getConnection();
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

  const consumeMessages = async () => { raw_query(`INSERT INTO crypto_pair (
    base_asset_id, quote_asset_id, pair_symbol, current_price, 
    min_base_qty, max_base_qty, min_quote_qty, max_quote_qty, 
    trade_fee, chart_url, icon, trade_status, pro_trade, 
    change_in_price, quantity_decimal, price_decimal, 
    popular, status, created_at, updated_at
)
SELECT 
    c.id AS base_asset_id, 
    1 AS quote_asset_id, 
    CONCAT(c.symbol, 'INR') AS pair_symbol, 
    c.buy_price AS current_price, 
    c.min_buy AS min_base_qty, 
    c.max_buy AS max_base_qty, 
    c.min_sale AS min_quote_qty, 
    c.max_sale AS max_quote_qty, 
    0.1 AS trade_fee,  -- Adjust as needed
    c.chartUrl AS chart_url, 
    c.icon, 
    c.trade_status, 
    c.pro_trade, 
    c.change_in_price, 
    c.pro_decimals AS quantity_decimal, 
    c.pro_price_decimals AS price_decimal, 
    c.popular, 
    c.status AS status, 
    NOW() AS created_at, 
    NOW() AS updated_at
FROM currencies c
WHERE c.id != 1;  -- Exclude INR itself
`)}

consumeMessages();


// await connection.query(`INSERT INTO crypto_pair (
//     base_asset_id, quote_asset_id, pair_symbol, current_price, 
//     min_base_qty, max_base_qty, min_quote_qty, max_quote_qty, 
//     trade_fee, chart_id, icon, trade_status, pro_trade, 
//     change_in_price, quantity_decimal, price_decimal, 
//     popular, status, created_at, updated_at
// )
// SELECT 
//     c.id AS base_asset_id, 
//     1 AS quote_asset_id, 
//     CONCAT(c.symbol, 'INR') AS pair_symbol, 
//     c.buy_price AS current_price, 
//     c.min_buy AS min_base_qty, 
//     c.max_buy AS max_base_qty, 
//     c.min_sale AS min_quote_qty, 
//     c.max_sale AS max_quote_qty, 
//     0.1 AS trade_fee,  -- Adjust as needed
//     c.chartUrl AS chart_id, 
//     c.icon, 
//     c.trade_status, 
//     c.pro_trade, 
//     c.change_in_price, 
//     c.deciml AS quantity_decimal, 
//     c.pro_price_decimals AS price_decimal, 
//     c.popular, 
//     'ONE' AS status, 
//     NOW() AS created_at, 
//     NOW() AS updated_at
// FROM currencies c
// WHERE c.id != 1;  -- Exclude INR itself
// `)



