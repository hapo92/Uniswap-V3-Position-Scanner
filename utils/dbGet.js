
require('dotenv').config();

const { Pool } = require('pg');
const useSSL = String(process.env.SSL).toLowerCase() === 'true';
const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
  ssl: useSSL ? { rejectUnauthorized: false } : false
});


async function getTokenIdById(tokenId, chainId, positionManager) {
  const res = await pool.query(
    'SELECT * FROM token_ids WHERE token_id = $1', 
    [tokenId]
  );
  return res.rows[0];
}



async function getTokenIdsWithLastSnapshotBatch(chainId, positionManager, limit = 200, offset = 0) {
  const query = `
    SELECT 
      t.id,
      t.token_id,
      t.chain_name,
      t.chain_id,
      t.protocol_name,
      t.position_manager,
      t.token_id_owner,
      t.is_burned,
      t.is_closed,
      t.tx_hash,
      t.extra_data,
      COALESCE(CAST(ls.snapshot_timestamp AS TEXT), CAST(t.block_timestamp AS TEXT)) AS last_snapshot_time,
      COALESCE(ls.snapshot_timestamp_readable, t.block_timestamp_readable) AS last_snapshot_time_readable,
      COALESCE(CAST(ls.snapshot_block AS TEXT), CAST(t.block_number AS TEXT)) AS last_snapshot_block,
      COALESCE(ls.events, '[]') AS last_snapshot_events,
      ls.token_id IS NULL AS has_no_snapshots
    FROM token_ids t
    LEFT JOIN (
      SELECT 
        ps.token_id,
        ps.snapshot_timestamp,
        ps.snapshot_block,  
        ps.snapshot_timestamp_readable,
        ps.events,
        ROW_NUMBER() OVER (PARTITION BY ps.token_id ORDER BY ps.snapshot_timestamp DESC) as rn
      FROM token_id_snapshots ps
      JOIN token_ids t2 ON ps.token_id = t2.token_id
      WHERE t2.chain_id = $3 AND t2.position_manager = $4
    ) ls ON t.token_id = ls.token_id AND ls.rn = 1
    WHERE t.chain_id = $3
      AND t.position_manager = $4
      AND (t.is_burned IS NULL OR t.is_burned = false)
      AND (t.is_closed IS NULL OR t.is_closed = false)
      AND (
        (ls.snapshot_timestamp IS NOT NULL AND ls.snapshot_timestamp > (EXTRACT(EPOCH FROM now()) - 3*3*24*3600))
        OR
        (ls.snapshot_timestamp IS NULL AND t.block_timestamp IS NOT NULL AND CAST(t.block_timestamp AS BIGINT) > (EXTRACT(EPOCH FROM now()) - 3*3*24*3600))
      )
    ORDER BY t.token_id ASC
    LIMIT $1 OFFSET $2
  `;

  return (await pool.query(query, [limit, offset, chainId, positionManager])).rows;
}





async function getLastProcessedBlock(chainId, positionManager) {
  // Get current timestamp and calculate 24 hours ago
  const currentTime = Math.floor(Date.now() / 1000);
  const twentyFourHoursAgo = currentTime - 86400; // 24 hours in seconds
  
  const result = await pool.query(
    `SELECT MAX(block_number::bigint) AS max_block 
     FROM token_ids 
     WHERE chain_id = $1 
       AND position_manager = $2 
       AND block_timestamp >= $3`,
    [chainId, positionManager, twentyFourHoursAgo]
  );
  
  // If no recent blocks found, return null to trigger fallback to current block
  if (!result.rows[0]?.max_block) {
    return null;
  }
  
  return Number(result.rows[0].max_block);
}

module.exports = { 
  getTokenIdById,
  getTokenIdsWithLastSnapshotBatch,
  getLastProcessedBlock
};
