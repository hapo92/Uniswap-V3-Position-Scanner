
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



async function saveNewTokenIds(position) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    
    const query = `
      INSERT INTO token_ids (
        token_id, block_number, block_timestamp, block_timestamp_readable, protocol_name,
        position_manager, chain_name, chain_id,
        tx_hash, extra_data, is_burned, is_closed, token_id_owner
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
      ON CONFLICT (token_id, chain_id, position_manager)
      DO UPDATE SET
        block_number = EXCLUDED.block_number,
        block_timestamp = EXCLUDED.block_timestamp,
        block_timestamp_readable = EXCLUDED.block_timestamp_readable,
        tx_hash = EXCLUDED.tx_hash,
        extra_data = EXCLUDED.extra_data,
        is_burned = EXCLUDED.is_burned,
        is_closed = EXCLUDED.is_closed,
        token_id_owner = EXCLUDED.token_id_owner,
        fetch_timestamp_readable = to_char(now(), 'YYYY-MM-DD HH24:MI:SS')
      RETURNING id, block_timestamp_readable
    `;
    
    const {
      tokenId,
      blockNumber,
      blockTimestamp,
      blockTimestampReadable,
      protocolName,
      positionManager,
      chainName,
      chainId,
      transactionHash,
      isBurned,
      isClosed,
      tokenIdOwner, 
      ...extra
    } = position;

    const extraData = stringifyBigInts(extra);

    const values = [
      tokenId,
      blockNumber,
      blockTimestamp,
      blockTimestampReadable,
      protocolName,
      positionManager,
      chainName,
      chainId,
      transactionHash,
      JSON.stringify(extraData),
      isBurned,
      isClosed,
      tokenIdOwner
    ];
    
    const res = await client.query(query, values);
    await client.query('COMMIT');
    return res.rows[0].id;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}



// Helper
function stringifyBigInts(obj) {
  if (typeof obj === 'bigint') {
    return obj.toString();
  } else if (Array.isArray(obj)) {
    return obj.map(stringifyBigInts);
  } else if (obj !== null && typeof obj === 'object') {
    const res = {};
    for (const [key, value] of Object.entries(obj)) {
      res[key] = stringifyBigInts(value);
    }
    return res;
  }
  return obj;
}


async function saveMarketSnapshots(snapshots) {
  if (!Array.isArray(snapshots) || snapshots.length === 0) return;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // MUST match your DB column order!
    const columns = [
      'track_position_url',
      'track_pool_url',
      'name',
      'pool_tvl',
      'range',
      'duration_hours',
      'avg_aum',
      'apr',
      'aum',
      'pnl',
      'roi',
      'yield',
      'is_bot_contract',
      'manager_account',
      'source_identifier',
      'owner_is_contract',
      'token_id_owner',
      'total_deposited',
      'total_withdrawn',
      'collected_fees',
      'uncollected_fees',
      'collected_rewards',
      'uncollected_rewards',
      'chain_name',
      'chain_id',
      'protocol',
      'position_manager',
      'collect_address',
      'pool_address',
      'pool_fee',
      'type',
      'tick_spacing',
      'tick_lower',    // <-- added here
      'tick_upper',    // <-- added here
      'current_tick',  // <-- added here
      'symbol_t0',
      'address_t0',
      'symbol_t1',
      'address_t1',
      'decimals_t0',
      'decimals_t1',
      'amount_t0',
      'amount_t1',
      'amount_t0_price_eth',
      'amount_t1_price_eth',
      'token0_price_eth',
      'token1_price_eth',
      'fee_growth_global_0_x128',
      'fee_growth_global_1_x128',
      'pool_active_liquidity',
      'liquidity',
      'symbol_reward_t',
      'address_reward_t',
      'reward_token_price_eth',
      'events',
      'combined_raw_events',
      'duration_seconds',
      'snapshot_block',
      'snapshot_timestamp',
      'snapshot_timestamp_readable',
      'is_closed',
      'token_id'
    ];

    const values = [];
    const placeholders = [];

    snapshots.forEach((snapshot, i) => {
      const valueOffset = i * columns.length;
      placeholders.push(
        `(${columns.map((_, idx) => `$${valueOffset + idx + 1}`).join(', ')})`
      );

      values.push(
        snapshot.trackPositionURL || snapshot.track_position_url || null,
        snapshot.trackPoolURL || snapshot.track_pool_url || null,
        snapshot.name || null,
        Number(snapshot.poolTVL ?? snapshot.pool_tvl ?? 0),
        snapshot.range || null,
        Number(snapshot.durationHours ?? snapshot.duration_hours ?? 0),
        Number(snapshot.avgAUM ?? snapshot.avg_aum ?? 0),
        Number(snapshot.apr ?? snapshot.APR ?? 0),
        Number(snapshot.aum ?? 0),
        Number(snapshot.pnl ?? snapshot.PnL ?? 0),
        Number(snapshot.roi ?? 0),
        Number(snapshot.yield ?? 0),
        snapshot.isBotContract !== undefined ? Boolean(snapshot.isBotContract) :
          snapshot.is_bot_contract !== undefined ? Boolean(snapshot.is_bot_contract) : null,
        snapshot.managerAccount || snapshot.manager_account || null,
        snapshot.protocolIdentifier || snapshot.source_identifier || null,
        snapshot.ownerIsContract !== undefined ? Boolean(snapshot.ownerIsContract) :
          snapshot.owner_is_contract !== undefined ? Boolean(snapshot.owner_is_contract) : null,
        snapshot.owner || snapshot.token_id_owner || null,
        Number(snapshot.totalDeposited ?? snapshot.total_deposited ?? 0),
        Number(snapshot.totalWithdrawn ?? snapshot.total_withdrawn ?? 0),
        Number(snapshot.collectedFees ?? snapshot.collected_fees ?? 0),
        Number(snapshot.uncollectedFees ?? snapshot.uncollected_fees ?? 0),
        Number(snapshot.collectedRewards ?? snapshot.collected_rewards ?? 0),
        Number(snapshot.uncollectedRewards ?? snapshot.uncollected_rewards ?? 0),
        snapshot.chainName || snapshot.chain_name || null,
        Number(snapshot.chainId ?? snapshot.chain_id ?? 0),
        snapshot.protocol || null,
        snapshot.positionManager || snapshot.position_manager || null,
        snapshot.collectAddress || snapshot.collect_address || null,
        snapshot.poolAddress || snapshot.pool_address || null,
        Number(snapshot.poolFee ?? snapshot.pool_fee ?? 0),
        snapshot.type || null,
        Number(snapshot.tickSpacing ?? snapshot.tick_spacing ?? 0),
        Number(snapshot.tickLower ?? snapshot.tick_lower ?? 0),   // <-- added here
        Number(snapshot.tickUpper ?? snapshot.tick_upper ?? 0),   // <-- added here
        Number(snapshot.currentTick ?? snapshot.current_tick ?? 0), // <-- added here
        snapshot.symbolT0 || snapshot.symbol_t0 || null,
        snapshot.addressT0 || snapshot.address_t0 || null,
        snapshot.symbolT1 || snapshot.symbol_t1 || null,
        snapshot.addressT1 || snapshot.address_t1 || null,
        Number(snapshot.decimalsT0 ?? snapshot.decimals_t0 ?? 0),
        Number(snapshot.decimalsT1 ?? snapshot.decimals_t1 ?? 0),
        Number(snapshot.amountT0 ?? snapshot.amount_t0 ?? 0),
        Number(snapshot.amountT1 ?? snapshot.amount_t1 ?? 0),
        Number(snapshot.amountT0Price_ETH ?? snapshot.amount_t0_price_eth ?? 0),
        Number(snapshot.amountT1Price_ETH ?? snapshot.amount_t1_price_eth ?? 0),
        Number(snapshot.token0PriceETH ?? snapshot.token0_price_eth ?? 0),
        Number(snapshot.token1PriceETH ?? snapshot.token1_price_eth ?? 0),
        snapshot.feeGrowthGlobal0X128 || null,
        snapshot.feeGrowthGlobal1X128 || null,
        snapshot.poolActiveLiquidity || null,
        snapshot.liquidity || null,
        snapshot.symbolTR || snapshot.symbol_reward_t || null,
        snapshot.addressTR || snapshot.address_reward_t || null,
        Number(snapshot.rewardTokenPriceETH ?? snapshot.reward_token_price_eth ?? 0),
        typeof snapshot.events === 'string'
          ? snapshot.events
          : JSON.stringify(snapshot.events || {}),
        typeof snapshot.rawEvents === 'string'
          ? snapshot.rawEvents
          : JSON.stringify(snapshot.rawEvents || {}),
        Number(snapshot.durationSeconds ?? snapshot.duration_seconds ?? 0),
        Number(snapshot.snapshot_block ?? 0),
        Number(snapshot.snapshot_timestamp ?? 0),
        snapshot.snapshot_timestamp_readable || null,
        snapshot.isClosed !== undefined ? Boolean(snapshot.isClosed) :
          snapshot.is_closed !== undefined ? Boolean(snapshot.is_closed) : false,
        Number(snapshot.tokenId ?? snapshot.token_id ?? 0)
      );
    });

    const query = `
      INSERT INTO token_id_snapshots (
        ${columns.join(', ')}
      ) VALUES
        ${placeholders.join(',\n')}
    `;

    await client.query(query, values);
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}



async function upsertPositionsFromSnapshots() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // NOTE: List all columns *except* id (as id is SERIAL)
    await client.query(`
      INSERT INTO positions (
        track_position_url, track_pool_url, name, pool_tvl, range,
        duration_hours, avg_aum, apr, aum, pnl, roi, yield,
        is_bot_contract, manager_account, source_identifier, owner_is_contract, token_id_owner,
        total_deposited, total_withdrawn, collected_fees, uncollected_fees, collected_rewards, uncollected_rewards,
        chain_name, chain_id, protocol, position_manager, collect_address, pool_address,
        pool_fee, type, tick_spacing,
        symbol_t0, address_t0, symbol_t1, address_t1,
        decimals_t0, decimals_t1, amount_t0, amount_t1,
        amount_t0_price_eth, amount_t1_price_eth, token0_price_eth, token1_price_eth,
        symbol_reward_t, address_reward_t, reward_token_price_eth,
        events, combined_raw_events, duration_seconds, snapshot_block, snapshot_timestamp, snapshot_timestamp_readable,
        is_closed, token_id
      )
      SELECT
        track_position_url, track_pool_url, name, pool_tvl, range,
        duration_hours, avg_aum, apr, aum, pnl, roi, yield,
        is_bot_contract, manager_account, source_identifier, owner_is_contract, token_id_owner,
        total_deposited, total_withdrawn, collected_fees, uncollected_fees, collected_rewards, uncollected_rewards,
        chain_name, chain_id, protocol, position_manager, collect_address, pool_address,
        pool_fee, type, tick_spacing,
        symbol_t0, address_t0, symbol_t1, address_t1,
        decimals_t0, decimals_t1, amount_t0, amount_t1,
        amount_t0_price_eth, amount_t1_price_eth, token0_price_eth, token1_price_eth,
        symbol_reward_t, address_reward_t, reward_token_price_eth,
        events, combined_raw_events, duration_seconds, snapshot_block, snapshot_timestamp, snapshot_timestamp_readable,
        is_closed, token_id
      FROM (
        SELECT DISTINCT ON (token_id, chain_id, position_manager) *
        FROM token_id_snapshots
        ORDER BY token_id, chain_id, position_manager, snapshot_timestamp DESC
      ) AS latest_snaps
      ON CONFLICT (token_id, chain_id, position_manager)
      DO UPDATE SET
        track_position_url = EXCLUDED.track_position_url,
        track_pool_url = EXCLUDED.track_pool_url,
        name = EXCLUDED.name,
        pool_tvl = EXCLUDED.pool_tvl,
        range = EXCLUDED.range,
        duration_hours = EXCLUDED.duration_hours,
        avg_aum = EXCLUDED.avg_aum,
        apr = EXCLUDED.apr,
        aum = EXCLUDED.aum,
        pnl = EXCLUDED.pnl,
        roi = EXCLUDED.roi,
        yield = EXCLUDED.yield,
        is_bot_contract = EXCLUDED.is_bot_contract,
        manager_account = EXCLUDED.manager_account,
        source_identifier = EXCLUDED.source_identifier,
        owner_is_contract = EXCLUDED.owner_is_contract,
        token_id_owner = EXCLUDED.token_id_owner,
        total_deposited = EXCLUDED.total_deposited,
        total_withdrawn = EXCLUDED.total_withdrawn,
        collected_fees = EXCLUDED.collected_fees,
        uncollected_fees = EXCLUDED.uncollected_fees,
        collected_rewards = EXCLUDED.collected_rewards,
        uncollected_rewards = EXCLUDED.uncollected_rewards,
        chain_name = EXCLUDED.chain_name,
        protocol = EXCLUDED.protocol,
        position_manager = EXCLUDED.position_manager,
        collect_address = EXCLUDED.collect_address,
        pool_address = EXCLUDED.pool_address,
        pool_fee = EXCLUDED.pool_fee,
        type = EXCLUDED.type,
        tick_spacing = EXCLUDED.tick_spacing,
        symbol_t0 = EXCLUDED.symbol_t0,
        address_t0 = EXCLUDED.address_t0,
        symbol_t1 = EXCLUDED.symbol_t1,
        address_t1 = EXCLUDED.address_t1,
        decimals_t0 = EXCLUDED.decimals_t0,
        decimals_t1 = EXCLUDED.decimals_t1,
        amount_t0 = EXCLUDED.amount_t0,
        amount_t1 = EXCLUDED.amount_t1,
        amount_t0_price_eth = EXCLUDED.amount_t0_price_eth,
        amount_t1_price_eth = EXCLUDED.amount_t1_price_eth,
        token0_price_eth = EXCLUDED.token0_price_eth,
        token1_price_eth = EXCLUDED.token1_price_eth,
        symbol_reward_t = EXCLUDED.symbol_reward_t,
        address_reward_t = EXCLUDED.address_reward_t,
        reward_token_price_eth = EXCLUDED.reward_token_price_eth,
        events = EXCLUDED.events,
        combined_raw_events = EXCLUDED.combined_raw_events,
        duration_seconds = EXCLUDED.duration_seconds,
        snapshot_block = EXCLUDED.snapshot_block,
        snapshot_timestamp = EXCLUDED.snapshot_timestamp,
        snapshot_timestamp_readable = EXCLUDED.snapshot_timestamp_readable,
        is_closed = EXCLUDED.is_closed
      ;
    `);
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}



async function syncTokenIdsIsClosedFromSnapshots(snapshots) {
  if (!Array.isArray(snapshots) || snapshots.length === 0) return;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const snap of snapshots) {
      await client.query(
        `UPDATE token_ids
         SET is_closed = $1
         WHERE token_id = $2 AND chain_id = $3 AND position_manager = $4`,
        [
          snap.isClosed !== undefined ? !!snap.isClosed : !!snap.is_closed,
          snap.tokenId ?? snap.token_id,
          snap.chainId ?? snap.chain_id,
          snap.positionManager ?? snap.position_manager,
        ]
      );
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}




const TABLES = [
  'public.token_ids',
  'public.token_id_snapshots',
  'public.positions',
  'public.vx_bots',
  'public.trending_positions',
  'public.pool_snapshots',
  'public.pools_rebalance_ranges',
  'public.managers',
  'public.market_bots'
];

async function truncateAllTables() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Truncate all tables and reset sequences, cascading if necessary
    await client.query(`
      TRUNCATE ${TABLES.join(', ')} RESTART IDENTITY CASCADE;
    `);
    await client.query('COMMIT');
    console.log('All relevant tables have been truncated and reset!');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error truncating tables:', err);
    throw err;
  } finally {
    client.release();
  }
}


module.exports = { 
  saveNewTokenIds,
  saveMarketSnapshots,
  upsertPositionsFromSnapshots,
  syncTokenIdsIsClosedFromSnapshots,
  truncateAllTables
};