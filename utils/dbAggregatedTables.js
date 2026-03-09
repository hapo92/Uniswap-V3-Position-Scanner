// upsertPositions.js
const { Pool } = require('pg');
require('dotenv').config();
const format = require('pg-format');

const useSSL = String(process.env.SSL).toLowerCase() === 'true';
const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
  ssl: useSSL ? { rejectUnauthorized: false } : false
});




////////////////////////////////////////////
//////////////   Positions    //////////////
////////////////////////////////////////////

async function updatePositions() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
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
    `);
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }

}

// Mutex to prevent concurrent execution
let isUpdatingTrendingPositions = false;

async function updateTrendingPositions() {
  // Prevent concurrent execution
  if (isUpdatingTrendingPositions) {
    console.log('updateTrendingPositions already running, skipping...');
    return;
  }
  
  isUpdatingTrendingPositions = true;
  
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const { rows: [{ db_now }] } = await client.query(`SELECT EXTRACT(EPOCH FROM NOW())::BIGINT AS db_now`);
    const cutoff = db_now - 86400; // 24 hours ago

    // Delete all existing trending positions
    await client.query(`DELETE FROM trending_positions`);

    await client.query(`
      INSERT INTO trending_positions (
        track_position_url, track_pool_url, name, pool_tvl, range,
        duration_hours, avg_aum, apr, aum, pnl, roi, yield,
        is_bot_contract, manager_account, source_identifier, owner_is_contract, token_id_owner,
        total_deposited, total_withdrawn, collected_fees, uncollected_fees, collected_rewards, uncollected_rewards,
        chain_name, chain_id, protocol, position_manager, collect_address, pool_address,
        pool_fee, type, tick_spacing, tick_lower, tick_upper, current_tick,
        symbol_t0, address_t0, symbol_t1, address_t1,
        decimals_t0, decimals_t1, amount_t0, amount_t1,
        amount_t0_price_eth, amount_t1_price_eth, token0_price_eth, token1_price_eth,
        symbol_reward_t, address_reward_t, reward_token_price_eth,
        events, combined_raw_events, duration_seconds, snapshot_block, snapshot_timestamp, open_time,
        is_closed, token_id, last_updated_hour
      )
      SELECT
        latest.track_position_url, latest.track_pool_url, latest.name, latest.pool_tvl, latest.range,
        latest.duration_seconds / 3600.0 AS duration_hours,
        latest.avg_aum, latest.apr, latest.aum, latest.pnl, latest.roi, latest.yield,
        latest.is_bot_contract, latest.manager_account, latest.source_identifier, latest.owner_is_contract, latest.token_id_owner,
        latest.total_deposited, latest.total_withdrawn, latest.collected_fees, latest.uncollected_fees, latest.collected_rewards, latest.uncollected_rewards,
        latest.chain_name, latest.chain_id, latest.protocol, latest.position_manager, latest.collect_address, latest.pool_address,
        latest.pool_fee, latest.type, latest.tick_spacing, latest.tick_lower, latest.tick_upper, latest.current_tick,
        latest.symbol_t0, latest.address_t0, latest.symbol_t1, latest.address_t1,
        latest.decimals_t0, latest.decimals_t1, latest.amount_t0, latest.amount_t1,
        latest.amount_t0_price_eth, latest.amount_t1_price_eth, latest.token0_price_eth, latest.token1_price_eth,
        latest.symbol_reward_t, latest.address_reward_t, latest.reward_token_price_eth,
        latest.events, latest.combined_raw_events, latest.duration_seconds, latest.snapshot_block, latest.snapshot_timestamp,
        latest.snapshot_timestamp_readable AS open_time,
        latest.is_closed, latest.token_id,
        ($2 - latest.snapshot_timestamp)::NUMERIC / 3600.0 AS last_updated_hour
      FROM (
        SELECT DISTINCT ON (token_id, chain_id, position_manager) *
        FROM token_id_snapshots
        WHERE snapshot_timestamp >= $1
          AND snapshot_timestamp <= $2
          AND is_closed = false
        ORDER BY token_id, chain_id, position_manager, snapshot_timestamp DESC
      ) AS latest
      WHERE
        latest.pool_tvl >= 5
        AND latest.avg_aum >= 0.05
        AND (latest.duration_seconds / 3600.0) > 1
        AND (latest.duration_seconds / 3600.0) < 12
        AND latest.apr >= 1000
        AND latest.tick_spacing >= 50
    `, [cutoff, db_now]);

    await client.query('COMMIT');
    console.log("Trending Positions Table Updated! (table fully refreshed)");
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error updating trending_positions:', err);
    throw err;
  } finally {
    client.release();
    isUpdatingTrendingPositions = false;
  }
}





////////////////////////////////////////////
//////////////      POOLS     //////////////
////////////////////////////////////////////


// Helper for X128 math using BigInt
function feesFromX128Delta(delta, liquidity) {
  const Q128 = BigInt(2) ** BigInt(128);
  return (delta * liquidity) / Q128;
}

// Helper to convert token amount to ETH
function toETH(tokenAmount, priceETH, decimals) {
  return Number(tokenAmount) * Number(priceETH) / Math.pow(10, Number(decimals));
}





async function updatePoolsSnapshots() {
  const client = await pool.connect();
  try {
    console.log('[tokenIdSnapshots Timings] - Starting Query');
    const startQuery = Date.now();

    await client.query('BEGIN');

    const { rows: snapshots } = await client.query(`
      SELECT * FROM (
        SELECT
          MAX(track_pool_url) AS track_pool_url,
          MAX(name) AS name,
          MAX(type) AS type,
          MAX(tick_spacing) AS tick_spacing,
          MAX(current_tick) AS current_tick,
          MAX(pool_fee) AS pool_fee,
          MAX(pool_tvl) AS pool_tvl,
          MAX(pool_tvl0) AS pool_tvl0,
          MAX(pool_tvl1) AS pool_tvl1,
          MAX(fee_growth_global_0_x128) AS fee_growth_global_0_x128,
          MAX(fee_growth_global_1_x128) AS fee_growth_global_1_x128,
          MAX(pool_active_liquidity) AS pool_active_liquidity,
          MAX(liquidity) AS liquidity,
          MAX(address_t0) AS token0,
          MAX(address_t1) AS token1,
          MAX(symbol_t0) AS symbol_t0,
          MAX(symbol_t1) AS symbol_t1,
          MAX(decimals_t0) AS decimals_t0,
          MAX(decimals_t1) AS decimals_t1,
          MAX(token0_price_eth) AS token0_price_eth,
          MAX(token1_price_eth) AS token1_price_eth,
          MAX(address_reward_t) AS reward_token,
          MAX(symbol_reward_t) AS reward_token_symbol,
          MAX(reward_token_price_eth) AS reward_token_price_eth,
          snapshot_timestamp AS last_snapshot_timestamp,
          pool_address,
          chain_id,
          MAX(protocol) AS protocol,
          MAX(position_manager) AS position_manager,
          MAX(collect_address) AS collect_address
        FROM token_id_snapshots
        WHERE pool_address IS NOT NULL
          AND (COALESCE(pool_active_liquidity, 0) > 0 OR COALESCE(liquidity, 0) > 0)
          AND (COALESCE(fee_growth_global_0_x128, 0) > 0)
          AND (COALESCE(fee_growth_global_1_x128, 0) > 0)
        GROUP BY pool_address, chain_id, snapshot_timestamp
      ) sub
      ORDER BY pool_address, chain_id, last_snapshot_timestamp
    `);

    console.log(`[tokenIdSnapshots Timings] - Query took ${(Date.now() - startQuery) / 1000}s`);
    console.log('[tokenIdSnapshots Timings] - Starting Processing & Inserting Snapshots');
    const startInsert = Date.now();

    const values = [];

    for (let i = 0; i < snapshots.length; i++) {
      const snap = snapshots[i];
      const prev = i > 0 &&
        snapshots[i - 1].pool_address === snap.pool_address &&
        snapshots[i - 1].chain_id === snap.chain_id
        ? snapshots[i - 1]
        : null;

      let interval_seconds = 0, fees0 = 0, fees1 = 0, fees_eth = 0, volume_eth = 0;

      if (prev) {
        interval_seconds = snap.last_snapshot_timestamp - prev.last_snapshot_timestamp;
        const liquidity = BigInt(snap.pool_active_liquidity || snap.liquidity || 0);
        const delta0 = BigInt(snap.fee_growth_global_0_x128 || 0) - BigInt(prev.fee_growth_global_0_x128 || 0);
        const delta1 = BigInt(snap.fee_growth_global_1_x128 || 0) - BigInt(prev.fee_growth_global_1_x128 || 0);
        fees0 = feesFromX128Delta(delta0, liquidity);
        fees1 = feesFromX128Delta(delta1, liquidity);
        const fees0_eth = toETH(fees0, snap.token0_price_eth, snap.decimals_t0);
        const fees1_eth = toETH(fees1, snap.token1_price_eth, snap.decimals_t1);
        fees_eth = fees0_eth + fees1_eth;
        // volume_eth = 0; // volume intentionally left blank/commented
      }

      values.push([
        snap.track_pool_url, snap.name, snap.type, snap.tick_spacing, snap.current_tick, snap.pool_fee,
        snap.pool_tvl, snap.pool_tvl0, snap.pool_tvl1, snap.fee_growth_global_0_x128, snap.fee_growth_global_1_x128,
        snap.pool_active_liquidity, snap.liquidity, snap.token0, snap.token1, snap.symbol_t0, snap.symbol_t1,
        snap.decimals_t0, snap.decimals_t1, snap.token0_price_eth, snap.token1_price_eth,
        snap.reward_token, snap.reward_token_symbol, snap.reward_token_price_eth, 0,
        snap.last_snapshot_timestamp, snap.pool_address, snap.chain_id,
        snap.protocol, snap.position_manager, snap.collect_address,
        fees_eth, volume_eth, interval_seconds
      ]);
    }

    if (values.length > 0) {
      const insertQuery = format(`
        INSERT INTO public.pool_snapshots (
          track_pool_url, name, type, tick_spacing, current_tick, pool_fee,
          pool_tvl, pool_tvl0, pool_tvl1, fee_growth_global_0_x128, fee_growth_global_1_x128,
          pool_active_liquidity, liquidity, token0, token1, symbol_t0, symbol_t1,
          decimals_t0, decimals_t1, token0_price_eth, token1_price_eth, reward_token,
          reward_token_symbol, reward_token_price_eth, number_of_active_positions,
          last_snapshot_timestamp, pool_address, chain_id, protocol, position_manager, collect_address,
          fees_interval_eth, volume_interval_eth, interval_seconds
        ) VALUES %L
        ON CONFLICT (pool_address, chain_id, last_snapshot_timestamp)
        DO UPDATE SET
          track_pool_url = EXCLUDED.track_pool_url,
          name = EXCLUDED.name,
          type = EXCLUDED.type,
          tick_spacing = EXCLUDED.tick_spacing,
          current_tick = EXCLUDED.current_tick,
          pool_fee = EXCLUDED.pool_fee,
          pool_tvl = EXCLUDED.pool_tvl,
          pool_tvl0 = EXCLUDED.pool_tvl0,
          pool_tvl1 = EXCLUDED.pool_tvl1,
          fee_growth_global_0_x128 = EXCLUDED.fee_growth_global_0_x128,
          fee_growth_global_1_x128 = EXCLUDED.fee_growth_global_1_x128,
          pool_active_liquidity = EXCLUDED.pool_active_liquidity,
          liquidity = EXCLUDED.liquidity,
          token0 = EXCLUDED.token0,
          token1 = EXCLUDED.token1,
          symbol_t0 = EXCLUDED.symbol_t0,
          symbol_t1 = EXCLUDED.symbol_t1,
          decimals_t0 = EXCLUDED.decimals_t0,
          decimals_t1 = EXCLUDED.decimals_t1,
          token0_price_eth = EXCLUDED.token0_price_eth,
          token1_price_eth = EXCLUDED.token1_price_eth,
          reward_token = EXCLUDED.reward_token,
          reward_token_symbol = EXCLUDED.reward_token_symbol,
          reward_token_price_eth = EXCLUDED.reward_token_price_eth,
          last_snapshot_timestamp = EXCLUDED.last_snapshot_timestamp,
          protocol = EXCLUDED.protocol,
          position_manager = EXCLUDED.position_manager,
          collect_address = EXCLUDED.collect_address,
          fees_interval_eth = EXCLUDED.fees_interval_eth,
          volume_interval_eth = EXCLUDED.volume_interval_eth,
          interval_seconds = EXCLUDED.interval_seconds
      `, values);

      await client.query(insertQuery);
    }

    await client.query('COMMIT');
    console.log(`[tokenIdSnapshots Timings] - Processing & Inserting took ${(Date.now() - startInsert) / 1000}s`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error updating pool_snapshots table:', err);
    throw err;
  } finally {
    client.release();
  }
}





async function updatePoolsTable() {
  const client = await pool.connect();
  try {
    const startAll = Date.now();
    console.log('[PoolsTable Timings] - Starting Fetch Affected Pools');

    await client.query('BEGIN');
    const now = Math.floor(Date.now() / 1000);

    const t0 = Date.now();
    const { rows: affectedPools } = await client.query(`
      SELECT DISTINCT pool_address, chain_id
      FROM (
        SELECT pool_address, chain_id
        FROM token_id_snapshots
        WHERE pool_address IS NOT NULL
        ORDER BY snapshot_timestamp DESC
        LIMIT 1000
      ) AS recent
    `);

    const { rows: latest } = await client.query(`
      SELECT DISTINCT ON (pool_address, chain_id) *
      FROM pool_snapshots
      WHERE (pool_address, chain_id) IN (
        SELECT pool_address, chain_id FROM (
          SELECT pool_address, chain_id
          FROM token_id_snapshots
          WHERE pool_address IS NOT NULL
          ORDER BY snapshot_timestamp DESC
          LIMIT 1000
        ) AS recent
      )
      ORDER BY pool_address, chain_id, last_snapshot_timestamp DESC
    `);

    const t1 = Date.now();
    console.log(`[PoolsTable Timings] - Fetch Affected Pools took ${(t1 - t0) / 1000}s`);

    console.log('[PoolsTable Timings] - Starting Main Processing Loop');
    const t2 = Date.now();

    for (const snap of latest) {
      const loopStart = Date.now();

      const targetTimestamp = snap.last_snapshot_timestamp - 86400;
      const { rows: snaps24h } = await client.query(`
        SELECT *, ABS(last_snapshot_timestamp - $3) AS time_diff
        FROM pool_snapshots
        WHERE pool_address = $1 AND chain_id = $2
          AND last_snapshot_timestamp < $4
        ORDER BY time_diff ASC, last_snapshot_timestamp DESC
        LIMIT 1
      `, [snap.pool_address, snap.chain_id, targetTimestamp, snap.last_snapshot_timestamp]);
      let snapOld = snaps24h[0];

      if (!snapOld || Math.abs(snapOld.last_snapshot_timestamp - targetTimestamp) > 2 * 3600) {
        const { rows: [within24h] } = await client.query(`
          SELECT * FROM pool_snapshots
          WHERE pool_address = $1 AND chain_id = $2
            AND last_snapshot_timestamp >= $3
            AND last_snapshot_timestamp < $4
          ORDER BY last_snapshot_timestamp ASC
          LIMIT 1
        `, [snap.pool_address, snap.chain_id, snap.last_snapshot_timestamp - 86400, snap.last_snapshot_timestamp]);
        if (within24h) snapOld = within24h;
      }

      if (!snapOld) {
        const { rows: [oldest] } = await client.query(`
          SELECT * FROM pool_snapshots
          WHERE pool_address = $1 AND chain_id = $2 AND last_snapshot_timestamp < $3
          ORDER BY last_snapshot_timestamp ASC
          LIMIT 1
        `, [snap.pool_address, snap.chain_id, snap.last_snapshot_timestamp]);
        snapOld = oldest;
      }

      let fees_eth = 0, apr = null, tvl_eth = Number(snap.pool_tvl || 0);
      if (snapOld) {
        const liquidity = BigInt(snap.pool_active_liquidity || snap.liquidity || 0);
        const delta0 = BigInt(snap.fee_growth_global_0_x128 || 0) - BigInt(snapOld.fee_growth_global_0_x128 || 0);
        const delta1 = BigInt(snap.fee_growth_global_1_x128 || 0) - BigInt(snapOld.fee_growth_global_1_x128 || 0);
        const Q128 = BigInt(2) ** BigInt(128);
        const fees0 = Number((delta0 * liquidity) / Q128);
        const fees1 = Number((delta1 * liquidity) / Q128);
        const fees0_eth = toETH(fees0, snap.token0_price_eth, snap.decimals_t0);
        const fees1_eth = toETH(fees1, snap.token1_price_eth, snap.decimals_t1);
        fees_eth = fees0_eth + fees1_eth;
        const durationHours = (snap.last_snapshot_timestamp - snapOld.last_snapshot_timestamp) / 3600;
        if (tvl_eth > 0 && durationHours > 0) {
          apr = (fees_eth * 365) / tvl_eth;
        }
      }

      const oneDayAgo = snap.last_snapshot_timestamp - 86400;
      const { rows: ticks } = await client.query(`
        SELECT current_tick, last_snapshot_timestamp AS snapshot_timestamp, tick_spacing, pool_tvl
        FROM public.pool_snapshots
        WHERE pool_address = $1 AND chain_id = $2 AND last_snapshot_timestamp >= $3
        ORDER BY last_snapshot_timestamp ASC
      `, [snap.pool_address, snap.chain_id, oneDayAgo]);

      const numSnapshots = ticks.length;
      let open_date = null;
      if (numSnapshots >= 1) open_date = new Date(ticks[0].snapshot_timestamp * 1000).toISOString();

      const { rows: [{ db_now }] } = await client.query(`SELECT EXTRACT(EPOCH FROM NOW()) AS db_now`);
      const last_updated_hour = (db_now - snap.last_snapshot_timestamp) / 3600;

      let data_avg_update_hour = null;
      if (numSnapshots >= 2) {
        let sumIntervals = 0;
        for (let i = 1; i < numSnapshots; i++) {
          sumIntervals += (ticks[i].snapshot_timestamp - ticks[i - 1].snapshot_timestamp);
        }
        sumIntervals += db_now - ticks[numSnapshots - 1].snapshot_timestamp;
        data_avg_update_hour = (sumIntervals / numSnapshots) / 3600;
      } else if (numSnapshots === 1) {
        data_avg_update_hour = (db_now - ticks[0].snapshot_timestamp) / 3600;
      }

      // ---- NEW CODE FOR last_3h_rebs, last_6h_rebs, last_12h_rebs ----
      function countRebalances(ticksArr, tickSpacing, multiplier) {
        if (!ticksArr.length) return null;
        const rangeSize = tickSpacing * multiplier;
        let count = 0, center = null;
        let duration = 0;
        for (let i = 0; i < ticksArr.length; i++) {
          const row = ticksArr[i];
          if (center === null) {
            center = row.current_tick;
            continue;
          }
          if (Math.abs(row.current_tick - center) >= rangeSize) {
            count++;
            center = row.current_tick;
          }
        }
        duration = (ticksArr[ticksArr.length - 1].snapshot_timestamp - ticksArr[0].snapshot_timestamp) / 3600;
        return duration > 0 ? Math.round((count / duration) * 24) : null;
      }

      let last_3h_rebs = null, last_6h_rebs = null, last_12h_rebs = null;
      let range_1_reb_day = null, range_2_reb_day = null, range_3_reb_day = null, range_4_reb_day = null, range_5_reb_day = null;
      let volatility = null, max_tick_deviation = null, tick_std_dev = null, pool_tvl_24h = null;
      if (numSnapshots >= 2) {
        const tickSpacing = ticks[0].tick_spacing;
        const nowTimestamp = ticks[numSnapshots - 1].snapshot_timestamp;

        // Slice for each window
        const ticks_3h = ticks.filter(t => t.snapshot_timestamp >= nowTimestamp - 3 * 3600);
        const ticks_6h = ticks.filter(t => t.snapshot_timestamp >= nowTimestamp - 6 * 3600);
        const ticks_12h = ticks.filter(t => t.snapshot_timestamp >= nowTimestamp - 12 * 3600);

        last_3h_rebs = countRebalances(ticks_3h, tickSpacing, 0.5);
        last_6h_rebs = countRebalances(ticks_6h, tickSpacing, 0.5);
        last_12h_rebs = countRebalances(ticks_12h, tickSpacing, 0.5);

        // The original
        range_1_reb_day = countRebalances(ticks, tickSpacing, 0.5);
        range_2_reb_day = countRebalances(ticks, tickSpacing, 1);
        range_3_reb_day = countRebalances(ticks, tickSpacing, 1.5);
        range_4_reb_day = countRebalances(ticks, tickSpacing, 2);
        range_5_reb_day = countRebalances(ticks, tickSpacing, 2.5);

        const tick_history = ticks.map(t => parseInt(t.current_tick));
        if (tick_history.length > 1) {
          const mean = tick_history.reduce((a, b) => a + b, 0) / tick_history.length;
          const variance = tick_history.reduce((sum, t) => sum + Math.pow(t - mean, 2), 0) / tick_history.length;
          tick_std_dev = Math.sqrt(variance);
        }
        volatility = tick_std_dev != null && tick_std_dev >= 0 ? +(Math.min((tick_std_dev / tickSpacing) / 10, 1) * 10).toFixed(2) : null;
        max_tick_deviation = tick_history.length > 1 ? Math.max(...tick_history) - Math.min(...tick_history) : null;
        pool_tvl_24h = ticks[numSnapshots - 1]?.pool_tvl ?? null;
      }
      // ---- END NEW CODE ----

      const insertStart = Date.now();
      await client.query(`
        INSERT INTO public.pools (
          name, track_pool_url, type, tick_spacing, current_tick, pool_fee, tvl_eth,
          fees_eth, volume_eth, pool_active_liquidity, liquidity,
          token0, token1, symbol_t0, symbol_t1, decimals_t0, decimals_t1,
          token0_price_eth, token1_price_eth, reward_token, reward_token_symbol, reward_token_price_eth,
          position_manager, collect_address, last_updated, last_updated_hour,
          pool_address, protocol, chain_id, apr,
          last_3h_rebs, last_6h_rebs, last_12h_rebs,
          range_1_reb_day, range_2_reb_day, range_3_reb_day, range_4_reb_day, range_5_reb_day,
          num_snapshots_used, open_date, data_avg_update_hour,
          volatility, max_tick_deviation, tick_std_dev, pool_tvl_24h
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,
          $8,null,$9,$10,
          $11,$12,$13,$14,$15,$16,
          $17,$18,$19,$20,$21,
          $22,$23,$24,$25,
          $26,$27,$28,$29,
          $30,$31,$32,
          $33,$34,$35,$36,$37,
          $38,$39,$40,
          $41,$42,$43,$44
        )
        ON CONFLICT (pool_address, chain_id)
        DO UPDATE SET
          name = EXCLUDED.name,
          track_pool_url = EXCLUDED.track_pool_url,
          type = EXCLUDED.type,
          tick_spacing = EXCLUDED.tick_spacing,
          current_tick = EXCLUDED.current_tick,
          pool_fee = EXCLUDED.pool_fee,
          tvl_eth = EXCLUDED.tvl_eth,
          fees_eth = EXCLUDED.fees_eth,
          volume_eth = EXCLUDED.volume_eth,
          pool_active_liquidity = EXCLUDED.pool_active_liquidity,
          liquidity = EXCLUDED.liquidity,
          token0 = EXCLUDED.token0,
          token1 = EXCLUDED.token1,
          symbol_t0 = EXCLUDED.symbol_t0,
          symbol_t1 = EXCLUDED.symbol_t1,
          decimals_t0 = EXCLUDED.decimals_t0,
          decimals_t1 = EXCLUDED.decimals_t1,
          token0_price_eth = EXCLUDED.token0_price_eth,
          token1_price_eth = EXCLUDED.token1_price_eth,
          reward_token = EXCLUDED.reward_token,
          reward_token_symbol = EXCLUDED.reward_token_symbol,
          reward_token_price_eth = EXCLUDED.reward_token_price_eth,
          position_manager = EXCLUDED.position_manager,
          collect_address = EXCLUDED.collect_address,
          last_updated = EXCLUDED.last_updated,
          last_updated_hour = EXCLUDED.last_updated_hour,
          apr = EXCLUDED.apr,
          last_3h_rebs = EXCLUDED.last_3h_rebs,
          last_6h_rebs = EXCLUDED.last_6h_rebs,
          last_12h_rebs = EXCLUDED.last_12h_rebs,
          range_1_reb_day = EXCLUDED.range_1_reb_day,
          range_2_reb_day = EXCLUDED.range_2_reb_day,
          range_3_reb_day = EXCLUDED.range_3_reb_day,
          range_4_reb_day = EXCLUDED.range_4_reb_day,
          range_5_reb_day = EXCLUDED.range_5_reb_day,
          num_snapshots_used = EXCLUDED.num_snapshots_used,
          open_date = EXCLUDED.open_date,
          data_avg_update_hour = EXCLUDED.data_avg_update_hour,
          volatility = EXCLUDED.volatility,
          max_tick_deviation = EXCLUDED.max_tick_deviation,
          tick_std_dev = EXCLUDED.tick_std_dev,
          pool_tvl_24h = EXCLUDED.pool_tvl_24h
      `, [
        snap.name, snap.track_pool_url, snap.type, snap.tick_spacing, snap.current_tick, snap.pool_fee, tvl_eth,
        fees_eth, snap.pool_active_liquidity, snap.liquidity,
        snap.token0, snap.token1, snap.symbol_t0, snap.symbol_t1, snap.decimals_t0, snap.decimals_t1,
        snap.token0_price_eth, snap.token1_price_eth, snap.reward_token, snap.reward_token_symbol, snap.reward_token_price_eth,
        snap.position_manager, snap.collect_address, snap.last_snapshot_timestamp, last_updated_hour,
        snap.pool_address, snap.protocol, snap.chain_id, apr,
        last_3h_rebs, last_6h_rebs, last_12h_rebs,
        range_1_reb_day, range_2_reb_day, range_3_reb_day, range_4_reb_day, range_5_reb_day,
        numSnapshots, open_date, data_avg_update_hour,
        volatility, max_tick_deviation, tick_std_dev, pool_tvl_24h
      ]);
      const insertEnd = Date.now();
      //console.log(`[PoolsTable Timings] - Total per pool loop took ${(insertEnd - loopStart) / 1000}s`);
    }

    const t3 = Date.now();
    console.log(`[PoolsTable Timings] - Main Processing Loop took ${(t3 - t2) / 1000}s`);

    await client.query('COMMIT');
    const tEnd = Date.now();
    console.log(`[PoolsTable Timings] - Total Time ${(tEnd - startAll) / 1000}s`);
    console.log('Pools table updated!');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error updating pools table:', err);
    throw err;
  } finally {
    client.release();
  }
}


function toETH(tokenAmount, priceETH, decimals) {
  return Number(tokenAmount) * Number(priceETH) / Math.pow(10, Number(decimals));
}





//////////////////////////////
async function updatePoolsRebalanceRanges() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const { rows: pools } = await client.query(`
      SELECT DISTINCT pool_address, chain_id
      FROM public.pool_snapshots
    `);

    const oneDayAgo = Math.floor(Date.now() / 1000) - 86400;

    for (const pool of pools) {
      const { rows: ticks } = await client.query(`
        SELECT current_tick, snapshot_timestamp, name, tick_spacing, pool_tvl
        FROM public.pool_snapshots
        WHERE pool_address = $1 AND chain_id = $2 AND snapshot_timestamp >= $3
        ORDER BY snapshot_timestamp ASC
      `, [pool.pool_address, pool.chain_id, oneDayAgo]);

      const numSnapshots = ticks.length;
      if (numSnapshots < 2) {
        await client.query(`
          DELETE FROM public.pools_rebalance_ranges
          WHERE pool_address = $1 AND chain_id = $2
        `, [pool.pool_address, pool.chain_id]);
        continue;
      }

      const firstTimestamp = ticks[0].snapshot_timestamp;
      const lastTimestamp = ticks[numSnapshots - 1].snapshot_timestamp;
      const calc_duration_hours = (lastTimestamp - firstTimestamp) / 3600;
      const tickSpacing = ticks[0].tick_spacing;
      const poolTvl = ticks[numSnapshots - 1].pool_tvl;

      function countRebalances(multiplier) {
        const rangeSize = tickSpacing * multiplier;
        let count = 0, center = null;
        for (const row of ticks) {
          if (center === null) { center = row.current_tick; continue; }
          if (Math.abs(row.current_tick - center) >= rangeSize) {
            count++;
            center = row.current_tick;
          }
        }
        return calc_duration_hours > 0
          ? Math.round((count / calc_duration_hours) * 24)
          : null;
      }

      const range_1_reb_day = countRebalances(0.5);
      const range_2_reb_day = countRebalances(1);
      const range_3_reb_day = countRebalances(1.5);
      const range_4_reb_day = countRebalances(2);
      const range_5_reb_day = countRebalances(2.5);

      const data_avg_update_hour = (calc_duration_hours > 0 && numSnapshots > 1)
        ? calc_duration_hours / (numSnapshots - 1)
        : null;

      const tick_history = ticks.map(t => parseInt(t.current_tick));
      let tick_std_dev = null;
      if (tick_history.length > 1) {
        const mean = tick_history.reduce((a, b) => a + b, 0) / tick_history.length;
        const variance = tick_history.reduce((sum, t) => sum + Math.pow(t - mean, 2), 0) / tick_history.length;
        tick_std_dev = Math.sqrt(variance);
      }

      const volatility = tick_std_dev != null && tick_std_dev >= 0
        ? +(Math.min((tick_std_dev / tickSpacing) / 10, 1) * 10).toFixed(2)
        : null;

      const max_tick_deviation = tick_history.length > 1
        ? Math.max(...tick_history) - Math.min(...tick_history)
        : null;

      const lastName = ticks[numSnapshots - 1]?.name ?? null;

      await client.query(`
        INSERT INTO public.pools_rebalance_ranges (
          name, tick_spacing, pool_address, chain_id, snapshot_timestamp,
          range_1_reb_day, range_2_reb_day, range_3_reb_day, range_4_reb_day, range_5_reb_day,
          num_snapshots_used, calc_duration_hours, data_avg_update_hour,
          volatility, max_tick_deviation, tick_std_dev, pool_tvl
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
        ON CONFLICT (pool_address, chain_id) DO UPDATE SET
          name = EXCLUDED.name,
          tick_spacing = EXCLUDED.tick_spacing,
          snapshot_timestamp = EXCLUDED.snapshot_timestamp,
          range_1_reb_day = EXCLUDED.range_1_reb_day,
          range_2_reb_day = EXCLUDED.range_2_reb_day,
          range_3_reb_day = EXCLUDED.range_3_reb_day,
          range_4_reb_day = EXCLUDED.range_4_reb_day,
          range_5_reb_day = EXCLUDED.range_5_reb_day,
          num_snapshots_used = EXCLUDED.num_snapshots_used,
          calc_duration_hours = EXCLUDED.calc_duration_hours,
          data_avg_update_hour = EXCLUDED.data_avg_update_hour,
          volatility = EXCLUDED.volatility,
          max_tick_deviation = EXCLUDED.max_tick_deviation,
          tick_std_dev = EXCLUDED.tick_std_dev,
          pool_tvl = EXCLUDED.pool_tvl
      `, [
        lastName,
        tickSpacing,
        pool.pool_address,
        pool.chain_id,
        lastTimestamp,
        range_1_reb_day,
        range_2_reb_day,
        range_3_reb_day,
        range_4_reb_day,
        range_5_reb_day,
        numSnapshots,
        calc_duration_hours,
        data_avg_update_hour,
        volatility,
        max_tick_deviation,
        tick_std_dev,
        poolTvl
      ]);
    }

    await client.query('COMMIT');
    console.log('Pools Rebalance Ranges Table Updated!');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error updating pools_rebalance_ranges table:', err);
    throw err;
  } finally {
    client.release();
  }
}




////////////////////////////////////////////
////////////// BOTS & MANAGERS//////////////
////////////////////////////////////////////

async function updateManagersTable() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`
      INSERT INTO public.managers (
        manager_account,
        total_active_positions,
        aero_active_positions,
        uniswap_active_positions,
        pancake_active_positions,
        contract_positions,
        manual_positions,
        total_aum,
        avg_roi,
        is_vx_bot_manager -- <-- NEW COLUMN
      )
      SELECT
        manager_account,
        COUNT(*) FILTER (WHERE is_closed = FALSE) AS total_active_positions,
        COUNT(*) FILTER (
          WHERE is_closed = FALSE AND LOWER(protocol) LIKE '%aero%'
        ) AS aero_active_positions,
        COUNT(*) FILTER (
          WHERE is_closed = FALSE AND LOWER(protocol) LIKE '%uniswap%'
        ) AS uniswap_active_positions,
        COUNT(*) FILTER (
          WHERE is_closed = FALSE AND LOWER(protocol) LIKE '%pancake%'
        ) AS pancake_active_positions,
        COUNT(*) FILTER (
          WHERE is_closed = FALSE AND owner_is_contract = TRUE
        ) AS contract_positions,
        COUNT(*) FILTER (
          WHERE is_closed = FALSE AND owner_is_contract = FALSE
        ) AS manual_positions,
        SUM(CASE WHEN is_closed = FALSE THEN aum ELSE 0 END) AS total_aum,
        CASE
          WHEN SUM(CASE WHEN is_closed = FALSE THEN aum ELSE 0 END) = 0 THEN NULL
          ELSE
            SUM(CASE WHEN is_closed = FALSE THEN aum * roi ELSE 0 END)
            / NULLIF(SUM(CASE WHEN is_closed = FALSE THEN aum ELSE 0 END), 0)
        END AS avg_roi,
        BOOL_OR(is_closed = FALSE AND is_bot_contract) AS is_vx_bot_manager -- <-- NEW LINE
      FROM
        public.positions
      WHERE manager_account IS NOT NULL
      GROUP BY manager_account
      HAVING
        COUNT(*) FILTER (WHERE is_closed = FALSE) > 0
        AND SUM(CASE WHEN is_closed = FALSE THEN aum ELSE 0 END) >= 1 -- <-- FILTER
      ON CONFLICT (manager_account)
      DO UPDATE SET
        total_active_positions = EXCLUDED.total_active_positions,
        aero_active_positions = EXCLUDED.aero_active_positions,
        uniswap_active_positions = EXCLUDED.uniswap_active_positions,
        pancake_active_positions = EXCLUDED.pancake_active_positions,
        contract_positions = EXCLUDED.contract_positions,
        manual_positions = EXCLUDED.manual_positions,
        total_aum = EXCLUDED.total_aum,
        avg_roi = EXCLUDED.avg_roi,
        is_vx_bot_manager = EXCLUDED.is_vx_bot_manager; -- <-- NEW LINE
    `);
    await client.query('COMMIT');
    console.log('Managers Table Updated!');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error updating managers table:', err);
    throw err;
  } finally {
    client.release();
  }
}




///////////////////////////////


async function updateVXBots() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Get current DB time
    const { rows: [{ db_now }] } = await client.query(`SELECT EXTRACT(EPOCH FROM NOW())::BIGINT AS db_now`);

    await client.query(`
      INSERT INTO public.vx_bots (
        vx_bot_address, name, range, total_duration_hours, rebalances_amount,
        est_rebalance_day, apr, max_roi, avg_aum, pnl, yield, aum,
        manager_account, total_deposited, total_withdrawn, total_collected_fees, total_collected_rewards,
        uncollected_fees, uncollected_rewards, track_pool_url, pool_tvl, type, is_closed,
        token_id, chain_name, chain_id, position_manager, collect_address, combined_events,
        last_updated_hour
      )
      SELECT
        token_id_owner AS vx_bot_address,
        (ARRAY_AGG(name ORDER BY token_id ASC))[array_length(ARRAY_AGG(name ORDER BY token_id ASC),1)],
        (ARRAY_AGG(range ORDER BY token_id ASC))[array_length(ARRAY_AGG(range ORDER BY token_id ASC),1)],
        SUM(duration_hours),
        COUNT(*),
        CASE WHEN SUM(duration_hours) = 0 THEN NULL
             ELSE (COUNT(*) * 24.0) / SUM(duration_hours)
        END,
        CASE 
          WHEN SUM(duration_hours) = 0 OR AVG(avg_aum) = 0 THEN NULL
          ELSE (
            (
              SUM(collected_rewards + collected_fees) +
              (ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC),1)] +
              (ARRAY_AGG(uncollected_fees ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_fees ORDER BY token_id ASC),1)]
            ) / NULLIF(AVG(avg_aum), 0)
          ) * (8760.0 / SUM(duration_hours)) * 100.0
        END,
        CASE 
          WHEN SUM(total_deposited) = 0 THEN NULL
          ELSE (
            (
              (ARRAY_AGG(aum ORDER BY token_id ASC))[array_length(ARRAY_AGG(aum ORDER BY token_id ASC),1)]
              + SUM(total_withdrawn)
              + SUM(collected_rewards + collected_fees)
              + (ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC),1)]
              + (ARRAY_AGG(uncollected_fees ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_fees ORDER BY token_id ASC),1)]
              - SUM(total_deposited)
            ) / SUM(total_deposited) * 100.0
          )
        END,
        AVG(avg_aum),
        (
          (ARRAY_AGG(aum ORDER BY token_id ASC))[array_length(ARRAY_AGG(aum ORDER BY token_id ASC),1)]
          + SUM(total_withdrawn)
          + SUM(collected_rewards + collected_fees)
          + (ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC),1)]
          + (ARRAY_AGG(uncollected_fees ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_fees ORDER BY token_id ASC),1)]
          - SUM(total_deposited)
        ),
        (
          SUM(collected_rewards + collected_fees)
          + (ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC),1)]
          + (ARRAY_AGG(uncollected_fees ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_fees ORDER BY token_id ASC),1)]
        ),
        (ARRAY_AGG(aum ORDER BY token_id ASC))[array_length(ARRAY_AGG(aum ORDER BY token_id ASC),1)],
        (ARRAY_AGG(manager_account ORDER BY token_id ASC))[array_length(ARRAY_AGG(manager_account ORDER BY token_id ASC),1)],
        SUM(total_deposited),
        SUM(total_withdrawn),
        SUM(collected_fees),
        SUM(collected_rewards),
        (ARRAY_AGG(uncollected_fees ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_fees ORDER BY token_id ASC),1)],
        (ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC),1)],
        (ARRAY_AGG(track_pool_url ORDER BY token_id ASC))[array_length(ARRAY_AGG(track_pool_url ORDER BY token_id ASC),1)],
        (ARRAY_AGG(pool_tvl ORDER BY token_id ASC))[array_length(ARRAY_AGG(pool_tvl ORDER BY token_id ASC),1)],
        (ARRAY_AGG(type ORDER BY token_id ASC))[array_length(ARRAY_AGG(type ORDER BY token_id ASC),1)],
        (ARRAY_AGG(is_closed ORDER BY token_id ASC))[array_length(ARRAY_AGG(is_closed ORDER BY token_id ASC),1)],
        (ARRAY_AGG(token_id ORDER BY token_id ASC))[array_length(ARRAY_AGG(token_id ORDER BY token_id ASC),1)],
        (ARRAY_AGG(chain_name ORDER BY token_id ASC))[array_length(ARRAY_AGG(chain_name ORDER BY token_id ASC),1)],
        (ARRAY_AGG(chain_id ORDER BY token_id ASC))[array_length(ARRAY_AGG(chain_id ORDER BY token_id ASC),1)],
        (ARRAY_AGG(position_manager ORDER BY token_id ASC))[array_length(ARRAY_AGG(position_manager ORDER BY token_id ASC),1)],
        (ARRAY_AGG(collect_address ORDER BY token_id ASC))[array_length(ARRAY_AGG(collect_address ORDER BY token_id ASC),1)],
        jsonb_agg(events ORDER BY token_id ASC),
        ($1 - MAX(snapshot_timestamp))::NUMERIC / 3600.0 AS last_updated_hour
      FROM public.positions
      WHERE is_bot_contract = true
      GROUP BY token_id_owner
      ORDER BY MIN(token_id) ASC
      ON CONFLICT (vx_bot_address, chain_id, position_manager) DO UPDATE SET
        name = EXCLUDED.name,
        range = EXCLUDED.range,
        total_duration_hours = EXCLUDED.total_duration_hours,
        rebalances_amount = EXCLUDED.rebalances_amount,
        est_rebalance_day = EXCLUDED.est_rebalance_day,
        apr = EXCLUDED.apr,
        max_roi = EXCLUDED.max_roi,
        avg_aum = EXCLUDED.avg_aum,
        pnl = EXCLUDED.pnl,
        yield = EXCLUDED.yield,
        aum = EXCLUDED.aum,
        manager_account = EXCLUDED.manager_account,
        total_deposited = EXCLUDED.total_deposited,
        total_withdrawn = EXCLUDED.total_withdrawn,
        total_collected_fees = EXCLUDED.total_collected_fees,
        total_collected_rewards = EXCLUDED.total_collected_rewards,
        uncollected_fees = EXCLUDED.uncollected_fees,
        uncollected_rewards = EXCLUDED.uncollected_rewards,
        track_pool_url = EXCLUDED.track_pool_url,
        pool_tvl = EXCLUDED.pool_tvl,
        type = EXCLUDED.type,
        is_closed = EXCLUDED.is_closed,
        token_id = EXCLUDED.token_id,
        chain_name = EXCLUDED.chain_name,
        chain_id = EXCLUDED.chain_id,
        position_manager = EXCLUDED.position_manager,
        collect_address = EXCLUDED.collect_address,
        combined_events = EXCLUDED.combined_events,
        last_updated_hour = EXCLUDED.last_updated_hour
    `, [db_now]);

    await client.query('COMMIT');
    console.log('VX Bots Table Updated!');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error updating vx_bots table:', err);
    throw err;
  } finally {
    client.release();
  }
}




//////////////////////////////
async function updateMarketBots() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Mark all existing market bots as inactive
    await client.query(`
      UPDATE public.market_bots
      SET is_active = false
    `);

    // Insert/update active market bots
    await client.query(`
      INSERT INTO public.market_bots (
        market_bot_address,
        name,
        range,
        total_duration_hours,
        rebalances_amount,
        est_rebalance_day,
        apr,
        max_roi,
        avg_aum,
        pnl,
        yield,
        aum,
        manager_account,
        source_identifier,
        total_deposited,
        total_withdrawn,
        total_collected_fees,
        total_collected_rewards,
        uncollected_fees,
        uncollected_rewards,
        track_pool_url,
        pool_tvl,
        type,
        is_closed,
        token_id,
        chain_name,
        chain_id,
        position_manager,
        collect_address,
        combined_events,
        is_active
      )
      SELECT
        token_id_owner AS market_bot_address,
        (ARRAY_AGG(name ORDER BY token_id ASC))[array_length(ARRAY_AGG(name ORDER BY token_id ASC),1)] AS name,
        (ARRAY_AGG(range ORDER BY token_id ASC))[array_length(ARRAY_AGG(range ORDER BY token_id ASC),1)] AS range,
        SUM(duration_hours) AS total_duration_hours,
        COUNT(*) AS rebalances_amount,
        CASE
          WHEN SUM(duration_hours) = 0 THEN NULL
          ELSE (COUNT(*) * 24.0) / SUM(duration_hours)
        END AS est_rebalance_day,
        CASE
          WHEN SUM(duration_hours) = 0 THEN NULL
          WHEN AVG(avg_aum) = 0 THEN NULL
          ELSE (
            (
              (
                SUM(collected_rewards) +
                SUM(collected_fees) +
                (ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC),1)] +
                (ARRAY_AGG(uncollected_fees ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_fees ORDER BY token_id ASC),1)]
              )
              / NULLIF(AVG(avg_aum), 0)
            )
            * (8760.0 / SUM(duration_hours))
            * 100.0
          )
        END AS apr,
        CASE 
          WHEN SUM(total_deposited) = 0 THEN NULL
          ELSE (
            (
              (ARRAY_AGG(aum ORDER BY token_id ASC))[array_length(ARRAY_AGG(aum ORDER BY token_id ASC),1)]
              + SUM(total_withdrawn)
              + (
                SUM(collected_rewards) + SUM(collected_fees)
                + (ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC),1)]
                + (ARRAY_AGG(uncollected_fees ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_fees ORDER BY token_id ASC),1)]
              )
              - SUM(total_deposited)
            ) / SUM(total_deposited) * 100.0
          )
        END AS max_roi,
        AVG(avg_aum) AS avg_aum,
        (
          (ARRAY_AGG(aum ORDER BY token_id ASC))[array_length(ARRAY_AGG(aum ORDER BY token_id ASC),1)]
          + SUM(total_withdrawn)
          + (
            SUM(collected_rewards) + SUM(collected_fees)
            + (ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC),1)]
            + (ARRAY_AGG(uncollected_fees ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_fees ORDER BY token_id ASC),1)]
          )
          - SUM(total_deposited)
        ) AS pnl,
        (
          SUM(collected_rewards) + SUM(collected_fees)
          + (ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC),1)]
          + (ARRAY_AGG(uncollected_fees ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_fees ORDER BY token_id ASC),1)]
        ) AS yield,
        (ARRAY_AGG(aum ORDER BY token_id ASC))[array_length(ARRAY_AGG(aum ORDER BY token_id ASC),1)] AS aum,
        (ARRAY_AGG(manager_account ORDER BY token_id ASC))[array_length(ARRAY_AGG(manager_account ORDER BY token_id ASC),1)] AS manager_account,
        (ARRAY_AGG(source_identifier ORDER BY token_id ASC))[array_length(ARRAY_AGG(source_identifier ORDER BY token_id ASC),1)] AS source_identifier,
        SUM(total_deposited) AS total_deposited,
        SUM(total_withdrawn) AS total_withdrawn,
        SUM(collected_fees) AS total_collected_fees,
        SUM(collected_rewards) AS total_collected_rewards,
        (ARRAY_AGG(uncollected_fees ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_fees ORDER BY token_id ASC),1)] AS uncollected_fees,
        (ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC))[array_length(ARRAY_AGG(uncollected_rewards ORDER BY token_id ASC),1)] AS uncollected_rewards,
        (ARRAY_AGG(track_pool_url ORDER BY token_id ASC))[array_length(ARRAY_AGG(track_pool_url ORDER BY token_id ASC),1)] AS track_pool_url,
        (ARRAY_AGG(pool_tvl ORDER BY token_id ASC))[array_length(ARRAY_AGG(pool_tvl ORDER BY token_id ASC),1)] AS pool_tvl,
        (ARRAY_AGG(type ORDER BY token_id ASC))[array_length(ARRAY_AGG(type ORDER BY token_id ASC),1)] AS type,
        (ARRAY_AGG(is_closed ORDER BY token_id ASC))[array_length(ARRAY_AGG(is_closed ORDER BY token_id ASC),1)] AS is_closed,
        (ARRAY_AGG(token_id ORDER BY token_id ASC))[array_length(ARRAY_AGG(token_id ORDER BY token_id ASC),1)] AS token_id,
        (ARRAY_AGG(chain_name ORDER BY token_id ASC))[array_length(ARRAY_AGG(chain_name ORDER BY token_id ASC),1)] AS chain_name,
        (ARRAY_AGG(chain_id ORDER BY token_id ASC))[array_length(ARRAY_AGG(chain_id ORDER BY token_id ASC),1)] AS chain_id,
        (ARRAY_AGG(position_manager ORDER BY token_id ASC))[array_length(ARRAY_AGG(position_manager ORDER BY token_id ASC),1)] AS position_manager,
        (ARRAY_AGG(collect_address ORDER BY token_id ASC))[array_length(ARRAY_AGG(collect_address ORDER BY token_id ASC),1)] AS collect_address,
        jsonb_agg(events ORDER BY token_id ASC) AS combined_events,
        true -- is_active
      FROM public.positions
      WHERE 
        owner_is_contract = TRUE
        AND (is_bot_contract = FALSE OR is_bot_contract IS NULL)
      GROUP BY token_id_owner, chain_id, position_manager
      ORDER BY MIN(token_id) ASC
      ON CONFLICT (market_bot_address, chain_id, position_manager) DO UPDATE SET
        name = EXCLUDED.name,
        range = EXCLUDED.range,
        total_duration_hours = EXCLUDED.total_duration_hours,
        rebalances_amount = EXCLUDED.rebalances_amount,
        est_rebalance_day = EXCLUDED.est_rebalance_day,
        apr = EXCLUDED.apr,
        max_roi = EXCLUDED.max_roi,
        avg_aum = EXCLUDED.avg_aum,
        pnl = EXCLUDED.pnl,
        yield = EXCLUDED.yield,
        aum = EXCLUDED.aum,
        manager_account = EXCLUDED.manager_account,
        source_identifier = EXCLUDED.source_identifier,
        total_deposited = EXCLUDED.total_deposited,
        total_withdrawn = EXCLUDED.total_withdrawn,
        total_collected_fees = EXCLUDED.total_collected_fees,
        total_collected_rewards = EXCLUDED.total_collected_rewards,
        uncollected_fees = EXCLUDED.uncollected_fees,
        uncollected_rewards = EXCLUDED.uncollected_rewards,
        track_pool_url = EXCLUDED.track_pool_url,
        pool_tvl = EXCLUDED.pool_tvl,
        type = EXCLUDED.type,
        is_closed = EXCLUDED.is_closed,
        token_id = EXCLUDED.token_id,
        chain_name = EXCLUDED.chain_name,
        chain_id = EXCLUDED.chain_id,
        position_manager = EXCLUDED.position_manager,
        collect_address = EXCLUDED.collect_address,
        combined_events = EXCLUDED.combined_events,
        is_active = EXCLUDED.is_active
    `);

    await client.query('COMMIT');
    console.log('Market Bots Table Updated (with is_active)');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error updating market_bots table:', err);
    throw err;
  } finally {
    client.release();
  }
}


async function deleteOldTokenData() {
  const { Pool } = require('pg');
  const useSSL = String(process.env.SSL).toLowerCase() === 'true';
  const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
    ssl: useSSL ? { rejectUnauthorized: false } : undefined
  });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const cutoff = Math.floor(Date.now() / 1000); // current time in seconds
    const threshold = cutoff - 24 * 3600; // 24 hours ago

    // Delete old snapshots
    await client.query(
      `DELETE FROM token_id_snapshots WHERE snapshot_timestamp < $1`,
      [threshold]
    );

    // Delete old pool snapshots
    await client.query(
      `DELETE FROM pool_snapshots WHERE last_snapshot_timestamp < $1`,
      [threshold]
    );

    // Delete old positions
    await client.query(
      `DELETE FROM positions WHERE snapshot_timestamp < $1`,
      [threshold]
    );

    // Delete token_ids using the new snapshot_timestamp column (if exists)
    await client.query(
      `DELETE FROM token_ids WHERE snapshot_timestamp < $1`,
      [threshold]
    );

    

    // Delete unused token_ids that have no more snapshots
    await client.query(`
      DELETE FROM token_ids 
      WHERE token_id IN (
        SELECT t.token_id
        FROM token_ids t
        LEFT JOIN token_id_snapshots s 
          ON t.token_id = s.token_id AND t.chain_id = s.chain_id AND t.position_manager = s.position_manager
        WHERE s.id IS NULL
      )
    `);

    await client.query('COMMIT');
    console.log('✅ Old data removed: token_id_snapshots, pool_snapshots, positions, and unused token_ids (older than 24h)');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('❌ Error deleting old data:', err);
    throw err;
  } finally {
    client.release();
  }
}




module.exports = { 
  updatePositions,
  updateTrendingPositions, 
  updatePoolsSnapshots,
  updatePoolsTable,
  updatePoolsRebalanceRanges,
  updateManagersTable,
  updateVXBots, 
  updateMarketBots,
  deleteOldTokenData
};
