// Position TokenId Scanner (Unified with continuous token ID scan, 10min snapshot scan)
const { Web3 } = require('web3');
require('dotenv').config({ path: './.env' });

const configChains = require('./configs/configChains.json');
const configProtocols = require('./configs/configProtocols.json');
const configs = require('./configs/configScanner.js');

const { loadCurProtocolABIs } = require('./utils/loadCurProtocolABIs.js');
const getNewTokenIds = require('./utils/getNewTokenIds.js');
const getNewSnapshots = require('./utils/getNewSnapshots.js');
const {
  updatePositions, updateTrendingPositions, updateVXBots, updateMarketBots,
  updatePoolsSnapshots, updatePoolsTable, updatePoolsRebalanceRanges, updateManagersTable,
  deleteOldTokenData
} = require('./utils/dbAggregatedTables.js');
const {
  saveNewTokenIds, saveMarketSnapshots, syncTokenIdsIsClosedFromSnapshots
} = require('./utils/dbSet.js');
const {
  getTokenIdsWithLastSnapshotBatch, getLastProcessedBlock
} = require('./utils/dbGet.js');

// --------------------------------------------------------------------------
//  GET MINTED TOKEN IDs and ADD TO DATABASE (continuous unified scan)
// --------------------------------------------------------------------------
async function runUnifiedTokenIdScanner(configList) {
  const chainIds = [...new Set(configList.map(cfg => cfg.chainId))];

  for (const chainId of chainIds) {
    const configCurChain = configChains.find(c => c.chainId === chainId);
    const web3 = new Web3(configCurChain.scannerRpcURL);
    const protocols = configList.filter(cfg => cfg.chainId === chainId);

    const lastBlocks = await Promise.all(protocols.map(cfg =>
      getLastProcessedBlock(cfg.chainId, cfg.positionManager)
    ));

    const validBlocks = lastBlocks.filter(b => b !== null && b !== undefined && b !== 0);
    
    // If no recent blocks found, start from the latest block (no historical scanning)
    let startBlock;
    if (validBlocks.length === 0) {
      const currentBlock = await web3.eth.getBlockNumber();
      startBlock = Number(currentBlock);
      console.log(`No recent blocks found, starting from latest block: ${startBlock}`);
    } else {
      startBlock = Math.min(...validBlocks.map(Number));
    }

    console.log(`Starting unified Token ID scan from block: ${startBlock} on chain: ${configCurChain.chain}`);

    while (true) {
      const latestBlock = await web3.eth.getBlockNumber();
      const fromBlock = Number(startBlock) + 1;
      const toBlock = latestBlock;

      if (fromBlock > toBlock) {
        await new Promise(resolve => setTimeout(resolve, 5000)); // wait a bit
        continue;
      }

      for (const config of protocols) {
        const configCurProtocol = configProtocols.find(p =>
          p.positionManager === config.positionManager && p.chainId === config.chainId
        );

        if (!configCurProtocol) continue;

        console.log(`>>> getPositions.js - getPositions function call!`);
        await getNewTokenIds(configCurChain, configCurProtocol, web3, async (position) => {
          try {
            await saveNewTokenIds(position);
            console.log(`✅ 🆔 - ${configCurProtocol.name} ID= ${position.tokenId}`);
          } catch (err) {
            console.error(`❌ Failed to save tokenId: ${position.tokenId} for ${configCurProtocol.name}, err:`, err.message);
          }
        }, fromBlock);
        console.log(`✅ Token ID scan completed for protocol: ${configCurProtocol.name}`);
      }

      startBlock = toBlock;
    }
  }
}

// --------------------------------------------------------------------------
//  GET TOKENID SNAPSHOTS and ADD TO DATABASE (loop + pause)
// --------------------------------------------------------------------------
function startSnapshotPolling(config) {
  async function tokenIdSnapshots() {
    const lastId = await fetchMarketSnapshotsForAll(config);
    const lastTokenIdInfo = lastId !== null ? lastId : 'none';
    console.log(`⏱️ [${config.protocol}] Waiting for 10 minutes before next snapshot batch... Last tokenId: ${lastTokenIdInfo}`);
    setTimeout(tokenIdSnapshots, 1 * 60 * 1000); // 10 minutes
  }
  tokenIdSnapshots();
}

async function fetchMarketSnapshotsForAll(config) {
  const configCurChain = configChains.find(item => item.chainId === config.chainId);
  const configCurProtocol = configProtocols.find(item =>
    item.positionManager === config.positionManager && item.chainId === config.chainId
  );
  const web3 = new Web3(configCurChain.scannerRpcURL);
  const curProtocolABIs = loadCurProtocolABIs(configCurProtocol.name);

  const batchSize = 500;
  let offset = 0;
  let lastTokenId = null;

  while (true) {
    const positions = await getTokenIdsWithLastSnapshotBatch(
      config.chainId, config.positionManager, batchSize, offset
    );
    if (!positions || positions.length === 0) break;

    const snapshotsData = await getNewSnapshots(positions, configCurChain, configCurProtocol, curProtocolABIs, web3);

    if (snapshotsData && snapshotsData.length > 0) {
      await saveMarketSnapshots(snapshotsData);
      await syncTokenIdsIsClosedFromSnapshots(snapshotsData);
      const firstId = snapshotsData[0].tokenId || snapshotsData[0].token_id;
      const lastId = snapshotsData.at(-1)?.tokenId || snapshotsData.at(-1)?.token_id;
      lastTokenId = lastId;
      console.log(`✅ 📸 ${snapshotsData.length} - ${configCurProtocol.name}, token_ids: ${firstId}...${lastId}`);

      await updatePositions();
      await updateTrendingPositions();
      await updatePoolsSnapshots();
      await updatePoolsTable();
      await updateVXBots();
      await updateMarketBots();
      await updateManagersTable();
      await deleteOldTokenData();

      console.log(`✅ 📊 All Aggregated Tables Updated`);
    } else {
      console.log(`📭 No snapshots to save for this batch.`);
    }

    offset += batchSize;
  }

  await removeOldSnapshots();

  const lastTokenIdInfo = lastTokenId !== null ? lastTokenId : 'none';
  console.log(`🏁 📸 - ⏱️ - ${config.protocol}, last tokenId = ${lastTokenIdInfo}`);
  return lastTokenId;
}

async function removeOldSnapshots() {
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
    const cutoff = Math.floor(Date.now() / 1000) - 24 * 3600;
    await client.query('BEGIN');

    await client.query(`DELETE FROM token_id_snapshots WHERE snapshot_timestamp < $1`, [cutoff]);
    await client.query(`DELETE FROM token_ids WHERE snapshot_timestamp < $1`, [cutoff]);

    await client.query('COMMIT');
    console.log('🧹 Removed old snapshot and token_id entries older than 24h.');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('❌ Error deleting old snapshots/token_ids:', err);
  } finally {
    client.release();
  }
}

// --------------------------------------------------------------------------
//  PROCESS CONTROLLERS
// --------------------------------------------------------------------------
function gracefulShutdown() {
  shuttingDown = true;
  console.log('🛑 Shutting down...');
  process.exit(0);
}
process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

// ---- RUN ALL CONFIGS ----
async function main() {
  runUnifiedTokenIdScanner(configs);
  configs.forEach(startSnapshotPolling);
}

main().catch(err => {
  console.error('💥 Initialization failed:', err);
  process.exit(1);
});
