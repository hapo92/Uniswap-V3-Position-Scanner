const fetchTokenIdMintEvents = require('./utils/fetchTokenIdMintEvents');
const fetchTokenIdInfos = require('./utils/fetchTokenIdInfoContract');
const checkInputForBot = require('./utils/checkIsBotContract');
const ZERO_ADDR = "0x0000000000000000000000000000000000000000";

function logMem(label = '') {
  const mb = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
  console.log(`[MEM][getNewTokenIds] ${label} heapUsed: ${mb} MB`);
}

async function getNewTokenIds(configCurChain, configCurProtocol, web3, callback, startBlock) {
  console.log(">>> getPositions.js - getPositions function call!");

  const positionManager = configCurProtocol.positionManager.toLowerCase();

  let lastScannedBlock = startBlock ?? 0;
  const batchSize = 1000;

  const latestBlock = Number(await web3.eth.getBlockNumber());

  while (lastScannedBlock < latestBlock) {
    const fromBlock = lastScannedBlock + 1;
    const toBlock = Math.min(fromBlock + batchSize - 1, latestBlock);
    let mintLogs = await fetchTokenIdMintEvents(web3, positionManager, fromBlock, toBlock);

    console.log(`Blocks: [${fromBlock}, ${toBlock}] - Mint events Found: ${mintLogs.length}`);

    for (const log of mintLogs) {
      let rawTokenId = log.topics[3];
      let tokenId = typeof rawTokenId === "bigint"
        ? rawTokenId.toString()
        : typeof rawTokenId === "string"
        ? web3.utils.hexToNumberString(rawTokenId)
        : String(rawTokenId);

      const txReceipt = await web3.eth.getTransactionReceipt(log.transactionHash);
      let owner = ZERO_ADDR;
      let managerAccount = txReceipt.from;

      const collectTopic = '0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01';
      const transferTopic = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';

      for (const evlog of txReceipt.logs) {
        if (evlog.address.toLowerCase() === positionManager) {
          if (evlog.topics[0].toLowerCase() === collectTopic) {
            const logTokenId = web3.utils.hexToNumberString(evlog.topics[1]);
            if (logTokenId === tokenId && evlog.data) {
              const recipient = '0x' + evlog.data.slice(26, 66);
              if (recipient !== ZERO_ADDR) {
                owner = recipient;
                break;
              }
            }
          }

          if (evlog.topics[0].toLowerCase() === transferTopic) {
            const logZeroAddr = web3.utils.toChecksumAddress('0x' + evlog.topics[1].slice(26));
            const logTokenId = web3.utils.hexToNumberString(evlog.topics[3]);
            if (logTokenId === tokenId && logZeroAddr === ZERO_ADDR) {
              const recipient = web3.utils.toChecksumAddress('0x' + evlog.topics[2].slice(26));
              owner = recipient;
              break;
            }
          }
        }
      }

      if (owner.toLowerCase() === ZERO_ADDR) {
        console.log("🛑 Owner is still Zero, owner:", owner);
        continue;
      }

      let tokenInfo;
      try {
        [tokenInfo] = await fetchTokenIdInfos({
          web3,
          tokenId: [tokenId],
          owner: [owner],
          configCurChain,
          configCurProtocol
        });
        if (!tokenInfo) continue;
      } catch {
        continue;
      }

      const { token0, token1, isBurned } = tokenInfo;
      const chainWETH = configCurChain.addressWETH?.toLowerCase();
      const chainUSDC = configCurChain.addressUSDC?.toLowerCase();

      if (
        isBurned ||
        (!token0 || !token1) ||
        (
          token0.toLowerCase() !== chainWETH &&
          //token0.toLowerCase() !== chainUSDC &&
          token1.toLowerCase() !== chainWETH 
          //token1.toLowerCase() !== chainUSDC
        )
      ) {
        continue;
      }

      let range = '';
      if (tokenInfo.tickLower !== undefined && tokenInfo.tickUpper !== undefined) {
        range = Math.abs(Number(tokenInfo.tickLower) - Number(tokenInfo.tickUpper)) / 100;
        range = range.toFixed(0);
      }

      let trackPositionURL = configCurProtocol.tracker
        ? configCurProtocol.tracker.replace('{strURL}', tokenId)
        : '';
      let trackPoolURL = configCurProtocol.tracker2 && tokenInfo.pool
        ? configCurProtocol.tracker2.replace('{strURL}', tokenInfo.pool)
        : '';

      const { isBotContract, protocolIdentifier } = await checkInputForBot(
        web3,
        log.transactionHash,
        configCurProtocol.botContractIdentifiers
      );

      const blockData = await web3.eth.getBlock(log.blockNumber);
      if (!blockData) {
        console.log(`⚠️ Block ${log.blockNumber} not found, skipping token ${tokenId}`);
        continue;
      }
      const blockTimestamp = blockData.timestamp;
      const blockTimestampReadable = blockTimestamp ? new Date(Number(blockTimestamp) * 1000).toISOString() : null;

      const dbData = {
        tokenId,
        blockNumber: Number(log.blockNumber),
        blockTimestamp: Number(blockTimestamp),
        blockTimestampReadable,
        protocolName: configCurProtocol.name,
        positionManager: configCurProtocol.positionManager,
        chainName: configCurChain.chain,
        chainId: Number(configCurChain.chainId),
        transactionHash: log.transactionHash,
        tokenIdOwner: owner,
        isClosed: !!tokenInfo.isBurned,
        isBurned: !!tokenInfo.isBurned,
        token0: tokenInfo.token0,
        token1: tokenInfo.token1,
        symbolT0: tokenInfo.symbolT0,
        symbolT1: tokenInfo.symbolT1,
        decimalsT0: tokenInfo.decimalsT0,
        decimalsT1: tokenInfo.decimalsT1,
        poolTVL0: tokenInfo.poolTVL0,
        poolTVL1: tokenInfo.poolTVL1,
        fee: tokenInfo.fee,
        tickLower: tokenInfo.tickLower,
        tickUpper: tokenInfo.tickUpper,
        currentTick: tokenInfo.currentTick,
        tickSpacing: tokenInfo.tickSpacing,
        pool: tokenInfo.pool,
        owner,
        positionType: tokenInfo.positionType,
        ownerIsContract: tokenInfo.ownerIsContract,
        collectAddress: tokenInfo.collectAddress,
        range,
        trackPositionURL,
        trackPoolURL,
        isBotContract,
        protocolIdentifier,
        managerAccount
      };

      await callback(dbData);
    }

    mintLogs = null;
    lastScannedBlock = toBlock;
  }
}

module.exports = getNewTokenIds;
