const BigNumber = require("bignumber.js");
const {tokenIdScannerABI, arbitrageABI, collectPositionManagerABI} = require('./ABIs/abisCommon.js');
const SimulateReturns = require('./utils/simulateReturnsV9.js');

const { getTokenAmountPrice, getTokenPrice } = require('./utils/arbitrage.js'); // RPC

const PositionManagerEvents = require('./utils/fetchPositionManagerEvents.js'); // RPC
const RewardManagerEvents = require('./utils/fetchRewardManagerEvents.js'); // RPC
const fetchTokenIdInfos = require('./utils/fetchTokenIdInfoContract.js'); // RPC

const fetchUncollectedFees = require('./utils/fetchUncollectedFees.js'); // HELPER + 
const { orgonizeRawEvents } = require('./utils/orgonizeEvents.js'); // HELPER

// Helper for fuzzy number comparison (0.1% default tolerance)
function isRoughlyEqual(a, b, tolerance = 0.001) {
    const aNum = Number(a);
    const bNum = Number(b);
    const maxVal = Math.max(Math.abs(aNum), Math.abs(bNum));
    return Math.abs(aNum - bNum) <= maxVal * tolerance;
}

// ETH Price Helper - can be ooved to arbitrage file
function tokenToEthAmount(amount, priceETH, decimals) {
    try {
        // All params to BigNumber (even undefined/NaN handled as 0)
        const safeAmount = new BigNumber(amount || 0);
        const safePrice = new BigNumber(priceETH || 0);
        const safeDecimals = Number.isFinite(Number(decimals)) ? Number(decimals) : 18; // fallback

        // If any input is non-numeric or absurd, treat as zero
        if (!safeAmount.isFinite() || !safePrice.isFinite() || isNaN(safeDecimals)) return 0;

        const result = safeAmount.times(safePrice).dividedBy(new BigNumber(10).pow(safeDecimals));

        // Final safety: If result is not finite, return 0
        return result.isFinite() ? result : 0;
    } catch (err) {
        console.error('[tokenToEthAmount] Calculation error:', err, { amount, priceETH, decimals });
        return 0;
    }
}




async function getNewSnapshots(tokenIdlist, configCurChain, configCurProtocol, curProtocolABIs, web3){
    console.log(">>> getNewSnapshots.js - fetchNewPositions function call!")

    // --------------------------------------------------------------------------
    //  Initilize Contracts
    // --------------------------------------------------------------------------
    const snapshotsData = [];
    let arbitrageContract; // Used for ETH Price Values 
    try{arbitrageContract = new web3.eth.Contract(arbitrageABI, configCurChain.arbitrageAddress);}
    catch{console.log("error while creating arbitrageContract");}

    let positionManagerContract; // Used for Uncollected Fees Simulation
    try{positionManagerContract = new web3.eth.Contract(collectPositionManagerABI, configCurProtocol.positionManager);}
    catch{console.log("error while creating positionManagerContract");}

    let getPositionInfoContract;
    getPositionInfoContract = new web3.eth.Contract(tokenIdScannerABI, configCurChain.tokenIdScannerAddress);
 
 


    // --------------------------------------------------------------------------
    // Bulk Fetch positionManager and rewardsManager Events BATCH
    // --------------------------------------------------------------------------
    // pass exact params not tokenIdlist - Refactor! 
    let rawPositionManagerEvents = [];
    const cPositionManagerEvents = new PositionManagerEvents(web3, configCurProtocol.positionManager);
    rawPositionManagerEvents = await cPositionManagerEvents.getEventsForPositions(tokenIdlist);
    
    let rawRewardManagerEvents = [];
    if(configCurProtocol.type != "noReward"){
        const cRewardManagerEvents = new RewardManagerEvents(web3);
        rawRewardManagerEvents = await cRewardManagerEvents.getEventsForPositions(tokenIdlist);
    }


    
    // --------------------------------------------------------------------------
    // Bulk Fetch Contract Data Batch
    // --------------------------------------------------------------------------
    
    let contractDataBatch = [];
    // if(configCurProtocol.type != "noReward"){  }
    let tokenIdsBatch = tokenIdlist.map(pos => String(pos.token_id));
    let tokenIdsOwnerBatch = tokenIdlist.map(pos => String(pos.token_id_owner));
    try {
        contractDataBatch = await fetchTokenIdInfos({
            web3,
            tokenId: tokenIdsBatch,
            owner: tokenIdsOwnerBatch,
            configCurChain,
            configCurProtocol
        });
    } catch {
        console.log("error fetching getPoolsInfos contract");
        return false;
    }
   


    // --------------------------------------------------------------------------
    // Bulk Fetch Uncollected Fees (positionManager Simulation BATCH)
    // --------------------------------------------------------------------------
     const uncollectedFeesArray = await fetchUncollectedFees({
        web3, // use your existing instance if possible
        configCurProtocol,
        rpcUrl: configCurChain.rpcUrl,
        tokenCollectArray: tokenIdlist.map(pos => ({
            tokenId: pos.token_id,
            collectAddress: configCurProtocol.positionManager
        })),
        chunkSize: 20 
    });
    //console.log("uncollectedFeesArray:",uncollectedFeesArray);



    // --------------------------------------------------------------------------
    // Bulk Fetch Current Price & Block Data Global (RPC & Time optimization)
    // --------------------------------------------------------------------------
    let tokenRPriceETH = await getTokenPrice(arbitrageContract, configCurChain, configCurProtocol.swapPoolRewardToken, configCurProtocol.rewardToken, configCurChain.addressWETH);
    thisSnapshotBlock = await web3.eth.getBlockNumber();
    const thisBlockData = await web3.eth.getBlock(thisSnapshotBlock);
    thisSnapshotTimestamps = Number(thisBlockData.timestamp);





    // --------------------------------------------------------------------------
    //  Loop for snapshotsData Construction
    // --------------------------------------------------------------------------
    console.log(`>>>>>>> Processing to snapshot calcs for for ${configCurProtocol.name} Protocol`);
    for (let i = 0; i < tokenIdlist.length; i++) {
    
        let isPosClosed = false;
       
        // --------------------------------------------------------------------------
        // GET DB DATA (token_ids, extras, histori events)
        // --------------------------------------------------------------------------
        const currentTokenId = tokenIdlist[i].token_id;
        const extra = typeof tokenIdlist[i].extra_data === 'string'
        ? JSON.parse(tokenIdlist[i].extra_data)
        : (tokenIdlist[i].extra_data || {});
        const {
            token0, token1, symbolT0, symbolT1, decimalsT0, decimalsT1,
            poolTVL0, poolTVL1, fee, tickLower, tickUpper, currentTick,
            tickSpacing, pool, owner, positionType, ownerIsContract, collectAddress, range,
            trackPositionURL, trackPoolURL, isBotContract, managerAccount, protocolIdentifier
        } = extra;

        const lastSnapshotEventsRaw = tokenIdlist[i].last_snapshot_events;
        const lastSnapshotEvents = Array.isArray(lastSnapshotEventsRaw)
        ? lastSnapshotEventsRaw
        : (typeof lastSnapshotEventsRaw === 'string'
            ? JSON.parse(lastSnapshotEventsRaw)
            : []);
        // console.log("lastSnapshot:",lastSnapshot)
    
        if(tokenIdlist[i].amount0 <= 0 & tokenIdlist[i].amount1 <= 0 ) {
            isPosClosed = true;
        }
        

        // --------------------------------------------------------------------------
        // T0 & T1 Price to ETH - Arbitrage.js
        // --------------------------------------------------------------------------
         // CURRENT PRICES T0, T1, R - Move TokenR oute of loop +  add USDC/WETH price
         let token0PriceETH = 0;
         let token1PriceETH = 0;
         let priceErr = false;
         
         try {
             token0PriceETH = await getTokenPrice(arbitrageContract, configCurChain, pool, token0, configCurChain.addressWETH);
             token1PriceETH = await getTokenPrice(arbitrageContract, configCurChain, pool, token1, configCurChain.addressWETH);
         } catch (err) {
             priceErr = true;
             console.log("T0 or T1 is not listed in arbitrage router!", err);
         }
         
         // Check for bad prices (0, NaN, negative, or general priceErr)
         if (
             priceErr ||
             !Number.isFinite(token0PriceETH) || token0PriceETH <= 0 ||
             !Number.isFinite(token1PriceETH) || token1PriceETH <= 0
         ) {
             isPosClosed = true;
             continue;
         }






        // --------------------------------------------------------------------------
        // GET SIMULATE DATA (to fetch UNCOLLECTED FEES)
        // --------------------------------------------------------------------------
        let uncollected0_ETH = 0;
        let uncollected1_ETH = 0;
        const simReturnFees = uncollectedFeesArray[i];
        //console.log("simReturnFees i=", i, ",returned:", simReturnFees);
        
        if (simReturnFees.amount0 > 0) {
            const res = tokenToEthAmount(Number(simReturnFees.amount0), token0PriceETH, decimalsT0);
            uncollected0_ETH = (res && res.toNumber) ? res.toNumber() : Number(res); // handle both BN and num
        }
        if (simReturnFees.amount1 > 0) {
            const res = tokenToEthAmount(Number(simReturnFees.amount1), token1PriceETH, decimalsT1);
            uncollected1_ETH = (res && res.toNumber) ? res.toNumber() : Number(res);
        }
        // console.log("uncollected0_ETH:", uncollected0_ETH);
        // console.log("uncollected1_ETH:", uncollected1_ETH);
    

        



        // --------------------------------------------------------------------------
        //  GET CONTRACT Data (to fetch UNCOLLECTED REWARDS)
        // --------------------------------------------------------------------------
        const { farmedRewards = 0 } = contractDataBatch[i] || {};
        let uncollectedR_ETH = 0;
        let uncollectedR = farmedRewards;
        if (Number(uncollectedR) > 0) {
            uncollectedR_ETH = Number(uncollectedR) * tokenRPriceETH / (10 ** 18);
            uncollectedR_ETH = uncollectedR_ETH / (10 ** 12); // AdditionalDecimals from contract
        }

        const { 
            tickLower: cTickLower = 0, 
            tickUpper: cTickUpper = 0, 
            currentTick: cCurrentTick = 0 
        } = contractDataBatch[i] || {};

        if(tickLower == 0 && tickUpper == 0){
            isPosClosed = true;
        }

        const {
            feeGrowthGlobal0X128: feeGrowthGlobal0X128 = 0,
            feeGrowthGlobal1X128: feeGrowthGlobal1X128 = 0,
            poolActiveLiquidity: poolActiveLiquidity = 0,
            liquidity: liquidity = 0,
        } = contractDataBatch[i] || {};

        if(feeGrowthGlobal0X128 == 0 || feeGrowthGlobal1X128 == 0){
            isPosClosed = true;
        }

        // console.log(" contractDataBatch[i] :", contractDataBatch[i] );
        // console.log("---- isPosClosed:",isPosClosed);
        // console.log("tokenId:",currentTokenId);
        // console.log("collectAddress:",collectAddress);
        // console.log("feeGrowthGlobal0X128:",feeGrowthGlobal0X128);
        // console.log("feeGrowthGlobal1X128:",feeGrowthGlobal1X128);
        // console.log("poolActiveLiquidity:",poolActiveLiquidity);
        // console.log("liquidity:",liquidity);



        // --------------------------------------------------------------------------
        // Manage / Orgonize RAW Events Data
        // --------------------------------------------------------------------------
        //console.log("rawRewardManagerEvents:",rawRewardManagerEvents);
        const { filteredEvents, combinedRawEvents } = orgonizeRawEvents(
            rawPositionManagerEvents,
            rawRewardManagerEvents,
            lastSnapshotEvents,
            currentTokenId
        );
        if (!filteredEvents.length || !filteredEvents[0] || !filteredEvents[0].timestamp) {
            // Log for debugging if you want:
            console.warn(`Skipping snapshot for tokenId=${currentTokenId} due to missing events or timestamp`, filteredEvents);
            continue;
        }
        //console.log("filteredEvents:",filteredEvents);
           
                


        // --------------------------------------------------------------------------
        // LiquidityMap calculations 
        // --------------------------------------------------------------------------
        //calculating time weighted average (avgAUM) for each the event periods to get total APR for token_id duration
        let liquidityMap = [];
        const lastDecreaseBlockByToken = {}; 
        let totalWeightedAUM = new BigNumber(0);
        let totalTime = new BigNumber(0);
        const periodStartTimestamp = Number(filteredEvents[0].timestamp);
        const lastTimestamp = periodStartTimestamp - 1;
        
        liquidityMap.push({
            type: 'InitialState',
            liquidity: 0,
            amount0: 0,
            amount1: 0,
            collected0: 0,
            collected1: 0,
            collectedR: 0,
            blockNumber: 0,
            timestamp: lastTimestamp,
            timestampReadable: new Date(lastTimestamp * 1000).toISOString(),
            duration: 0,
            // Only needed fields for your loop:
            withdraw_ETH: 0,
            durationAum_ETH: 0,
            totalCollected_ETH: 0,
            // Add more here ONLY IF used later
            totalDeposit_ETH: 0,
            totalWithdraw_ETH: 0,
            collectedFees_ETH: 0,
            collectedRewards_ETH: 0,
        });

        for (const event of filteredEvents) {
            const lastEntry = liquidityMap[liquidityMap.length - 1]; // Last liquidityMap.values
            const eventBlock = BigInt(event.blockNumber);
            const duration = Number(event.timestamp) - lastEntry.timestamp; 
        
           
            const newEntry = { // Clone last entry and update baseline fields
                ...lastEntry,
                blockNumber: eventBlock,
                timestamp: Number(event.timestamp),
                timestampReadable: event.timestampReadable,
                type: event.type,
                duration: duration,
            };
        
            // ----> DEEP DEBUG BLOCK <----
            const amountT0Price_ETH = tokenToEthAmount(newEntry.amount0, token0PriceETH, decimalsT0);
            const amountT1Price_ETH = tokenToEthAmount(newEntry.amount1, token1PriceETH, decimalsT1);
            const aumEth = amountT0Price_ETH.plus(amountT1Price_ETH);
            // console.log(`
            //     DEBUG: Block ${event.blockNumber}
            //     Type: ${event.type}
            //     Amount0: ${newEntry.amount0}
            //     Amount1: ${newEntry.amount1}
            //     AmountT0Price_ETH: ${newEntry.amount0 * token0PriceETH / (10 ** decimalsT0)}
            //     AmountT1Price_ETH: ${newEntry.amount1 * token1PriceETH / (10 ** decimalsT1)}
            //     AUM_ETH: ${(newEntry.amount0 * token0PriceETH / (10 ** decimalsT0)) + (newEntry.amount1 * token1PriceETH / (10 ** decimalsT1))}
            // `);

            // --- Update amounts based on event type ---
            if (event.type === 'IncreaseLiquidity') {
                newEntry.liquidity = lastEntry.liquidity + Number(event.liquidity);
                newEntry.amount0 = lastEntry.amount0 + Number(event.amount0);
                newEntry.amount1 = lastEntry.amount1 + Number(event.amount1);
        
                newEntry.totalDeposit_ETH = new BigNumber(lastEntry.totalDeposit_ETH || 0)
                .plus(tokenToEthAmount(event.amount0, token0PriceETH, decimalsT0))
                .plus(tokenToEthAmount(event.amount1, token1PriceETH, decimalsT1));
            
            } else if (event.type === 'DecreaseLiquidity') {
                newEntry.liquidity = lastEntry.liquidity - Number(event.liquidity);
                newEntry.amount0 = lastEntry.amount0 - Number(event.amount0);
                newEntry.amount1 = lastEntry.amount1 - Number(event.amount1);
                lastDecreaseBlockByToken[event.tokenId] = Number(event.blockNumber);
        
                newEntry.totalWithdraw_ETH = new BigNumber(lastEntry.totalWithdraw_ETH || 0)
                .plus(tokenToEthAmount(event.amount0, token0PriceETH, decimalsT0))
                .plus(tokenToEthAmount(event.amount1, token1PriceETH, decimalsT1));
        
                // If this is a merged event (has collect0 and collect1), count those too
                newEntry.collected0 = lastEntry.collected0;
                newEntry.collected1 = lastEntry.collected1;
                if (event.collect0 !== undefined && event.collect1 !== undefined) {
                    newEntry.collected0 += Number(event.collect0);
                    newEntry.collected1 += Number(event.collect1);
                }
                 // ---- NEW CLOSE CHECK ----
                // Only check if lastEntry.liquidity was nonzero (to avoid div by 0)
                if (lastEntry.liquidity > 0 && newEntry.liquidity / lastEntry.liquidity < 0.02) {
                    isPosClosed = true;
                }
        
            } else if (event.type === 'Collect') {
                // Calculate collected fee in ETH for debugging:
                const feeT0_ETH = tokenToEthAmount(event.amount0, token0PriceETH, decimalsT0);
                const feeT1_ETH = tokenToEthAmount(event.amount1, token1PriceETH, decimalsT1);
        
                const lastDecreaseBlock = lastDecreaseBlockByToken[event.tokenId] || 0;
                const blockDiff = Number(event.blockNumber) - lastDecreaseBlock;
        
                const isNearMatchToDecrease = (
                    lastDecreaseBlock &&
                    lastEntry.type === 'DecreaseLiquidity' &&
                    isRoughlyEqual(event.amount0, lastEntry.amount0)  &&
                    isRoughlyEqual(event.amount1, lastEntry.amount1)
                );
        
                // TRUE if fees are genuinely earned (not part of liquidity withdrawal)
                const isFeeCollection = !lastDecreaseBlock || (blockDiff > 10 && !isNearMatchToDecrease) || !isPosClosed;
        
                newEntry.collected0 = lastEntry.collected0;
                newEntry.collected1 = lastEntry.collected1;
        
                if (isFeeCollection) {
                    newEntry.collected0 += Number(event.amount0);
                    newEntry.collected1 += Number(event.amount1);
                    newEntry.collectedFees_ETH = new BigNumber(lastEntry.collectedFees_ETH || 0)
                        .plus(feeT0_ETH)
                        .plus(feeT1_ETH);
                }
        
                // Reset lastDecreaseBlock if we've seen a Collect now
                lastDecreaseBlockByToken[event.tokenId] = 0;
        
                // Collect debug:
                // console.log(
                //     `[FEE-DEBUG] tokenId=${currentTokenId} | Block ${event.blockNumber}\n` +
                //     `    Collected Fees: ${event.amount0} ${symbolT0} (${feeT0_ETH} ETH), ` +
                //     `${event.amount1} ${symbolT1} (${feeT1_ETH} ETH), Total: ${feeT0_ETH + feeT1_ETH} ETH`
                // );

            } else if (event.type === 'Harvest' || event.type === 'ClaimRewards') {
                // Both event types now use .amountR (string)
                // Always treat as BigInt for safety, but convert to Number for math
                const rewardAmount = event.amountR !== undefined ? event.amountR : 0;
                newEntry.collectedRewards_ETH = new BigNumber(lastEntry.collectedRewards_ETH || 0)
                    .plus(tokenToEthAmount(rewardAmount, tokenRPriceETH, 18));
            }
                
            ////durationAUM and twaAUM - needed for avgAUM and avgAPR
            totalWeightedAUM = totalWeightedAUM.plus(new BigNumber(lastEntry.durationAum_ETH || 0).times(duration));
            totalTime = totalTime.plus(duration);
            
            newEntry.durationAum_ETH = isPosClosed ? 0 : tokenToEthAmount(newEntry.amount0, token0PriceETH, decimalsT0)
                                                      .plus(tokenToEthAmount(newEntry.amount1, token1PriceETH, decimalsT1)); // ROI CALC ISSUES ? RECONSIDER (?0)!
        
            liquidityMap.push(newEntry);
        }
        //console.log("initial liquidityMap:",liquidityMap);
        // Handle final time period (position closure or current snapshot)
        const periodEndTimestamp = thisSnapshotTimestamps;
        isPosClosed = isPosClosed || (liquidityMap.length > 0 && liquidityMap[liquidityMap.length-1].durationAum_ETH === 0);
        const finalTimestamp = isPosClosed ? liquidityMap[liquidityMap.length-1].timestamp : periodEndTimestamp;
        const lastEntry = liquidityMap[liquidityMap.length-1];
        const finalDuration = finalTimestamp - lastEntry.timestamp;
        if (finalDuration > 0) {
            totalWeightedAUM = totalWeightedAUM.plus(
                new BigNumber(lastEntry.durationAum_ETH || 0).times(finalDuration)
            );
            totalTime = totalTime.plus(finalDuration);
            liquidityMap[liquidityMap.length-1].duration = finalDuration;
        }
        //console.log("finalDuration:",finalDuration);


        // --------------------------------------------------------------------------
        // ROI & PnL Calculations
        // --------------------------------------------------------------------------
        let PnL_ETH = 0;
        let ROI = 0;
        let totalDeposited_ETH = 0;
        let totalWithdrawn_ETH = 0;
        let yield_ETH = 0;
        let AUM_ETH = 0; // Last AUM (final state)
        let collectedFees_ETH =0;
        let collectedRewards_ETH =0;
        let uncollectedFees_ETH = Number(uncollected0_ETH) + Number(uncollected1_ETH);
        let uncollectedRewards_ETH = uncollectedR_ETH;

        if (liquidityMap.length > 0) {
            const lastEntry = liquidityMap[liquidityMap.length - 1];
            // Cashflow (already accumulated in the loop above)
            totalDeposited_ETH = lastEntry.totalDeposit_ETH || 0;
            totalWithdrawn_ETH = lastEntry.totalWithdraw_ETH || 0;
            // Final AUM (ETH value of current assets)
            AUM_ETH = lastEntry.durationAum_ETH || 0;
            // yield generated (collected + uncollected)
            collectedFees_ETH = lastEntry.collectedFees_ETH || 0;
            collectedRewards_ETH = lastEntry.collectedRewards_ETH || 0;
            yield_ETH = Number(collectedFees_ETH) + Number(collectedRewards_ETH) + Number(uncollectedFees_ETH) + Number(uncollectedRewards_ETH);
            //sanity check before PnL calc
            AUM_ETH = Number(AUM_ETH) || 0;
            totalWithdrawn_ETH = Number(totalWithdrawn_ETH) || 0;
            yield_ETH = Number(yield_ETH) || 0;
            totalDeposited_ETH = Number(totalDeposited_ETH) || 0;
            collectedFees_ETH = Number(collectedFees_ETH) || 0;
            collectedRewards_ETH = Number(collectedRewards_ETH) || 0;
            // Net Profit/Loss (PnL): everything withdrawn or still in the position, plus all yield, minus what was deposited
            PnL_ETH = (AUM_ETH + totalWithdrawn_ETH + yield_ETH) - totalDeposited_ETH;
            // ROI: profit over capital invested
            ROI = (totalDeposited_ETH > 0) ? ((PnL_ETH / totalDeposited_ETH) * 100) : 0;
        }


        // --------------------------------------------------------------------------
        // APR & Yield Calculations (time-weighted)
        // --------------------------------------------------------------------------
        let APR = 0;
        let avgAUM_ETH = 0;
        let _totalWeightedAUM = totalWeightedAUM && totalWeightedAUM.toNumber ? totalWeightedAUM.toNumber() : Number(totalWeightedAUM);
        let _totalTime = totalTime && totalTime.toNumber ? totalTime.toNumber() : Number(totalTime);

        const annualizationFactor = (_totalTime > 0) ? (31536000 / _totalTime) : 0;

        if (_totalWeightedAUM > 0 && _totalTime > 0 && liquidityMap.length > 0) {
            avgAUM_ETH = _totalWeightedAUM / _totalTime;
            if (Math.abs(avgAUM_ETH) < 1e-12) avgAUM_ETH = 0; // Safety clamp

            // APR: annualized PnL/avgAUM, as you want PnL to always include all yield
            if (avgAUM_ETH > 0) {
                //APR = (Number(PnL_ETH) / avgAUM_ETH) * annualizationFactor * 100;
                APR = (Number(yield_ETH) / avgAUM_ETH) * annualizationFactor * 100;
                
                if (!Number.isFinite(APR)) APR = 0; // NaN/Inf guard
            } else {
                APR = 0;
            }
        }


        // T0, T0 amountPrice_ETH
        let amountT0Price_ETH = tokenToEthAmount(lastEntry.amount0, token0PriceETH, decimalsT0);
        let amountT1Price_ETH = tokenToEthAmount(lastEntry.amount1, token1PriceETH, decimalsT1);
        
        // TVL - static for token_id
        let ethTVL0 = tokenToEthAmount(poolTVL0, token0PriceETH, decimalsT0);
        let ethTVL1 = tokenToEthAmount(poolTVL1, token1PriceETH, decimalsT1);
        let ethPoolTVL = ethTVL0.plus(ethTVL1);

        // FEE 
        let poolFee = Number(fee) / 10000;

        //IF POSITION were CLOSED or close NOW for AUM
        isPosClosed = isPosClosed || (AUM_ETH < 0.0001);


        const snapshot_timestamp_readable = new Date(thisSnapshotTimestamps * 1000).toISOString();
        
        //Stanity check before sending to databse! 
        PnL_ETH = Number(PnL_ETH) || 0;
        ROI = Number(ROI) || 0;
       

        const snapshotData = {
            // main
            trackPositionURL: trackPositionURL,
            trackPoolURL: trackPoolURL,
            name: `${configCurChain.chain} ${configCurProtocol.name} ${symbolT0}/${symbolT1} ${Number(poolFee)}%`,
            poolTVL: ethPoolTVL ? Number(ethPoolTVL) : 0,
            range: `[${(range/2)}%,${(range/2)}%]`,
            // Time Weighted 
            apr: Number.isFinite(APR) ? Number(APR.toFixed(4)) : 0,
            avgAUM: Number(avgAUM_ETH),
            durationHours: _totalTime ? (_totalTime / 3600).toFixed(2) : '0.00',
            pnl: Number(PnL_ETH),
            roi: Number(ROI),
            yield: Number(yield_ETH),
            aum: Number(AUM_ETH),
            //owner
            isBotContract: isBotContract,
            managerAccount: managerAccount,
            protocolIdentifier: protocolIdentifier,
            ownerIsContract: ownerIsContract,
            owner: owner,
            // cashflow 
            totalDeposited: Number(totalDeposited_ETH),
            totalWithdrawn: Number(totalWithdrawn_ETH),          
            // yield breakdown
            collectedFees: Number(collectedFees_ETH),
            collectedRewards: Number(collectedRewards_ETH),
            uncollectedFees: Number(uncollectedFees_ETH),
            uncollectedRewards: Number(uncollectedRewards_ETH),
            //chain protocol
            chainName: configCurChain.chain,
            chainId: configCurChain.chainId,
            protocol: configCurProtocol.name,
            positionManager: configCurProtocol.positionManager,
            collectAddress: collectAddress,
            tokenId: currentTokenId,
            //pool
            poolAddress: pool,
            poolFee: poolFee,
            type: positionType,
            tickSpacing: tickSpacing,
            tickLower: cTickLower,
            tickUpper: cTickUpper,
            currentTick: cCurrentTick,

            symbolT0: symbolT0,
            symbolT1: symbolT1,
            addressT0: token0,
            addressT1: token1,
            decimalsT0: decimalsT0,
            decimalsT1: decimalsT1,
            amountT0:  lastEntry.amount0,
            amountT1:  lastEntry.amount1,
            amountT0Price_ETH: Number(amountT0Price_ETH),
            amountT1Price_ETH: Number(amountT1Price_ETH),
            token0PriceETH: token0PriceETH,
            token1PriceETH: token1PriceETH,
            // Active Liquidity and Volume Growth
            feeGrowthGlobal0X128: feeGrowthGlobal0X128 ? feeGrowthGlobal0X128.toString() : '0',
            feeGrowthGlobal1X128: feeGrowthGlobal1X128 ? feeGrowthGlobal1X128.toString() : '0',
            poolActiveLiquidity: poolActiveLiquidity ? poolActiveLiquidity.toString() : '0',
            liquidity: liquidity ? liquidity.toString() : '0',
            //Reward Token
            addressTR: configCurProtocol.rewardToken,
            symbolTR: configCurProtocol.rewardTokenSymbol,
            rewardTokenPriceETH: tokenRPriceETH,
            //status: positionClosed ? 'Closed' : 'Active', & inRange,
            events: JSON.stringify(filteredEvents, (k, v) => typeof v === 'bigint' ? v.toString() : v, 2),
            //rawEvents: JSON.stringify(combinedRawEvents, (k, v) => typeof v === 'bigint' ? v.toString() : v, 2),
            durationSeconds: totalTime,
            snapshot_block: Number(thisSnapshotBlock)+ 1,
            snapshot_timestamp: thisSnapshotTimestamps,
            snapshot_timestamp_readable: snapshot_timestamp_readable,
            isClosed: isPosClosed
        };
       
        //console.log(`snapshot for ${snapshotData.name}:`, snapshotData)
        snapshotsData.push(snapshotData); // Add to the array
    }
    
    return snapshotsData;
    //send/add information into Databse!
  }


  module.exports = getNewSnapshots;