# PositionScanner

Scans newly minted Uniswap V3–style NFT positions (per `positionManager`) across chains/protocols, stores them in Postgres, and periodically snapshots position state to drive aggregated tables/analytics.

## What it does

- **TokenId discovery loop**: continuously scans for newly minted position token IDs and upserts them into `token_ids`.
- **Snapshot loop**: periodically batches active token IDs, fetches onchain state, inserts into `token_id_snapshots`, and updates aggregated tables (positions/pools/bots/managers/etc).

The scanner is driven by:

- `configs/configScanner.js`: which chains + position managers to scan
- `configs/configChains.json`: RPC endpoints + chain-specific addresses
- `configs/configProtocols.json`: protocol metadata (position manager, trackers, reward token, etc)

## Requirements

- Node.js (>= 18 recommended)
- Postgres (reachable from the machine running the scanner)
- RPC URLs for each chain you enable (set in `configs/configChains.json`)

## Install

```bash
npm install
```

## Configuration

### 1) Chain config

Edit `configs/configChains.json` and set at minimum:

- `scannerRpcURL`: RPC endpoint for that chain

Other fields are project-specific contract addresses and can remain empty if your code paths don’t use them.

### 2) Protocol config

Edit `configs/configProtocols.json` to define each protocol and its `positionManager` per chain.

### 3) Enable scanners

Edit `configs/configScanner.js` and list which `(chainId, positionManager)` pairs to run.

### 4) Environment variables

Create `.env` in the project root:

```dotenv
# Postgres
DB_HOST=localhost
DB_PORT=5432
DB_NAME=positionscanner
DB_USER=postgres
DB_PASSWORD=postgres

# If your DB requires SSL (e.g. managed Postgres)
SSL=false
```

Notes:

- DB is used by `utils/dbGet.js`, `utils/dbSet.js`, and parts of `index.js`.
- There is a **dangerous** helper `truncateAllTables()` exported from `utils/dbSet.js`. Don’t call it unless you intentionally want to wipe tables.

## Run

```bash
node index.js
```

What happens on start:

- TokenId scanner starts (continuous loop). If there are no recent DB rows, it starts from the latest block (no historical backfill).
- Snapshot pollers start for every config in `configs/configScanner.js`.

### Poll interval

In `index.js`, snapshot polling currently uses:

- `setTimeout(tokenIdSnapshots, 1 * 60 * 1000)`

So it’s **1 minute** between snapshot batches (despite the log message mentioning 10 minutes).

## Database expectations (high level)

At minimum, the code writes/reads these tables:

- `token_ids` (upserted by `saveNewTokenIds()`)
- `token_id_snapshots` (inserted by `saveMarketSnapshots()`)

It also updates/derives aggregated tables via `utils/dbAggregatedTables.js` (e.g. `positions`, `pool_snapshots`, `trending_positions`, `vx_bots`, `market_bots`, `managers`, etc.).

If you’re setting up a fresh DB, read `utils/dbSet.js` to see the exact snapshot column list used for inserts.

## Repo layout

```text
configs/
  configChains.json
  configProtocols.json
  configScanner.js
utils/
  getNewTokenIds.js
  getNewSnapshots.js
  dbGet.js
  dbSet.js
  dbAggregatedTables.js
index.js
```

## Operational notes

- **RPC reliability** matters. If your RPC rate-limits you, expect slowdowns and partial batch failures.
- This is a long-running process: consider running it under `pm2`:

```bash
npx pm2 start index.js --name position-scanner
npx pm2 logs position-scanner
```

