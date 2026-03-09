require('dotenv').config();

const { Pool } = require('pg');

// Database connection configuration
const useSSL = String(process.env.SSL).toLowerCase() === 'true';
const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
  ssl: useSSL ? { rejectUnauthorized: false } : false
});

async function cleanupTables() {
  const client = await pool.connect();
  
  try {
    console.log('🔍 Starting database cleanup...');
    
    // Get current counts before deletion
    const beforeCounts = await getTableCounts(client);
    console.log('📊 Current table counts:');
    console.log(`   token_ids: ${beforeCounts.tokenIds}`);
    console.log(`   token_id_snapshots: ${beforeCounts.tokenIdSnapshots}`);
    console.log(`   positions: ${beforeCounts.positions}`);
    console.log(`   pools: ${beforeCounts.pools}`);
    console.log(`   pool_snapshots: ${beforeCounts.poolsSnapshots}`);
    console.log(`   trending_positions: ${beforeCounts.trendingPositions}`);
    console.log(`   vx_bots: ${beforeCounts.vxBots}`);
    
    const totalRows = beforeCounts.tokenIds + beforeCounts.tokenIdSnapshots + beforeCounts.positions + 
                     beforeCounts.pools + beforeCounts.poolsSnapshots + beforeCounts.trendingPositions + beforeCounts.vxBots;
    
    if (totalRows === 0) {
      console.log('✅ All tables are already empty. No cleanup needed.');
      return;
    }
    
    // Confirm deletion
    console.log('\n⚠️  WARNING: This will delete ALL data from the following tables:');
    console.log('   - token_ids');
    console.log('   - token_id_snapshots');
    console.log('   - positions');
    console.log('   - pools');
    console.log('   - pool_snapshots');
    console.log('   - trending_positions');
    console.log('   - vx_bots');
    console.log('\nPress Ctrl+C to cancel, or wait 5 seconds to continue...');
    
    // Wait 5 seconds for user to cancel
    await new Promise(resolve => setTimeout(resolve, 5000));
    
    console.log('\n🗑️  Proceeding with deletion...');
    
    // Start transaction
    await client.query('BEGIN');
    
    // Delete from tables in order (respecting foreign key constraints)
    console.log('🗑️  Deleting from token_id_snapshots...');
    const snapshotsResult = await client.query('DELETE FROM token_id_snapshots');
    console.log(`   Deleted ${snapshotsResult.rowCount} rows from token_id_snapshots`);
    
    console.log('🗑️  Deleting from trending_positions...');
    const trendingResult = await client.query('DELETE FROM trending_positions');
    console.log(`   Deleted ${trendingResult.rowCount} rows from trending_positions`);
    
    console.log('🗑️  Deleting from positions...');
    const positionsResult = await client.query('DELETE FROM positions');
    console.log(`   Deleted ${positionsResult.rowCount} rows from positions`);
    
    console.log('🗑️  Deleting from pool_snapshots...');
    const poolsSnapshotsResult = await client.query('DELETE FROM pool_snapshots');
    console.log(`   Deleted ${poolsSnapshotsResult.rowCount} rows from pool_snapshots`);
    
    console.log('🗑️  Deleting from pools...');
    const poolsResult = await client.query('DELETE FROM pools');
    console.log(`   Deleted ${poolsResult.rowCount} rows from pools`);
    
    console.log('🗑️  Deleting from vx_bots...');
    const vxBotsResult = await client.query('DELETE FROM vx_bots');
    console.log(`   Deleted ${vxBotsResult.rowCount} rows from vx_bots`);
    
    console.log('🗑️  Deleting from token_ids...');
    const tokenIdsResult = await client.query('DELETE FROM token_ids');
    console.log(`   Deleted ${tokenIdsResult.rowCount} rows from token_ids`);
    
    // Commit transaction
    await client.query('COMMIT');
    
    // Get counts after deletion
    const afterCounts = await getTableCounts(client);
    console.log('\n📊 Table counts after cleanup:');
    console.log(`   token_ids: ${afterCounts.tokenIds}`);
    console.log(`   token_id_snapshots: ${afterCounts.tokenIdSnapshots}`);
    console.log(`   positions: ${afterCounts.positions}`);
    console.log(`   pools: ${afterCounts.pools}`);
    console.log(`   pools_snapshots: ${afterCounts.poolsSnapshots}`);
    console.log(`   trending_positions: ${afterCounts.trendingPositions}`);
    console.log(`   vx_bots: ${afterCounts.vxBots}`);
    
    console.log('\n✅ Cleanup completed successfully!');
    
  } catch (error) {
    // Rollback on error
    try {
      await client.query('ROLLBACK');
    } catch (rollbackError) {
      console.error('Failed to rollback transaction:', rollbackError.message);
    }
    
    console.error('❌ Cleanup failed:', error.message);
    process.exit(1);
    
  } finally {
    client.release();
    await pool.end();
  }
}

async function getTableCounts(client) {
  const tokenIdsResult = await client.query('SELECT COUNT(*) FROM token_ids');
  const snapshotsResult = await client.query('SELECT COUNT(*) FROM token_id_snapshots');
  const positionsResult = await client.query('SELECT COUNT(*) FROM positions');
  const poolsResult = await client.query('SELECT COUNT(*) FROM pools');
  const poolsSnapshotsResult = await client.query('SELECT COUNT(*) FROM pool_snapshots');
  const trendingPositionsResult = await client.query('SELECT COUNT(*) FROM trending_positions');
  const vxBotsResult = await client.query('SELECT COUNT(*) FROM vx_bots');
  
  return {
    tokenIds: parseInt(tokenIdsResult.rows[0].count),
    tokenIdSnapshots: parseInt(snapshotsResult.rows[0].count),
    positions: parseInt(positionsResult.rows[0].count),
    pools: parseInt(poolsResult.rows[0].count),
    poolsSnapshots: parseInt(poolsSnapshotsResult.rows[0].count),
    trendingPositions: parseInt(trendingPositionsResult.rows[0].count),
    vxBots: parseInt(vxBotsResult.rows[0].count)
  };
}

// Handle process termination gracefully
process.on('SIGINT', () => {
  console.log('\n❌ Cleanup cancelled by user');
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('\n❌ Cleanup terminated');
  process.exit(0);
});

// Run cleanup
cleanupTables().catch(error => {
  console.error('❌ Unexpected error:', error.message);
  process.exit(1);
});
