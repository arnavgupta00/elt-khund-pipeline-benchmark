import { Case1Sequential } from './cases/case1-sequential';
import { Case2Bulk } from './cases/case2-bulk';
import { Case3Pipeline } from './cases/case3-pipeline';
import { Case4aEdge } from './cases/case4a-edge';
import { sourcePool, targetPool } from './config/database';
import * as fs from 'fs';
import * as path from 'path';

async function main() {
  const args = process.argv.slice(2);
  const caseArg = args.find(arg => arg.startsWith('--case='));
  const limitArg = args.find(arg => arg.startsWith('--limit='));
  
  const caseNumber = caseArg ? caseArg.split('=')[1] : 'all';
  const limit = limitArg ? parseInt(limitArg.split('=')[1]) : undefined;

  console.log('\n===========================================');
  console.log('ETL Research Project - Performance Testing');
  console.log('===========================================');
  console.log(`Case: ${caseNumber}`);
  console.log(`Limit: ${limit || 'No limit (full dataset)'}`);
  console.log('===========================================\n');

  const results: any[] = [];

  try {
    if (caseNumber === '1' || caseNumber === 'all') {
      console.log('\nExecuting Case 1: Sequential Row-by-Row...');
      const case1 = new Case1Sequential();
      const metrics = await case1.execute(limit);
      results.push(metrics);
      console.log('Case 1 completed!\n');
    }

    if (caseNumber === '2' || caseNumber === 'all') {
      console.log('\nExecuting Case 2: Bulk File Export/Import...');
      const case2 = new Case2Bulk();
      const metrics = await case2.execute(limit);
      results.push(metrics);
      console.log('Case 2 completed!\n');
    }

    if (caseNumber === '3' || caseNumber === 'all') {
      console.log('\nExecuting Case 3: Multi-threaded Pipeline...');
      const case3 = new Case3Pipeline();
      const metrics = await case3.execute(limit);
      results.push(metrics);
      console.log('Case 3 completed!\n');
    }

    if (caseNumber === '4a' || caseNumber === '4' || caseNumber === 'all') {
      console.log('\nExecuting Case 4a: Edge Computing (Cloudflare Workers)...');
      const case4a = new Case4aEdge();
      const metrics = await case4a.execute(limit);
      results.push(metrics);
      console.log('Case 4a completed!\n');
    }

    // Generate comparison report
    if (results.length > 1) {
      generateComparisonReport(results);
    }

  } catch (error) {
    console.error('Error in main execution:', error);
  } finally {
    await sourcePool.end();
    await targetPool.end();
  }
}

function generateComparisonReport(results: any[]) {
  console.log('\n===========================================');
  console.log('PERFORMANCE COMPARISON REPORT');
  console.log('===========================================\n');

  // Sort by total duration
  results.sort((a, b) => a.total_duration_ms - b.total_duration_ms);

  console.table(results.map(r => ({
    'Case': r.case_name,
    'Total Time (s)': (r.total_duration_ms / 1000).toFixed(2),
    'Records/sec': r.records_per_second.toFixed(2),
    'Peak Memory (MB)': r.memory_peak_mb.toFixed(2),
    'CPU Time (s)': ((r.cpu_user_ms + r.cpu_system_ms) / 1000).toFixed(2),
    'Errors': r.error_count
  })));

  // Calculate speedup
  const baseline = results.find(r => r.case_number === 1) || results[0];
  console.log('\nSpeedup Analysis (compared to Case 1):');
  results.forEach(r => {
    const speedup = baseline.total_duration_ms / r.total_duration_ms;
    console.log(`  ${r.case_name}: ${speedup.toFixed(2)}x faster`);
  });

  // Save full report
  const reportPath = path.join(process.cwd(), 'research_logs', `comparison_${Date.now()}.json`);
  fs.writeFileSync(reportPath, JSON.stringify(results, null, 2));
  console.log(`\nFull report saved to: ${reportPath}`);
}

// Run the main function
main().catch(console.error);