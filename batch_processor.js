#!/usr/bin/env node
/**
 * Batch Processor for GitHub Actions
 * Processes symbols in batches based on start_index and count
 */

import fs from 'fs/promises';
import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function main() {
    try {
        // Get command line arguments
        const startIndex = parseInt(process.argv[2]) || 0;
        const count = parseInt(process.argv[3]) || 100;
        
        console.log(`🚀 Starting batch processing:`);
        console.log(`   Start Index: ${startIndex}`);
        console.log(`   Count: ${count}`);
        
        // Load symbols
        const symbolsPath = path.join(__dirname, 'symbols.json');
        const symbolsData = await fs.readFile(symbolsPath, 'utf8');
        const symbolsFile = JSON.parse(symbolsData);
        
        // Extract the symbols array from the file structure
        const symbols = symbolsFile.symbols || symbolsFile;
        
        if (!Array.isArray(symbols)) {
            throw new Error(`Invalid symbols format. Expected array, got: ${typeof symbols}`);
        }
        
        console.log(`📊 Total symbols available: ${symbols.length}`);
        
        // Extract batch
        const batchSymbols = symbols.slice(startIndex, startIndex + count);
        console.log(`🎯 Processing batch: ${batchSymbols.length} symbols`);
        console.log(`   Symbols: ${batchSymbols.join(', ')}`);
        
        // Process each symbol sequentially to avoid overwhelming the system
        for (let i = 0; i < batchSymbols.length; i++) {
            const symbol = batchSymbols[i];
            console.log(`\n🔄 Processing symbol ${i + 1}/${batchSymbols.length}: ${symbol}`);
            
            try {
                await processSymbol(symbol);
                console.log(`✅ Completed: ${symbol}`);
            } catch (error) {
                console.error(`❌ Failed: ${symbol} - ${error.message}`);
                // Continue with next symbol instead of stopping the entire batch
            }
        }
        
        console.log(`\n🎉 Batch processing completed!`);
        console.log(`   Processed: ${batchSymbols.length} symbols`);
        console.log(`   Start Index: ${startIndex}`);
        console.log(`   End Index: ${startIndex + batchSymbols.length - 1}`);
        
    } catch (error) {
        console.error(`💥 Batch processor failed: ${error.message}`);
        process.exit(1);
    }
}

function processSymbol(symbol) {
    return new Promise((resolve, reject) => {
        console.log(`   🚀 Starting worker for ${symbol}...`);
        
        const worker = spawn('node', ['github_actions_worker.js', symbol], {
            stdio: 'inherit',
            cwd: __dirname
        });
        
        worker.on('close', (code) => {
            if (code === 0) {
                console.log(`   ✅ Worker for ${symbol} completed successfully`);
                resolve();
            } else {
                console.log(`   ⚠️  Worker for ${symbol} exited with code ${code}`);
                // Don't reject for non-zero exit codes, just log and continue
                resolve();
            }
        });
        
        worker.on('error', (error) => {
            console.error(`   ❌ Worker for ${symbol} failed: ${error.message}`);
            reject(error);
        });
        
        // Set a timeout to prevent hanging workers
        setTimeout(() => {
            console.log(`   ⏰ Worker for ${symbol} timed out, killing process...`);
            worker.kill('SIGTERM');
            resolve(); // Resolve to continue with next symbol
        }, 300000); // 5 minutes timeout
    });
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
    main().catch(error => {
        console.error(`Fatal error: ${error.message}`);
        process.exit(1);
    });
}

export { main, processSymbol };
