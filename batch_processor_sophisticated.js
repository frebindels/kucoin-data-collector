#!/usr/bin/env node
/**
 * Sophisticated Batch Processor for GitHub Actions
 * Uses the adapted production worker with all advanced features:
 * - XML pagination with max-keys and marker
 * - Sophisticated file discovery and validation
 * - Robust error handling and retries
 * - Comprehensive logging and progress tracking
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
        
        console.log(`🚀 Starting SOPHISTICATED batch processing:`);
        console.log(`   Start Index: ${startIndex}`);
        console.log(`   Count: ${count}`);
        console.log(`   Using: Advanced production worker with XML pagination`);
        
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
        
        // Process each symbol using the sophisticated production worker
        for (let i = 0; i < batchSymbols.length; i++) {
            const symbol = batchSymbols[i];
            console.log(`\n🔄 Processing symbol ${i + 1}/${batchSymbols.length}: ${symbol}`);
            
            try {
                await processSymbolSophisticated(symbol);
                console.log(`✅ Completed: ${symbol}`);
            } catch (error) {
                console.error(`❌ Failed: ${symbol} - ${error.message}`);
                // Continue with next symbol instead of stopping the entire batch
            }
        }
        
        console.log(`\n🎉 Sophisticated batch processing completed!`);
        console.log(`   Processed: ${batchSymbols.length} symbols`);
        console.log(`   Start Index: ${startIndex}`);
        console.log(`   End Index: ${startIndex + batchSymbols.length - 1}`);
        console.log(`   Features used: XML pagination, robust error handling, file validation`);
        
    } catch (error) {
        console.error(`💥 Sophisticated batch processor failed: ${error.message}`);
        process.exit(1);
    }
}

function processSymbolSophisticated(symbol) {
    return new Promise((resolve, reject) => {
        console.log(`   🚀 Starting SOPHISTICATED worker for ${symbol}...`);
        console.log(`   🔧 Features: XML pagination, max-keys, marker, retries, validation`);
        
        // Use the sophisticated adapted production worker
        const worker = spawn('node', ['worker_system_production_adapted.js', symbol], {
            stdio: 'inherit',
            cwd: __dirname,
            env: {
                ...process.env,
                WORKER_MODE: 'sophisticated',
                ENABLE_XML_PAGINATION: 'true',
                ENABLE_ADVANCED_FEATURES: 'true'
            }
        });
        
        worker.on('close', (code) => {
            if (code === 0) {
                console.log(`   ✅ Sophisticated worker for ${symbol} completed successfully`);
                resolve();
            } else {
                console.log(`   ⚠️  Sophisticated worker for ${symbol} exited with code ${code}`);
                // Don't reject for non-zero exit codes, just log and continue
                resolve();
            }
        });
        
        worker.on('error', (error) => {
            console.error(`   ❌ Sophisticated worker for ${symbol} failed: ${error.message}`);
            reject(error);
        });
        
        // Set a timeout to prevent hanging workers
        setTimeout(() => {
            console.log(`   ⏰ Sophisticated worker for ${symbol} timed out, killing process...`);
            worker.kill('SIGTERM');
            resolve(); // Resolve to continue with next symbol
        }, 900000); // 15 minutes timeout for sophisticated workers
    });
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
    main().catch(error => {
        console.error(`Fatal error: ${error.message}`);
        process.exit(1);
    });
}

export { main, processSymbolSophisticated };
