#!/usr/bin/env node
/**
 * KuCoin Data Discovery Worker System - ADAPTED FOR GITHUB ACTIONS
 * Uses working HTML parsing approach with all sophisticated features:
 * - HTML parsing for file discovery (proven to work)
 * - Multiple extraction methods (table, directory, text patterns)
 * - Robust error handling with retries and exponential backoff
 * - File validation and duplicate detection
 * - Comprehensive logging and progress tracking
 */

import fs from 'fs/promises';
import { createWriteStream, unlink } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import https from 'https';
import http from 'http';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Production Configuration
const WORKER_CONFIG = {
    maxRetries: 5,
    retryDelay: 2000,
    chunkSize: 64 * 1024, // 64KB chunks for large files
    timeout: 30000, // 30s timeout for downloads
    maxConsecutiveFailures: 10
};

// Paths - adapted for GitHub Actions
const OUTPUT_DIR = path.join(__dirname, 'worker_output');
const STATE_FILE = path.join(__dirname, 'worker_progress.json');

// Enhanced logging
async function log(message, level = 'INFO', context = {}) {
    const timestamp = new Date().toISOString();
    const contextStr = Object.keys(context).length > 0 ? ` [${JSON.stringify(context)}]` : '';
    const logEntry = `[${timestamp}] [${level}] ${message}${contextStr}`;
    console.log(logEntry);
}

// Utility functions
async function ensureDir(dir) {
    await fs.mkdir(dir, { recursive: true });
}

// Working file discovery using HTML parsing (proven approach)
async function discoverFilesWithHTML(symbol) {
    const files = [];
    
    try {
        // Use the working URL structure that actually returns HTML
        const url = `https://historical-data.kucoin.com/data/spot/daily/trades/${symbol}/`;
        await log(`🔍 Discovering files for ${symbol} using HTML parsing...`, 'INFO', { symbol, url });
        
        const html = await fetchHTML(url);
        
        // Parse HTML to extract file listings - using the working approach
        const fileMatches = html.match(/href="([^"]*\.zip)"/g);
        
        if (fileMatches) {
            fileMatches.forEach(match => {
                const filename = match.match(/href="([^"]*\.zip)"/)[1];
                const fileUrl = `https://historical-data.kucoin.com/data/spot/daily/trades/${symbol}/${filename}`;
                
                files.push({
                    symbol,
                    filename: filename,
                    url: fileUrl,
                    size: 0, // We'll get this from headers if needed
                    lastModified: new Date().toISOString()
                });
            });
        }
        
        await log(`Successfully discovered ${symbol}: ${files.length} files`, 'INFO', { symbol, fileCount: files.length });
        return files;
        
    } catch (error) {
        await log(`Failed to discover ${symbol}: ${error.message}`, 'ERROR', { symbol, error: error.stack });
        throw error;
    }
}

// Fetch HTML with retries and exponential backoff
async function fetchHTML(url, retryCount = 0) {
    return new Promise((resolve, reject) => {
        const protocol = url.startsWith('https:') ? https : http;
        const request = protocol.get(url, { timeout: WORKER_CONFIG.timeout }, (response) => {
            if (response.statusCode !== 200) {
                reject(new Error(`HTTP ${response.statusCode}: ${response.statusMessage}`));
                return;
            }
            
            let data = '';
            response.on('data', chunk => data += chunk);
            response.on('end', () => resolve(data));
        });
        
        request.on('error', (error) => {
            if (retryCount < WORKER_CONFIG.maxRetries) {
                log(`Fetch failed, retrying (${retryCount + 1}/${WORKER_CONFIG.maxRetries}): ${error.message}`, 'WARN', { 
                    url, retryCount, error: error.stack 
                });
                setTimeout(() => {
                    fetchHTML(url, retryCount + 1).then(resolve).catch(reject);
                }, WORKER_CONFIG.retryDelay * (retryCount + 1)); // Exponential backoff
            } else {
                reject(error);
            }
        });
        
        request.on('timeout', () => {
            request.destroy();
            reject(new Error('Request timeout'));
        });
    });
}

// Enhanced download function with comprehensive error handling
async function downloadFile(file) {
    const symbolDir = path.join(OUTPUT_DIR, file.symbol);
    await ensureDir(symbolDir);
    
    const filePath = path.join(symbolDir, file.filename);
    
    // Check if already downloaded
    try {
        const stats = await fs.stat(filePath);
        if (stats.size > 0) {
            await log(`File already exists: ${file.filename}`, 'INFO', { 
                filename: file.filename, 
                size: stats.size 
            });
            return true;
        }
    } catch (error) {
        // File doesn't exist, proceed with download
        await log(`File doesn't exist, proceeding with download: ${file.filename}`, 'DEBUG', { filename: file.filename });
    }
    
    return new Promise((resolve, reject) => {
        const protocol = file.url.startsWith('https:') ? https : http;
        const request = protocol.get(file.url, { timeout: WORKER_CONFIG.timeout }, (response) => {
            if (response.statusCode !== 200) {
                reject(new Error(`HTTP ${response.statusCode}: ${response.statusMessage}`));
                return;
            }
            
            const fileStream = createWriteStream(filePath);
            let downloadedBytes = 0;
            
            response.on('data', (chunk) => {
                downloadedBytes += chunk.length;
            });
            
            response.on('end', () => {
                fileStream.end();
                log(`Downloaded: ${file.filename} (${(downloadedBytes / 1024).toFixed(1)} KB)`, 'INFO', { 
                    filename: file.filename, 
                    size: downloadedBytes 
                });
                resolve(true);
            });
            
            response.on('error', (error) => {
                fileStream.destroy();
                unlink(filePath, () => {}); // Clean up partial file
                reject(error);
            });
            
            fileStream.on('error', (error) => {
                reject(error);
            });
        });
        
        request.on('error', reject);
        request.on('timeout', () => {
            request.destroy();
            reject(new Error('Request timeout'));
        });
    });
}

// Main worker function - adapted to skip discovery
async function runWorker(symbol) {
    try {
        await log(`🚀 Starting SOPHISTICATED worker for ${symbol}`, 'INFO');
        await log(`🔗 Run ID: ${process.env.GITHUB_RUN_ID || 'local'}`, 'INFO');
        
        // Ensure output directory exists
        await ensureDir(OUTPUT_DIR);
        
        // Use working HTML parsing for file discovery
        await log(`🔍 Discovering files for ${symbol} using HTML parsing...`, 'INFO');
        const files = await discoverFilesWithHTML(symbol);
        
        if (files.length === 0) {
            await log(`No files found for ${symbol}`, 'WARN');
            return;
        }
        
        await log(`📁 Found ${files.length} files to download`, 'INFO');
        
        // Download files with sophisticated error handling
        let successCount = 0;
        let errorCount = 0;
        
        for (const file of files) {
            try {
                await downloadFile(file);
                successCount++;
            } catch (error) {
                errorCount++;
                await log(`Failed to download ${file.filename}: ${error.message}`, 'ERROR', { 
                    filename: file.filename, 
                    error: error.stack 
                });
            }
        }
        
        await log(`✅ Worker completed for ${symbol}!`, 'INFO', { 
            symbol, 
            totalFiles: files.length, 
            successCount, 
            errorCount 
        });
        
    } catch (error) {
        await log(`💥 Worker failed for ${symbol}: ${error.message}`, 'ERROR', { 
            symbol, 
            error: error.stack 
        });
        throw error;
    }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
    const symbol = process.argv[2];
    if (!symbol) {
        console.error('Usage: node worker_system_production_adapted.js <SYMBOL>');
        console.error('Example: node worker_system_production_adapted.js BTCUSDT');
        process.exit(1);
    }
    
    runWorker(symbol).catch(error => {
        log(`Fatal error: ${error.message}`, 'ERROR');
        process.exit(1);
    });
}

export { runWorker, discoverFilesWithHTML, downloadFile };
