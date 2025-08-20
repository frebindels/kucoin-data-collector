#!/usr/bin/env node
/**
 * GitHub Actions Worker - Single Symbol Handler
 * Optimized for running in GitHub Actions environment
 */

import fs from 'fs/promises';
import { createWriteStream, unlink } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { XMLParser } from 'fast-xml-parser';
import https from 'https';
import http from 'http';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// GitHub Actions optimized configuration
const GITHUB_CONFIG = {
    symbol: process.argv[2] || 'BTCUSDT',
    maxConcurrentDownloads: 15,           // Higher concurrency for cloud
    downloadDelay: 20,                    // Faster for cloud (no rate limit concerns)
    timeout: 30000,                       // Request timeout
    chunkSize: 1024 * 1024,              // 1MB chunks for cloud
    maxRetries: 2,                        // Fewer retries for speed
    retryDelay: 500,                      // Faster retries
    outputDir: './worker_output',         // Output directory
    progressFile: './worker_progress.json',
    webhookUrl: process.env.WEBHOOK_URL || null
};

// XML parser
const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '', allowBooleanAttributes: true });

// Worker state
const workerState = {
    symbol: GITHUB_CONFIG.symbol,
    startTime: Date.now(),
    filesDiscovered: 0,
    filesDownloaded: 0,
    bytesDownloaded: 0,
    errors: 0,
    currentDownloads: new Set(),
    completedFiles: new Set()
};

// GitHub Actions optimized logging
function log(message, level = 'INFO') {
    const timestamp = new Date().toISOString();
    const logEntry = `[${timestamp}] [${GITHUB_CONFIG.symbol}-${level}] ${message}`;
    console.log(logEntry);
    
    // Also log to GitHub Actions
    if (process.env.GITHUB_ACTIONS) {
        if (level === 'ERROR') {
            console.error(`::error::${message}`);
        } else if (level === 'WARN') {
            console.warn(`::warning::${message}`);
        }
    }
}

// Save progress
async function saveProgress() {
    try {
        const progress = {
            symbol: workerState.symbol,
            lastUpdate: new Date().toISOString(),
            filesDiscovered: workerState.filesDiscovered,
            filesDownloaded: workerState.filesDownloaded,
            bytesDownloaded: workerState.bytesDownloaded,
            errors: workerState.errors,
            uptime: Math.floor((Date.now() - workerState.startTime) / 1000),
            githubRunId: process.env.GITHUB_RUN_ID || 'local'
        };
        
        await fs.writeFile(GITHUB_CONFIG.progressFile, JSON.stringify(progress, null, 2));
        return progress;
    } catch (error) {
        log(`Failed to save progress: ${error.message}`, 'ERROR');
        return null;
    }
}

// Send completion to webhook
async function sendToWebhook(metadata) {
    if (!GITHUB_CONFIG.webhookUrl) {
        log('No webhook URL configured, skipping webhook notification', 'INFO');
        return;
    }
    
    try {
        const webhookData = {
            symbol: workerState.symbol,
            status: 'completed',
            files: workerState.filesDownloaded,
            metadata: metadata,
            progress: await saveProgress()
        };
        
        const response = await fetch(GITHUB_CONFIG.webhookUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'User-Agent': 'KuCoin-GitHub-Worker/1.0'
            },
            body: JSON.stringify(webhookData)
        });
        
        if (response.ok) {
            log(`✅ Webhook notification sent successfully`, 'INFO');
        } else {
            log(`⚠️ Webhook notification failed: ${response.status}`, 'WARN');
        }
        
    } catch (error) {
        log(`⚠️ Failed to send webhook notification: ${error.message}`, 'WARN');
    }
}

// Discover files for a symbol using the working TURBO approach
async function discoverFiles(symbol) {
    const url = `https://historical-data.kucoin.com/data/spot/daily/trades/${symbol}/`;
    
    return new Promise((resolve, reject) => {
        const protocol = url.startsWith('https:') ? https : http;
        const request = protocol.get(url, { timeout: GITHUB_CONFIG.timeout }, (response) => {
            if (response.statusCode !== 200) {
                reject(new Error(`HTTP ${response.statusCode}: ${response.statusMessage}`));
                return;
            }
            
            let data = '';
            response.on('data', chunk => data += chunk);
            response.on('end', () => {
                try {
                    const files = [];
                    
                    // Use the working TURBO approach - look for .zip files
                    const fileMatches = data.match(/href="([^"]*\.zip)"/g);
                    
                    if (fileMatches) {
                        fileMatches.forEach(match => {
                            const filename = match.match(/href="([^"]*\.zip)"/)[1];
                            const fileUrl = `https://historical-data.kucoin.com/data/spot/daily/trades/${symbol}/${filename}`;
                            
                            files.push({
                                filename: filename,
                                url: fileUrl,
                                size: 0, // We'll get this from headers if needed
                                lastModified: new Date().toISOString()
                            });
                        });
                    }
                    
                    workerState.filesDiscovered = files.length;
                    log(`Discovered ${files.length} files for ${symbol}`, 'INFO');
                    resolve(files);
                } catch (error) {
                    reject(new Error(`Failed to parse HTML: ${error.message}`));
                }
            });
        });
        
        request.on('error', reject);
        request.on('timeout', () => {
            request.destroy();
            reject(new Error('Request timeout'));
        });
    });
}

// Download a single file
async function downloadFile(fileInfo) {
    if (workerState.currentDownloads.has(fileInfo.filename)) {
        return false;
    }
    
    workerState.currentDownloads.add(fileInfo.filename);
    
    try {
        const outputPath = path.join(GITHUB_CONFIG.outputDir, GITHUB_CONFIG.symbol, fileInfo.filename);
        
        // Ensure directory exists
        await fs.mkdir(path.dirname(outputPath), { recursive: true });
        
        // Check if file already exists
        try {
            const stats = await fs.stat(outputPath);
            if (stats.size > 0) {
                log(`File already exists: ${fileInfo.filename}`, 'INFO');
                workerState.completedFiles.add(fileInfo.filename);
                return true;
            }
        } catch (error) {
            // File doesn't exist, continue with download
        }
        
        return new Promise((resolve, reject) => {
            const protocol = fileInfo.url.startsWith('https:') ? https : http;
            const request = protocol.get(fileInfo.url, { timeout: GITHUB_CONFIG.timeout }, (response) => {
                if (response.statusCode !== 200) {
                    reject(new Error(`HTTP ${response.statusCode}: ${response.statusMessage}`));
                    return;
                }
                
                const fileStream = createWriteStream(outputPath);
                let downloadedBytes = 0;
                
                response.on('data', (chunk) => {
                    downloadedBytes += chunk.length;
                    workerState.bytesDownloaded += chunk.length;
                });
                
                response.on('end', () => {
                    fileStream.end();
                    workerState.filesDownloaded++;
                    workerState.completedFiles.add(fileInfo.filename);
                    
                    log(`Downloaded: ${fileInfo.filename} (${(downloadedBytes / 1024).toFixed(1)} KB)`, 'INFO');
                    resolve(true);
                });
                
                response.on('error', (error) => {
                    fileStream.destroy();
                    unlink(outputPath, () => {}); // Clean up partial file
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
        
    } catch (error) {
        workerState.errors++;
        log(`Download failed for ${fileInfo.filename}: ${error.message}`, 'ERROR');
        return false;
    } finally {
        workerState.currentDownloads.delete(fileInfo.filename);
    }
}

// Main worker function
async function runWorker() {
    try {
        log(`🚀 GitHub Actions worker started for ${GITHUB_CONFIG.symbol}`, 'INFO');
        log(`🔗 Run ID: ${process.env.GITHUB_RUN_ID || 'local'}`, 'INFO');
        
        // Create output directory
        await fs.mkdir(path.join(GITHUB_CONFIG.outputDir, GITHUB_CONFIG.symbol), { recursive: true });
        
        // Discover files
        log(`🔍 Discovering files for ${GITHUB_CONFIG.symbol}...`, 'INFO');
        const files = await discoverFiles(GITHUB_CONFIG.symbol);
        
        if (files.length === 0) {
            log(`No files found for ${GITHUB_CONFIG.symbol}`, 'WARN');
            await sendToWebhook({ message: 'No files found' });
            return;
        }
        
        log(`📁 Found ${files.length} files to download`, 'INFO');
        
        // Sort files by date (newest first for priority)
        files.sort((a, b) => {
            const dateA = new Date(a.lastModified);
            const dateB = new Date(b.lastModified);
            return dateB - dateA;
        });
        
        // Download files with aggressive concurrency for cloud
        let currentIndex = 0;
        const activeDownloads = new Set();
        
        while (currentIndex < files.length || activeDownloads.size > 0) {
            // Start new downloads if we have capacity
            while (activeDownloads.size < GITHUB_CONFIG.maxConcurrentDownloads && currentIndex < files.length) {
                const file = files[currentIndex];
                currentIndex++;
                
                const downloadPromise = downloadFile(file).then(() => {
                    activeDownloads.delete(downloadPromise);
                });
                
                activeDownloads.add(downloadPromise);
                
                // Minimal delay for cloud
                await new Promise(resolve => setTimeout(resolve, GITHUB_CONFIG.downloadDelay));
            }
            
            // Wait for at least one download to complete
            if (activeDownloads.size > 0) {
                await Promise.race(activeDownloads);
            }
            
            // Progress update
            const progress = ((workerState.filesDownloaded / workerState.filesDiscovered) * 100).toFixed(1);
            log(`Progress: ${progress}% (${workerState.filesDownloaded}/${workerState.filesDiscovered})`, 'INFO');
        }
        
        // Save final progress
        const finalProgress = await saveProgress();
        
        log(`✅ GitHub Actions worker completed for ${GITHUB_CONFIG.symbol}!`, 'INFO');
        log(`📊 Final stats: ${workerState.filesDownloaded} files, ${(workerState.bytesDownloaded / 1024 / 1024).toFixed(1)} MB`, 'INFO');
        
        // Send completion to webhook
        await sendToWebhook({
            filesDiscovered: workerState.filesDiscovered,
            filesDownloaded: workerState.filesDownloaded,
            bytesDownloaded: workerState.bytesDownloaded,
            errors: workerState.errors,
            uptime: Math.floor((Date.now() - workerState.startTime) / 1000)
        });
        
    } catch (error) {
        log(`💥 GitHub Actions worker failed: ${error.message}`, 'ERROR');
        
        // Send failure notification to webhook
        await sendToWebhook({
            error: error.message,
            status: 'failed'
        });
        
        throw error;
    }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
    log('🛑 Received SIGINT, saving progress and shutting down...', 'INFO');
    await saveProgress();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    log('🛑 Received SIGTERM, saving progress and shutting down...', 'INFO');
    await saveProgress();
    process.exit(0);
});

// Run worker
if (import.meta.url === `file://${process.argv[1]}`) {
    if (!process.argv[2]) {
        console.error('Usage: node github_actions_worker.js <SYMBOL>');
        console.error('Example: node github_actions_worker.js BTCUSDT');
        process.exit(1);
    }
    
    runWorker().catch(error => {
        log(`Fatal error: ${error.message}`, 'ERROR');
        process.exit(1);
    });
}

export { runWorker, GITHUB_CONFIG };
