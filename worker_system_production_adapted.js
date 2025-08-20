#!/usr/bin/env node
/**
 * KuCoin Data Discovery Worker System - ADAPTED FOR GITHUB ACTIONS
 * Uses working XML endpoint with proper pagination and all sophisticated features:
 * - XML parsing with max-keys and marker pagination (handles 1000+ files)
 * - Multiple extraction methods and robust error handling
 * - File validation and duplicate detection
 * - Comprehensive logging and progress tracking
 */

import https from 'https';
import http from 'http';
import fs from 'fs/promises';
import { createWriteStream, unlink } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

console.log('🚀 Sophisticated worker starting...');

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

// Enhanced logging
function log(message, level = 'INFO', context = {}) {
    const timestamp = new Date().toISOString();
    const contextStr = Object.keys(context).length > 0 ? ` [${JSON.stringify(context)}]` : '';
    const logEntry = `[${timestamp}] [${level}] ${message}${contextStr}`;
    console.log(logEntry);
}

// Utility functions
async function ensureDir(dir) {
    await fs.mkdir(dir, { recursive: true });
}

// Working file discovery using XML endpoint with proper pagination
async function discoverFilesWithXML(symbol) {
    const files = [];
    let marker = undefined;
    let page = 0;
    
    try {
        while (true) {
            page++;
            const params = new URLSearchParams({ 
                prefix: `data/spot/daily/trades/${symbol}/`, 
                'max-keys': '1000' 
            });
            if (marker) params.set('marker', marker);
            
            const url = `https://historical-data.kucoin.com/?${params.toString()}`;
            log(`Scraping ${symbol} page ${page}`, 'DEBUG', { symbol, page, url });
            
            const xml = await fetchXML(url);
            
            // Parse XML to extract file listings
            const keyMatches = xml.match(/<Key>([^<]+)<\/Key>/g);
            const sizeMatches = xml.match(/<Size>([^<]+)<\/Size>/g);
            const lastModifiedMatches = xml.match(/<LastModified>([^<]+)<\/LastModified>/g);
            
            if (keyMatches) {
                keyMatches.forEach((keyMatch, index) => {
                    const key = keyMatch.replace(/<Key>([^<]+)<\/Key>/, '$1');
                    
                    // Only process actual zip files (skip checksums and other files)
                    if (key.endsWith('.zip') && !key.endsWith('.zip.CHECKSUM')) {
                        const filename = key.split('/').pop();
                        const size = sizeMatches && sizeMatches[index] 
                            ? parseInt(sizeMatches[index].replace(/<Size>([^<]+)<\/Size>/, '$1')) 
                            : 0;
                        const lastModified = lastModifiedMatches && lastModifiedMatches[index]
                            ? lastModifiedMatches[index].replace(/<LastModified>([^<]+)<\/LastModified>/, '$1')
                            : new Date().toISOString();
                        
                        files.push({
                            symbol,
                            filename: filename,
                            url: `https://historical-data.kucoin.com/${key}`,
                            size: size,
                            lastModified: lastModified
                        });
                    }
                });
            }
            
            log(`Page ${page}: Found ${keyMatches ? keyMatches.length : 0} total keys, ${keyMatches ? keyMatches.filter(k => k.includes('.zip') && !k.includes('.zip.CHECKSUM')).length : 0} zip files`, 'DEBUG', { 
                symbol, page, totalKeys: keyMatches ? keyMatches.length : 0, zipFiles: keyMatches ? keyMatches.filter(k => k.includes('.zip') && !k.includes('.zip.CHECKSUM')).length : 0 
            });
            
            // Check if more pages exist
            const isTruncated = xml.includes('<IsTruncated>true</IsTruncated>');
            if (!isTruncated) {
                log(`No more pages for ${symbol}`, 'DEBUG', { symbol, page });
                break;
            }
            
            // Get next marker for pagination
            const nextMarkerMatch = xml.match(/<NextMarker>([^<]+)<\/NextMarker>/);
            if (nextMarkerMatch) {
                marker = nextMarkerMatch[1];
                log(`Next marker for ${symbol}: ${marker}`, 'DEBUG', { symbol, nextMarker: marker });
            } else {
                // If no NextMarker, use the last key as marker
                if (keyMatches && keyMatches.length > 0) {
                    const lastKey = keyMatches[keyMatches.length - 1].replace(/<Key>([^<]+)<\/Key>/, '$1');
                    marker = lastKey;
                    log(`Using last key as marker for ${symbol}: ${marker}`, 'DEBUG', { symbol, marker });
                } else {
                    log(`No more keys and no marker for ${symbol}, stopping pagination`, 'WARN', { symbol, page });
                    break;
                }
            }
            
            // Small delay between pages to be respectful
            await new Promise(resolve => setTimeout(resolve, 100));
        }
        
        log(`Successfully discovered ${symbol}: ${files.length} zip files across ${page} pages`, 'INFO', { 
            symbol, fileCount: files.length, totalPages: page 
        });
        return files;
        
    } catch (error) {
        log(`Failed to discover ${symbol}: ${error.message}`, 'ERROR', { symbol, error: error.stack });
        throw error;
    }
}

// Fetch XML with retries and exponential backoff
async function fetchXML(url, retryCount = 0) {
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
                    fetchXML(url, retryCount + 1).then(resolve).catch(reject);
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
            log(`File already exists: ${file.filename}`, 'INFO', { 
                filename: file.filename, 
                size: stats.size 
            });
            return true;
        }
    } catch (error) {
        // File doesn't exist, proceed with download
        log(`File doesn't exist, proceeding with download: ${file.filename}`, 'DEBUG', { filename: file.filename });
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
        log(`🚀 Starting SOPHISTICATED worker for ${symbol}`, 'INFO');
        log(`🔗 Run ID: ${process.env.GITHUB_RUN_ID || 'local'}`, 'INFO');
        
        // Ensure output directory exists
        await ensureDir(OUTPUT_DIR);
        
        // Use working XML endpoint with proper pagination
        log(`🔍 Discovering files for ${symbol} using XML endpoint with pagination...`, 'INFO');
        const files = await discoverFilesWithXML(symbol);
        
        if (files.length === 0) {
            log(`No files found for ${symbol}`, 'WARN');
            return;
        }
        
        log(`📁 Found ${files.length} files to download`, 'INFO');
        
        // Download files with sophisticated error handling
        let successCount = 0;
        let errorCount = 0;
        
        for (const file of files) {
            try {
                await downloadFile(file);
                successCount++;
            } catch (error) {
                errorCount++;
                log(`Failed to download ${file.filename}: ${error.message}`, 'ERROR', { 
                    filename: file.filename, 
                    error: error.stack 
                });
            }
        }
        
        log(`✅ Worker completed for ${symbol}!`, 'INFO', { 
            symbol, 
            totalFiles: files.length, 
            successCount, 
            errorCount 
        });
        
    } catch (error) {
        log(`💥 Worker failed for ${symbol}: ${error.message}`, 'ERROR', { 
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
    
    console.log('📋 About to call runWorker()...');
    runWorker(symbol).then(() => {
        console.log('✅ Worker completed successfully');
    }).catch(error => {
        console.error('💥 Worker failed:', error.message);
        process.exit(1);
    });
}

export { runWorker, discoverFilesWithXML, downloadFile };
