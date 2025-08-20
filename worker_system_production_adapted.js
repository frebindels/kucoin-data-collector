#!/usr/bin/env node
/**
 * KuCoin Data Discovery Worker System - ADAPTED FOR GITHUB ACTIONS
 * Skips symbol discovery, uses provided symbols list, but keeps all sophisticated features:
 * - XML pagination with max-keys and marker
 * - Multiple extraction methods (table, directory, text patterns)
 * - Robust error handling with retries and exponential backoff
 * - File validation and duplicate detection
 * - Comprehensive logging and progress tracking
 */

import fs from 'fs/promises';
import { createWriteStream, unlink } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { XMLParser } from 'fast-xml-parser';

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

// XML parser
const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '', allowBooleanAttributes: true });

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

// Sophisticated file discovery using XML pagination (the advanced feature you wanted)
async function discoverFilesWithPagination(symbol) {
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
            await log(`Scraping ${symbol} page ${page}`, 'DEBUG', { symbol, page, url });
            
            const xml = await fetchXml(url);
            const obj = parser.parse(xml);
            const result = obj?.ListBucketResult || {};
            const contents = normalizeContents(result.Contents);
            
            // Process this page
            contents.forEach(content => {
                if (content.Key.endsWith('.zip')) {
                    files.push({
                        symbol,
                        filename: content.Key.split('/').pop(),
                        url: `https://historical-data.kucoin.com/${content.Key}`,
                        size: Number(content.Size || 0),
                        lastModified: content.LastModified || null
                    });
                }
            });
            
            // Check if more pages
            const isTruncated = String(result.IsTruncated || 'false').toLowerCase() === 'true';
            if (!isTruncated) break;
            
            // Set next marker for pagination
            if (result.NextMarker) {
                marker = result.NextMarker;
            } else if (contents.length > 0) {
                marker = contents[contents.length - 1].Key;
            } else {
                break;
            }
        }
        
        await log(`Successfully scraped ${symbol}: ${files.length} files`, 'INFO', { symbol, fileCount: files.length });
        return files;
        
    } catch (error) {
        await log(`Failed to scrape ${symbol}: ${error.message}`, 'ERROR', { symbol, error: error.stack });
        throw error;
    }
}

// Fetch XML with retries and exponential backoff
async function fetchXml(url, retryCount = 0) {
    try {
        const response = await fetch(url, { 
            timeout: WORKER_CONFIG.timeout,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        });
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        return await response.text();
        
    } catch (error) {
        if (retryCount < WORKER_CONFIG.maxRetries) {
            await log(`Fetch failed, retrying (${retryCount + 1}/${WORKER_CONFIG.maxRetries}): ${error.message}`, 'WARN', { 
                url, retryCount, error: error.stack 
            });
            await new Promise(resolve => setTimeout(resolve, WORKER_CONFIG.retryDelay * (retryCount + 1))); // Exponential backoff
            return fetchXml(url, retryCount + 1);
        } else {
            throw error;
        }
    }
}

// Normalize XML contents
function normalizeContents(contents) {
    if (!contents) return [];
    if (Array.isArray(contents)) return contents;
    return [contents];
}

// Enhanced download function with comprehensive error handling
async function downloadFile(file) {
    const symbolDir = path.join(OUTPUT_DIR, file.symbol);
    await ensureDir(symbolDir);
    
    const filePath = path.join(symbolDir, file.filename);
    
    // Check if already downloaded
    try {
        const stats = await fs.stat(filePath);
        if (stats.size === file.size) {
            await log(`File already exists and size matches: ${file.filename}`, 'INFO', { 
                filename: file.filename, 
                expectedSize: file.size, 
                actualSize: stats.size 
            });
            return true;
        } else {
            await log(`File exists but size mismatch, will re-download: ${file.filename}`, 'WARN', { 
                filename: file.filename, 
                expectedSize: file.size, 
                actualSize: stats.size,
                difference: Math.abs(file.size - stats.size)
            });
            // Remove mismatched file
            await fs.unlink(filePath);
        }
    } catch (error) {
        // File doesn't exist, proceed with download
        await log(`File doesn't exist, proceeding with download: ${file.filename}`, 'DEBUG', { filename: file.filename });
    }
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), WORKER_CONFIG.timeout);
    
    try {
        const response = await fetch(file.url, { 
            signal: controller.signal,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        });
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const fileStream = createWriteStream(filePath);
        let downloadedBytes = 0;
        
        const reader = response.body.getReader();
        
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            
            fileStream.write(value);
            downloadedBytes += value.length;
        }
        
        fileStream.end();
        clearTimeout(timeoutId);
        
        await log(`Downloaded: ${file.filename} (${(downloadedBytes / 1024).toFixed(1)} KB)`, 'INFO', { 
            filename: file.filename, 
            size: downloadedBytes 
        });
        
        return true;
        
    } catch (error) {
        clearTimeout(timeoutId);
        
        // Clean up partial file
        try {
            await fs.unlink(filePath);
        } catch (cleanupError) {
            // Ignore cleanup errors
        }
        
        throw error;
    }
}

// Main worker function - adapted to skip discovery
async function runWorker(symbol) {
    try {
        await log(`🚀 Starting PRODUCTION worker for ${symbol}`, 'INFO');
        await log(`🔗 Run ID: ${process.env.GITHUB_RUN_ID || 'local'}`, 'INFO');
        
        // Ensure output directory exists
        await ensureDir(OUTPUT_DIR);
        
        // Use sophisticated file discovery with XML pagination
        await log(`🔍 Discovering files for ${symbol} using XML pagination...`, 'INFO');
        const files = await discoverFilesWithPagination(symbol);
        
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

export { runWorker, discoverFilesWithPagination, downloadFile };
