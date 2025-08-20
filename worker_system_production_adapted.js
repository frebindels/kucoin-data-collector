#!/usr/bin/env node
/**
 * KuCoin Data Discovery Worker System - ADAPTED FOR GITHUB ACTIONS
 * Uses working XML endpoint with proper pagination and FULL PIPELINE:
 * - XML parsing with max-keys and marker pagination (handles 1000+ files)
 * - Download ZIP + Checksum files
 * - Verify checksums using MD5
 * - Validate ZIP integrity
 * - Extract to CSV
 * - Validate CSV structure and data quality
 * - Comprehensive logging and progress tracking
 */

import https from 'https';
import http from 'http';
import fs from 'fs/promises';
import { createWriteStream, unlink } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import crypto from 'crypto';
import AdmZip from 'adm-zip';

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
                            checksumUrl: `https://historical-data.kucoin.com/${key}.CHECKSUM`,
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

// Download file with retries
async function downloadFileWithRetry(url, filePath, retryCount = 0) {
    return new Promise((resolve, reject) => {
        const protocol = url.startsWith('https:') ? https : http;
        const request = protocol.get(url, { timeout: WORKER_CONFIG.timeout }, (response) => {
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
                resolve({ success: true, bytes: downloadedBytes });
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
        
        request.on('error', (error) => {
            if (retryCount < WORKER_CONFIG.maxRetries) {
                setTimeout(() => {
                    downloadFileWithRetry(url, filePath, retryCount + 1).then(resolve).catch(reject);
                }, WORKER_CONFIG.retryDelay * (retryCount + 1));
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

// Verify checksum using MD5
async function verifyChecksum(zipPath, checksumPath) {
    try {
        const checksumContent = await fs.readFile(checksumPath, 'utf8');
        const expectedChecksum = checksumContent.trim().split()[0].toLowerCase();
        
        const md5 = crypto.createHash('md5');
        const zipData = await fs.readFile(zipPath);
        md5.update(zipData);
        const actualChecksum = md5.digest('hex').toLowerCase();
        
        const isValid = expectedChecksum === actualChecksum;
        return { 
            valid: isValid, 
            expected: expectedChecksum, 
            actual: actualChecksum,
            error: isValid ? null : 'Checksum mismatch'
        };
    } catch (error) {
        return { valid: false, error: error.message };
    }
}

// Validate ZIP file integrity
async function validateZipFile(zipPath) {
    try {
        const zip = new AdmZip(zipPath);
        const zipEntries = zip.getEntries();
        
        if (zipEntries.length === 0) {
            return { valid: false, error: 'ZIP file is empty' };
        }
        
        // Check for CSV file
        const csvEntry = zipEntries.find(entry => entry.entryName.endsWith('.csv'));
        if (!csvEntry) {
            return { valid: false, error: 'No CSV file found in ZIP' };
        }
        
        // Try to read CSV content
        const csvData = zip.readAsText(csvEntry);
        if (!csvData || csvData.length === 0) {
            return { valid: false, error: 'CSV content is empty' };
        }
        
        // Check CSV structure
        const lines = csvData.split('\n').filter(line => line.trim().length > 0);
        if (lines.length < 2) {
            return { valid: false, error: 'CSV has insufficient data' };
        }
        
        const header = lines[0];
        const expectedHeaders = ['trade_id', 'trade_time', 'price', 'size', 'side'];
        const hasRequiredHeaders = expectedHeaders.every(h => header.toLowerCase().includes(h));
        
        if (!hasRequiredHeaders) {
            return { valid: false, error: 'CSV missing required headers' };
        }
        
        return { 
            valid: true, 
            csvSize: csvData.length,
            csvLines: lines.length,
            dataRows: lines.length - 1,
            csvHash: crypto.createHash('md5').update(csvData).digest('hex').toLowerCase()
        };
        
    } catch (error) {
        return { valid: false, error: `ZIP validation failed: ${error.message}` };
    }
}

// Extract ZIP to CSV
async function extractZipToCsv(zipPath, extractDir) {
    try {
        const zip = new AdmZip(zipPath);
        const zipEntries = zip.getEntries();
        
        const csvEntry = zipEntries.find(entry => entry.entryName.endsWith('.csv'));
        if (!csvEntry) {
            throw new Error('No CSV file found in ZIP');
        }
        
        const csvFilename = csvEntry.entryName.split('/').pop();
        const csvPath = path.join(extractDir, csvFilename);
        
        // Extract CSV
        zip.extractEntryTo(csvEntry.entryName, extractDir, false, true);
        
        // Verify extraction
        const extractedData = await fs.readFile(csvPath, 'utf8');
        const extractedHash = crypto.createHash('md5').update(extractedData).digest('hex').toLowerCase();
        
        return { 
            success: true, 
            csvPath: csvPath,
            csvSize: extractedData.length,
            csvHash: extractedHash
        };
        
    } catch (error) {
        return { success: false, error: error.message };
    }
}

// Full pipeline: Download, verify, validate, extract
async function processFileComplete(file) {
    const symbolDir = path.join(OUTPUT_DIR, file.symbol);
    const extractDir = path.join(symbolDir, 'extracted');
    await ensureDir(symbolDir);
    await ensureDir(extractDir);
    
    const zipPath = path.join(symbolDir, file.filename);
    const checksumPath = path.join(symbolDir, file.filename + '.CHECKSUM');
    
    try {
        // Step 1: Download ZIP file
        log(`Downloading ZIP: ${file.filename}`, 'INFO');
        const zipResult = await downloadFileWithRetry(file.url, zipPath);
        log(`Downloaded ZIP: ${file.filename} (${(zipResult.bytes / 1024).toFixed(1)} KB)`, 'INFO');
        
        // Step 2: Download checksum file
        log(`Downloading checksum: ${file.filename}.CHECKSUM`, 'INFO');
        const checksumResult = await downloadFileWithRetry(file.checksumUrl, checksumPath);
        log(`Downloaded checksum: ${file.filename}.CHECKSUM`, 'INFO');
        
        // Step 3: Verify checksum
        log(`Verifying checksum: ${file.filename}`, 'INFO');
        const checksumValidation = await verifyChecksum(zipPath, checksumPath);
        if (!checksumValidation.valid) {
            throw new Error(`Checksum verification failed: ${checksumValidation.error}`);
        }
        log(`Checksum verified: ${file.filename}`, 'INFO');
        
        // Step 4: Validate ZIP integrity
        log(`Validating ZIP: ${file.filename}`, 'INFO');
        const zipValidation = await validateZipFile(zipPath);
        if (!zipValidation.valid) {
            throw new Error(`ZIP validation failed: ${zipValidation.error}`);
        }
        log(`ZIP validated: ${file.filename} (${zipValidation.dataRows} data rows)`, 'INFO');
        
        // Step 5: Extract to CSV
        log(`Extracting to CSV: ${file.filename}`, 'INFO');
        const extractResult = await extractZipToCsv(zipPath, extractDir);
        if (!extractResult.success) {
            throw new Error(`CSV extraction failed: ${extractResult.error}`);
        }
        log(`CSV extracted: ${file.filename} -> ${extractResult.csvPath}`, 'INFO');
        
        return { 
            success: true, 
            filename: file.filename,
            zipSize: zipResult.bytes,
            csvSize: extractResult.csvSize,
            dataRows: zipValidation.dataRows
        };
        
    } catch (error) {
        // Clean up partial files on error
        try {
            if (await fs.access(zipPath).then(() => true).catch(() => false)) {
                await fs.unlink(zipPath);
            }
            if (await fs.access(checksumPath).then(() => true).catch(() => false)) {
                await fs.unlink(checksumPath);
            }
        } catch (cleanupError) {
            // Ignore cleanup errors
        }
        
        throw error;
    }
}

// Main worker function - adapted to skip discovery
async function runWorker(symbol) {
    try {
        console.log('🔍 DEBUG: Entering runWorker function');
        log(`🚀 Starting SOPHISTICATED worker for ${symbol}`, 'INFO');
        log(`🔗 Run ID: ${process.env.GITHUB_RUN_ID || 'local'}`, 'INFO');
        
        console.log('🔍 DEBUG: About to ensure output directory');
        // Ensure output directory exists
        await ensureDir(OUTPUT_DIR);
        console.log('🔍 DEBUG: Output directory ensured');
        
        // Use working XML endpoint with proper pagination
        console.log('🔍 DEBUG: About to discover files');
        log(`🔍 Discovering files for ${symbol} using XML endpoint with pagination...`, 'INFO');
        const files = await discoverFilesWithXML(symbol);
        console.log('🔍 DEBUG: Files discovered, count:', files.length);
        
        if (files.length === 0) {
            log(`No files found for ${symbol}`, 'WARN');
            return;
        }
        
        log(`📁 Found ${files.length} files to process`, 'INFO');
        
        // Process files with full pipeline
        let successCount = 0;
        let errorCount = 0;
        
        for (const file of files) {
            try {
                const result = await processFileComplete(file);
                successCount++;
                log(`✅ File processed successfully: ${file.filename}`, 'INFO', { 
                    zipSize: result.zipSize, 
                    csvSize: result.csvSize, 
                    dataRows: result.dataRows 
                });
            } catch (error) {
                errorCount++;
                log(`Failed to process ${file.filename}: ${error.message}`, 'ERROR', { 
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
        console.error('🔍 DEBUG: Error in runWorker:', error.message);
        console.error('🔍 DEBUG: Error stack:', error.stack);
        log(`💥 Worker failed for ${symbol}: ${error.message}`, 'ERROR', { 
            symbol, 
            error: error.stack 
        });
        throw error;
    }
}

// Run if called directly
console.log('🔍 DEBUG: Script loaded, checking if run directly');
console.log('🔍 DEBUG: process.argv[1]:', process.argv[1]);
console.log('🔍 DEBUG: import.meta.url:', import.meta.url);

if (process.argv[1] && process.argv[1].endsWith('worker_system_production_adapted.js')) {
    console.log('🔍 DEBUG: Script is being run directly');
    const symbol = process.argv[2];
    console.log('🔍 DEBUG: Symbol argument:', symbol);
    
    if (!symbol) {
        console.error('Usage: node worker_system_production_adapted.js <SYMBOL>');
        console.error('Example: node worker_system_production_adapted.js BTCUSDT');
        process.exit(1);
    }
    
    console.log('📋 About to call runWorker()...');
    console.log('🔍 DEBUG: Calling runWorker with symbol:', symbol);
    
    runWorker(symbol).then(() => {
        console.log('✅ Worker completed successfully');
    }).catch(error => {
        console.error('💥 Worker failed:', error.message);
        console.error('🔍 DEBUG: Full error:', error);
        process.exit(1);
    });
} else {
    console.log('🔍 DEBUG: Script loaded as module, not running directly');
}

export { runWorker, discoverFilesWithXML, processFileComplete };
