#!/usr/bin/env node
/**
 * KuCoin Data Discovery Worker System - PRODUCTION VERSION
 * 2 HTTP Scrapers + 2 Downloaders with real download logic and robust error handling
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
	scrapers: 2,
	downloaders: 2,
	scraperDelay: 150, // ms between requests (optimized)
	downloaderDelay: 50, // ms between downloads (optimized)
	maxRetries: 5, // increased retries for perfection
	retryDelay: 2000, // ms before retry
	chunkSize: 64 * 1024, // 64KB chunks for large files
	timeout: 30000, // 30s timeout for downloads
	stateSaveInterval: 5000, // save state every 5s
	progressInterval: 10000, // progress report every 10s
	maxConsecutiveFailures: 10, // stop if too many consecutive failures
	healthCheckInterval: 30000 // health check every 30s
};

// Paths
const DISCOVERED_DIR = path.join(__dirname, 'discovered');
const INDEXED_DIR = path.join(__dirname, 'indexed');
const DOWNLOADS_DIR = path.join(__dirname, 'downloads');
const SYMBOLS_FILE = path.join(DISCOVERED_DIR, 'symbols.json');
const STATE_FILE = path.join(INDEXED_DIR, 'worker_state.json');
const LOG_FILE = path.join(INDEXED_DIR, 'worker_log.txt');

// Shared state with better tracking
const state = {
	scrapedSymbols: new Set(),
	queuedFiles: new Map(), // symbol -> [file1, file2, ...]
	downloadedFiles: new Set(),
	failedSymbols: new Set(),
	failedDownloads: new Map(), // url -> {retries, lastError, timestamp}
	startTime: Date.now(),
	stats: {
		symbolsProcessed: 0,
		filesQueued: 0,
		filesDownloaded: 0,
		bytesDownloaded: 0,
		errors: 0,
		retries: 0
	},
	lastProgressTime: Date.now(), // Track last time progress was made
	lastDownloadedCount: 0 // Track last downloaded count for progress check
};

// Queue management with priority
const downloadQueue = [];
const retryQueue = [];
let isShuttingDown = false;

// XML parser
const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '', allowBooleanAttributes: true });

// Enhanced logging with failure tracking
async function log(message, level = 'INFO', context = {}) {
	const timestamp = new Date().toISOString();
	const contextStr = Object.keys(context).length > 0 ? ` [${JSON.stringify(context)}]` : '';
	const logEntry = `[${timestamp}] [${level}] ${message}${contextStr}`;
	console.log(logEntry);
	
	try {
		await fs.appendFile(LOG_FILE, logEntry + '\n');
	} catch (error) {
		// Don't fail if logging fails, but log to console
		console.error('Logging failed:', error.message);
	}
}

// Utility functions
async function ensureDir(dir) {
	await fs.mkdir(dir, { recursive: true });
}

async function loadState() {
	try {
		const data = await fs.readFile(STATE_FILE, 'utf8');
		const saved = JSON.parse(data);
		state.scrapedSymbols = new Set(saved.scrapedSymbols || []);
		state.queuedFiles = new Map(saved.queuedFiles || []);
		state.downloadedFiles = new Set(saved.downloadedFiles || []);
		state.failedSymbols = new Set(saved.failedSymbols || []);
		state.failedDownloads = new Map(saved.failedDownloads || []);
		state.stats = saved.stats || state.stats;
		state.lastProgressTime = saved.lastProgressTime ? new Date(saved.lastProgressTime).getTime() : state.startTime;
		state.lastDownloadedCount = saved.lastDownloadedCount || 0;
		await log(`Loaded state: ${state.scrapedSymbols.size} scraped, ${state.queuedFiles.size} queued, ${state.downloadedFiles.size} downloaded`);
	} catch (error) {
		await log('No saved state found, starting fresh', 'WARN');
	}
}

async function saveState() {
	try {
		const data = {
			scrapedSymbols: Array.from(state.scrapedSymbols),
			queuedFiles: Array.from(state.queuedFiles.entries()),
			downloadedFiles: Array.from(state.downloadedFiles),
			failedSymbols: Array.from(state.failedSymbols),
			failedDownloads: Array.from(state.failedDownloads.entries()),
			stats: state.stats,
			lastProgressTime: state.lastProgressTime,
			lastDownloadedCount: state.lastDownloadedCount,
			lastSaved: new Date().toISOString()
		};
		await fs.writeFile(STATE_FILE, JSON.stringify(data, null, 2));
	} catch (error) {
		await log(`Failed to save state: ${error.message}`, 'ERROR');
	}
}

async function fetchXml(url) {
	const controller = new AbortController();
	const timeoutId = setTimeout(() => controller.abort(), WORKER_CONFIG.timeout);
	
	try {
		const res = await fetch(url, {
			headers: {
				'User-Agent': 'Mozilla/5.0 (compatible; DataDiscoveryBot/1.0)'
			},
			signal: controller.signal
		});
		
		clearTimeout(timeoutId);
		
		if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
		return await res.text();
	} catch (error) {
		clearTimeout(timeoutId);
		if (error.name === 'AbortError') {
			throw new Error(`Timeout after ${WORKER_CONFIG.timeout}ms`);
		}
		throw error;
	}
}

function normalizeContents(contents) {
	if (!contents) return [];
	return Array.isArray(contents) ? contents : [contents];
}

// Enhanced HTTP Scraper Worker with comprehensive error handling
async function scraperWorker(workerId) {
	let consecutiveFailures = 0;
	await log(`Scraper ${workerId} started`);
	
	while (!isShuttingDown) {
		try {
			// Get next symbol to process
			const symbolsData = await fs.readFile(SYMBOLS_FILE, 'utf8');
			const allSymbols = JSON.parse(symbolsData);
			const unprocessedSymbols = allSymbols.filter(s => !state.scrapedSymbols.has(s));
			
			if (unprocessedSymbols.length === 0) {
				await log(`Scraper ${workerId}: All symbols processed, waiting...`);
				await new Promise(resolve => setTimeout(resolve, 5000));
				continue;
			}
			
			// Take next symbol
			const symbol = unprocessedSymbols[0];
			await log(`Scraper ${workerId}: Processing ${symbol}`);
			
			// Scrape files for this symbol
			const files = await scrapeSymbolFiles(symbol);
			
			if (files.length > 0) {
				state.queuedFiles.set(symbol, files);
				state.stats.filesQueued += files.length;
				await log(`Scraper ${workerId}: ${symbol} -> ${files.length} files queued`);
				
				// Add to download queue
				files.forEach(file => {
					downloadQueue.push({
						symbol,
						filename: file.filename,
						url: file.url,
						size: file.size,
						queuedAt: Date.now(),
						retries: 0
					});
				});
				
				// Reset failure counter on success
				consecutiveFailures = 0;
			} else {
				state.failedSymbols.add(symbol);
				await log(`Scraper ${workerId}: ${symbol} -> no files found`, 'WARN', { symbol, workerId });
			}
			
			state.scrapedSymbols.add(symbol);
			state.stats.symbolsProcessed++;
			
			// Delay between requests
			await new Promise(resolve => setTimeout(resolve, WORKER_CONFIG.scraperDelay));
			
		} catch (error) {
			consecutiveFailures++;
			state.stats.errors++;
			
			await log(`Scraper ${workerId} error: ${error.message}`, 'ERROR', { 
				workerId, 
				consecutiveFailures, 
				error: error.stack,
				timestamp: Date.now()
			});
			
			// If too many consecutive failures, wait longer
			const delay = consecutiveFailures >= WORKER_CONFIG.maxConsecutiveFailures ? 10000 : 2000;
			await new Promise(resolve => setTimeout(resolve, delay));
			
			// If way too many failures, log critical warning
			if (consecutiveFailures >= WORKER_CONFIG.maxConsecutiveFailures) {
				await log(`Scraper ${workerId}: CRITICAL - ${consecutiveFailures} consecutive failures`, 'CRITICAL', { workerId, consecutiveFailures });
			}
		}
	}
	
	await log(`Scraper ${workerId} stopped`);
}

// Enhanced scrape function with better error context
async function scrapeSymbolFiles(symbol, retryCount = 0) {
	try {
		const prefix = `data/spot/daily/trades/${symbol}/`;
		const files = [];
		let marker = undefined;
		let page = 0;
		
		while (true) {
			page++;
			const params = new URLSearchParams({ prefix, 'max-keys': '1000' });
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
			
			// Set next marker
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
		if (retryCount < WORKER_CONFIG.maxRetries) {
			await log(`Scraping ${symbol} failed, retrying (${retryCount + 1}/${WORKER_CONFIG.maxRetries}): ${error.message}`, 'WARN', { 
				symbol, 
				retryCount, 
				error: error.stack,
				attempt: retryCount + 1
			});
			await new Promise(resolve => setTimeout(resolve, WORKER_CONFIG.retryDelay * (retryCount + 1))); // Exponential backoff
			return scrapeSymbolFiles(symbol, retryCount + 1);
		} else {
			const finalError = `Failed to scrape ${symbol} after ${WORKER_CONFIG.maxRetries} retries: ${error.message}`;
			await log(finalError, 'ERROR', { symbol, retryCount, finalError: error.stack });
			throw new Error(finalError);
		}
	}
}

// Enhanced download function with comprehensive error handling
async function downloadFile(file) {
	const symbolDir = path.join(DOWNLOADS_DIR, file.symbol);
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
		await log(`Starting download: ${file.filename}`, 'INFO', { 
			filename: file.filename, 
			url: file.url, 
			expectedSize: file.size 
		});
		
		const response = await fetch(file.url, {
			headers: {
				'User-Agent': 'Mozilla/5.0 (compatible; DataDiscoveryBot/1.0)'
			},
			signal: controller.signal
		});
		
		clearTimeout(timeoutId);
		
		if (!response.ok) {
			throw new Error(`HTTP ${response.status} for ${file.url}`);
		}
		
		const contentLength = response.headers.get('content-length');
		const expectedSize = contentLength ? parseInt(contentLength, 10) : file.size;
		
		// Stream download to file
		const fileStream = createWriteStream(filePath);
		const reader = response.body.getReader();
		let downloadedBytes = 0;
		
		try {
			while (true) {
				const { done, value } = await reader.read();
				if (done) break;
				
				fileStream.write(Buffer.from(value));
				downloadedBytes += value.length;
				
				// Verify we're not exceeding expected size
				if (expectedSize && downloadedBytes > expectedSize) {
					throw new Error(`Download size mismatch: expected ${expectedSize}, got ${downloadedBytes}`);
				}
			}
			
			fileStream.end();
			await new Promise((resolve, reject) => {
				fileStream.on('finish', resolve);
				fileStream.on('error', reject);
			});
			
			// Verify final file size
			const finalStats = await fs.stat(filePath);
			if (expectedSize && finalStats.size !== expectedSize) {
				throw new Error(`Final size mismatch: expected ${expectedSize}, got ${finalStats.size}`);
			}
			
			state.stats.bytesDownloaded += finalStats.size;
			await log(`Successfully downloaded ${file.filename}`, 'INFO', { 
				filename: file.filename, 
				symbol: file.symbol,
				bytes: finalStats.size,
				path: filePath
			});
			return true;
			
		} catch (error) {
			fileStream.end();
			await fs.unlink(filePath).catch(() => {}); // Clean up partial file
			throw error;
		}
		
	} catch (error) {
		clearTimeout(timeoutId);
		await log(`Download failed: ${file.filename}`, 'ERROR', { 
			filename: file.filename, 
			url: file.url, 
			error: error.message,
			stack: error.stack,
			symbol: file.symbol
		});
		throw error;
	}
}

// Enhanced downloader worker with better error handling
async function downloaderWorker(workerId) {
	let consecutiveFailures = 0;
	await log(`Downloader ${workerId} started`);
	
	while (!isShuttingDown) {
		try {
			// Get next file from queue (prioritize retries)
			let file = null;
			if (retryQueue.length > 0) {
				file = retryQueue.shift();
			} else if (downloadQueue.length > 0) {
				file = downloadQueue.shift();
			}
			
			if (!file) {
				await new Promise(resolve => setTimeout(resolve, 1000));
				continue;
			}
			
			await log(`Downloader ${workerId}: Downloading ${file.filename} (${file.symbol})`, 'INFO', { 
				workerId, 
				filename: file.filename, 
				symbol: file.symbol,
				retries: file.retries 
			});
			
			try {
				// Attempt download
				const success = await downloadFile(file);
				
				if (success) {
					state.downloadedFiles.add(`${file.symbol}/${file.filename}`);
					state.stats.filesDownloaded++;
					
					// Remove from failed downloads if it was there
					state.failedDownloads.delete(file.url);
					
					// Reset failure counter on success
					consecutiveFailures = 0;
				}
				
			} catch (error) {
				file.retries++;
				state.stats.errors++;
				consecutiveFailures++;
				
				await log(`Download attempt failed for ${file.filename}`, 'WARN', { 
					filename: file.filename, 
					symbol: file.symbol,
					retries: file.retries,
					error: error.message,
					consecutiveFailures
				});
				
				if (file.retries < WORKER_CONFIG.maxRetries) {
					// Add to retry queue with exponential backoff
					const backoffDelay = WORKER_CONFIG.retryDelay * Math.pow(2, file.retries - 1);
					await log(`Will retry ${file.filename} in ${backoffDelay}ms`, 'INFO', { 
						filename: file.filename, 
						retryDelay: backoffDelay,
						attempt: file.retries + 1
					});
					
					// Add to retry queue with delay
					setTimeout(() => {
						retryQueue.push(file);
					}, backoffDelay);
					
					state.stats.retries++;
				} else {
					// Max retries exceeded
					await log(`Download failed for ${file.filename} after ${WORKER_CONFIG.maxRetries} retries`, 'ERROR', { 
						filename: file.filename, 
						symbol: file.symbol,
						finalError: error.message,
						totalRetries: file.retries
					});
					
					state.failedDownloads.set(file.url, {
						retries: file.retries,
						lastError: error.message,
						timestamp: Date.now(),
						filename: file.filename,
						symbol: file.symbol
					});
				}
			}
			
			// Delay between downloads
			await new Promise(resolve => setTimeout(resolve, WORKER_CONFIG.downloaderDelay));
			
		} catch (error) {
			consecutiveFailures++;
			state.stats.errors++;
			
			await log(`Downloader ${workerId} critical error: ${error.message}`, 'ERROR', { 
				workerId, 
				error: error.stack,
				consecutiveFailures,
				timestamp: Date.now()
			});
			
			// If too many consecutive failures, wait longer
			const delay = consecutiveFailures >= WORKER_CONFIG.maxConsecutiveFailures ? 10000 : 1000;
			await new Promise(resolve => setTimeout(resolve, delay));
			
			if (consecutiveFailures >= WORKER_CONFIG.maxConsecutiveFailures) {
				await log(`Downloader ${workerId}: CRITICAL - ${consecutiveFailures} consecutive failures`, 'CRITICAL', { 
					workerId, 
					consecutiveFailures 
				});
			}
		}
	}
	
	await log(`Downloader ${workerId} stopped`);
}

// Health check function to detect silent failures
async function healthChecker() {
	await log('Health checker started');
	
	while (!isShuttingDown) {
		try {
			// Check if workers are making progress
			const now = Date.now();
			const lastProgress = state.lastProgressTime || state.startTime;
			const timeSinceProgress = now - lastProgress;
			
			// If no progress for 5 minutes, log warning
			if (timeSinceProgress > 5 * 60 * 1000) {
				await log('WARNING: No progress detected for 5+ minutes', 'WARN', { 
					timeSinceProgress: Math.round(timeSinceProgress / 1000),
					lastProgress: new Date(lastProgress).toISOString()
				});
			}
			
			// Check queue health
			const queueSize = downloadQueue.length;
			const retrySize = retryQueue.length;
			const totalQueued = Array.from(state.queuedFiles.values()).reduce((sum, files) => sum + files.length, 0);
			
			if (queueSize > 100000) {
				await log('WARNING: Download queue very large', 'WARN', { 
					queueSize, 
					retrySize, 
					totalQueued 
				});
			}
			
			// Check error rate
			const uptime = now - state.startTime;
			const errorRate = state.stats.errors / (uptime / 1000); // errors per second
			
			if (errorRate > 0.1) { // More than 0.1 errors per second
				await log('WARNING: High error rate detected', 'WARN', { 
					errorRate: errorRate.toFixed(3),
					totalErrors: state.stats.errors,
					uptime: Math.round(uptime / 1000)
				});
			}
			
			// Update progress time if we're making progress
			if (state.stats.filesDownloaded > (state.lastDownloadedCount || 0)) {
				state.lastProgressTime = now;
				state.lastDownloadedCount = state.stats.filesDownloaded;
			}
			
		} catch (error) {
			await log(`Health checker error: ${error.message}`, 'ERROR', { error: error.stack });
		}
		
		await new Promise(resolve => setTimeout(resolve, WORKER_CONFIG.healthCheckInterval));
	}
	
	await log('Health checker stopped');
}

// Enhanced status reporting
async function statusReporter() {
	while (!isShuttingDown) {
		const uptime = Date.now() - state.startTime;
		const queueSize = downloadQueue.length;
		const retrySize = retryQueue.length;
		const totalQueued = Array.from(state.queuedFiles.values()).reduce((sum, files) => sum + files.length, 0);
		const failedCount = state.failedDownloads.size;
		
		const mbDownloaded = Math.round(state.stats.bytesDownloaded / (1024 * 1024));
		const mbPerSecond = Math.round(mbDownloaded / (uptime / 1000));
		
		console.log(`\n📊 PRODUCTION STATUS REPORT (${new Date().toLocaleTimeString()})`);
		console.log(`   ⏱️  Uptime: ${Math.round(uptime/1000)}s`);
		console.log(`   🔍 Symbols processed: ${state.stats.symbolsProcessed}/${state.scrapedSymbols.size}`);
		console.log(`   📁 Files queued: ${state.stats.filesQueued} (${queueSize} in queue, ${retrySize} retries)`);
		console.log(`   ⬇️  Files downloaded: ${state.stats.filesDownloaded}`);
		console.log(`   💾 Data downloaded: ${mbDownloaded} MB (${mbPerSecond} MB/s)`);
		console.log(`   ❌ Errors: ${state.stats.errors}, Retries: ${state.stats.retries}`);
		console.log(`   🚫 Failed symbols: ${state.failedSymbols.size}, Failed downloads: ${failedCount}`);
		
		await new Promise(resolve => setTimeout(resolve, WORKER_CONFIG.progressInterval));
		await saveState();
	}
}

// Main function
async function main() {
	await log('🚀 Starting KuCoin Data Discovery Worker System - PRODUCTION VERSION');
	
	// Ensure directories exist
	await ensureDir(INDEXED_DIR);
	await ensureDir(DOWNLOADS_DIR);
	
	// Load previous state
	await loadState();
	
	// Start workers
	const workers = [];
	
	// Start scrapers
	for (let i = 1; i <= WORKER_CONFIG.scrapers; i++) {
		workers.push(scraperWorker(i));
	}
	
	// Start downloaders
	for (let i = 1; i <= WORKER_CONFIG.downloaders; i++) {
		workers.push(downloaderWorker(i));
	}
	
	// Start status reporter
	workers.push(statusReporter());
	
	// Start health checker
	workers.push(healthChecker());
	
	// Handle shutdown gracefully
	process.on('SIGINT', async () => {
		await log('🛑 Shutting down gracefully...');
		isShuttingDown = true;
		await saveState();
		await log('✅ Shutdown complete');
		process.exit(0);
	});
	
	process.on('SIGTERM', async () => {
		await log('🛑 Received SIGTERM, shutting down...');
		isShuttingDown = true;
		await saveState();
		await log('✅ Shutdown complete');
		process.exit(0);
	});
	
	// Wait for all workers
	await Promise.all(workers);
}

// Run the production system
main().catch(async (error) => {
	await log(`💥 Fatal error: ${error.message}`, 'FATAL');
	await saveState();
	process.exit(1);
});
