#!/usr/bin/env node
/**
 * Simple Test Worker - Debug step by step
 */

import https from 'https';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

console.log('🚀 Simple test worker starting...');

async function testXMLFetch(symbol) {
    console.log(`🔍 Testing XML fetch for ${symbol}...`);
    
    const url = `https://historical-data.kucoin.com/?prefix=data/spot/daily/trades/${symbol}/&max-keys=1000`;
    console.log(`📡 URL: ${url}`);
    
    try {
        const xml = await fetchXML(url);
        console.log(`✅ Got XML: ${xml.length} characters`);
        
        // Look for zip files
        const keyMatches = xml.match(/<Key>([^<]+)<\/Key>/g);
        console.log(`📁 Found ${keyMatches ? keyMatches.length : 0} keys`);
        
        if (keyMatches) {
            const zipKeys = keyMatches.filter(k => k.includes('.zip'));
            console.log(`🗜️  Found ${zipKeys.length} ZIP files`);
            
            if (zipKeys.length > 0) {
                console.log('📄 Sample ZIP files:');
                zipKeys.slice(0, 3).forEach((key, index) => {
                    const keyValue = key.replace(/<Key>([^<]+)<\/Key>/, '$1');
                    console.log(`   ${index + 1}. ${keyValue}`);
                });
            }
        }
        
        return keyMatches ? keyMatches.filter(k => k.includes('.zip')).length : 0;
        
    } catch (error) {
        console.error(`❌ Error: ${error.message}`);
        throw error;
    }
}

function fetchXML(url) {
    return new Promise((resolve, reject) => {
        console.log(`🌐 Fetching: ${url}`);
        
        const request = https.get(url, { timeout: 15000 }, (response) => {
            console.log(`📊 Response status: ${response.statusCode}`);
            
            if (response.statusCode !== 200) {
                reject(new Error(`HTTP ${response.statusCode}: ${response.statusMessage}`));
                return;
            }
            
            let data = '';
            response.on('data', chunk => {
                data += chunk;
                console.log(`📥 Received chunk: ${chunk.length} bytes`);
            });
            
            response.on('end', () => {
                console.log(`✅ Fetch complete: ${data.length} total bytes`);
                resolve(data);
            });
        });
        
        request.on('error', (error) => {
            console.error(`💥 Request error: ${error.message}`);
            reject(error);
        });
        
        request.on('timeout', () => {
            console.error(`⏰ Request timeout`);
            request.destroy();
            reject(new Error('Request timeout'));
        });
    });
}

async function main() {
    try {
        console.log('🎯 Starting main function...');
        
        const symbol = process.argv[2] || 'BTCUSDT';
        console.log(`🎯 Processing symbol: ${symbol}`);
        
        const fileCount = await testXMLFetch(symbol);
        console.log(`🎉 Success! Found ${fileCount} ZIP files for ${symbol}`);
        
    } catch (error) {
        console.error(`💥 Main function failed: ${error.message}`);
        console.error(error.stack);
        process.exit(1);
    }
}

console.log('📋 About to call main()...');
main().then(() => {
    console.log('✅ Main completed successfully');
}).catch(error => {
    console.error('💥 Main failed:', error.message);
    process.exit(1);
});
