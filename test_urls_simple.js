#!/usr/bin/env node
/**
 * Simple URL Test - Check which KuCoin URLs actually work
 */

import https from 'https';

async function testUrl(url) {
    return new Promise((resolve) => {
        console.log(`📡 Testing: ${url}`);
        
        const request = https.get(url, { timeout: 10000 }, (response) => {
            let data = '';
            
            response.on('data', (chunk) => {
                data += chunk;
            });
            
            response.on('end', () => {
                console.log(`   ✅ Status: ${response.statusCode}`);
                console.log(`   📊 Content length: ${data.length}`);
                if (data.length < 1000) {
                    console.log(`   📄 Content preview: ${data.substring(0, 200)}...`);
                }
                
                // Look for zip files
                const zipMatches = data.match(/href="([^"]*\.zip)"/g);
                if (zipMatches) {
                    console.log(`   🗜️  Found ${zipMatches.length} zip files`);
                    console.log(`   📁 Sample: ${zipMatches.slice(0, 3).join(', ')}`);
                } else {
                    console.log(`   ❌ No zip files found`);
                }
                
                resolve(true);
            });
        });
        
        request.on('error', (error) => {
            console.log(`   ❌ Error: ${error.message}`);
            resolve(false);
        });
        
        request.on('timeout', () => {
            request.destroy();
            console.log(`   ⏰ Timeout`);
            resolve(false);
        });
    });
}

async function main() {
    const symbol = 'BTCUSDT';
    
    console.log('🔍 Testing KuCoin URL patterns...\n');
    
    // Test different URL patterns
    const urlPatterns = [
        `https://historical-data.kucoin.com/data/spot/daily/trades/${symbol}/`,
        `https://historical-data.kucoin.com/index.html?prefix=data/spot/daily/trades/${symbol}/`,
        `https://historical-data.kucoin.com/data/spot/daily/trades/${symbol}/index.html`,
        `https://historical-data.kucoin.com/data/spot/daily/trades/${symbol}/list.html`
    ];
    
    for (const url of urlPatterns) {
        await testUrl(url);
        console.log('');
    }
    
    console.log('✅ Testing completed!');
}

main().catch(console.error);
