#!/usr/bin/env node
/**
 * Test XML Endpoint - Check if KuCoin has an XML endpoint for file listings
 */

import https from 'https';

async function testXmlEndpoint(symbol) {
    console.log('🔍 Testing XML endpoints for file discovery...\n');
    
    // Test potential XML endpoints
    const xmlEndpoints = [
        `https://historical-data.kucoin.com/?prefix=data/spot/daily/trades/${symbol}/&max-keys=1000`,
        `https://historical-data.kucoin.com/?list-type=2&prefix=data/spot/daily/trades/${symbol}/`,
        `https://historical-data.kucoin.com/?delimiter=/&prefix=data/spot/daily/trades/${symbol}/`,
        `https://historical-data.kucoin.com/data/spot/daily/trades/${symbol}/?list-type=2`,
        `https://historical-data.kucoin.com/data/spot/daily/trades/${symbol}/?prefix=&max-keys=1000`
    ];
    
    for (const url of xmlEndpoints) {
        console.log(`📡 Testing: ${url}`);
        
        try {
            const result = await testUrl(url);
            if (result.success) {
                console.log(`   ✅ Status: ${result.status}`);
                console.log(`   📊 Content length: ${result.dataLength}`);
                
                // Check if it's XML
                if (result.data.includes('<?xml') || result.data.includes('<ListBucketResult')) {
                    console.log(`   🗂️  XML detected!`);
                    
                    // Look for file entries
                    const keyMatches = result.data.match(/<Key>([^<]+)<\/Key>/g);
                    if (keyMatches) {
                        console.log(`   📁 Found ${keyMatches.length} file keys`);
                        const keys = keyMatches.map(m => m.replace(/<Key>([^<]+)<\/Key>/, '$1'));
                        console.log(`   📄 Sample keys: ${keys.slice(0, 3).join(', ')}`);
                    }
                } else {
                    console.log(`   📄 Not XML content`);
                }
            } else {
                console.log(`   ❌ Failed: ${result.error}`);
            }
        } catch (error) {
            console.log(`   💥 Error: ${error.message}`);
        }
        console.log('');
    }
}

function testUrl(url) {
    return new Promise((resolve) => {
        const request = https.get(url, { timeout: 10000 }, (response) => {
            let data = '';
            
            response.on('data', (chunk) => {
                data += chunk;
            });
            
            response.on('end', () => {
                resolve({
                    success: true,
                    status: response.statusCode,
                    dataLength: data.length,
                    data: data
                });
            });
        });
        
        request.on('error', (error) => {
            resolve({
                success: false,
                status: 0,
                error: error.message
            });
        });
        
        request.on('timeout', () => {
            request.destroy();
            resolve({
                success: false,
                status: 0,
                error: 'Request timeout'
            });
        });
    });
}

async function main() {
    const symbol = 'BTCUSDT';
    await testXmlEndpoint(symbol);
    console.log('✅ XML endpoint testing completed!');
}

main().catch(console.error);
