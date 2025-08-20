#!/usr/bin/env node
/**
 * Test XML Parsing - Debug the XML response structure
 */

import https from 'https';

async function testXmlParsing(symbol) {
    console.log('🔍 Testing XML parsing for file discovery...\n');
    
    const url = `https://historical-data.kucoin.com/?prefix=data/spot/daily/trades/${symbol}/&max-keys=1000`;
    console.log(`📡 Testing: ${url}`);
    
    try {
        const xml = await fetchXML(url);
        console.log(`✅ Got XML response: ${xml.length} characters`);
        
        // Check XML structure
        console.log('\n📋 XML Structure Analysis:');
        console.log(`   Contains <?xml: ${xml.includes('<?xml')}`);
        console.log(`   Contains ListBucketResult: ${xml.includes('<ListBucketResult')}`);
        console.log(`   Contains IsTruncated: ${xml.includes('<IsTruncated>')}`);
        console.log(`   Contains NextMarker: ${xml.includes('<NextMarker>')}`);
        
        // Look for file entries
        console.log('\n📁 File Entry Analysis:');
        const keyMatches = xml.match(/<Key>([^<]+)<\/Key>/g);
        const sizeMatches = xml.match(/<Size>([^<]+)<\/Size>/g);
        const lastModifiedMatches = xml.match(/<LastModified>([^<]+)<\/LastModified>/g);
        
        console.log(`   Key matches: ${keyMatches ? keyMatches.length : 0}`);
        console.log(`   Size matches: ${sizeMatches ? sizeMatches.length : 0}`);
        console.log(`   LastModified matches: ${lastModifiedMatches ? lastModifiedMatches.length : 0}`);
        
        if (keyMatches) {
            console.log('\n📄 Sample Keys:');
            keyMatches.slice(0, 5).forEach((key, index) => {
                const keyValue = key.replace(/<Key>([^<]+)<\/Key>/, '$1');
                const size = sizeMatches && sizeMatches[index] 
                    ? sizeMatches[index].replace(/<Size>([^<]+)<\/Size>/, '$1')
                    : 'N/A';
                console.log(`   ${index + 1}. ${keyValue} (${size} bytes)`);
            });
        }
        
        // Check for zip files specifically
        const zipKeys = keyMatches ? keyMatches.filter(k => k.includes('.zip')) : [];
        console.log(`\n🗜️  ZIP Files: ${zipKeys.length} found`);
        
        if (zipKeys.length > 0) {
            console.log('   Sample ZIP files:');
            zipKeys.slice(0, 3).forEach((key, index) => {
                const keyValue = key.replace(/<Key>([^<]+)<\/Key>/, '$1');
                console.log(`   ${index + 1}. ${keyValue}`);
            });
        }
        
        // Check pagination info
        console.log('\n📖 Pagination Info:');
        const isTruncated = xml.includes('<IsTruncated>true</IsTruncated>');
        console.log(`   IsTruncated: ${isTruncated}`);
        
        if (isTruncated) {
            const nextMarkerMatch = xml.match(/<NextMarker>([^<]+)<\/NextMarker>/);
            if (nextMarkerMatch) {
                console.log(`   NextMarker: ${nextMarkerMatch[1]}`);
            } else {
                console.log(`   NextMarker: Not found (will use last key)`);
            }
        }
        
    } catch (error) {
        console.error(`❌ Error: ${error.message}`);
    }
}

function fetchXML(url) {
    return new Promise((resolve, reject) => {
        const request = https.get(url, { timeout: 15000 }, (response) => {
            if (response.statusCode !== 200) {
                reject(new Error(`HTTP ${response.statusCode}: ${response.statusMessage}`));
                return;
            }
            
            let data = '';
            response.on('data', chunk => data += chunk);
            response.on('end', () => resolve(data));
        });
        
        request.on('error', reject);
        request.on('timeout', () => {
            request.destroy();
            reject(new Error('Request timeout'));
        });
    });
}

async function main() {
    const symbol = 'BTCUSDT';
    await testXmlParsing(symbol);
    console.log('\n✅ XML parsing test completed!');
}

main().catch(console.error);
