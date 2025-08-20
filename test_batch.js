#!/usr/bin/env node

console.log('Test script starting...');
console.log('Arguments:', process.argv);

import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

console.log('Current directory:', __dirname);

try {
    const symbolsPath = path.join(__dirname, 'symbols.json');
    console.log('Symbols path:', symbolsPath);
    
    const symbolsData = await fs.readFile(symbolsPath, 'utf8');
    console.log('File read successfully, length:', symbolsData.length);
    
    const symbolsFile = JSON.parse(symbolsData);
    console.log('JSON parsed successfully');
    console.log('Keys:', Object.keys(symbolsFile));
    
    const symbols = symbolsFile.symbols || symbolsFile;
    console.log('Symbols array length:', symbols.length);
    console.log('First 5 symbols:', symbols.slice(0, 5));
    
} catch (error) {
    console.error('Error:', error.message);
}
