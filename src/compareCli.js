#!/usr/bin/env node
/**
 * CLI: node src/compareCli.js <anshul.csv> <abhinav.csv> [outputDiff.csv]
 */
const { runCsvCompare } = require('./csvCompare');

const anshul = process.argv[2];
const abhinav = process.argv[3];
const out = process.argv[4];

if (!anshul || !abhinav) {
  console.error('Usage: node src/compareCli.js <anshul.csv> <abhinav.csv> [outputDiff.csv]');
  process.exit(1);
}

const result = runCsvCompare(anshul, abhinav, out);
console.log(JSON.stringify(result, null, 2));
process.exit(result.success ? 0 : 1);
