import fs from 'fs';
import path from 'path';
import solc from 'solc';

function compileCode(source, runs = 1) {
  const input = {
    language: 'Solidity',
    sources: { 'FractalFlowMatrix27.sol': { content: source } },
    settings: {
      viaIR: true,
      optimizer: { enabled: true, runs },
      outputSelection: { '*': { '*': ['evm.deployedBytecode.object'] } },
    },
  };
  const t0 = Date.now();
  const output = JSON.parse(solc.compile(JSON.stringify(input)));
  if (output.errors) {
    for (const err of output.errors) {
      if (err.severity === 'error') {
        console.error('COMPILE ERROR:', err.formattedMessage);
        return null;
      }
    }
  }
  const contract = output.contracts['FractalFlowMatrix27.sol']['FractalFlowMatrix27'];
  const deployed = contract.evm.deployedBytecode.object;
  const size = deployed.length / 2;
  return { size, time: ((Date.now() - t0) / 1000).toFixed(2) };
}

const orig = fs.readFileSync('contracts/FractalFlowMatrix27.sol', 'utf8');
console.log('Original contract length (chars):', orig.length);
