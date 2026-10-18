// Builds dist/: a standalone kafka-config.html (the page wrapped in a full HTML document, since outside
// claude.ai there is no host to add one) plus licence notices, zipped as kafka-config-<version>.zip.
const fs = require('fs'), path = require('path'), zlib = require('zlib');
// A minimal zip writer (deflate, no external tools): local headers, data, central directory
const CRC = Array.from({ length: 256 }, (_, n) => { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; return c >>> 0; });
const crc32 = b => { let c = 0xffffffff; for (const x of b) c = CRC[(c ^ x) & 0xff] ^ (c >>> 8); return (c ^ 0xffffffff) >>> 0; };
function writeZip(file, entries) {
  const parts = [], central = []; let off = 0;
  for (const [name, data] of entries) {
    const n = Buffer.from(name), z = zlib.deflateRawSync(data), crc = crc32(data);
    const h = Buffer.alloc(30); h.writeUInt32LE(0x04034b50, 0); h.writeUInt16LE(20, 4); h.writeUInt16LE(8, 8);
    h.writeUInt32LE(crc, 14); h.writeUInt32LE(z.length, 18); h.writeUInt32LE(data.length, 22); h.writeUInt16LE(n.length, 26);
    const c = Buffer.alloc(46); c.writeUInt32LE(0x02014b50, 0); c.writeUInt16LE(20, 4); c.writeUInt16LE(20, 6); c.writeUInt16LE(8, 10);
    c.writeUInt32LE(crc, 16); c.writeUInt32LE(z.length, 20); c.writeUInt32LE(data.length, 24); c.writeUInt16LE(n.length, 28); c.writeUInt32LE(off, 42);
    parts.push(h, n, z); central.push(c, n); off += 30 + n.length + z.length;
  }
  const cd = Buffer.concat(central), e = Buffer.alloc(22);
  e.writeUInt32LE(0x06054b50, 0); e.writeUInt16LE(entries.length, 8); e.writeUInt16LE(entries.length, 10); e.writeUInt32LE(cd.length, 12); e.writeUInt32LE(off, 16);
  fs.writeFileSync(file, Buffer.concat([...parts, cd, e]));
}
const ROOT = path.join(__dirname, '..'), DIST = path.join(ROOT, 'dist');
const version = require(path.join(ROOT, 'package.json')).version;
fs.rmSync(DIST, { recursive: true, force: true }); fs.mkdirSync(DIST);
const page = fs.readFileSync(path.join(ROOT, 'index.html'), 'utf8');
fs.writeFileSync(path.join(DIST, 'kafka-config.html'),
  '<!doctype html>\n<html lang="en">\n<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">'
  + '<style>[hidden]{display:none!important}</style></head>\n<body>\n' + page + '\n</body>\n</html>\n');
for (const f of fs.readdirSync(ROOT).filter(f => f.startsWith('THIRD_PARTY_LICENSE'))) fs.copyFileSync(path.join(ROOT, f), path.join(DIST, f));
const zip = `kafka-config-${version}.zip`;
writeZip(path.join(DIST, zip), fs.readdirSync(DIST).sort().map(f => [f, fs.readFileSync(path.join(DIST, f))]));
console.log(`dist/${zip} (${(fs.statSync(path.join(DIST, zip)).size / 1024).toFixed(0)} KB) and dist/kafka-config.html, version ${version}`);
