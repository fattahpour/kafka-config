// Writes the files Kafka Pilot generates for one setup into a folder, for tests/kafka-smoke.sh
// usage: node tests/kafka/dump.js <outdir> <kind-of-data> [assign]
const { chromium } = require('playwright-core');
const fs = require('fs'), path = require('path');
const [out, goal = 'balanced', assign = 'range', security = 'none', extra = '{}'] = process.argv.slice(2);
(async () => {
  const page_ = path.join(__dirname, '..', 'out', 'page.html');
  fs.mkdirSync(path.dirname(page_), { recursive: true });
  fs.writeFileSync(page_, '<!doctype html><html><head><meta charset="utf-8"></head><body>' + fs.readFileSync(path.join(__dirname, '..', '..', 'index.html'), 'utf8') + '</body></html>');
  const b = await chromium.launch(), ctx = await b.newContext({ reducedMotion: 'reduce' });
  await ctx.addInitScript(() => { window.__KP_TEST__ = true; });
  const page = await ctx.newPage(); await page.goto('file://' + page_);
  await page.selectOption('#pb-goal', goal);
  await page.click('.steps button[data-goto="3"]'); await page.click(`input[name="assign"][value="${assign}"]`, { force: true });
  await page.click('.steps button[data-goto="1"]'); await page.click(`input[name="security"][value="${security}"]`, { force: true });
  // optional state overrides, e.g. '{"racks":true}'
  await page.evaluate(x => Object.assign(window.__kp.S(), x), JSON.parse(extra));
  const files = await page.evaluate(() => window.__kp.files());
  fs.mkdirSync(out, { recursive: true });
  for (const [n, t] of Object.entries(files)) fs.writeFileSync(path.join(out, n), t + '\n');
  // where the page says each key goes, so the smoke test can compare with real Kafka
  const keys = ['customer-42', 'customer-7', 'order-1001', 'abc', 'foobar'];
  fs.writeFileSync(path.join(out, 'expected.json'), JSON.stringify(Object.fromEntries(await page.evaluate(k => k.map(x => [x, window.__kp.partitionFor(x)]), keys))));
  console.log(Object.keys(files).join(' '));
  await b.close();
})();
