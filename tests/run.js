// Kafka Pilot browser tests. Run with `npm test`. Exits non-zero when any check fails.
// The page is wrapped the way the artifact host wraps it, then driven in headless Chromium.
const { chromium } = require('playwright-core');
const fs = require('fs'), path = require('path');

const ROOT = path.join(__dirname, '..'), OUT = path.join(__dirname, 'out');
fs.mkdirSync(OUT, { recursive: true });
const PAGE = path.join(OUT, 'page.html');
fs.writeFileSync(PAGE, '<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"></head><body>'
  + fs.readFileSync(path.join(ROOT, 'index.html'), 'utf8') + '</body></html>');
const only = process.argv[2];

const VIEWPORTS = [['desktop-dark', 1440, 960, 'dark'], ['desktop-light', 1440, 960, 'light'], ['tablet', 900, 1100, 'dark'], ['phone', 390, 844, 'light']];
// murmur2 vectors from Kafka's own UtilsTest
const MURMUR2 = { '21': -973932308, foobar: -790332482, 'a-little-bit-long-string': -985981536, 'a-little-bit-longer-string': -1486304829,
  lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8: -58897971, abc: 479470107 };

let browser;
async function open(opts = {}) {
  const ctx = await browser.newContext({ viewport: { width: opts.w || 1440, height: opts.h || 960 }, colorScheme: opts.scheme || 'light', reducedMotion: opts.reduced ? 'reduce' : 'no-preference' });
  await ctx.addInitScript(() => { window.__KP_TEST__ = true; });
  const page = await ctx.newPage();
  page.errors = [];
  page.on('pageerror', e => page.errors.push(e.message));
  page.on('console', m => { if (m.type() === 'error') page.errors.push(m.text()); });
  await page.goto('file://' + PAGE);
  await page.waitForTimeout(opts.settle ?? 400);
  return page;
}
const assert = (ok, msg) => { if (!ok) throw new Error(msg); };
const noErrors = page => assert(!page.errors.length, 'page errors: ' + page.errors.join(' | '));

const tests = [];
const test = (name, fn) => tests.push({ name, fn });

test('every step renders without errors or sideways scrolling', async () => {
  for (const [name, w, h, scheme] of VIEWPORTS) {
    const page = await open({ w, h, scheme });
    for (const st of [0, 1, 2, 3, 4]) {
      await page.click(`.steps button[data-goto="${st}"]`); await page.waitForTimeout(750);
      const ovf = await page.evaluate(() => document.documentElement.scrollWidth - innerWidth);
      assert(ovf <= 0, `${name} step ${st}: page scrolls sideways by ${ovf}px`);
      const rail = await page.evaluate(() => { const r = document.getElementById('rail').getBoundingClientRect(), b = document.querySelector('.steps button[aria-current="step"]').getBoundingClientRect(); return Math.abs(r.left - b.left) + Math.abs(r.top - b.top); });
      assert(rail < 2, `${name} step ${st}: step highlight is ${rail}px off its button`);
    }
    noErrors(page); await page.context().close();
  }
});

test('every kind of data answers every question', async () => {
  const page = await open();
  const goals = await page.$$eval('input[name="goal"]', a => a.map(i => i.value));
  assert(goals.length >= 9, `expected at least 9 kinds of data, found ${goals.length}`);
  for (const g of goals) {
    await page.click('.steps button[data-goto="0"]'); await page.click(`input[name="goal"][value="${g}"]`, { force: true });
    for (const st of [1, 2, 3]) {
      await page.click(`.steps button[data-goto="${st}"]`); await page.waitForTimeout(80);
      const open = await page.$$eval('fieldset.q', f => f.filter(x => x.querySelector('input[type=radio]') && !x.querySelector('input:checked')).map(x => x.querySelector('legend').textContent.trim()));
      assert(!open.length, `${g}: unanswered on step ${st}: ${open.join('; ')}`);
    }
  }
  noErrors(page); await page.context().close();
});

test('partitioner matches Kafka murmur2 and modulo', async () => {
  const page = await open();
  for (const [key, h] of Object.entries(MURMUR2)) {
    const expected = (h & 0x7fffffff) % 3;
    const got = await page.evaluate(k => window.__kp.partitionFor(k), key);
    assert(got === expected, `${key}: partition ${got}, Kafka gives ${expected}`);
  }
  await page.click('.partbtn'); await page.waitForTimeout(400);
  await page.fill('#hash-try', 'foobar');
  const shown = await page.textContent('#hash-out');
  assert(shown.includes(((MURMUR2.foobar & 0x7fffffff)).toLocaleString('en-US')), `hash card shows the wrong hash: ${shown}`);
  noErrors(page); await page.context().close();
});

test('every button, input and select has a tooltip', async () => {
  const page = await open();
  const missing = new Set();
  for (const st of [0, 1, 2, 3, 4]) {
    await page.click(`.steps button[data-goto="${st}"]`); await page.waitForTimeout(150);
    (await page.evaluate(() => [...document.querySelectorAll('button, input, select, textarea, [role="button"]')]
      .filter(el => el.getClientRects().length && !el.closest('[hidden]') && el.type !== 'hidden')
      .filter(el => { const t = el.closest('[data-tip]'); return !t || !window.__kp.tipText(t.dataset.tip); })
      .map(el => (el.id || el.getAttribute('aria-label') || el.name || el.textContent.trim().slice(0, 30) || el.tagName))))
      .forEach(x => missing.add(`step ${st}: ${x}`));
  }
  assert(!missing.size, `${missing.size} controls without a tooltip: ${[...missing].slice(0, 12).join(' | ')}${missing.size > 12 ? ' …' : ''}`);
  await page.context().close();
});

test('every diagram tooltip key has text', async () => {
  const page = await open();
  await page.click('#pb-groups-inc'); await page.click('#pb-groups-inc'); await page.waitForTimeout(200);
  const bad = await page.evaluate(() => [...new Set([...document.querySelectorAll('[data-tip]')].map(e => e.dataset.tip))].filter(k => !window.__kp.tipText(k)));
  assert(!bad.length, 'tooltip keys without text: ' + bad.join(', '));
  noErrors(page); await page.context().close();
});

test('lag grows under load and the suggested fixes drain it', async () => {
  const page = await open({ settle: 2600 });
  const trend = () => page.evaluate(() => window.__kp.lagTrend(0));
  await page.waitForTimeout(6000);
  assert(await trend() !== 'growing', 'lag grows with the starting setup');
  for (let i = 0; i < 4; i++) await page.click('#pb-producers-inc');
  await page.waitForTimeout(9000);
  assert(await trend() === 'growing', `lag should grow with 5 producers and 1 consumer, trend is ${await trend()}`);
  for (let i = 0; i < 8 && await trend() === 'growing'; i++) {
    const ids = await page.$$eval('[data-lagfix]:not([disabled])', a => a.map(x => x.dataset.lagfix)); if (!ids.length) break;
    await page.click(`[data-lagfix="${ids.find(x => x.startsWith('fast')) && i >= 2 ? ids.find(x => x.startsWith('fast')) : ids[0]}"]`);
    await page.waitForTimeout(5500);
  }
  await page.waitForTimeout(4000);
  assert(await trend() !== 'growing', 'lag still growing after applying the suggested fixes');
  noErrors(page); await page.context().close();
});

test('consumer groups: add, configure in the diagram, highlight, remove', async () => {
  const page = await open();
  await page.click('#pb-groups-inc'); await page.click('#pb-groups-inc'); await page.waitForTimeout(150);
  assert((await page.evaluate(() => window.__kp.S().more.length)) === 2, 'two extra groups expected');
  await page.click('[data-gctl="cons:1:1"]'); await page.click('[data-gctl="speed:2"]');
  const more = await page.evaluate(() => window.__kp.S().more);
  assert(more[0].consumers === 2 && more[1].speed === 'slow', 'diagram group controls did not apply: ' + JSON.stringify(more));
  await page.click('.gname[data-g="1"]');
  assert(await page.evaluate(() => document.getElementById('map').dataset.gf) === '1', 'group highlight did not turn on');
  await page.click('#pb-groups-dec'); await page.click('#pb-groups-dec'); await page.waitForTimeout(150);
  assert((await page.evaluate(() => window.__kp.S().more.length)) === 0, 'groups were not removed');
  noErrors(page); await page.context().close();
});

test('a broker switched off refuses writes when too few copies remain', async () => {
  const page = await open();
  await page.selectOption('#pb-goal', 'balanced'); await page.waitForTimeout(150);
  await page.click('.bhead[data-b="1"]'); await page.click('.bhead[data-b="2"]'); await page.waitForTimeout(150);
  const refused = await page.evaluate(() => window.__kp.parts().filter(p => !p.writable).length);
  assert(refused > 0, 'with 2 of 3 brokers off and min.insync.replicas=2, some partitions must refuse writes');
  assert((await page.textContent('#status')).includes('refuse'), 'status text does not explain the refused writes');
  noErrors(page); await page.context().close();
});

test('partition assignment follows Kafka assignors, and sticky moves the fewest', async () => {
  const page = await open();
  const own = (P, C, st, prev) => page.evaluate(a => window.__kp.assignOwners(...a), [P, C, st, prev]);
  const eq = (a, b, msg) => assert(JSON.stringify(a) === JSON.stringify(b), `${msg}: got ${JSON.stringify(a)}, expected ${JSON.stringify(b)}`);
  // RangeAssignor: contiguous blocks, the first P % C consumers get one more
  eq(await own(3, 2, 'range'), [0, 0, 1], 'range 3 partitions / 2 consumers');
  eq(await own(6, 4, 'range'), [0, 0, 1, 1, 2, 3], 'range 6/4');
  eq(await own(6, 4, 'roundrobin'), [0, 1, 2, 3, 0, 1], 'round robin 6/4');
  const before = await own(6, 2, 'range'), range3 = await own(6, 3, 'range'), sticky3 = await own(6, 3, 'sticky', before);
  const moves = a => a.filter((j, p) => j !== before[p]).length;
  assert(moves(sticky3) < moves(range3), `sticky moved ${moves(sticky3)} partitions, range ${moves(range3)}`);
  const load = a => [0, 1, 2].map(j => a.filter(x => x === j).length);
  assert(Math.max(...load(sticky3)) - Math.min(...load(sticky3)) <= 1, 'sticky result is unbalanced: ' + JSON.stringify(sticky3));
  // a rebalance shows in the diagram and the files name the strategy
  await page.click('#pb-consumers-inc'); await page.waitForTimeout(100);
  assert(await page.$('.rbadge'), 'no rebalancing badge after adding a consumer');
  await page.waitForTimeout(2200);
  assert(!(await page.$('.rbadge')), 'rebalancing badge did not clear');
  await page.click('.steps button[data-goto="3"]'); await page.click('input[name="assign"][value="sticky"]', { force: true });
  await page.click('.steps button[data-goto="4"]'); await page.waitForTimeout(150);
  const files = (await page.$$eval('.file pre', p => p.map(x => x.textContent))).join('\n');
  assert(files.includes('partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor'), 'consumer.properties lacks the strategy');
  noErrors(page); await page.context().close();
});

test('a shared link or code restores the same setup', async () => {
  const page = await open();
  await page.click('#pb-producers-inc'); await page.click('#pb-partitions-inc'); await page.click('#pb-groups-inc');
  await page.click('.steps button[data-goto="3"]'); await page.click('input[name="assign"][value="sticky"]', { force: true });
  await page.click('#share');
  const code = await page.inputValue('#share-code'), link = await page.inputValue('#share-link');
  assert(/^KP1\.[A-Za-z0-9_-]+$/.test(code) && link.includes('#s=' + code), 'share code or link malformed');
  const want = await page.evaluate(() => { const s = window.__kp.S(); return JSON.stringify([s.producers, s.partitions, s.assign, s.more.length]); });
  await page.context().close();
  // fresh browser opening the link
  const ctx = await browser.newContext(); await ctx.addInitScript(() => { window.__KP_TEST__ = true; });
  const p2 = await ctx.newPage(); p2.errors = []; p2.on('pageerror', e => p2.errors.push(e.message));
  await p2.goto('file://' + PAGE + '#s=' + code); await p2.waitForTimeout(400);
  const got = await p2.evaluate(() => { const s = window.__kp.S(); return JSON.stringify([s.producers, s.partitions, s.assign, s.more.length]); });
  assert(got === want, `link restored ${got}, expected ${want}`);
  assert(!(await p2.evaluate(() => location.hash)).includes('s='), 'the code should be removed from the address after loading');
  noErrors(p2); await ctx.close();
  // fresh browser pasting the code into Import
  const p3 = await open();
  await p3.fill('#importText', 'here is my setup: ' + code); await p3.click('[data-act="import"]'); await p3.waitForTimeout(150);
  const got3 = await p3.evaluate(() => { const s = window.__kp.S(); return JSON.stringify([s.producers, s.partitions, s.assign, s.more.length]); });
  assert(got3 === want, `import restored ${got3}, expected ${want}`);
  assert((await p3.textContent('#import-report')).includes('shared setup'), 'import report does not mention the shared setup');
  // a broken code is ignored, not crashing
  await p3.fill('#importText', 'KP1.!!!not-a-code'); await p3.click('[data-act="import"]');
  noErrors(p3); await p3.context().close();
});

test('every challenge can be solved with the controls', async () => {
  const page = await open({ settle: 2600 });
  const start = async id => { if (await page.$('#chal[hidden]')) await page.click('#chal-btn'); if (await page.$('[data-chal-act="list"]')) await page.click('[data-chal-act="list"]'); await page.click(`.chal-pick[data-chal="${id}"]`); await page.waitForTimeout(150); };
  const solved = async id => { await page.waitForTimeout(300); assert(await page.$('.chal-win'), `challenge "${id}" not marked solved; open checks: ` + (await page.$$eval('.chal-checks li:not(.ok)', l => l.map(x => x.textContent).join(' | ')))); };
  const tile = (k, v) => page.click(`input[name="${k}"][value="${v}"]`, { force: true });
  const step = n => page.click(`.steps button[data-goto="${n}"]`);

  await start('survive'); assert(!(await page.$('.chal-win')), 'survive: solved before doing anything');
  await page.click('#pb-rf-inc'); await page.click('#pb-rf-inc'); await page.click('.ackseg[data-acks="all"]');
  await step(1); await page.click('#minIsr-inc'); await page.click('.bhead[data-b="0"]'); await solved('survive');

  await start('order'); await step(2);
  await page.fill('#newKey', 'customer-9'); await page.click('[data-act="addkey"]');
  await tile('acks', 'all'); if (!(await page.isChecked('#idem'))) await page.click('#idem', { force: true });
  await step(3); await tile('onFail', 'retry'); await solved('order');

  await start('replay'); await step(1); await tile('retention', '30'); await page.click('#pb-groups-inc');
  await step(3); await tile('reset', 'earliest'); await solved('replay');

  await start('sticky'); await step(3); await tile('assign', 'sticky');
  await page.click('#pb-consumers-inc'); await page.waitForTimeout(200); await page.click('#pb-consumers-inc'); await solved('sticky');

  await start('lag'); await page.waitForTimeout(6000);
  assert(!(await page.$('.chal-win')), 'lag: solved before fixing anything');
  for (let i = 0; i < 8 && !(await page.$('.chal-win')); i++) {
    const ids = await page.$$eval('[data-lagfix]:not([disabled])', a => a.map(x => x.dataset.lagfix));
    if (ids.length) await page.click(`[data-lagfix="${ids.find(x => x.startsWith('fast')) || ids[0]}"]`);
    await page.waitForTimeout(5000);
  }
  await page.waitForTimeout(9000); await solved('lag');
  noErrors(page); await page.context().close();
});

test('theme switch cycles and is remembered', async () => {
  const page = await open();
  const theme = () => page.evaluate(() => document.documentElement.getAttribute('data-theme') || 'auto');
  await page.click('#theme'); assert(await theme() === 'light', 'auto -> light');
  await page.click('#theme'); assert(await theme() === 'dark', 'light -> dark');
  await page.reload(); await page.waitForTimeout(300);
  assert(await theme() === 'dark', 'dark choice not remembered after reload');
  await page.click('#theme'); assert(await theme() === 'auto', 'dark -> auto');
  noErrors(page); await page.context().close();
});

test('Dark Reader backs off and the page uses its own dark theme', async () => {
  const lib = path.join(ROOT, 'node_modules', 'darkreader', 'darkreader.js');
  if (!fs.existsSync(lib)) return 'skipped (npm install darkreader to run)';
  const dr = fs.readFileSync(lib, 'utf8'), file = path.join(OUT, 'darkreader.html');
  fs.writeFileSync(file, `<!doctype html><html><head><meta charset="utf-8"><script>${dr}</script><script>DarkReader.enable({ brightness: 100, contrast: 90, sepia: 10 });</script></head><body>${fs.readFileSync(path.join(ROOT, 'index.html'), 'utf8')}</body></html>`);
  const page = await (await browser.newContext({ colorScheme: 'light' })).newPage();
  await page.goto('file://' + file); await page.waitForTimeout(1500);
  const r = await page.evaluate(() => ({ styles: document.querySelectorAll('style.darkreader').length, theme: document.documentElement.getAttribute('data-theme') }));
  assert(r.styles === 0, `Dark Reader still injected ${r.styles} stylesheets`);
  assert(r.theme === 'dark', 'page did not switch to its own dark theme');
  await page.context().close();
});

test('reduced motion: no animation, still usable', async () => {
  const page = await open({ reduced: true });
  assert((await page.textContent('#play')).includes('Play'), 'animation should start paused with reduced motion');
  await page.click('.steps button[data-goto="2"]'); await page.click('#pb-producers-inc');
  noErrors(page); await page.context().close();
});

(async () => {
  browser = await chromium.launch();
  let failed = 0;
  for (const t of tests) {
    if (only && !t.name.includes(only)) continue;
    const t0 = Date.now();
    try {
      const note = await t.fn();
      console.log(`  ✓ ${t.name}${note ? ` (${note})` : ''} ${((Date.now() - t0) / 1000).toFixed(1)}s`);
    } catch (e) { failed++; console.log(`  ✗ ${t.name}\n      ${e.message}`); }
  }
  await browser.close();
  console.log(failed ? `\n${failed} failed` : '\nall passed');
  process.exitCode = failed ? 1 : 0;
})();
