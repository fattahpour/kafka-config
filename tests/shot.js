// Screenshot helper for visual review: node tests/shot.js <name> [scheme] [width] [height] then steps as JS in argv[6]
const { chromium } = require('playwright-core');
const fs = require('fs'), path = require('path');
const [name, scheme = 'light', w = 1440, h = 1000, script = ''] = process.argv.slice(2);
(async () => {
  const OUT = path.join(__dirname, 'out'); fs.mkdirSync(OUT, { recursive: true });
  const b = await chromium.launch();
  const ctx = await b.newContext({ viewport: { width: +w, height: +h }, colorScheme: scheme });
  await ctx.addInitScript(() => { window.__KP_TEST__ = true; });
  const page = await ctx.newPage(); const errs = []; page.on('pageerror', e => errs.push(e.message));
  await page.goto('file://' + path.join(OUT, 'page.html')); await page.waitForTimeout(2600);
  if (script) await eval(`(async () => { ${script} })()`);
  await page.screenshot({ path: path.join(OUT, name + '.png') });
  console.log(name, errs.length ? errs : 'ok');
  await b.close();
})();
