// index.js
const fs = require("fs/promises");
const path = require("path");
const { fetchJsonViaBrowser } = require("./lib/browserFetch");

const CONFIG_PATH = path.resolve(__dirname, "dataLocations.json");
const OUT_DIR = path.resolve(__dirname, "dataDownloads");

/** derive a nice file name from the URL's base */
function fileNameFromUrl(u) {
  const { hostname } = new URL(u);
  // strip leading www.
  const base = hostname.replace(/^www\./, "");
  return `${base}.json`;
}

async function main() {
  // ensure output dir exists
  await fs.mkdir(OUT_DIR, { recursive: true });

  // read config
  const cfgRaw = await fs.readFile(CONFIG_PATH, "utf8");
  const cfg = JSON.parse(cfgRaw);

  const defaults = cfg.defaults || {};
  const sources = Array.isArray(cfg.sources) ? cfg.sources : [];

  for (const src of sources) {
    const url = src.url;
    if (!url) continue;

    // Merge per-source overrides over defaults (if you add any later)
    const opts = {
      headless: src.headless ?? defaults.headless ?? true,
      timeoutMs: src.timeoutMs ?? defaults.timeoutMs ?? 60_000,
      retries: src.retries ?? defaults.retries ?? 2,
    };

    try {
      console.log(`Fetching: ${url}`);
      const { raw } = await fetchJsonViaBrowser(url, opts);

      const outFile = path.join(OUT_DIR, fileNameFromUrl(url));
      await fs.writeFile(outFile, raw, "utf8");

      console.log(`Saved → ${outFile}`);
    } catch (e) {
      console.error(`Failed for ${url}: ${e.message}`);
    }
  }
}

if (require.main === module) {
  main().catch(err => {
    console.error(err);
    process.exitCode = 1;
  });
}
