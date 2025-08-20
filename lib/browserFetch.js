// lib/browserFetch.js
const puppeteer = require("puppeteer");

/**
 * Fetch JSON via a real browser (Puppeteer), returning { data, raw }.
 * @param {string} url
 * @param {{headless?: boolean, timeoutMs?: number, retries?: number}} opts
 */
async function fetchJsonViaBrowser(url, opts = {}) {
  const {
    headless = true,
    timeoutMs = 60_000,
    retries = 2,
  } = opts;

  let lastError;
  for (let attempt = 0; attempt <= retries; attempt++) {
    let browser;
    try {
      browser = await puppeteer.launch({
        headless,
        args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
      });

      const page = await browser.newPage();
      await page.setUserAgent(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
      );
      await page.setExtraHTTPHeaders({
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
      });

      await page.setRequestInterception(true);
      page.on("request", (req) => {
        const t = req.resourceType();
        if (t === "image" || t === "media" || t === "font") req.abort();
        else req.continue();
      });

      const resp = await page.goto(url, {
        // Changed to be more suitable for large files
        waitUntil: "domcontentloaded",
        timeout: timeoutMs,
      });

      if (!resp) throw new Error("No response.");
      const status = resp.status();
      if (status < 200 || status >= 300) throw new Error(`HTTP ${status}`);

      // Switched to a more robust way to get large content.
      // When a browser opens a .json file, it usually wraps it in a <pre> tag.
      const raw = await page.evaluate(() => document.body.querySelector('pre')?.innerText || document.body.innerText);

      if (!raw?.trim()) throw new Error("Empty body.");
      if (/<!doctype html>|<html[\s>]/i.test(raw)) throw new Error("HTML interstitial received.");

      let data;
      try { data = JSON.parse(raw); }
      catch (e) { throw new Error(`Invalid JSON: ${e.message}`); }

      await browser.close();
      return { data, raw };
    } catch (err) {
      lastError = err;
      if (browser) { try { await browser.close(); } catch {} }
      if (attempt < retries) await new Promise(r => setTimeout(r, 500 + 250 * attempt));
    }
  }
  throw new Error(`Failed after ${retries + 1} attempt(s): ${lastError?.message}`);
}

module.exports = { fetchJsonViaBrowser };