const fs = require('fs');
const path = require('path');
const {
  ENABLED_APP_ADS_TXT_URL_FILE,
  approvedAppBundlesFilename,
  adlinesMatchFilename,
  filenameTimestamp,
} = require('./outputNames');
const { filterEnabledAdsdocsForPlatform } = require('./adsdocsPlatform');
const {
  fetchWebsiteAdsTxtWithFallbacks,
  isWebsiteInventoryRow,
  normalizeWebsiteDomainHost,
} = require('./websiteInventory');

const MATCH_VALID_PLATFORMS = ['ctv', 'mobile', 'website', 'all'];

/**
 * Escape a value for CSV (RFC 4180): quote if contains comma, quote, or newline.
 */
function escapeCsvField(val) {
  const s = val == null ? '' : String(val);
  if (/[",\r\n]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function rowsToCsv(rows) {
  return rows.map((row) => row.map(escapeCsvField).join(',')).join('\n');
}
/**
 * If URL does not start with http:// or https://, prepend https://
 */
function normalizeUrl(url) {
  if (!url || typeof url !== 'string') return null;
  const u = url.trim();
  if (!u) return null;
  if (u.toLowerCase().startsWith('http://') || u.toLowerCase().startsWith('https://')) {
    return u;
  }
  return `https://${u}`;
}

/**
 * Trim and strip leading "inventorypartnerdomain=" (as in ads.txt lines) if present.
 */
function normalizeInventoryPartnerDomain(raw) {
  if (raw == null) return '';
  let s = String(raw).trim();
  if (!s) return '';
  const m = s.match(/^inventorypartnerdomain\s*=\s*(.+)$/i);
  if (m) s = m[1].trim();
  return s;
}

/**
 * True if inventory_partner_domain looks like a direct .txt file URL (takes priority over app_ads_txt_url).
 * Bare hosts like xapads.com (no .txt) do not qualify — those use app_ads_txt_url.
 */
function inventoryPartnerDomainIsTxtLink(raw) {
  const s = normalizeInventoryPartnerDomain(raw);
  if (!s) return false;
  return /\.txt\b/i.test(s);
}

/**
 * Fetch URL from inventory_partner_domain when it is a .txt link (any path ending in .txt).
 */
function urlFromInventoryPartnerTxtLink(raw) {
  const s = normalizeInventoryPartnerDomain(raw);
  if (!s || !inventoryPartnerDomainIsTxtLink(raw)) return null;
  return normalizeUrl(s);
}

/**
 * Host for partner fallbacks when value is not a .txt URL (bare domain or URL → hostname only).
 */
function partnerHostForAppAdsFallback(raw) {
  const s = normalizeInventoryPartnerDomain(raw);
  if (!s || /\.txt\b/i.test(s)) return null;
  return normalizeWebsiteDomainHost(s);
}

/**
 * Try app-ads.txt on host: http, http+www, https, https+www (same order as website /ads.txt).
 */
async function fetchPartnerAppAdsTxtWithFallbacks(host) {
  const h = String(host).trim().replace(/^www\./i, '');
  if (!h) return { content: '', effectiveUrl: null };
  const candidates = [
    `http://${h}/app-ads.txt`,
    `http://www.${h}/app-ads.txt`,
    `https://${h}/app-ads.txt`,
    `https://www.${h}/app-ads.txt`,
  ];
  for (const url of candidates) {
    const text = await fetchUrlContent(url);
    if (text != null) return { content: text, effectiveUrl: url };
  }
  return { content: '', effectiveUrl: null };
}

/**
 * Best-effort single URL for logging / previews (actual fetch may try fallbacks).
 * - Partner .txt → that URL
 * - Partner host (no .txt) → first fallback candidate
 * - Else → app_ads_txt_url
 */
function resolveAdlinesFetchUrl(app) {
  const normPartner =
    app.inventory_partner_domain != null ? normalizeInventoryPartnerDomain(app.inventory_partner_domain) : '';
  if (normPartner && /\.txt\b/i.test(normPartner)) {
    return urlFromInventoryPartnerTxtLink(app.inventory_partner_domain);
  }
  if (normPartner) {
    const ph = partnerHostForAppAdsFallback(app.inventory_partner_domain);
    if (ph) return `http://${ph}/app-ads.txt`;
  }
  return normalizeUrl(app.app_ads_txt_url);
}

/**
 * Fetched text + URL shown in CSV. Caches by txt URL, partner host, and app_ads_txt_url across apps.
 */
async function fetchAppRowAdlinesContent(app, caches) {
  const { txtUrlCache, partnerHostCache, appAdsUrlCache } = caches;
  const normPartner =
    app.inventory_partner_domain != null ? normalizeInventoryPartnerDomain(app.inventory_partner_domain) : '';

  if (normPartner && /\.txt\b/i.test(normPartner)) {
    const u = urlFromInventoryPartnerTxtLink(app.inventory_partner_domain);
    if (u) {
      if (!txtUrlCache.has(u)) {
        const c = await fetchUrlContent(u);
        txtUrlCache.set(u, { content: c || '', effectiveUrl: u });
      }
      return txtUrlCache.get(u);
    }
  }

  if (normPartner) {
    const host = partnerHostForAppAdsFallback(app.inventory_partner_domain);
    if (host) {
      if (!partnerHostCache.has(host)) {
        partnerHostCache.set(host, await fetchPartnerAppAdsTxtWithFallbacks(host));
      }
      const pr = partnerHostCache.get(host);
      if (pr.content) return pr;
    }
  }

  const au = normalizeUrl(app.app_ads_txt_url);
  if (au) {
    if (!appAdsUrlCache.has(au)) {
      const c = await fetchUrlContent(au);
      appAdsUrlCache.set(au, { content: c || '', effectiveUrl: au });
    }
    return appAdsUrlCache.get(au);
  }

  return { content: '', effectiveUrl: null };
}

/**
 * Fetch URL as text. Returns content string or null on failure.
 */
async function fetchUrlContent(url) {
  const normalized = normalizeUrl(url);
  if (!normalized) return null;
  try {
    const res = await fetch(normalized, {
      method: 'GET',
      headers: { Accept: 'text/plain, text/html, */*' },
      signal: AbortSignal.timeout(15000),
    });
    if (!res.ok) return null;
    return await res.text();
  } catch {
    return null;
  }
}

/**
 * Normalize adline for comparison: trim, lowercase, strip whitespace only next to commas.
 * Spaces inside a field (e.g. "sg 1248060") are kept; only comma-padding is removed.
 * Used only for the base (comma-part) before any " #hashtag" segment.
 */
function normalizeAdlineForMatch(str) {
  if (str == null) return '';
  return String(str)
    .trim()
    .toLowerCase()
    .replace(/\s*,\s*/g, ',');
}

/**
 * Split at first " #..." (whitespace + hash). Suffix is kept exact from DB/file — spacing before # must not be normalized.
 */
function splitAtHashtagSuffix(s) {
  const t = String(s).trim();
  const m = t.match(/^(.*?)(\s+#.+)$/s);
  if (!m) return { hasHashtag: false, basePart: t, suffixExact: '' };
  return { hasHashtag: true, basePart: m[1], suffixExact: m[2] };
}

/**
 * - Needle without " #tag": full-line compare with normalizeAdlineForMatch; substring OK on one line for multi-ad.
 * - Needle with " #tag": base (comma-part before ` #`) uses comma-padding-only normalize; the suffix (` #...` as in DB)
 *   must match the file byte-for-byte. No extra space before the suffix: raw text before suffix must equal trimEnd
 *   (so `...b  #SOVRN` does not match DB `...b #SOVRN`).
 */
function physicalLineMatchesNeedle(needleFromDb, physicalLine) {
  const line = String(physicalLine).trim();
  if (!line || line.startsWith('#')) return false;

  const needleTrim = String(needleFromDb).trim();
  const ns = splitAtHashtagSuffix(needleTrim);

  if (ns.hasHashtag) {
    const nb = normalizeAdlineForMatch(ns.basePart);
    if (!nb) return false;
    const suf = ns.suffixExact;
    let from = 0;
    while (from <= line.length) {
      const idx = line.indexOf(suf, from);
      if (idx === -1) return false;
      const rawPrefix = line.slice(0, idx);
      if (normalizeAdlineForMatch(rawPrefix) !== nb) {
        from = idx + 1;
        continue;
      }
      if (rawPrefix !== rawPrefix.trimEnd()) {
        from = idx + 1;
        continue;
      }
      const merged = rawPrefix + suf;
      if (line.includes(merged)) return true;
      from = idx + 1;
    }
    return false;
  }

  const needleNorm = normalizeAdlineForMatch(needleTrim);
  if (!needleNorm) return false;
  const lineNorm = normalizeAdlineForMatch(line);
  return lineNorm === needleNorm || lineNorm.includes(needleNorm);
}

/**
 * True if the adsdocs/DB `adlines` string appears in fetched ads.txt or app-ads.
 */
function isAdlineInContent(adlines, content) {
  if (adlines == null || content == null) return false;
  const needle = String(adlines).trim();
  if (!needle) return false;
  const lines = content.split(/\r?\n/);
  return lines.some((line) => physicalLineMatchesNeedle(needle, line));
}

/**
 * Build CSV (final output) per requested format:
 * - Columns: App Name | App Bundle | [CTV: app_ads_txt_url | inventory_partner_domain |] App Ads TXT URL | <adline1> | …
 *   CTV rows include raw inventory fields; App Ads TXT URL is still the effective fetched URL.
 * - Rows: inventory_partner_domain .txt → fetch that URL; else partner host → /app-ads.txt via http/www/https/https+www; else app_ads_txt_url
 * - Adline columns: YES if that adline is found in fetched text, else NO
 * approvedApps: app rows or website rows (inventorySource pix_inv_web_data: domain, app_bundle, app_name)
 * enabledRecords: array of { name, adlines }
 * @param {string} platform - ctv | mobile | website | all — output adlines_match_<platform>_<DD_MM_YYYY_HH_MM>.csv
 */
async function buildAndWriteCsv(approvedApps, enabledRecords, outputDir, platform = 'ctv') {
  const outPath = path.join(path.resolve(outputDir), adlinesMatchFilename(platform));
  const isCtv = platform === 'ctv';

  const adlineList = enabledRecords.map((r) => r.adlines ?? '');

  const caches = {
    txtUrlCache: new Map(),
    partnerHostCache: new Map(),
    appAdsUrlCache: new Map(),
  };
  for (const app of approvedApps) {
    if (isWebsiteInventoryRow(app)) continue;
    await fetchAppRowAdlinesContent(app, caches);
  }

  const domainToContent = new Map();
  const domainToEffectiveUrl = new Map();
  for (const app of approvedApps) {
    if (!isWebsiteInventoryRow(app)) continue;
    const dom = (app.domain || app.app_bundle || '').trim().toLowerCase();
    if (!dom || domainToContent.has(dom)) continue;
    const { content, effectiveUrl } = await fetchWebsiteAdsTxtWithFallbacks(dom);
    domainToContent.set(dom, content || '');
    domainToEffectiveUrl.set(dom, effectiveUrl || '');
  }

  const headers = isCtv
    ? ['App Name', 'App Bundle', 'app_ads_txt_url', 'inventory_partner_domain', 'App Ads TXT URL', ...adlineList]
    : ['App Name', 'App Bundle', 'App Ads TXT URL', ...adlineList];
  const rows = [headers];

  for (const app of approvedApps) {
    let content = '';
    let displayUrl = '';

    if (isWebsiteInventoryRow(app)) {
      if (platform !== 'website' && platform !== 'all') continue;
      const dom = (app.domain || app.app_bundle || '').trim().toLowerCase();
      if (!dom) continue;
      content = domainToContent.get(dom) ?? '';
      displayUrl = domainToEffectiveUrl.get(dom) ?? '';
    } else {
      if (platform === 'website') continue;
      const payload = await fetchAppRowAdlinesContent(app, caches);
      content = payload.content;
      displayUrl = payload.effectiveUrl ?? '';
    }

    const row = isCtv
      ? [
          app.app_name ?? '',
          app.app_bundle ?? '',
          app.app_ads_txt_url ?? '',
          app.inventory_partner_domain ?? '',
          displayUrl,
        ]
      : [app.app_name ?? '', app.app_bundle ?? '', displayUrl];
    for (const adline of adlineList) {
      row.push(isAdlineInContent(adline, content) ? 'YES' : 'NO');
    }
    rows.push(row);
  }

  fs.writeFileSync(outPath, rowsToCsv(rows), 'utf8');

  return outPath;
}

/**
 * Run adlines match from existing JSON files in outputDir.
 * Reads approved_app_bundles_<platform>.json and enabled_app_ads_txt_url.json (full catalog);
 * uses enabled rows for that platform from the catalog for YES/NO columns.
 * @param {string} outputDir - directory containing JSON files (default: 'output')
 * @param {{ platform?: string }} [options] - platform default 'ctv'
 */
async function runMatchFromOutputFiles(outputDir = 'output', options = {}) {
  const platform = (options.platform || 'ctv').toLowerCase();
  if (!MATCH_VALID_PLATFORMS.includes(platform)) {
    return {
      success: false,
      error: `Invalid platform. Must be one of: ${MATCH_VALID_PLATFORMS.join(', ')}`,
    };
  }

  const dir = path.resolve(outputDir);
  const approvedPath = path.join(dir, approvedAppBundlesFilename(platform));
  const catalogPath = path.join(dir, ENABLED_APP_ADS_TXT_URL_FILE);

  if (!fs.existsSync(approvedPath)) {
    return {
      success: false,
      error: `Missing ${approvedAppBundlesFilename(platform)}. Run POST /process first with platform "${platform}".`,
    };
  }
  if (!fs.existsSync(catalogPath)) {
    return {
      success: false,
      error: `Missing ${ENABLED_APP_ADS_TXT_URL_FILE}. Run POST /process first.`,
    };
  }

  let approvedPayload;
  let catalogPayload;
  try {
    approvedPayload = JSON.parse(fs.readFileSync(approvedPath, 'utf8'));
    catalogPayload = JSON.parse(fs.readFileSync(catalogPath, 'utf8'));
  } catch (err) {
    return { success: false, error: `Invalid JSON in output files: ${err.message}` };
  }

  const approvedRecords = Array.isArray(approvedPayload.records) ? approvedPayload.records : [];
  const catalogRecords = Array.isArray(catalogPayload.records) ? catalogPayload.records : [];

  if (approvedRecords.length === 0) {
    return { success: false, error: `${approvedAppBundlesFilename(platform)} has no records.` };
  }
  if (catalogRecords.length === 0) {
    return { success: false, error: `${ENABLED_APP_ADS_TXT_URL_FILE} has no records.` };
  }

  const enabledRecords = filterEnabledAdsdocsForPlatform(catalogRecords, platform);
  if (enabledRecords.length === 0) {
    return {
      success: false,
      error: `No enabled adsdocs for platform "${platform}" in ${ENABLED_APP_ADS_TXT_URL_FILE}.`,
    };
  }

  try {
    const csvPath = await buildAndWriteCsv(approvedRecords, enabledRecords, dir, platform);
    return {
      success: true,
      csvPath,
      platform,
      approvedCount: approvedRecords.length,
      enabledCount: enabledRecords.length,
      catalogCount: catalogRecords.length,
    };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

module.exports = {
  normalizeUrl,
  normalizeInventoryPartnerDomain,
  inventoryPartnerDomainIsTxtLink,
  urlFromInventoryPartnerTxtLink,
  /** @deprecated use urlFromInventoryPartnerTxtLink — bare host no longer becomes /app-ads.txt */
  urlFromInventoryPartnerDomain: urlFromInventoryPartnerTxtLink,
  resolveAdlinesFetchUrl,
  partnerHostForAppAdsFallback,
  fetchPartnerAppAdsTxtWithFallbacks,
  fetchAppRowAdlinesContent,
  normalizeAdlineForMatch,
  splitAtHashtagSuffix,
  physicalLineMatchesNeedle,
  fetchUrlContent,
  isAdlineInContent,
  buildAndWriteCsv,
  /** @deprecated use buildAndWriteCsv */
  buildAndWriteExcel: (...args) => buildAndWriteCsv(...args),
  runMatchFromOutputFiles,
  /** @deprecated use adlinesMatchFilename(platform) from outputNames */
  CSV_FILENAME: 'adlines_match.csv',
  /** @deprecated use adlinesMatchFilename(platform) from outputNames */
  EXCEL_FILENAME: 'adlines_match.csv',
  ENABLED_APP_ADS_TXT_URL_FILE,
  approvedAppBundlesFilename,
  adlinesMatchFilename,
  filenameTimestamp,
};
