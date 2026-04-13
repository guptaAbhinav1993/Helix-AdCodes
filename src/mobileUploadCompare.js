const { parse } = require('csv-parse/sync');
const {
  normalizeUrl,
  fetchUrlContent,
  fetchPartnerAppAdsTxtWithFallbacks,
  normalizeAdlineForMatch,
  splitAtHashtagSuffix,
  physicalLineMatchesNeedle,
} = require('./adlinesMatcher');
const { buildColumnPlan, parseFoundCell } = require('./ctvUploadCompare');

function stripBom(s) {
  if (s && s.charCodeAt(0) === 0xfeff) return s.slice(1);
  return s;
}

function normalizeCell(v) {
  if (v == null) return '';
  return String(v).trim();
}

function cellAtRow(cells, colIndex) {
  if (colIndex == null || colIndex < 0) return '';
  const v = cells[colIndex];
  return v == null ? '' : String(v);
}

function labelFound(v) {
  if (v === true) return 'Found';
  if (v === false) return 'Missing';
  return '';
}

function escapeCsvField(val) {
  const s = val == null ? '' : String(val);
  if (/[",\r\n]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function rowsToCsv(rows) {
  return rows.map((row) => row.map(escapeCsvField).join(',')).join('\r\n');
}

/**
 * Mobile-only: match CSV adline to one physical app-ads line.
 * - Needle **with** ` #tag`: same rules as CTV (`physicalLineMatchesNeedle`) — file must include the suffix when DB has it.
 * - Needle **without** hashtag: compare to the file line’s **base** (strip optional ` #...` from the file, like CTV’s
 *   “DB without #, file may have #”). Exact normalized base match OR same leading comma-separated fields (extra file
 *   fields OK). No raw substring on the full line (still avoids ...f5ab79cb980f11d1 vs ...f5ab79cb980f11d1r).
 * @param {string} needleFromCsv
 * @param {string} physicalLine
 */
function physicalLineMatchesNeedleMobile(needleFromCsv, physicalLine) {
  const line = String(physicalLine).trim();
  if (!line || line.startsWith('#')) return false;

  const needleTrim = String(needleFromCsv).trim();
  const ns = splitAtHashtagSuffix(needleTrim);
  if (ns.hasHashtag) {
    return physicalLineMatchesNeedle(needleFromCsv, physicalLine);
  }

  const needleNorm = normalizeAdlineForMatch(needleTrim);
  if (!needleNorm) return false;

  const lineSplit = splitAtHashtagSuffix(line);
  const lineBase = String(lineSplit.basePart ?? '').trim();
  const lineBaseNorm = normalizeAdlineForMatch(lineBase);
  if (!lineBaseNorm) return false;

  if (lineBaseNorm === needleNorm) return true;

  const needleFs = needleNorm.split(',');
  const lineFs = lineBaseNorm.split(',');
  if (lineFs.length < needleFs.length) return false;
  for (let i = 0; i < needleFs.length; i++) {
    if (needleFs[i] !== lineFs[i]) return false;
  }
  return true;
}

/**
 * @param {string} adlines
 * @param {string} content
 */
function isAdlineInContentMobile(adlines, content) {
  if (adlines == null || content == null) return false;
  const needle = String(adlines).trim();
  if (!needle) return false;
  const lines = content.split(/\r?\n/);
  return lines.some((ln) => physicalLineMatchesNeedleMobile(needle, ln));
}

/**
 * Mobile: CSV app-ads URL only (no inventory_partner_domain). If the path does not end in .txt,
 * fetch `origin/app-ads.txt` (bare host or domain-style URL without a .txt resource).
 * @param {string} raw
 * @returns {string|null}
 */
function normalizeMobileAppAdsFetchUrl(raw) {
  const base = normalizeUrl(raw);
  if (!base) return null;
  try {
    const u = new URL(base);
    const pathOnly = (u.pathname || '').replace(/\/+$/, '') || '';
    if (/\.txt$/i.test(pathOnly)) {
      return u.href;
    }
    u.pathname = '/app-ads.txt';
    u.search = '';
    u.hash = '';
    return u.href;
  } catch {
    return base;
  }
}

/**
 * Fetch app-ads.txt using mobile rules: primary URL from CSV (normalized), then host fallbacks if empty.
 * @param {string|null} canonicalUrl
 * @param {{ directCache: Map<string, { content: string, effectiveUrl: string|null }> }} caches
 */
async function fetchMobileRowAdlinesContent(canonicalUrl, caches) {
  const { directCache } = caches;
  if (!canonicalUrl) {
    return { content: '', effectiveUrl: null };
  }
  if (!directCache.has(canonicalUrl)) {
    let content = await fetchUrlContent(canonicalUrl);
    let effectiveUrl = content != null && content !== '' ? canonicalUrl : null;
    if (content == null || content === '') {
      try {
        const u = new URL(canonicalUrl);
        const host = u.hostname.replace(/^www\./i, '');
        if (host) {
          const fb = await fetchPartnerAppAdsTxtWithFallbacks(host);
          if (fb.content) {
            content = fb.content;
            effectiveUrl = fb.effectiveUrl;
          }
        }
      } catch {
        /* ignore */
      }
    }
    directCache.set(canonicalUrl, {
      content: content || '',
      effectiveUrl: effectiveUrl || null,
    });
  }
  return directCache.get(canonicalUrl);
}

/**
 * @param {Buffer|string} fileBuffer
 * @returns {{ success: boolean, error?: string, plan?: object, rows?: object[], csvExport?: string }}
 */
async function runMobileUploadCompare(fileBuffer) {
  const raw = stripBom(Buffer.isBuffer(fileBuffer) ? fileBuffer.toString('utf8') : String(fileBuffer));
  let matrix;
  try {
    matrix = parse(raw, {
      columns: false,
      skip_empty_lines: true,
      relax_column_count: true,
      trim: false,
    });
  } catch (e) {
    return { success: false, error: `CSV parse error: ${e.message}` };
  }

  if (!matrix.length) {
    return { success: false, error: 'CSV has no data rows.' };
  }

  const headerCells = matrix[0].map((c) => String(c ?? ''));
  const plan = buildColumnPlan(headerCells);

  if (plan.colBundleIndex < 0 || !plan.colBundle) {
    return {
      success: false,
      error:
        'Missing required column: App Bundle (or app_bundle). Add a column with that header.',
    };
  }

  if (plan.colAppAdsPrimaryIndex < 0 && plan.colAppAdsEffectiveIndex < 0) {
    return {
      success: false,
      error:
        'Missing app-ads URL column. Use "app-ads.txt URL", "app_ads_txt_url", or "App Ads TXT URL".',
    };
  }

  const caches = {
    directCache: new Map(),
  };

  const outRows = [];

  for (let r = 1; r < matrix.length; r++) {
    const cells = matrix[r];

    const bundle = normalizeCell(cellAtRow(cells, plan.colBundleIndex));
    if (!bundle) continue;

    const primaryAdsVal =
      plan.colAppAdsPrimaryIndex >= 0
        ? normalizeCell(cellAtRow(cells, plan.colAppAdsPrimaryIndex))
        : '';
    const effectiveAdsVal =
      plan.colAppAdsEffectiveIndex >= 0
        ? normalizeCell(cellAtRow(cells, plan.colAppAdsEffectiveIndex))
        : '';

    const mergedAppAdsUrl = primaryAdsVal || effectiveAdsVal;
    const fetchUrl = normalizeMobileAppAdsFetchUrl(mergedAppAdsUrl);

    const payload = await fetchMobileRowAdlinesContent(fetchUrl, caches);
    const content = payload.content || '';
    const effectiveUrl = payload.effectiveUrl ?? '';

    const adlineResults = [];
    for (const { header: adHeader, index: adIdx } of plan.adlineColumns) {
      const adlineText = adHeader.trim();
      const rawCell = cellAtRow(cells, adIdx);

      if (!adlineText) {
        adlineResults.push({
          header: adHeader,
          adline: '',
          anshulRaw: normalizeCell(rawCell),
          anshulLabel: '',
          abhinavLabel: '',
          match: false,
        });
        continue;
      }

      const anshulBool = parseFoundCell(rawCell);
      const abhinavBool = isAdlineInContentMobile(adlineText, content);

      const match = anshulBool !== null && abhinavBool === anshulBool;

      adlineResults.push({
        header: adHeader,
        adline: adlineText,
        anshulRaw: normalizeCell(rawCell),
        anshulLabel: labelFound(anshulBool),
        abhinavLabel: labelFound(abhinavBool),
        match,
      });
    }

    outRows.push({
      appBundle: bundle,
      appAdsTxtUrlAnshul: mergedAppAdsUrl,
      appAdsTxtUrlAbhinav: effectiveUrl,
      inventoryPartnerAnshul: '',
      adlines: adlineResults,
    });
  }

  const csvExport = `\uFEFF${buildExportCsv(plan, outRows)}`;

  return {
    success: true,
    plan: {
      colBundle: plan.colBundle,
      adlineColumnCount: plan.adlineColumns.length,
      exportAdlineHeaders: plan.adlineColumns.map((c) => c.header),
    },
    rows: outRows,
    csvExport,
  };
}

function buildExportCsv(plan, outRows) {
  const headerRow = [
    'App Bundle',
    'app_ads_txt_url_anshul',
    'app_ads_txt_url_abhinav',
    'inventory_partner_domain_anshul',
  ];
  const adlineSlots = plan.adlineColumns;
  for (const { header: ad } of adlineSlots) {
    headerRow.push(`${ad} (anshul)`, `${ad} (abhinav)`);
  }

  const lines = [headerRow];

  for (const row of outRows) {
    const dataRow = [
      row.appBundle,
      row.appAdsTxtUrlAnshul,
      row.appAdsTxtUrlAbhinav,
      row.inventoryPartnerAnshul,
    ];
    for (let i = 0; i < adlineSlots.length; i++) {
      const cell = row.adlines[i];
      dataRow.push(
        cell ? cell.anshulLabel || cell.anshulRaw : '',
        cell ? cell.abhinavLabel : ''
      );
    }
    lines.push(dataRow);
  }

  return rowsToCsv(lines);
}

module.exports = {
  runMobileUploadCompare,
  normalizeMobileAppAdsFetchUrl,
  isAdlineInContentMobile,
  physicalLineMatchesNeedleMobile,
};
