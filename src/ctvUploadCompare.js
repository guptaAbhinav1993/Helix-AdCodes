const { parse } = require('csv-parse/sync');
const {
  fetchAppRowAdlinesContent,
  isAdlineInContent,
} = require('./adlinesMatcher');

/**
 * @param {string} h
 */
function normalizeHeaderKey(h) {
  return String(h ?? '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

/**
 * @param {string} original
 * @returns {'bundle'|'app_name'|'app_ads_source'|'inventory_partner'|'adline'}
 */
function classifyHeader(original) {
  const k = normalizeHeaderKey(original);
  const compact = k.replace(/[\s._-]+/g, '');

  if (k === 'app bundle' || compact === 'appbundle') return 'bundle';
  if (k === 'app name' || compact === 'appname') return 'app_name';

  /** Effective fetch URL column from adlines_match export (spelled with spaces). */
  if (k === 'app ads txt url') {
    return 'app_ads_effective';
  }

  /** Inventory CSV / snake_case / dashed: primary app-ads URL for fetch + anshul display when present. */
  if (
    k === 'app_ads_txt_url' ||
    k === 'app-ads.txt url' ||
    k === 'app adstxt url' ||
    (compact === 'appadstxturl' && k !== 'app ads txt url')
  ) {
    return 'app_ads_source';
  }

  if (
    k === 'inventory partner domain' ||
    k === 'inventory_partner_domain' ||
    compact === 'inventorypartnerdomain'
  ) {
    return 'inventory_partner';
  }

  return 'adline';
}

/** Columns from some exports that are not adlines — hide from CTV compare UI and export. */
function isExcludedUiMetadataColumn(original) {
  const k = normalizeHeaderKey(original);
  if (/^domain\s*\((anshul|abhinav)\)$/.test(k)) return true;
  if (/^app-ads\.txt\s+accessible\s*\((anshul|abhinav)\)$/.test(k)) return true;
  if (/^app\s+ads\s+txt\s+accessible\s*\((anshul|abhinav)\)$/.test(k)) return true;
  if (/^last\s+crawl\s*\((anshul|abhinav)\)$/.test(k)) return true;
  if (k === 'domain' || k === 'last crawl') return true;
  if (k === 'app-ads.txt accessible' || k === 'app ads txt accessible') return true;
  return false;
}

/**
 * @param {string[]} headers - raw CSV header cells (one per column, preserves order & duplicates)
 */
function buildColumnPlan(headers) {
  const classes = headers.map((raw, index) => ({
    raw: String(raw ?? ''),
    index,
    role: classifyHeader(String(raw ?? '')),
  }));

  let colBundle;
  let colBundleIndex = -1;
  let colAppAdsSource;
  let colAppAdsSourceIndex = -1;
  let colAppAdsEffective;
  let colAppAdsEffectiveIndex = -1;
  let colInv;
  let colInvIndex = -1;
  /** @type {{ header: string, index: number }[]} */
  const adlineColumns = [];

  for (const { raw, index, role } of classes) {
    if (role === 'bundle' && colBundle == null) {
      colBundle = raw;
      colBundleIndex = index;
    } else if (role === 'app_ads_source' && colAppAdsSource == null) {
      colAppAdsSource = raw;
      colAppAdsSourceIndex = index;
    } else if (role === 'app_ads_effective' && colAppAdsEffective == null) {
      colAppAdsEffective = raw;
      colAppAdsEffectiveIndex = index;
    } else if (role === 'inventory_partner' && colInv == null) {
      colInv = raw;
      colInvIndex = index;
    } else if (role === 'adline' && !isExcludedUiMetadataColumn(raw)) {
      adlineColumns.push({ header: raw, index });
    }
  }

  return {
    colBundle,
    colBundleIndex,
    colAppAdsPrimary: colAppAdsSource ?? null,
    colAppAdsPrimaryIndex: colAppAdsSourceIndex,
    colAppAdsEffective: colAppAdsEffective ?? null,
    colAppAdsEffectiveIndex,
    colInv,
    colInvIndex,
    adlineColumns,
  };
}

function stripBom(s) {
  if (s && s.charCodeAt(0) === 0xfeff) return s.slice(1);
  return s;
}

function normalizeCell(v) {
  if (v == null) return '';
  return String(v).trim();
}

/**
 * @returns {boolean|null} null = unknown / empty
 */
function parseFoundCell(val) {
  const s = normalizeCell(val).toUpperCase();
  if (!s) return null;
  if (['YES', 'Y', 'FOUND', 'TRUE', '1'].includes(s)) return true;
  if (['NO', 'N', 'MISSING', 'FALSE', '0'].includes(s)) return false;
  return null;
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
 * Parse uploaded CTV match CSV; fetch app-ads per row (same priority as adlinesMatcher); compare to YES/NO/Found/Missing.
 * @param {Buffer|string} fileBuffer
 * @returns {{ success: boolean, error?: string, plan?: object, rows?: object[], csvExport?: string }}
 */
function cellAtRow(cells, colIndex) {
  if (colIndex == null || colIndex < 0) return '';
  const v = cells[colIndex];
  return v == null ? '' : String(v);
}

async function runCtvUploadCompare(fileBuffer) {
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
    txtUrlCache: new Map(),
    partnerHostCache: new Map(),
    appAdsUrlCache: new Map(),
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
    const invAnshul =
      plan.colInvIndex >= 0 ? normalizeCell(cellAtRow(cells, plan.colInvIndex)) : '';

    /** Prefer app-ads.txt URL (primary); if empty (e.g. no inventory partner), use App Ads TXT URL column. */
    const mergedAppAdsUrl = primaryAdsVal || effectiveAdsVal;

    const app = {
      app_bundle: bundle,
      app_ads_txt_url: mergedAppAdsUrl || null,
      inventory_partner_domain: invAnshul || null,
    };

    const payload = await fetchAppRowAdlinesContent(app, caches);
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
      const abhinavBool = isAdlineInContent(adlineText, content);

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
      inventoryPartnerAnshul: invAnshul,
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

/**
 * @param {ReturnType<typeof buildColumnPlan>} plan
 * @param {object[]} outRows
 */
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

  for (const r of outRows) {
    const dataRow = [
      r.appBundle,
      r.appAdsTxtUrlAnshul,
      r.appAdsTxtUrlAbhinav,
      r.inventoryPartnerAnshul,
    ];
    for (let i = 0; i < adlineSlots.length; i++) {
      const cell = r.adlines[i];
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
  runCtvUploadCompare,
  buildColumnPlan,
  classifyHeader,
  isExcludedUiMetadataColumn,
  parseFoundCell,
};
