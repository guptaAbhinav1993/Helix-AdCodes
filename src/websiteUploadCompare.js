const { parse } = require('csv-parse/sync');
const { normalizeWebsiteDomainHost, fetchWebsiteAdsTxtWithFallbacks } = require('./websiteInventory');
const { classifyHeader, isExcludedUiMetadataColumn, parseFoundCell } = require('./ctvUploadCompare');
const { isAdlineInContentMobile } = require('./mobileUploadCompare');

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
 * @returns {'domain'|null}
 */
function classifyDomainHeader(original) {
  const k = normalizeHeaderKey(original);
  const compact = k.replace(/[\s._-]+/g, '');
  if (k === 'domain' || compact === 'domain') return 'domain';
  if (k === 'domain name' || compact === 'domainname') return 'domain';
  if (k === 'website domain' || compact === 'websitedomain') return 'domain';
  if (k === 'site domain' || compact === 'sitedomain') return 'domain';
  if (k === 'publisher domain' || compact === 'publisherdomain') return 'domain';
  if (k === 'website_domain' || k === 'site_domain' || k === 'publisher_domain') return 'domain';
  return null;
}

/** Website compare: hide metadata columns (includes ads.txt Accessible variants not covered by CTV list). */
function isWebsiteExcludedMetadataColumn(original) {
  if (isExcludedUiMetadataColumn(original)) return true;
  const k = normalizeHeaderKey(original);
  if (/^ads\.txt\s+accessible\s*\((anshul|abhinav)\)$/i.test(k)) return true;
  if (k === 'ads.txt accessible') return true;
  return false;
}

/**
 * @param {string[]} headers
 */
function buildWebsiteColumnPlan(headers) {
  let colDomain;
  let colDomainIndex = -1;
  /** @type {{ header: string, index: number }[]} */
  const adlineColumns = [];

  for (let index = 0; index < headers.length; index++) {
    const raw = String(headers[index] ?? '');
    if (classifyDomainHeader(raw) === 'domain' && colDomain == null) {
      colDomain = raw;
      colDomainIndex = index;
      continue;
    }
    const role = classifyHeader(raw);
    if (role === 'adline' && !isWebsiteExcludedMetadataColumn(raw)) {
      adlineColumns.push({ header: raw, index });
    }
  }

  return {
    colDomain,
    colDomainIndex,
    adlineColumns,
  };
}

/**
 * @param {string} host - canonical host from normalizeWebsiteDomainHost
 * @param {{ byHost: Map<string, { content: string, effectiveUrl: string|null, fetchError: string }> }} caches
 */
async function fetchWebsiteRowAdsTxt(host, caches) {
  if (!host) {
    return { content: '', effectiveUrl: null, fetchError: 'Empty hostname after normalization' };
  }
  if (!caches.byHost.has(host)) {
    const payload = await fetchWebsiteAdsTxtWithFallbacks(host);
    caches.byHost.set(host, {
      content: payload.content || '',
      effectiveUrl: payload.effectiveUrl ?? null,
      fetchError: payload.fetchError ?? '',
    });
  }
  return caches.byHost.get(host);
}

/**
 * @param {ReturnType<typeof buildWebsiteColumnPlan>} plan
 * @param {string[]} cells
 * @param {string} content - fetched ads.txt text (may be empty)
 */
function buildAdlineResultsForRow(plan, cells, content) {
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
  return adlineResults;
}

/**
 * Parse website CSV: Domain column + adline columns. Fetch ads.txt via http/https + www fallbacks (websiteInventory).
 * Adline matching uses the same rules as Mobile (`isAdlineInContentMobile`).
 * @param {Buffer|string} fileBuffer
 */
async function runWebsiteUploadCompare(fileBuffer) {
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
  const plan = buildWebsiteColumnPlan(headerCells);

  if (plan.colDomainIndex < 0 || !plan.colDomain) {
    return {
      success: false,
      error:
        'Missing required column: Domain (or Website domain, site_domain, publisher_domain, etc.).',
    };
  }

  const caches = { byHost: new Map() };
  const outRows = [];

  for (let r = 1; r < matrix.length; r++) {
    const cells = matrix[r];
    const domainRaw = normalizeCell(cellAtRow(cells, plan.colDomainIndex));
    if (!domainRaw) {
      outRows.push({
        appBundle: '',
        appAdsTxtUrlAnshul: '',
        appAdsTxtUrlAbhinav: '',
        fetchError: 'Empty domain cell',
        adlines: buildAdlineResultsForRow(plan, cells, ''),
      });
      continue;
    }

    const host = normalizeWebsiteDomainHost(domainRaw);
    if (!host) {
      outRows.push({
        appBundle: domainRaw,
        appAdsTxtUrlAnshul: domainRaw,
        appAdsTxtUrlAbhinav: '',
        fetchError: 'Could not parse domain (invalid URL or hostname)',
        adlines: buildAdlineResultsForRow(plan, cells, ''),
      });
      continue;
    }

    const payload = await fetchWebsiteRowAdsTxt(host, caches);
    const content = payload.content || '';
    const effectiveUrl = payload.effectiveUrl ?? '';
    const fetchError = payload.fetchError ?? '';

    outRows.push({
      appBundle: host,
      appAdsTxtUrlAnshul: domainRaw,
      appAdsTxtUrlAbhinav: effectiveUrl,
      fetchError,
      adlines: buildAdlineResultsForRow(plan, cells, content),
    });
  }

  const csvExport = `\uFEFF${buildExportCsv(plan, outRows)}`;

  return {
    success: true,
    plan: {
      colDomain: plan.colDomain,
      adlineColumnCount: plan.adlineColumns.length,
      exportAdlineHeaders: plan.adlineColumns.map((c) => c.header),
    },
    rows: outRows,
    csvExport,
  };
}

function buildExportCsv(plan, outRows) {
  const headerRow = ['Domain', 'domain_anshul', 'ads_txt_url_abhinav', 'error'];
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
      row.fetchError ?? '',
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
  runWebsiteUploadCompare,
  buildWebsiteColumnPlan,
  classifyDomainHeader,
};
