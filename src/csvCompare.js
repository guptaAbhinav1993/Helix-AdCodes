const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const { filenameTimestamp } = require('./outputNames');

const COL_BUNDLE = 'App Bundle';
const COL_URL = 'App Ads TXT URL';
const COL_APP_ADS_TXT_URL = 'app_ads_txt_url';
const COL_INVENTORY_PARTNER = 'inventory_partner_domain';

/** Join key; not compared cell-by-cell */
const SKIP_COMPARE = new Set([COL_BUNDLE]);

function stripBom(s) {
  if (s && s.charCodeAt(0) === 0xfeff) return s.slice(1);
  return s;
}

function escapeCsvField(val) {
  const s = val == null ? '' : String(val);
  if (/[",\r\n]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function rowToCsvLine(cells) {
  return cells.map(escapeCsvField).join(',');
}

function normalizeCell(v) {
  if (v == null) return '';
  return String(v).trim();
}

function rowKey(row) {
  return normalizeCell(row[COL_BUNDLE]);
}

/**
 * Parse CSV to array of row objects (header keys trimmed).
 */
function readCsvRecords(absPath) {
  const raw = stripBom(fs.readFileSync(absPath, 'utf8'));
  const records = parse(raw, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    trim: false,
  });
  if (records.length === 0) {
    return { headers: [], rows: [], records: [] };
  }
  const first = records[0];
  const headers = Object.keys(first).map((h) => h.trim());
  const normalized = records.map((rec) => {
    const out = {};
    for (const h of headers) {
      const rawKey = Object.keys(rec).find((k) => k.trim() === h) ?? h;
      out[h] = rec[rawKey];
    }
    return out;
  });
  return { headers, rows: normalized, records: normalized };
}

/**
 * Build Map key -> row; warn and keep last on duplicate keys.
 */
function rowsToMap(rows, label) {
  const map = new Map();
  for (const row of rows) {
    const k = rowKey(row);
    if (!k) continue;
    if (map.has(k)) {
      console.warn(`[csvCompare] Duplicate key in ${label} (using last row): ${k.slice(0, 80)}...`);
    }
    map.set(k, row);
  }
  return map;
}

/**
 * Compare two CSVs by column name (order can differ). Rows matched by App Bundle only.
 * Compares every column present in both files (except App Bundle). Diff rows include
 * app_ads_txt_url / inventory_partner_domain / App Ads TXT URL from each side when those columns exist in either file.
 *
 * @param {string} anshulPath - path to Anshul's CSV
 * @param {string} abhinavPath - path to Abhinav's CSV
 * @param {string} [outputPath] - where to write diff CSV (default: same dir as anshul CSV, csv_diff_<a>_vs_<b>_<DD_MM_YYYY_HH_MM>.csv)
 * @returns {{ success: boolean, diffPath?: string, error?: string, stats?: object }}
 */
function runCsvCompare(anshulPath, abhinavPath, outputPath) {
  const absAnshul = path.resolve(anshulPath);
  const absAbhinav = path.resolve(abhinavPath);

  if (!fs.existsSync(absAnshul)) {
    return { success: false, error: `File not found: ${absAnshul}` };
  }
  if (!fs.existsSync(absAbhinav)) {
    return { success: false, error: `File not found: ${absAbhinav}` };
  }

  const dataAnshul = readCsvRecords(absAnshul);
  const dataAbhinav = readCsvRecords(absAbhinav);

  if (dataAnshul.headers.length === 0 || dataAbhinav.headers.length === 0) {
    return { success: false, error: 'One or both CSVs have no data rows.' };
  }

  const hA = dataAnshul.headers;
  const hB = dataAbhinav.headers;
  const setA = new Set(hA);
  const setB = new Set(hB);

  if (!setA.has(COL_BUNDLE) || !setB.has(COL_BUNDLE)) {
    return {
      success: false,
      error: `Both CSVs must include "${COL_BUNDLE}" column.`,
    };
  }

  const compareCols = [...setA]
    .filter((h) => setB.has(h) && !SKIP_COMPARE.has(h))
    .sort((a, b) => a.localeCompare(b));
  if (compareCols.length === 0) {
    return {
      success: false,
      error: 'No comparable columns besides App Bundle (need at least one column name shared by both files).',
    };
  }

  const hasAppAdsCol = setA.has(COL_APP_ADS_TXT_URL) || setB.has(COL_APP_ADS_TXT_URL);
  const hasInvPartnerCol = setA.has(COL_INVENTORY_PARTNER) || setB.has(COL_INVENTORY_PARTNER);
  const hasFetchedUrlCol = setA.has(COL_URL) || setB.has(COL_URL);

  const mapAnshul = rowsToMap(dataAnshul.rows, 'anshul');
  const mapAbhinav = rowsToMap(dataAbhinav.rows, 'abhinav');

  const allKeys = new Set([...mapAnshul.keys(), ...mapAbhinav.keys()]);
  const diffLines = [];

  /** Append inventory context from both rows (empty string if row or column missing). */
  function pushContextColumns(cells, rowAnshul, rowAbhinav) {
    if (hasAppAdsCol) {
      cells.push(
        normalizeCell(rowAnshul?.[COL_APP_ADS_TXT_URL]),
        normalizeCell(rowAbhinav?.[COL_APP_ADS_TXT_URL])
      );
    }
    if (hasInvPartnerCol) {
      cells.push(
        normalizeCell(rowAnshul?.[COL_INVENTORY_PARTNER]),
        normalizeCell(rowAbhinav?.[COL_INVENTORY_PARTNER])
      );
    }
    if (hasFetchedUrlCol) {
      cells.push(normalizeCell(rowAnshul?.[COL_URL]), normalizeCell(rowAbhinav?.[COL_URL]));
    }
  }

  const headerOut = ['issue_type', COL_BUNDLE];
  if (hasAppAdsCol) {
    headerOut.push('app_ads_txt_url_anshul', 'app_ads_txt_url_abhinav');
  }
  if (hasInvPartnerCol) {
    headerOut.push('inventory_partner_domain_anshul', 'inventory_partner_domain_abhinav');
  }
  if (hasFetchedUrlCol) {
    headerOut.push('App Ads TXT URL_anshul', 'App Ads TXT URL_abhinav');
  }
  headerOut.push('column_name', 'anshul', 'abhinav');
  diffLines.push(rowToCsvLine(headerOut));

  let countRowOnlyAnshul = 0;
  let countRowOnlyAbhinav = 0;
  let countCellDiff = 0;

  for (const key of allKeys) {
    const rowAnshul = mapAnshul.get(key);
    const rowAbhinav = mapAbhinav.get(key);

    if (rowAnshul && !rowAbhinav) {
      countRowOnlyAnshul++;
      {
        const cells = ['row_only_in_anshul', normalizeCell(rowAnshul[COL_BUNDLE])];
        pushContextColumns(cells, rowAnshul, undefined);
        cells.push('', '', '');
        diffLines.push(rowToCsvLine(cells));
      }
      continue;
    }

    if (!rowAnshul && rowAbhinav) {
      countRowOnlyAbhinav++;
      {
        const cells = ['row_only_in_abhinav', normalizeCell(rowAbhinav[COL_BUNDLE])];
        pushContextColumns(cells, undefined, rowAbhinav);
        cells.push('', '', '');
        diffLines.push(rowToCsvLine(cells));
      }
      continue;
    }

    if (!rowAnshul || !rowAbhinav) continue;

    for (const col of compareCols) {
      const va = normalizeCell(rowAnshul[col]);
      const vb = normalizeCell(rowAbhinav[col]);
      if (va !== vb) {
        countCellDiff++;
        {
          const cells = ['cell_mismatch', normalizeCell(rowAnshul[COL_BUNDLE])];
          pushContextColumns(cells, rowAnshul, rowAbhinav);
          cells.push(col, va, vb);
          diffLines.push(rowToCsvLine(cells));
        }
      }
    }
  }

  const ts = filenameTimestamp();
  const baseOut =
    outputPath ||
    path.join(
      path.dirname(absAnshul),
      `csv_diff_${path.basename(absAnshul, path.extname(absAnshul))}_vs_${path.basename(absAbhinav, path.extname(absAbhinav))}_${ts}.csv`
    );
  const absOut = path.resolve(baseOut);
  fs.mkdirSync(path.dirname(absOut), { recursive: true });
  fs.writeFileSync(absOut, diffLines.join('\n'), 'utf8');

  const onlyInAnshul = [...setA].filter((h) => !setB.has(h) && h !== COL_BUNDLE).sort();
  const onlyInAbhinav = [...setB].filter((h) => !setA.has(h) && h !== COL_BUNDLE).sort();

  return {
    success: true,
    diffPath: absOut,
    stats: {
      rowsAnshul: mapAnshul.size,
      rowsAbhinav: mapAbhinav.size,
      rowOnlyInAnshul: countRowOnlyAnshul,
      rowOnlyInAbhinav: countRowOnlyAbhinav,
      cellMismatches: countCellDiff,
      diffRowsWritten: diffLines.length - 1,
      columnsCompared: compareCols.length,
      columnsOnlyInAnshul: onlyInAnshul,
      columnsOnlyInAbhinav: onlyInAbhinav,
    },
  };
}

module.exports = {
  runCsvCompare,
  COL_BUNDLE,
  COL_URL,
  SKIP_COMPARE,
};
