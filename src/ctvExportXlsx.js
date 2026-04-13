const ExcelJS = require('exceljs');

const FILL_MATCH = {
  type: 'pattern',
  pattern: 'solid',
  fgColor: { argb: 'FFD4EDDA' },
};
const FILL_MISMATCH = {
  type: 'pattern',
  pattern: 'solid',
  fgColor: { argb: 'FFF8D7DA' },
};
const FILL_HEADER = {
  type: 'pattern',
  pattern: 'solid',
  fgColor: { argb: 'FFF8F8F8' },
};
const FILL_BASE = {
  type: 'pattern',
  pattern: 'solid',
  fgColor: { argb: 'FFFFFFFF' },
};

/**
 * Longest adlines[] among rows (for header labels when row lengths differ).
 * @param {object[]} rows
 */
function referenceAdlines(rows) {
  let best = [];
  for (const r of rows) {
    const a = r.adlines;
    if (Array.isArray(a) && a.length > best.length) best = a;
  }
  return best;
}

/**
 * Build .xlsx buffer with same green/red styling as the CTV compare UI.
 * @param {object[]} rows - same shape as API `rows` from /api/ctv-compare
 * @returns {Promise<Buffer>}
 */
async function buildCtvCompareXlsxBuffer(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    throw new Error('rows must be a non-empty array');
  }

  const refAdlines = referenceAdlines(rows);
  const websiteStyle = rows.some((r) => Object.prototype.hasOwnProperty.call(r, 'fetchError'));
  const baseCount = 4;

  const wb = new ExcelJS.Workbook();
  wb.creator = 'Helix AdsCodes';
  const ws = wb.addWorksheet(websiteStyle ? 'Website Compare' : 'CTV Compare');
  ws.views = [{ state: 'frozen', xSplit: 0, ySplit: 1 }];

  const headerCells = websiteStyle
    ? ['Domain', 'domain (anshul)', 'ads.txt URL (abhinav)', 'error']
    : [
        'App Bundle',
        'app_ads_txt_url (anshul)',
        'app_ads_txt_url (abhinav)',
        'inventory_partner_domain (anshul)',
      ];
  for (const a of refAdlines) {
    const label = (a.adline || a.header || '').trim();
    headerCells.push(`${label}\n(anshul)`, `${label}\n(abhinav)`);
  }

  const headerRow = ws.addRow(headerCells);
  headerRow.eachCell((cell) => {
    cell.fill = FILL_HEADER;
    cell.font = { bold: true, size: 10 };
    cell.alignment = { wrapText: true, vertical: 'top' };
  });

  const adlineStartCol = baseCount + 1;

  for (const r of rows) {
    const values = websiteStyle
      ? [
          r.appBundle ?? '',
          r.appAdsTxtUrlAnshul ?? '',
          r.appAdsTxtUrlAbhinav ?? '',
          r.fetchError ?? '',
        ]
      : [
          r.appBundle ?? '',
          r.appAdsTxtUrlAnshul ?? '',
          r.appAdsTxtUrlAbhinav ?? '',
          r.inventoryPartnerAnshul ?? '',
        ];
    const ad = r.adlines || [];
    for (let i = 0; i < refAdlines.length; i++) {
      const c = ad[i];
      values.push(c ? c.anshulLabel || c.anshulRaw || '' : '', c ? c.abhinavLabel || '' : '');
    }

    const excelRow = ws.addRow(values);
    excelRow.eachCell((cell, colNumber) => {
      cell.alignment = { wrapText: true, vertical: 'top' };
      if (colNumber <= baseCount) {
        cell.fill = FILL_BASE;
        return;
      }
      const idx = Math.floor((colNumber - adlineStartCol) / 2);
      const c = ad[idx];
      if (!c) {
        cell.fill = FILL_BASE;
        return;
      }
      cell.fill = c.match ? FILL_MATCH : FILL_MISMATCH;
    });
  }

  ws.columns = headerCells.map((_, i) => ({
    width:
      i < baseCount
        ? Math.min(52, (websiteStyle && i === 3 ? 36 : 14) + (i === 0 ? 10 : 0))
        : 12,
  }));

  const buf = await wb.xlsx.writeBuffer();
  return Buffer.isBuffer(buf) ? buf : Buffer.from(buf);
}

module.exports = {
  buildCtvCompareXlsxBuffer,
};
