const path = require('path');
const express = require('express');
const multer = require('multer');
const { connect, getDb, getClient, DB_NAME } = require('./config/db');
const { processPayload, VALID_PLATFORMS, INVENTORY_COLLECTIONS } = require('./processor');
const { runMatchFromOutputFiles } = require('./adlinesMatcher');
const { runCsvCompare } = require('./csvCompare');
const { runCtvUploadCompare } = require('./ctvUploadCompare');
const { buildCtvCompareXlsxBuffer } = require('./ctvExportXlsx');
const { filenameTimestamp } = require('./outputNames');

const app = express();
app.use(express.json({ limit: '80mb' }));
app.use(express.static(path.join(__dirname, '..', 'public')));

const uploadCsv = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 80 * 1024 * 1024 },
});

const PORT = process.env.PORT || 3000;

async function main() {
  try {
    console.log('Connecting to MongoDB...');
    await connect();
    const db = getDb();
    const client = getClient();

    const admin = client.db('admin').admin();
    const { version } = await admin.buildInfo();
    const collections = await db.listCollections().toArray();
    const names = collections.map((c) => c.name);

    console.log('\n--- Connection successful ---');
    console.log('Project: USP → Cluster: USP-cluster → Database:', DB_NAME);
    console.log('MongoDB server version:', version);
    // console.log('Collections:', names.length ? names.join(', ') : '(none)');
    console.log('------------------------------\n');

    /**
     * POST /process
     * Body: { "platform": "ctv" | "mobile" | "website" | "all" }
     * Runs platform-based extraction from pix_inv_* and adsdocs; writes per-platform JSON/CSV plus full adsdocs catalog.
     */
    app.post('/process', async (req, res) => {
      try {
        const payload = req.body;
        if (!payload || typeof payload.platform !== 'string') {
          return res.status(400).json({
            success: false,
            error: 'Missing or invalid body. Send JSON: { "platform": "ctv" | "mobile" | "website" | "all" }',
          });
        }

        const result = await processPayload(payload);
        if (!result.success) {
          return res.status(400).json(result);
        }

        return res.json(result);
      } catch (err) {
        console.error('Process error:', err);
        return res.status(500).json({ success: false, error: err.message });
      }
    });

    /** GET /process — show usage */
    app.get('/process', (req, res) => {
      res.json({
        usage: 'POST /process with JSON body: { "platform": "ctv" | "mobile" | "website" | "all" }',
        validPlatforms: VALID_PLATFORMS,
        inventoryCollections: INVENTORY_COLLECTIONS,
        note:
          'Writes approved_app_bundles_<platform>.json, adlines_match_<platform>_<DD_MM_YYYY_HH_MM>.csv, enabled_app_ads_txt_url.json. Website/all also write website_unique_domains.json; web rows use /ads.txt with http/https + www fallbacks.',
      });
    });

    /**
     * POST /match-adlines
     * Reads approved_app_bundles_<platform>.json and enabled_app_ads_txt_url.json (full adsdocs catalog),
     * uses enabled rows for that platform from the catalog, fetches URLs, writes adlines_match_<platform>_<DD_MM_YYYY_HH_MM>.csv.
     * Body optional: { "outputDir": "output", "platform": "ctv" | "mobile" | "website" | "all" } (platform defaults to ctv).
     */
    app.post('/match-adlines', async (req, res) => {
      try {
        const body = req.body || {};
        const outputDir = body.outputDir || 'output';
        const platform = body.platform || 'ctv';
        const result = await runMatchFromOutputFiles(outputDir, { platform });
        if (!result.success) {
          return res.status(400).json(result);
        }
        return res.json(result);
      } catch (err) {
        console.error('Match adlines error:', err);
        return res.status(500).json({ success: false, error: err.message });
      }
    });

    /** GET /match-adlines — show usage */
    app.get('/match-adlines', (req, res) => {
      res.json({
        description: 'Find adlines in app-ads.txt files and write per-platform CSV',
        usage:
          'POST /match-adlines (optional body: { "outputDir": "output", "platform": "ctv" | "mobile" | "website" | "all" })',
        note: 'Needs approved_app_bundles_<platform>.json and enabled_app_ads_txt_url.json (run POST /process first).',
        output:
          'Writes output/adlines_match_<platform>_<DD_MM_YYYY_HH_MM>.csv; catalog JSON lists all adsdocs (any status).',
      });
    });

    /**
     * POST /compare-csv
     * Body: { "anshul": "path/to/anshul.csv", "abhinav": "path/to/abhinav.csv", "outputPath": "optional/out.csv" }
     * Paths relative to process cwd. Column order may differ; columns are matched by name.
     * Rows keyed by App Bundle; shared columns (except App Bundle) are compared. Diff CSV includes
     * app_ads_txt_url / inventory_partner_domain / App Ads TXT URL from each file when present.
     */
    app.post('/compare-csv', (req, res) => {
      try {
        const body = req.body || {};
        const anshul = body.anshul;
        const abhinav = body.abhinav;
        if (typeof anshul !== 'string' || typeof abhinav !== 'string' || !anshul.trim() || !abhinav.trim()) {
          return res.status(400).json({
            success: false,
            error:
              'Send JSON: { "anshul": "relative/or/absolute/path.csv", "abhinav": "other.csv" } optional "outputPath"',
          });
        }
        const outputPath =
          typeof body.outputPath === 'string' && body.outputPath.trim() ? body.outputPath.trim() : undefined;
        const result = runCsvCompare(anshul, abhinav, outputPath);
        if (!result.success) {
          return res.status(400).json(result);
        }
        return res.json(result);
      } catch (err) {
        console.error('compare-csv error:', err);
        return res.status(500).json({ success: false, error: err.message });
      }
    });

    app.get('/compare-csv', (req, res) => {
      res.json({
        description: 'Diff two CSVs with identical columns; match rows by App Bundle only (required column)',
        usage:
          'POST /compare-csv with JSON: { "anshul": "output/run1.csv", "abhinav": "output/run2.csv" }',
        optional: '{ "outputPath": "output/my_diff.csv" } (omit for auto name with timestamp)',
        output:
          'Default: csv_diff_<a>_vs_<b>_<timestamp>.csv; paired *_anshul / *_abhinav context columns; issue_type: row_only_* | cell_mismatch',
      });
    });

    app.get('/ctv-compare', (req, res) => {
      res.sendFile(path.join(__dirname, '..', 'public', 'ctv-compare.html'));
    });

    /**
     * POST /api/ctv-compare — multipart field "csv"
     * Upload CTV-style CSV; fetches app-ads per row (inventory partner priority); compares YES/NO vs fetch.
     */
    app.post('/api/ctv-compare', uploadCsv.single('csv'), async (req, res) => {
      try {
        if (!req.file?.buffer) {
          return res.status(400).json({
            success: false,
            error: 'Missing CSV file. Use multipart field name "csv".',
          });
        }
        const result = await runCtvUploadCompare(req.file.buffer);
        if (!result.success) {
          return res.status(400).json(result);
        }
        return res.json(result);
      } catch (err) {
        console.error('api/ctv-compare error:', err);
        return res.status(500).json({ success: false, error: err.message });
      }
    });

    /**
     * POST /api/mobile-compare — multipart field "csv"
     * Placeholder until mobile compare pipeline is wired (same multipart contract as CTV).
     */
    app.post('/api/mobile-compare', uploadCsv.single('csv'), async (req, res) => {
      if (!req.file?.buffer) {
        return res.status(400).json({
          success: false,
          error: 'Missing CSV file. Use multipart field name "csv".',
        });
      }
      return res.status(501).json({
        success: false,
        error: 'Mobile compare is not implemented yet.',
      });
    });

    /**
     * POST /api/website-compare — multipart field "csv"
     * Placeholder until website compare pipeline is wired (same multipart contract as CTV).
     */
    app.post('/api/website-compare', uploadCsv.single('csv'), async (req, res) => {
      if (!req.file?.buffer) {
        return res.status(400).json({
          success: false,
          error: 'Missing CSV file. Use multipart field name "csv".',
        });
      }
      return res.status(501).json({
        success: false,
        error: 'Website compare is not implemented yet.',
      });
    });

    /**
     * POST /api/ctv-export-xlsx — JSON body: { "rows": [ ... same as /api/ctv-compare response rows ] }
     * Returns colored .xlsx (green/red) matching the CTV compare UI. Plain CSV cannot carry colors.
     */
    app.post('/api/ctv-export-xlsx', async (req, res) => {
      try {
        const rows = req.body?.rows;
        if (!Array.isArray(rows) || rows.length === 0) {
          return res.status(400).json({
            success: false,
            error: 'Send JSON: { "rows": [ ... ] } from the compare result (non-empty).',
          });
        }
        const buf = await buildCtvCompareXlsxBuffer(rows);
        const stamp = filenameTimestamp();
        const name = `ctv_compare_${stamp}.xlsx`;
        res.setHeader(
          'Content-Type',
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        );
        res.setHeader('Content-Disposition', `attachment; filename="${name}"`);
        return res.send(buf);
      } catch (err) {
        console.error('api/ctv-export-xlsx error:', err);
        return res.status(500).json({ success: false, error: err.message });
      }
    });

    app.listen(PORT, () => {
      console.log(`Server listening on http://localhost:${PORT}`);
      console.log('POST /process — platform-based extraction');
      console.log('POST /match-adlines — find adlines in app-ads.txt files, write CSV');
      console.log('POST /compare-csv — diff two CSVs by App Bundle');
      console.log(`GET  /ctv-compare — CTV upload UI`);
      console.log('POST /api/ctv-compare — multipart csv, compare vs live fetch');
      console.log('POST /api/mobile-compare — multipart csv (placeholder)');
      console.log('POST /api/website-compare — multipart csv (placeholder)');
      console.log('POST /api/ctv-export-xlsx — JSON rows → colored Excel (UI colors)\n');
    });
  } catch (err) {
    console.error('Connection failed:', err.message);
    process.exit(1);
  }
}

main();
