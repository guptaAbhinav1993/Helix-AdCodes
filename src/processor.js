const fs = require('fs');
const path = require('path');
const { getCollection } = require('./config/db');
const { adsdocsPlatformFilter } = require('./adsdocsPlatform');
const {
  ENABLED_APP_ADS_TXT_URL_FILE,
  WEBSITE_UNIQUE_DOMAINS_FILE,
  approvedAppBundlesFilename,
  adlinesMatchFilename,
} = require('./outputNames');
const {
  domainFromWebDoc,
  buildWebsiteInventoryRecord,
  WEBSITE_INVENTORY_SOURCE,
} = require('./websiteInventory');

const VALID_PLATFORMS = ['ctv', 'mobile', 'website', 'all'];

/** Platform → inventory collection name(s) */
const INVENTORY_COLLECTIONS = {
  ctv: ['pix_inv_ctv_data'],
  mobile: ['pix_inv_mobile_data'],
  website: ['pix_inv_web_data'],
  all: ['pix_inv_ctv_data', 'pix_inv_mobile_data', 'pix_inv_web_data'],
};

const WEB_PROJECTION = {
  domain: 1,
  website_domain: 1,
  site_domain: 1,
  publisher_domain: 1,
  url: 1,
  _id: 0,
};

/**
 * Dedupe key for ctv/mobile inventory: same logical row if all three match.
 * @param {{ app_bundle?: unknown, app_ads_txt_url?: unknown, inventory_partner_domain?: unknown }} row
 * @returns {string}
 */
function inventoryTripleKey(row) {
  const b = row.app_bundle != null ? String(row.app_bundle).trim() : '';
  const u = row.app_ads_txt_url != null ? String(row.app_ads_txt_url).trim() : '';
  const d =
    row.inventory_partner_domain != null ? String(row.inventory_partner_domain).trim() : '';
  return `${b}\0${u}\0${d}`;
}

/**
 * Unique domains from pix_inv_web_data as inventory rows (for website / all).
 */
async function collectUniqueWebDomainRecords() {
  const coll = getCollection('pix_inv_web_data');
  const seen = new Set();
  const result = [];
  const cursor = coll.find({}, { projection: WEB_PROJECTION });

  for await (const doc of cursor) {
    const d = domainFromWebDoc(doc);
    if (!d) continue;
    const key = d.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(buildWebsiteInventoryRecord(d));
  }
  return result;
}

/**
 * Extract inventory data from selected pix_inv_* collection(s).
 * - ctv → pix_inv_ctv_data (unique app_bundle + app_ads_txt_url + inventory_partner_domain)
 * - mobile → pix_inv_mobile_data (same triple uniqueness)
 * - website → pix_inv_web_data (unique domain only; ads.txt URL built at match time)
 * - all → three collections; web rows tagged inventorySource pix_inv_web_data
 */
async function extractInventoryData(platform) {
  const collectionNames = INVENTORY_COLLECTIONS[platform];
  if (!collectionNames) return [];

  if (platform === 'website') {
    return collectUniqueWebDomainRecords();
  }

  const result = [];
  const seenTriples = new Set();
  const seenDomains = new Set();

  for (const collName of collectionNames) {
    if (collName === 'pix_inv_web_data') {
      const webRows = await collectUniqueWebDomainRecords();
      for (const row of webRows) {
        const key = row.domain.toLowerCase();
        if (seenDomains.has(key)) continue;
        seenDomains.add(key);
        result.push(row);
      }
      continue;
    }

    const coll = getCollection(collName);
    const cursor = coll.find(
      {},
      { projection: { app_bundle: 1, app_name: 1, app_ads_txt_url: 1, inventory_partner_domain: 1, _id: 0 } }
    );

    for await (const doc of cursor) {
      if (doc.app_bundle == null) continue;
      const tripleKey = inventoryTripleKey(doc);
      if (seenTriples.has(tripleKey)) continue;
      seenTriples.add(tripleKey);
      result.push({
        app_bundle: doc.app_bundle,
        app_name: doc.app_name ?? null,
        app_ads_txt_url: doc.app_ads_txt_url ?? null,
        inventory_partner_domain: doc.inventory_partner_domain ?? null,
      });
    }
  }

  return result;
}

/**
 * Extract app_bundle (and app_name, app_ads_txt_url, inventory_partner_domain) only from records where status === "approved".
 * Returns array unique by (app_bundle, app_ads_txt_url, inventory_partner_domain). Same collections as platform selection.
 */
async function extractApprovedAppBundles(platform) {
  const collectionNames = INVENTORY_COLLECTIONS[platform];
  if (!collectionNames) return [];

  const seenTriples = new Set();
  const result = [];

  for (const collName of collectionNames) {
    const coll = getCollection(collName);
    const cursor = coll.find(
      { status: 'approved' },
      { projection: { app_bundle: 1, app_name: 1, app_ads_txt_url: 1, inventory_partner_domain: 1, _id: 0 } }
    );

    for await (const doc of cursor) {
      if (doc.app_bundle == null) continue;
      const tripleKey = inventoryTripleKey(doc);
      if (seenTriples.has(tripleKey)) continue;
      seenTriples.add(tripleKey);
      result.push({
        app_bundle: doc.app_bundle,
        app_name: doc.app_name ?? null,
        app_ads_txt_url: doc.app_ads_txt_url ?? null,
        inventory_partner_domain: doc.inventory_partner_domain ?? null,
      });
    }
  }

  return result;
}

/**
 * Extract from adsdocs where platform matches payload AND status === "enabled".
 * Used only for YES/NO column list in adlines_match_<platform>.csv (not for the full catalog file).
 */
async function extractEnabledAdsdocsByNameAndAdlines(platform) {
  const coll = getCollection('adsdocs');
  const platformFilter = adsdocsPlatformFilter(platform);

  const cursor = coll.find(
    { ...platformFilter, status: 'enabled' },
    { projection: { name: 1, adlines: 1, _id: 0 } }
  );

  return cursor.toArray();
}

const { buildAndWriteCsv } = require('./adlinesMatcher');

/**
   * Extract from adsdocs: name, adlines, platform.
 * Include only documents where the payload platform is in the document's platform array.
 * For platform "all", include docs whose platform array contains any of ctv, mobile, website.
 */
async function extractAdsdocs(platform) {
  const coll = getCollection('adsdocs');
  const platformFilter = adsdocsPlatformFilter(platform);

  const cursor = coll.find(platformFilter, {
    projection: { name: 1, adlines: 1, platform: 1, _id: 0 },
  });

  return cursor.toArray();
}

/**
 * All adsdocs ads (enabled or not) for catalog JSON.
 */
async function extractAllAdsdocsCatalog() {
  const coll = getCollection('adsdocs');
  const cursor = coll.find(
    {},
    { projection: { name: 1, adlines: 1, status: 1, platform: 1, _id: 0 } }
  );
  return cursor.toArray();
}

/**
 * Process payload: extract inventory + adsdocs, save to JSON file, return result.
 * @param {{ platform: string }} payload - { platform: 'ctv' | 'mobile' | 'website' | 'all' }
 * @param {{ outputDir?: string }} options - optional output directory (default: ./output)
 * @returns {{ success: boolean, filePath?: string, data?: object, error?: string }}
 */
async function processPayload(payload, options = {}) {
  const platform = payload?.platform?.toLowerCase?.();

  if (!platform || !VALID_PLATFORMS.includes(platform)) {
    return {
      success: false,
      error: `Invalid platform. Must be one of: ${VALID_PLATFORMS.join(', ')}`,
    };
  }

  try {
    const outputDir = path.resolve(options.outputDir || 'output');
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }

    const [
      inventoryData,
      adsdocsData,
      enabledAdsdocsByNameAndAdlines,
      allAdsdocsCatalog,
    ] = await Promise.all([
      extractInventoryData(platform),
      extractAdsdocs(platform),
      extractEnabledAdsdocsByNameAndAdlines(platform),
      extractAllAdsdocsCatalog(),
    ]);

    // All apps from inventory (approved or not) — used for approved_app_bundles_<platform>.json and adlines match
    const allAppBundles = inventoryData;

    const approvedPath = path.join(outputDir, approvedAppBundlesFilename(platform));
    const approvedPayload = {
      platform,
      updatedAt: new Date().toISOString(),
      count: allAppBundles.length,
      records: allAppBundles,
    };
    fs.writeFileSync(approvedPath, JSON.stringify(approvedPayload, null, 2), 'utf8');

    // Single catalog: every adsdoc in Mongo (enabled or not), with status
    const catalogPath = path.join(outputDir, ENABLED_APP_ADS_TXT_URL_FILE);
    const catalogPayload = {
      updatedAt: new Date().toISOString(),
      count: allAdsdocsCatalog.length,
      records: allAdsdocsCatalog,
    };
    fs.writeFileSync(catalogPath, JSON.stringify(catalogPayload, null, 2), 'utf8');

    let websiteUniqueDomainsPath;
    if (platform === 'website' || platform === 'all') {
      const webDomainList = allAppBundles
        .filter((r) => r.inventorySource === WEBSITE_INVENTORY_SOURCE)
        .map((r) => r.domain)
        .filter(Boolean)
        .sort();
      websiteUniqueDomainsPath = path.join(outputDir, WEBSITE_UNIQUE_DOMAINS_FILE);
      fs.writeFileSync(
        websiteUniqueDomainsPath,
        JSON.stringify(
          {
            updatedAt: new Date().toISOString(),
            count: webDomainList.length,
            domains: webDomainList,
          },
          null,
          2
        ),
        'utf8'
      );
    }

    const csvPath = await buildAndWriteCsv(
      allAppBundles,
      enabledAdsdocsByNameAndAdlines,
      outputDir,
      platform
    );

    return {
      success: true,
      approvedAppBundlesFile: approvedPath,
      enabledAppAdsTxtUrlFile: catalogPath,
      adlinesMatchCsvFile: csvPath,
      websiteUniqueDomainsFile: websiteUniqueDomainsPath,
      data: {
        platform,
        inventoryRecords: inventoryData.length,
        adsdocsRecords: adsdocsData.length,
        appBundlesCount: allAppBundles.length,
        enabledAdsdocsCount: enabledAdsdocsByNameAndAdlines.length,
        adsdocsCatalogCount: allAdsdocsCatalog.length,
        websiteUniqueDomainCount:
          platform === 'website' || platform === 'all'
            ? allAppBundles.filter((r) => r.inventorySource === WEBSITE_INVENTORY_SOURCE).length
            : undefined,
      },
    };
  } catch (err) {
    return {
      success: false,
      error: err.message,
    };
  }
}

module.exports = {
  processPayload,
  extractInventoryData,
  extractAdsdocs,
  extractApprovedAppBundles,
  extractEnabledAdsdocsByNameAndAdlines,
  extractAllAdsdocsCatalog,
  adsdocsPlatformFilter,
  VALID_PLATFORMS,
  INVENTORY_COLLECTIONS,
  ENABLED_APP_ADS_TXT_URL_FILE,
  WEBSITE_UNIQUE_DOMAINS_FILE,
  approvedAppBundlesFilename,
  adlinesMatchFilename,
  WEBSITE_INVENTORY_SOURCE,
};
