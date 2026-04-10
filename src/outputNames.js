/** Single file: every adsdoc from Mongo (enabled or not), with status on each record */
const ENABLED_APP_ADS_TXT_URL_FILE = 'enabled_app_ads_txt_url.json';

/** Unique website domains from pix_inv_web_data (website or all runs) */
const WEBSITE_UNIQUE_DOMAINS_FILE = 'website_unique_domains.json';

function approvedAppBundlesFilename(platform) {
  return `approved_app_bundles_${platform}.json`;
}

/**
 * DD_MM_YYYY_HH_MM in local time, e.g. 08_04_2026_16_17
 * @param {Date} [date]
 */
function filenameTimestamp(date = new Date()) {
  const d = date instanceof Date ? date : new Date();
  const pad = (n) => String(n).padStart(2, '0');
  return `${pad(d.getDate())}_${pad(d.getMonth() + 1)}_${d.getFullYear()}_${pad(d.getHours())}_${pad(d.getMinutes())}`;
}

/**
 * @param {string} platform
 * @param {Date} [at] - defaults to now when the file is written
 */
function adlinesMatchFilename(platform, at = new Date()) {
  return `adlines_match_${platform}_${filenameTimestamp(at)}.csv`;
}

module.exports = {
  ENABLED_APP_ADS_TXT_URL_FILE,
  WEBSITE_UNIQUE_DOMAINS_FILE,
  approvedAppBundlesFilename,
  filenameTimestamp,
  adlinesMatchFilename,
};
