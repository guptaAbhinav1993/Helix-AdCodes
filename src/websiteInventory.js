const WEBSITE_INVENTORY_SOURCE = 'pix_inv_web_data';

/**
 * Parse host from URL or return bare hostname (no path, lowercase).
 */
function normalizeWebsiteDomainHost(raw) {
  if (raw == null) return null;
  let s = String(raw).trim().toLowerCase();
  if (!s) return null;
  if (/^https?:\/\//i.test(s) || s.includes('/')) {
    try {
      const withProto = /^https?:\/\//i.test(s) ? s : `https://${s}`;
      const u = new URL(withProto);
      return u.hostname.replace(/^www\./i, '') || null;
    } catch {
      return null;
    }
  }
  s = s.replace(/^www\./i, '');
  const host = s.split('/')[0].split(':')[0];
  return host || null;
}

/**
 * Resolve domain from a pix_inv_web_data document (field fallbacks).
 */
function domainFromWebDoc(doc) {
  if (!doc) return null;
  const candidates = [doc.domain, doc.website_domain, doc.site_domain, doc.publisher_domain, doc.url];
  for (const c of candidates) {
    const h = normalizeWebsiteDomainHost(c);
    if (h) return h;
  }
  return null;
}

function buildWebsiteInventoryRecord(domain) {
  return {
    domain,
    app_bundle: domain,
    app_name: domain,
    app_ads_txt_url: null,
    inventory_partner_domain: null,
    inventorySource: WEBSITE_INVENTORY_SOURCE,
  };
}

async function fetchTextIfOk(url) {
  try {
    const res = await fetch(url, {
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
 * Try ads.txt URLs in order until one returns a successful response body.
 * 1) http://{domain}/ads.txt
 * 2) http://www.{domain}/ads.txt
 * 3) https://{domain}/ads.txt
 * 4) https://www.{domain}/ads.txt
 */
async function fetchWebsiteAdsTxtWithFallbacks(canonicalDomain) {
  const host = String(canonicalDomain).trim().replace(/^www\./i, '');
  if (!host) return { content: '', effectiveUrl: null };
  const candidates = [
    `http://${host}/ads.txt`,
    `http://www.${host}/ads.txt`,
    `https://${host}/ads.txt`,
    `https://www.${host}/ads.txt`,
  ];
  for (const url of candidates) {
    const text = await fetchTextIfOk(url);
    if (text != null) return { content: text, effectiveUrl: url };
  }
  return { content: '', effectiveUrl: null };
}

function isWebsiteInventoryRow(record) {
  return record && record.inventorySource === WEBSITE_INVENTORY_SOURCE;
}

module.exports = {
  WEBSITE_INVENTORY_SOURCE,
  normalizeWebsiteDomainHost,
  domainFromWebDoc,
  buildWebsiteInventoryRecord,
  fetchWebsiteAdsTxtWithFallbacks,
  isWebsiteInventoryRow,
};
