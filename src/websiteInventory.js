const { execFile } = require('child_process');
const { promisify } = require('util');

const execFileAsync = promisify(execFile);

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

/** Many publishers (e.g. CDN/WAF) return 403 unless a browser-like UA is sent. */
const ADS_TXT_FETCH_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  Accept: 'text/plain,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
};

/**
 * @returns {{ ok: boolean, status: number, text: string } | null}
 */
async function fetchTextWithMeta(url) {
  try {
    const origin = new URL(url).origin;
    const res = await fetch(url, {
      method: 'GET',
      headers: {
        ...ADS_TXT_FETCH_HEADERS,
        Referer: `${origin}/`,
        'Accept-Language': 'en-US,en;q=0.9',
      },
      redirect: 'follow',
      signal: AbortSignal.timeout(20000),
    });
    const text = await res.text();
    return { ok: res.ok, status: res.status, text: text == null ? '' : String(text) };
  } catch {
    return null;
  }
}

/**
 * Cloudflare / bot walls often block Node’s fetch (distinct TLS/JA3) while curl succeeds on the same host.
 */
function looksLikeBotWallHtml(s) {
  if (s == null || String(s).length < 120) return false;
  const head = String(s).slice(0, 12000).toLowerCase();
  if (head.includes('just a moment') && (head.includes('cloudflare') || head.includes('cf-ray'))) return true;
  if (head.includes('cf-browser-verification')) return true;
  if (head.includes('attention required') && head.includes('cloudflare')) return true;
  if (head.includes('enable javascript') && head.includes('challenge')) return true;
  return false;
}

/**
 * Fallback fetch using system curl (same behavior as manual browser/curl checks on many WAF setups).
 * @param {string} url
 * @param {string[]} [extraArgs] e.g. ['--http1.1']
 */
async function fetchTextViaCurl(url, extraArgs = []) {
  try {
    const origin = new URL(url).origin;
    const { stdout } = await execFileAsync(
      'curl',
      [
        '-sS',
        '-L',
        '--compressed',
        '--max-time',
        '30',
        ...extraArgs,
        '-e',
        `${origin}/`,
        '-A',
        ADS_TXT_FETCH_HEADERS['User-Agent'],
        '-H',
        'Accept: text/plain,text/html,application/xhtml+xml;q=0.9,*/*;q=0.8',
        '-H',
        'Accept-Language: en-US,en;q=0.9',
        url,
      ],
      { maxBuffer: 15 * 1024 * 1024, encoding: 'utf8' }
    );
    if (stdout == null || stdout === '') return null;
    return String(stdout);
  } catch (e) {
    if (e && (e.code === 'ENOENT' || e.code === 127)) return null;
    return null;
  }
}

/**
 * Prefer the response that actually contains usable ads.txt (after HTML extraction).
 * Node often gets 403 + generic HTML while curl gets 200 + plain ads.txt from the same host.
 */
function adsTxtResponseQuality(body) {
  if (body == null || body === '') return 0;
  if (looksLikeBotWallHtml(body)) return -100;
  const prep = prepareAdsTxtBodyForMatching(body);
  if (hasPlausibleAdsTxtLines(prep)) return 10000 + prep.length;
  if (!looksLikeHtmlPayload(body) && body.trim().length > 80) return 100 + body.trim().length;
  if (looksLikeHtmlPayload(body)) return 1;
  return 10 + body.trim().length;
}

/**
 * Try Node fetch + curl; pick best body. Always try curl when Node is not OK or has no extractable ads.
 */
async function fetchAdsTxtRaw(url) {
  const meta = await fetchTextWithMeta(url);
  let nodeBody = meta?.text ?? null;
  if (nodeBody != null && looksLikeBotWallHtml(nodeBody)) {
    nodeBody = null;
  }

  const nodeScore = adsTxtResponseQuality(nodeBody);
  const needCurl =
    meta == null ||
    nodeBody == null ||
    !meta.ok ||
    nodeScore < 10000;

  let curlBody = null;
  if (needCurl) {
    curlBody = await fetchTextViaCurl(url);
    if (curlBody != null && looksLikeBotWallHtml(curlBody)) {
      curlBody = await fetchTextViaCurl(url, ['--http1.1']);
    }
    if (curlBody != null && looksLikeBotWallHtml(curlBody)) {
      curlBody = await fetchTextViaCurl(url, ['--ipv4']);
    }
    if (curlBody != null && looksLikeBotWallHtml(curlBody)) {
      curlBody = null;
    }
  }

  const curlScore = adsTxtResponseQuality(curlBody);
  if (curlScore > nodeScore) return curlBody;
  if (nodeScore > curlScore) return nodeBody;
  return curlBody ?? nodeBody;
}

function looksLikeHtmlPayload(s) {
  if (s == null || s.length === 0) return false;
  const head = String(s).slice(0, 4096).trimStart();
  return (
    /^<!DOCTYPE/i.test(head) ||
    /^<\s*html[\s>]/i.test(head) ||
    /<\s*head[\s>]/i.test(head) ||
    /<\s*body[\s>]/i.test(head)
  );
}

function decodeHtmlEntities(str) {
  if (str == null) return '';
  return String(str)
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)))
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCharCode(parseInt(h, 16)));
}

/** Commas and whitespace variants that appear in encoded or CMS-stored ads snippets. */
function normalizeAdsExtractionText(s) {
  return decodeHtmlEntities(String(s))
    .replace(/%2c/gi, ',')
    .replace(/\uFF0C/g, ',')
    .replace(/\t/g, ' ')
    .replace(/\r\n/g, '\n');
}

/**
 * Declarations are often stored in value= / content= / data-*= and are LOST when tags are stripped wholesale.
 */
function extractQuotedAttributeBlobs(html) {
  const chunks = [];
  const re = /\b(?:value|content|data-[a-z0-9_.-]+)\s*=\s*("([^"]*)"|'([^']*)')/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const inner = normalizeAdsExtractionText(m[2] ?? m[3] ?? '').trim();
    if (inner.length >= 12 && (/,/.test(inner) || /pub-\d/i.test(inner))) {
      chunks.push(inner);
    }
  }
  return chunks.join('\n');
}

function extractHtmlComments(html) {
  const chunks = [];
  const re = /<!--([\s\S]*?)-->/g;
  let m;
  while ((m = re.exec(html)) !== null) {
    const t = normalizeAdsExtractionText(m[1]).trim();
    if (t.length >= 8) chunks.push(t);
  }
  return chunks.join('\n\n');
}

/** Raw &lt;script&gt; bodies (excluding obvious JSON-LD @context) — sometimes hold pasted ads.txt. */
function extractScriptInnerRaw(html) {
  const chunks = [];
  const re = /<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const t = m[1].trim();
    if (t.length < 20) continue;
    if (/"@context"\s*:\s*"https?:\/\/schema\.org"/i.test(t) && !/RESELLER|DIRECT/i.test(t)) continue;
    chunks.push(normalizeAdsExtractionText(t));
  }
  return chunks.join('\n\n');
}

function extractApproxBodyInner(html) {
  const m = String(html).match(/<body[^>]*>([\s\S]*?)<\/body>/i);
  if (!m) return '';
  return normalizeAdsExtractionText(m[1]);
}

/**
 * Many sites serve ads.txt inside &lt;pre&gt;, &lt;textarea&gt;, or &lt;code&gt; within an HTML shell.
 */
function extractPreTextareaCodeInnerHtml(html) {
  const re = /<(?:pre|textarea|code)(?:\s[^>]*)?>([\s\S]*?)<\/(?:pre|textarea|code)>/gi;
  const chunks = [];
  let m;
  while ((m = re.exec(html)) !== null) {
    const inner = decodeHtmlEntities(m[1]).trim();
    if (inner) chunks.push(inner);
  }
  return chunks.join('\n\n');
}

/**
 * Pull IAB-style declaration rows out of noisy text (HTML remnants, attributes, minified blobs).
 * Uses a tolerant middle field and works on whitespace-collapsed single-line strings too.
 */
function extractAdsDeclarationLinesByRegex(text) {
  if (text == null || !String(text).trim()) return '';
  const s = String(text);
  const re =
    /(?:^|[^\w.-])([*a-z0-9][a-z0-9.\-*]*)\s*,\s*([^,\r\n<]{1,220}?)\s*,\s*(RESELLER|DIRECT|OWNERDOMAIN)(?:\s*,\s*([a-zA-Z0-9]+))?/gi;
  const seen = new Set();
  const lines = [];
  let m;
  while ((m = re.exec(s)) !== null) {
    const domain = m[1].trim();
    if (domain.length < 1) continue;
    if (
      /^(var|let|const|function|return|window|document|padding|margin|font|display|src|type|title|path|fill|stroke|opacity)$/i.test(
        domain
      )
    ) {
      continue;
    }
    const id = m[2].trim().replace(/\s+/g, ' ');
    if (!id) continue;
    const line = `${domain}, ${id}, ${m[3]}${m[4] ? `, ${m[4]}` : ''}`;
    const key = line.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      lines.push(line);
    }
  }
  return lines.join('\n');
}

/** One-line blob so declarations split across tags still match (e.g. domain&gt;&lt;/span&gt;, &lt;span&gt;pub-…). */
function extractAdsLinesFromCollapsedWhitespace(text) {
  const flat = normalizeAdsExtractionText(text).replace(/\s+/g, ' ').trim();
  return extractAdsDeclarationLinesByRegex(flat);
}

/**
 * Turn misconfigured “ads.txt” HTML pages into newline-separated text so adline matchers can scan lines.
 */
function stripHtmlToPlainTextForAdsScan(html) {
  let s = String(html);
  s = s.replace(/<script[\s\S]*?<\/script>/gi, '\n');
  s = s.replace(/<style[\s\S]*?<\/style>/gi, '\n');
  s = s.replace(/<br\s*\/?>/gi, '\n');
  s = s.replace(/<\/(p|div|tr|td|th|li|h[1-6])\s*>/gi, '\n');
  s = s.replace(/<[^>]+>/g, '\n');
  return s.replace(/\n{3,}/g, '\n\n').trim();
}

function dedupeLinesPreserveOrder(text) {
  if (text == null || !String(text).trim()) return '';
  const seen = new Set();
  const out = [];
  for (const line of String(text).split(/\r?\n/)) {
    const t = line.trim();
    if (!t) continue;
    const k = t.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(t);
  }
  return out.join('\n');
}

/** True if text has at least one non-comment line that looks like an ads.txt / app-ads declaration row. */
function hasPlausibleAdsTxtLines(text) {
  if (text == null || !String(text).trim()) return false;
  return String(text).split(/\r?\n/).some((line) => {
    const L = line.trim();
    if (!L || L.startsWith('#')) return false;
    if (!/,/.test(L)) return false;
    if (/(RESELLER|DIRECT|OWNERDOMAIN)/i.test(L)) return true;
    if (/\bpub-\d+/i.test(L)) return true;
    return false;
  });
}

/**
 * Normalize body for adline matching: plain ads.txt as-is; HTML → attributes, comments, scripts, pre/code,
 * stripped text, tagless text, collapsed scan, and regex on raw (403 shells often hide rows in markup).
 */
function prepareAdsTxtBodyForMatching(raw) {
  if (raw == null || raw === '') return '';
  if (!looksLikeHtmlPayload(raw)) {
    return String(raw);
  }
  const html = String(raw);
  const fromAttrs = extractQuotedAttributeBlobs(html);
  const fromComments = extractHtmlComments(html);
  const fromScripts = extractScriptInnerRaw(html);
  const fromBlocks = extractPreTextareaCodeInnerHtml(html);
  const fromBody = extractApproxBodyInner(html);
  const stripped = normalizeAdsExtractionText(stripHtmlToPlainTextForAdsScan(html));
  const noTags = normalizeAdsExtractionText(
    html
      .replace(/<script[\s\S]*?<\/script>/gi, '\n')
      .replace(/<style[\s\S]*?<\/style>/gi, '\n')
      .replace(/<[^>]+>/g, '\n')
  );
  const rawDecoded = normalizeAdsExtractionText(html);
  const blob = [
    fromAttrs,
    fromComments,
    fromScripts,
    fromBlocks,
    fromBody,
    stripped,
    noTags,
  ]
    .filter(Boolean)
    .join('\n');

  const fromRegexBlob = extractAdsDeclarationLinesByRegex(blob);
  const fromRegexRaw = extractAdsDeclarationLinesByRegex(rawDecoded);
  const fromCollapsedBlob = extractAdsLinesFromCollapsedWhitespace(blob);
  const fromCollapsedRaw = extractAdsLinesFromCollapsedWhitespace(rawDecoded);

  const regexMerged = [fromRegexBlob, fromRegexRaw, fromCollapsedBlob, fromCollapsedRaw].filter(Boolean).join('\n');
  if (regexMerged) {
    return [blob, regexMerged].join('\n');
  }
  return blob;
}

/**
 * Human-readable hint for why a URL did not yield usable ads.txt.
 * @param {string|null} raw
 */
function describeUnusableAdsResponse(raw) {
  if (raw == null || raw === '') return 'no response (timeout, connection error, or HTTP error)';
  if (looksLikeBotWallHtml(raw)) return 'blocked by bot/WAF challenge (e.g. Cloudflare)';
  if (looksLikeHtmlPayload(raw)) {
    return 'HTML response had no extractable ads.txt-style lines (after strip + pattern scan)';
  }
  return 'response had no recognizable ads.txt declaration lines';
}

/**
 * Try ads.txt URLs until one yields usable declaration lines.
 * Order: HTTPS first (common TLS-only hosts), then HTTP; www variants — many sites block non-browser UAs without HTTPS.
 * @returns {{ content: string, effectiveUrl: string|null, fetchError: string }}
 */
async function fetchWebsiteAdsTxtWithFallbacks(canonicalDomain) {
  const host = String(canonicalDomain).trim().replace(/^www\./i, '');
  if (!host) {
    return { content: '', effectiveUrl: null, fetchError: 'Empty hostname after normalization' };
  }
  const candidates = [
    `https://${host}/ads.txt`,
    `https://www.${host}/ads.txt`,
    `http://${host}/ads.txt`,
    `http://www.${host}/ads.txt`,
  ];
  /** @type {string[]} */
  const attemptNotes = [];
  for (const url of candidates) {
    const raw = await fetchAdsTxtRaw(url);
    if (raw == null) {
      attemptNotes.push(`${url}: ${describeUnusableAdsResponse(null)}`);
      continue;
    }
    const prepared = dedupeLinesPreserveOrder(prepareAdsTxtBodyForMatching(raw));
    if (hasPlausibleAdsTxtLines(prepared)) {
      return { content: prepared, effectiveUrl: url, fetchError: '' };
    }
    /**
     * Plain-text ads.txt without RESELLER/pub keywords — accept only if it looks like real rows, not a one-line error.
     */
    if (!looksLikeHtmlPayload(raw) && String(raw).trim().length > 0) {
      const nonCommentLines = String(raw)
        .split(/\r?\n/)
        .map((l) => l.trim())
        .filter((l) => l && !l.startsWith('#'));
      if (nonCommentLines.length > 0 && nonCommentLines.some((l) => l.includes(','))) {
        return { content: String(raw), effectiveUrl: url, fetchError: '' };
      }
    }
    attemptNotes.push(`${url}: ${describeUnusableAdsResponse(raw)}`);
  }
  const summary = attemptNotes.length
    ? attemptNotes.join(' | ')
    : 'No ads.txt URLs were tried';
  return {
    content: '',
    effectiveUrl: null,
    fetchError: `Could not load valid ads.txt for ${host}. ${summary}`,
  };
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
