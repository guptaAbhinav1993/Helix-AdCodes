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

/** Match local OS so the default UA looks like Chrome on this machine (same as typical dev browser). */
const ADS_TXT_CHROME_UA = (() => {
  if (process.platform === 'darwin') {
    return 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
  }
  if (process.platform === 'win32') {
    return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
  }
  return 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
})();

const ADS_TXT_CHROME_VER = '120';

/** Many publishers (e.g. CDN/WAF) expect browser-like headers, not a bare script UA. */
const ADS_TXT_FETCH_HEADERS = {
  'User-Agent': ADS_TXT_CHROME_UA,
  Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
};

function secChUaPlatformToken() {
  if (process.platform === 'darwin') return '"macOS"';
  if (process.platform === 'win32') return '"Windows"';
  return '"Linux"';
}

function isChromeLikeUserAgent(ua) {
  const u = String(ua || '');
  if (/postmanruntime/i.test(u)) return false;
  return /chrome|edg|safari|webkit/i.test(u);
}

/**
 * Chrome top-navigation style headers. WAFs often key off Sec-Fetch-* / Client Hints (Node TLS still differs from Chrome).
 * @param {'omnibox' | 'sameOrigin'} profile — typed URL vs follow-from-site
 */
function buildBrowserLikeFetchHeaders(url, userAgent, profile) {
  const origin = new URL(url).origin;
  const cookie = optionalCookieHeaders();
  if (!isChromeLikeUserAgent(userAgent)) {
    return {
      Accept: '*/*',
      'User-Agent': userAgent,
      'Accept-Language': 'en-US,en;q=0.9',
      ...(profile === 'sameOrigin' ? { Referer: `${origin}/` } : {}),
      ...cookie,
    };
  }
  const secFetchSite = profile === 'sameOrigin' ? 'same-origin' : 'none';
  const referer = profile === 'sameOrigin' ? `${origin}/` : undefined;
  const base = {
    Accept: ADS_TXT_FETCH_HEADERS.Accept,
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'User-Agent': userAgent,
    'Upgrade-Insecure-Requests': '1',
    'Sec-Ch-Ua': `"Chromium";v="${ADS_TXT_CHROME_VER}", "Google Chrome";v="${ADS_TXT_CHROME_VER}", "Not_A Brand";v="99"`,
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': secChUaPlatformToken(),
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': secFetchSite,
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
    ...cookie,
  };
  if (referer) {
    base.Referer = referer;
  }
  return base;
}

/** Header profiles to try (order: like pasting URL in bar, then like following a link on the site). */
const ADS_TXT_HEADER_PROFILES = /** @type {const} */ (['omnibox', 'sameOrigin']);

const ADS_TXT_TIMEOUT_MS = (() => {
  const n = Number(process.env.ADS_TXT_FETCH_TIMEOUT_MS);
  if (Number.isFinite(n) && n >= 5000) return Math.min(n, 120000);
  return 45000;
})();

const ADS_TXT_CURL_MAX_SEC = Math.ceil(ADS_TXT_TIMEOUT_MS / 1000);

/** Postman-style UA: some SiteGround setups allow it while blocking default Chrome for certain IPs. */
const ADS_TXT_UA_POSTMAN = 'PostmanRuntime/7.36.3';

function optionalCookieHeaders() {
  const c = process.env.ADS_TXT_COOKIE?.trim();
  if (!c) return {};
  return { Cookie: c };
}

/**
 * User agents to try in order. Override with ADS_TXT_USER_AGENT (tried first). Postman UA matches many manual checks.
 */
function buildAdsTxtUserAgentList() {
  const envUa = process.env.ADS_TXT_USER_AGENT?.trim();
  const list = [];
  if (envUa) list.push(envUa);
  list.push(ADS_TXT_FETCH_HEADERS['User-Agent']);
  list.push(ADS_TXT_UA_POSTMAN);
  return [...new Set(list)];
}

/**
 * @param {string} userAgent
 * @param {'omnibox' | 'sameOrigin'} headerProfile
 * @returns {{ ok: boolean, status: number, text: string, wafChallenge: boolean } | null}
 */
async function fetchTextWithMetaOnce(url, userAgent, headerProfile) {
  try {
    const res = await fetch(url, {
      method: 'GET',
      headers: buildBrowserLikeFetchHeaders(url, userAgent, headerProfile),
      redirect: 'follow',
      signal: AbortSignal.timeout(ADS_TXT_TIMEOUT_MS),
    });
    const text = await res.text();
    const body = text == null ? '' : String(text);
    /** Ads.txt / static .txt should be 200; 202 “Accepted” is often a host captcha or interim page, not the file. */
    const sgHdr = res.headers.get('sg-captcha');
    const sgChallenge = sgHdr && String(sgHdr).toLowerCase().includes('challenge');
    const wafHdr = res.headers.get('x-amzn-waf-action');
    const wafChallenge = wafHdr != null && String(wafHdr).toLowerCase().includes('challenge');
    const ok = res.ok && res.status === 200 && !sgChallenge && !wafChallenge;
    return { ok, status: res.status, text: body, wafChallenge };
  } catch {
    return null;
  }
}

/**
 * Tries several User-Agents (and ADS_TXT_COOKIE if set) until one returns HTTP 200 + usable headers.
 * @returns {{ ok: boolean, status: number, text: string, wafChallenge: boolean } | null}
 */
async function fetchTextWithMeta(url) {
  let last = null;
  for (const ua of buildAdsTxtUserAgentList()) {
    for (const profile of ADS_TXT_HEADER_PROFILES) {
      const meta = await fetchTextWithMetaOnce(url, ua, profile);
      last = meta ?? last;
      if (meta?.ok) return meta;
    }
  }
  return last;
}

/**
 * SiteGround (and similar) returns HTTP 202 + tiny HTML that meta-refreshes to /.well-known/sgcaptcha/
 * — not ads.txt. Browsers may show real ads.txt after cookies/JS; automated GETs often get this shell.
 */
function looksLikeHostingCaptchaHtml(s) {
  if (s == null || !String(s).trim()) return false;
  const t = String(s).slice(0, 8000);
  return /\.well-known\/sgcaptcha/i.test(t) || /\bsg-captcha\b/i.test(t) || /\bsgcaptcha\b/i.test(t);
}

/**
 * Cloudflare / bot walls often block Node’s fetch (distinct TLS/JA3) while curl succeeds on the same host.
 */
function looksLikeBotWallHtml(s) {
  if (s == null || String(s).length < 80) return false;
  if (looksLikeHostingCaptchaHtml(s)) return true;
  const head = String(s).slice(0, 12000).toLowerCase();
  if (head.includes('just a moment') && (head.includes('cloudflare') || head.includes('cf-ray'))) return true;
  if (head.includes('cf-browser-verification')) return true;
  if (head.includes('attention required') && head.includes('cloudflare')) return true;
  if (head.includes('enable javascript') && head.includes('challenge')) return true;
  return false;
}

/**
 * Extra -H args so curl matches the same Chrome navigation profiles as Node fetch.
 */
function curlBrowserHeaderArgs(url, userAgent, headerProfile) {
  const origin = new URL(url).origin;
  if (!isChromeLikeUserAgent(userAgent)) {
    const h = ['-H', 'Accept: */*', '-H', 'Accept-Language: en-US,en;q=0.9'];
    if (headerProfile === 'sameOrigin') {
      h.push('-H', `Referer: ${origin}/`);
    }
    return h;
  }
  const secSite = headerProfile === 'sameOrigin' ? 'same-origin' : 'none';
  const plat = secChUaPlatformToken();
  return [
    '-H',
    `Accept: ${ADS_TXT_FETCH_HEADERS.Accept}`,
    '-H',
    'Accept-Language: en-US,en;q=0.9',
    '-H',
    'Accept-Encoding: gzip, deflate, br',
    '-H',
    'Upgrade-Insecure-Requests: 1',
    '-H',
    `Sec-Ch-Ua: "Chromium";v="${ADS_TXT_CHROME_VER}", "Google Chrome";v="${ADS_TXT_CHROME_VER}", "Not_A Brand";v="99"`,
    '-H',
    'Sec-Ch-Ua-Mobile: ?0',
    '-H',
    `Sec-Ch-Ua-Platform: ${plat}`,
    '-H',
    'Sec-Fetch-Dest: document',
    '-H',
    'Sec-Fetch-Mode: navigate',
    '-H',
    `Sec-Fetch-Site: ${secSite}`,
    '-H',
    'Sec-Fetch-User: ?1',
    '-H',
    'Cache-Control: max-age=0',
    ...(headerProfile === 'sameOrigin' ? ['-H', `Referer: ${origin}/`] : []),
  ];
}

/** Fallback fetch via system curl (browser-like headers, optional cookie). */
async function fetchTextViaCurl(url, extraArgs = [], userAgent, headerProfile = 'omnibox') {
  try {
    const ua = userAgent || ADS_TXT_FETCH_HEADERS['User-Agent'];
    const cookie = process.env.ADS_TXT_COOKIE?.trim();
    /** @type {string[]} */
    const args = [
      '-sS',
      '-L',
      '--compressed',
      '--max-time',
      String(ADS_TXT_CURL_MAX_SEC),
      ...extraArgs,
      '-A',
      ua,
    ];
    if (cookie) {
      args.push('-b', cookie);
    }
    args.push(...curlBrowserHeaderArgs(url, userAgent, headerProfile));
    args.push(url);
    const { stdout } = await execFileAsync('curl', args, { maxBuffer: 15 * 1024 * 1024, encoding: 'utf8' });
    if (stdout == null || stdout === '') return null;
    return String(stdout);
  } catch (e) {
    if (e && (e.code === 'ENOENT' || e.code === 127)) return null;
    return null;
  }
}

/** Curl with http1.1 / ipv4 retries; tries omnibox + same-origin header profiles. */
async function fetchTextViaCurlWithTransportFallbacks(url, userAgent) {
  let best = null;
  let bestScore = -999;
  for (const profile of ADS_TXT_HEADER_PROFILES) {
    let b = await fetchTextViaCurl(url, [], userAgent, profile);
    if (b != null && looksLikeBotWallHtml(b)) b = await fetchTextViaCurl(url, ['--http1.1'], userAgent, profile);
    if (b != null && looksLikeBotWallHtml(b)) b = await fetchTextViaCurl(url, ['--ipv4'], userAgent, profile);
    if (b != null && looksLikeBotWallHtml(b)) b = null;
    const sc = adsTxtResponseQuality(b);
    if (sc > bestScore) {
      bestScore = sc;
      best = b;
    }
    if (sc >= 10000) {
      return b;
    }
  }
  return best;
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
 * @returns {{ body: string|null, diag: { status: number|null, wafChallenge: boolean, networkError: boolean } }}
 */
async function fetchAdsTxtRaw(url) {
  const meta = await fetchTextWithMeta(url);
  const diag = {
    status: meta?.status ?? null,
    wafChallenge: Boolean(meta?.wafChallenge),
    networkError: meta == null,
  };

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
    let bestCurl = null;
    let bestCurlScore = -999;
    for (const ua of buildAdsTxtUserAgentList()) {
      const b = await fetchTextViaCurlWithTransportFallbacks(url, ua);
      const sc = adsTxtResponseQuality(b);
      if (sc > bestCurlScore) {
        bestCurlScore = sc;
        bestCurl = b;
      }
      if (sc >= 10000) {
        curlBody = b;
        break;
      }
    }
    if (curlBody == null) {
      curlBody = bestCurl;
    }
  }

  const curlScore = adsTxtResponseQuality(curlBody);
  let chosen = curlScore > nodeScore ? curlBody : nodeScore > curlScore ? nodeBody : curlBody ?? nodeBody;
  if (chosen != null && String(chosen).trim() === '') {
    chosen = null;
  }
  if (chosen != null) {
    return { body: chosen, diag };
  }
  /** Node/curl scrubbed bot/captcha HTML to null — keep original fetch body for fetchError diagnostics. */
  const metaText = meta?.text != null ? String(meta.text).trim() : '';
  if (metaText) {
    return { body: meta.text, diag };
  }
  return { body: null, diag };
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
 * @param {string|null|undefined} raw
 * @param {{ status?: number|null, wafChallenge?: boolean, networkError?: boolean }} [diag]
 */
function describeUnusableAdsResponse(raw, diag) {
  if (diag?.wafChallenge) {
    return 'blocked by AWS WAF/CloudFront (x-amzn-waf-action: challenge) — HTTP 202 with no ads.txt body; browsers pass a JS challenge that this tool cannot run';
  }
  if (raw == null || raw === '') {
    if (diag?.status === 202) {
      return 'HTTP 202 (not 200) with empty body — usually a CDN/WAF gate, not the ads.txt file';
    }
    return diag?.networkError
      ? 'no response (timeout, connection error, or HTTP error)'
      : 'no usable body (timeout, blocked, or non-200 response)';
  }
  if (looksLikeHostingCaptchaHtml(raw)) {
    return 'hosting captcha page (e.g. SiteGround) instead of ads.txt — allow crawlers/whitelist IP in hosting, or set ADS_TXT_COOKIE (after passing the challenge in a browser) or ADS_TXT_USER_AGENT to match a client that gets HTTP 200';
  }
  if (looksLikeBotWallHtml(raw)) return 'blocked by bot/WAF challenge (e.g. Cloudflare)';
  if (looksLikeHtmlPayload(raw)) {
    return 'HTML response had no extractable ads.txt-style lines (after strip + pattern scan)';
  }
  return 'response had no recognizable ads.txt declaration lines';
}

/**
 * Shared fetch for app-ads.txt / ads.txt URLs (adlinesMatcher + upload flows): Node + curl, browser-like headers,
 * reject 202/captcha shells and non-ads HTML so we do not treat WAF pages as file content.
 * @returns {string|null} body suitable for line-by-line matching (prepared when HTML-wrapped)
 */
async function fetchUrlContentRobust(url) {
  const { body: raw } = await fetchAdsTxtRaw(url);
  if (raw == null || raw === '') return null;
  if (looksLikeBotWallHtml(raw) || looksLikeHostingCaptchaHtml(raw)) return null;
  const prepared = dedupeLinesPreserveOrder(prepareAdsTxtBodyForMatching(raw));
  if (hasPlausibleAdsTxtLines(prepared)) return prepared;
  if (!looksLikeHtmlPayload(raw) && String(raw).trim().length > 0) {
    const nonCommentLines = String(raw)
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter((l) => l && !l.startsWith('#'));
    if (nonCommentLines.length > 0 && nonCommentLines.some((l) => l.includes(','))) {
      return String(raw);
    }
  }
  return null;
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
    const { body: raw, diag } = await fetchAdsTxtRaw(url);
    if (raw == null || raw === '') {
      attemptNotes.push(`${url}: ${describeUnusableAdsResponse(raw, diag)}`);
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
    attemptNotes.push(`${url}: ${describeUnusableAdsResponse(raw, diag)}`);
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
  fetchUrlContentRobust,
  isWebsiteInventoryRow,
};
