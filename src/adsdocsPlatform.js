/**
 * Mongo-style platform filter for adsdocs (same rules as extractEnabledAdsdocsByNameAndAdlines).
 */
function adsdocsPlatformFilter(platform) {
  if (platform === 'all') {
    return { platform: { $in: ['ctv', 'mobile', 'website'] } };
  }
  if (platform === 'mobile') {
    return { platform: { $in: ['mobile'] } };
  }
  if (platform === 'ctv') {
    return { platform: { $in: ['ctv'] } };
  }
  if (platform === 'website') {
    return { platform: { $in: ['website'] } };
  }
  return { platform: platform };
}

/**
 * In-memory match for adsdocs platform field (string or array) vs process platform.
 */
function matchesAdsdocPlatform(doc, platform) {
  const docPlats = Array.isArray(doc.platform) ? doc.platform : doc.platform != null ? [doc.platform] : [];
  if (docPlats.length === 0) return false;
  const filter = adsdocsPlatformFilter(platform);
  if (filter.platform && filter.platform.$in) {
    const allowed = new Set(filter.platform.$in);
    return docPlats.some((p) => allowed.has(p));
  }
  if (filter.platform !== undefined) {
    return docPlats.includes(filter.platform);
  }
  return false;
}

/**
 * From full-catalog records, rows used for YES/NO columns (enabled + platform match).
 * Shape matches extractEnabledAdsdocsByNameAndAdlines: { name, adlines }[].
 */
function filterEnabledAdsdocsForPlatform(records, platform) {
  return records
    .filter((doc) => doc && String(doc.status).toLowerCase() === 'enabled' && matchesAdsdocPlatform(doc, platform))
    .map(({ name, adlines }) => ({ name, adlines }));
}

module.exports = {
  adsdocsPlatformFilter,
  matchesAdsdocPlatform,
  filterEnabledAdsdocsForPlatform,
};
