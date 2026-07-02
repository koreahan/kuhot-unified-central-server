
import express from 'express';
import cors from 'cors';
import crypto from 'crypto';
import pg from 'pg';
import { Expo } from 'expo-server-sdk';

const { Pool } = pg;
const app = express();
const expo = new Expo();

const PORT = Number(process.env.PORT || 8787);
const SERVER_VERSION = 'v073-no-server-partners-pass-through';
const HEAVY_MAX_ACTIVE = Number(process.env.HEAVY_MAX_ACTIVE || 12);
const HEAVY_RETRY_AFTER_MS = Number(process.env.HEAVY_RETRY_AFTER_MS || 10000);
const DB_QUERY_TIMEOUT_MS = Number(process.env.DB_QUERY_TIMEOUT_MS || 2500);
const DB_CONNECT_TIMEOUT_MS = Number(process.env.DB_CONNECT_TIMEOUT_MS || 1500);
const STATS_TIMEOUT_MS = Number(process.env.STATS_TIMEOUT_MS || 1500);
const OBSERVE_BATCH_LIMIT = Number(process.env.OBSERVE_BATCH_LIMIT || 120);
const EMUL_STATS_BATCH_LIMIT = Number(process.env.EMUL_STATS_BATCH_LIMIT || 120);
const DATABASE_URL = process.env.DATABASE_URL || '';
const INGEST_KEY = process.env.INGEST_KEY || '';
const ALERT_RETENTION_MS = Number(process.env.ALERT_RETENTION_MS || 24 * 60 * 60 * 1000); // 앱 알림함 기본 24시간 보관
const PRICE_RETENTION_MS = Number(process.env.PRICE_RETENTION_MS || 7 * 24 * 60 * 60 * 1000); // 가격 관측 DB 기본 7일 보관
const PRUNE_INTERVAL_MS = Number(process.env.PRUNE_INTERVAL_MS || 10 * 60 * 1000); // v056: 요청마다 prune하지 않고 주기적으로만 정리
const COLLECTOR_KEY = process.env.COLLECTOR_KEY || INGEST_KEY || '';
const OBS_BUCKET_MS = Number(process.env.OBS_BUCKET_MS || 60 * 60 * 1000); // 같은 상품/옵션/가격은 기본 1시간 1건만 관측 저장
const ALERT_MIN_AVG_DROP_PCT = Number(process.env.ALERT_MIN_AVG_DROP_PCT || 20);
const ALERT_REQUIRE_LOW_MATCH = String(process.env.ALERT_REQUIRE_LOW_MATCH || 'true').toLowerCase() !== 'false';
const MIN_HISTORY_COUNT = Number(process.env.MIN_HISTORY_COUNT || 2);
const COUPANG_PARTNERS_ACCESS_KEY = process.env.COUPANG_PARTNERS_ACCESS_KEY || process.env.CP_ACCESS_KEY || '';
const COUPANG_PARTNERS_SECRET_KEY = process.env.COUPANG_PARTNERS_SECRET_KEY || process.env.CP_SECRET_KEY || '';
const COUPANG_PARTNERS_SUB_ID = process.env.COUPANG_PARTNERS_SUB_ID || process.env.CP_SUB_ID || '';
const PARTNERS_DEEPLINK_DISABLE_API = String(process.env.PARTNERS_DEEPLINK_DISABLE_API || 'false').toLowerCase() === 'true';
const PARTNERS_DEEPLINK_CACHE_MS = Number(process.env.PARTNERS_DEEPLINK_CACHE_MS || 6 * 60 * 60 * 1000);
const PARTNERS_DEEPLINK_FAIL_CACHE_MS = Number(process.env.PARTNERS_DEEPLINK_FAIL_CACHE_MS || 2 * 60 * 1000);
const PARTNERS_DEEPLINK_CIRCUIT_MS = Number(process.env.PARTNERS_DEEPLINK_CIRCUIT_MS || 70 * 1000);
const partnersDeeplinkCache = new Map();
const partnersDeeplinkFailCache = new Map();
let partnersDeeplinkCircuitOpenUntil = 0;
const FAST_NOTIFY_RESPONSE = String(process.env.FAST_NOTIFY_RESPONSE || 'true').toLowerCase() !== 'false'; // v061: 텔레그램/푸시는 백그라운드로 보내고 응답은 먼저 반환
const SKIP_SILENT_OBSERVE_STATS = String(process.env.SKIP_SILENT_OBSERVE_STATS || 'false').toLowerCase() === 'true'; // v062: 평균/최저 보존 기본값. true로 넣은 경우에만 무알림 관측 stats 생략
const OBSERVE_STATS_TIMEOUT_MS = Number(process.env.OBSERVE_STATS_TIMEOUT_MS || STATS_TIMEOUT_MS);
const STATS_CACHE_TTL_MS = Number(process.env.STATS_CACHE_TTL_MS || 60 * 1000); // v062: 평균/최저 조회 결과 60초 메모리 캐시
const STATS_ENABLE_TITLE_ILIKE = String(process.env.STATS_ENABLE_TITLE_ILIKE || 'false').toLowerCase() === 'true'; // v062: 느린 title ILIKE 기본 비활성화
const DAILY_SAVE_ENABLE_TITLE_ILIKE = String(process.env.DAILY_SAVE_ENABLE_TITLE_ILIKE || 'false').toLowerCase() === 'true'; // v064: 일일 저장정책의 느린 title ILIKE도 기본 비활성화
const STATS_SMART_SCAN_ENABLE = String(process.env.STATS_SMART_SCAN_ENABLE || 'false').toLowerCase() === 'true'; // v064: 5000-row JS smart scan 기본 비활성화, exact aggregate 우선
const PERF_LOG_ENABLED = String(process.env.PERF_LOG_ENABLED || 'true').toLowerCase() !== 'false'; // v065: 느린 요청 단계별 시간 로그
const PERF_SLOW_MS = Number(process.env.PERF_SLOW_MS || 3000);
const PERF_DEBUG_RESPONSE = String(process.env.PERF_DEBUG_RESPONSE || 'true').toLowerCase() !== 'false';
const BOOT_SCHEMA_INDEXES = String(process.env.BOOT_SCHEMA_INDEXES || process.env.BOOT_FAST_INDEXES || 'false').toLowerCase() === 'true'; // v068: 부팅 중 모든 CREATE INDEX 기본 금지. 대용량 DB 배포 실패 방지
const BOOT_FAST_INDEXES = BOOT_SCHEMA_INDEXES;


app.use(cors());
app.use(express.json({ limit: '2mb' }));

// [서버] v053: 폭주 방어. DB가 느려져도 /health는 무조건 가볍게 살아있고,
// PC/에뮬 요청이 몰리면 서버가 오래 물고 있지 않고 503 + Retry-After로 빠르게 반환한다.
let activeHeavyRequests = 0;
let rejectedHeavyRequests = 0;
let timedOutStatsRequests = 0;
const startedAt = Date.now();

function isHeavyPath(path = '') {
  return path.startsWith('/collector/') || path === '/telegram/ingest' || path === '/telegram-ingest' || path === '/ingest';
}

function busyPayload(extra = {}) {
  return {
    ok: false,
    retry: true,
    reason: 'SERVER_BUSY',
    retryAfterMs: HEAVY_RETRY_AFTER_MS,
    activeHeavyRequests,
    maxActiveHeavyRequests: HEAVY_MAX_ACTIVE,
    ...extra
  };
}

app.use((req, res, next) => {
  if (!isHeavyPath(req.path)) return next();
  if (activeHeavyRequests >= HEAVY_MAX_ACTIVE) {
    rejectedHeavyRequests += 1;
    res.set('Retry-After', String(Math.ceil(HEAVY_RETRY_AFTER_MS / 1000)));
    return res.status(503).json(busyPayload({ path: req.path }));
  }
  activeHeavyRequests += 1;
  res.on('finish', () => { activeHeavyRequests = Math.max(0, activeHeavyRequests - 1); });
  res.on('close', () => { activeHeavyRequests = Math.max(0, activeHeavyRequests - 1); });
  return next();
});

function timeoutPromise(ms, label = 'TIMEOUT') {
  return new Promise((_, reject) => setTimeout(() => reject(new Error(label)), ms));
}

async function withTimeout(promise, ms, label = 'TIMEOUT') {
  return Promise.race([promise, timeoutPromise(ms, label)]);
}

let backgroundTelegramQueued = 0;
let backgroundTelegramSent = 0;
let backgroundTelegramFailed = 0;
let backgroundPushQueued = 0;
let backgroundPushSent = 0;
let backgroundPushFailed = 0;

function runBackground(label, fn) {
  setImmediate(async () => {
    try { await fn(); }
    catch (e) { console.warn(`[background:${label}] failed`, String(e?.message || e)); }
  });
}

function dispatchTelegramPush(alert) {
  if (!alert) return { telegram: { queued: false, skipped: true }, push: { queued: false, skipped: true } };
  backgroundTelegramQueued += 1;
  backgroundPushQueued += 1;
  runBackground('telegram', async () => {
    try {
      const r = await sendTelegram(alert);
      if (r?.sent) backgroundTelegramSent += 1;
      else backgroundTelegramFailed += 1;
    } catch (e) {
      backgroundTelegramFailed += 1;
      console.warn('[telegram-bg] failed', String(e?.message || e));
    }
  });
  runBackground('push', async () => {
    try {
      const r = await sendPush(alert);
      backgroundPushSent += n(r?.sent || 0);
    } catch (e) {
      backgroundPushFailed += 1;
      console.warn('[push-bg] failed', String(e?.message || e));
    }
  });
  return { telegram: { queued: true, background: true }, push: { queued: true, background: true } };
}

async function sendTelegramPushForResponse(alert) {
  if (!alert) return { telegram: { sent: false, skipped: true }, push: { sent: 0, skipped: true } };
  if (FAST_NOTIFY_RESPONSE) return dispatchTelegramPush(alert);
  const telegram = await sendTelegram(alert);
  const push = await sendPush(alert);
  return { telegram, push };
}

function emptyStatsFallback(obs, reason = 'stats_timeout') {
  const cutoff = Date.now() - PRICE_RETENTION_MS;
  const variants = (() => { try { return productKeyLookupVariants(obs); } catch { return []; } })();
  const base = {
    count: 0,
    avg: 0,
    low: 0,
    high: 0,
    dbAvg: 0,
    dbLow: 0,
    dbHigh: 0,
    avgSource: reason,
    cutoff,
    match: reason,
    productKeyVariants: variants
  };
  try { return applyClientFallbackStats(base, obs?.raw || {}, Number(obs?.price || 0)); }
  catch { return base; }
}

function statsCacheKeyForObservation(obs = {}) {
  const wanted = (() => { try { return looseIdentityFromParts(obs.title, obs.option); } catch { return {}; } })();
  const titleKey = wanted.titleKey || obs.titleKey || normKey(obs.title || '');
  const optionKey = wanted.optionMatchKey || canonicalOptionMatchKey(obs.option || obs.optionKey || '');
  return `${obs.productKey || ''}|${titleKey || ''}|${optionKey || ''}`;
}

function getCachedObservationStats(obs) {
  if (!STATS_CACHE_TTL_MS || STATS_CACHE_TTL_MS <= 0) return null;
  const key = statsCacheKeyForObservation(obs);
  const hit = statsMemoryCache.get(key);
  if (!hit) return null;
  if (Date.now() - n(hit.cachedAt) > STATS_CACHE_TTL_MS) {
    statsMemoryCache.delete(key);
    return null;
  }
  return {
    ...hit.stats,
    fromMemoryCache: true,
    match: `${hit.stats?.match || 'stats'}_memory_cache`
  };
}

function setCachedObservationStats(obs, stats) {
  if (!STATS_CACHE_TTL_MS || STATS_CACHE_TTL_MS <= 0) return;
  if (!stats || n(stats.avg) <= 0 || n(stats.count) <= 0) return;
  const key = statsCacheKeyForObservation(obs);
  statsMemoryCache.set(key, { cachedAt: Date.now(), stats: { ...stats } });
  if (statsMemoryCache.size > 2000) {
    const firstKey = statsMemoryCache.keys().next().value;
    if (firstKey) statsMemoryCache.delete(firstKey);
  }
}


let pool = null;
const memory = { devices: new Map(), alerts: [], opens: [], observations: [], emulStats: [] };
const statsMemoryCache = new Map();
let lastPruneAt = 0;
let lastPruneResult = null;
let pruneInFlight = null;

if (DATABASE_URL) {
  pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false },
    max: Number(process.env.PG_POOL_MAX || 5),
    connectionTimeoutMillis: DB_CONNECT_TIMEOUT_MS,
    idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || 10000),
    query_timeout: DB_QUERY_TIMEOUT_MS,
    statement_timeout: DB_QUERY_TIMEOUT_MS
  });
}

function now() { return Date.now(); }
function id(prefix) { return `${prefix}_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`; }
function s(v, max = 500) { return String(v ?? '').trim().slice(0, max); }
function n(v) { const x = Number(v || 0); return Number.isFinite(x) ? Math.round(x) : 0; }
function f(v) { const x = Number(v || 0); return Number.isFinite(x) ? x : 0; }

function makePerf(label, meta = {}) {
  const t0 = Date.now();
  let last = t0;
  const steps = [];
  return {
    step(name, extra = {}) {
      const ts = Date.now();
      steps.push({ name, ms: ts - last, atMs: ts - t0, ...extra });
      last = ts;
    },
    done(extra = {}) {
      const totalMs = Date.now() - t0;
      const out = { label, totalMs, steps, ...extra };
      if (PERF_LOG_ENABLED && totalMs >= PERF_SLOW_MS) {
        const line = steps.map(x => `${x.name}=${x.ms}ms@${x.atMs}`).join(' | ');
        console.warn(`[perf:${label}] total=${totalMs}ms ${line}`, JSON.stringify({ ...meta, ...extra }).slice(0, 1400));
      }
      return out;
    }
  };
}


// [서버] v060: 폴센트/에뮬에서 잠근 상품명·옵션·가격은 최종 발송 기준값이다.
// 쿠팡/PC 상세 파싱값은 링크 검증용으로만 쓰고, price/finalPrice/payPrice로 덮어쓰지 않는다.
function truthy(v) {
  return v === true || String(v ?? '').trim().toLowerCase() === 'true' || String(v ?? '').trim() === '1';
}

function isFallcentLockedPayload(body = {}) {
  const raw = body && typeof body.raw === 'object' ? body.raw : {};
  const nested = raw && typeof raw.raw === 'object' ? raw.raw : {};
  const vals = [
    body.priceSource, raw.priceSource, nested.priceSource,
    body.lockedBy, raw.lockedBy, nested.lockedBy,
    body.source, raw.source, nested.source,
    body.workerId, raw.workerId, nested.workerId,
    body.from, raw.from, nested.from
  ].map(v => String(v ?? '').trim().toLowerCase());

  return Boolean(
    truthy(body.noCoupangPriceOverride) || truthy(raw.noCoupangPriceOverride) || truthy(nested.noCoupangPriceOverride) ||
    truthy(body.linkOnlyVerified) || truthy(raw.linkOnlyVerified) || truthy(nested.linkOnlyVerified) ||
    vals.some(v => v.includes('fallcent') || v.includes('hotdeal_app') || v.includes('emulator_hotdeal') || v.includes('emu_hotdeal'))
  );
}

function lockedFallcentPrice(body = {}) {
  const raw = body && typeof body.raw === 'object' ? body.raw : {};
  const nested = raw && typeof raw.raw === 'object' ? raw.raw : {};
  const candidates = [
    body.lockedPrice, body.fallcentPrice, body.fallcentLockedPrice, body.hotdealAppPrice, body.appLockedPrice,
    raw.lockedPrice, raw.fallcentPrice, raw.fallcentLockedPrice, raw.hotdealAppPrice, raw.appLockedPrice,
    nested.lockedPrice, nested.fallcentPrice, nested.fallcentLockedPrice, nested.hotdealAppPrice, nested.appLockedPrice
  ];
  for (const v of candidates) {
    const p = n(v);
    if (p > 0) return p;
  }
  if (isFallcentLockedPayload(body)) {
    const p = n(body.price || raw.price || nested.price);
    if (p > 0) return p;
  }
  return 0;
}

function alertPriceFromBody(body = {}) {
  const locked = lockedFallcentPrice(body);
  if (locked > 0) return locked;
  return n(body.price || body.payPrice);
}

function observationPriceFromBody(body = {}) {
  const locked = lockedFallcentPrice(body);
  if (locked > 0) return locked;
  return n(body.price || body.payPrice || body.finalPrice);
}


// [서버] v3.184: 로켓프레시/로켓직구/쿠팡직구는 템플릿 표시용 배지일 뿐,
// productKey/titleKey/optionKey/중복키에는 절대 넣지 않는다.
function stripDeliveryBadgeForKey(v = '') {
  return String(v || '')
    // [로켓프레시❄️], [로켓직구🌏], [쿠팡직구] 같은 대괄호 배지 제거
    .replace(/\s*\[\s*(?:로켓\s*프레시|로켓프레시|로켓\s*직구|로켓직구|쿠팡\s*직구|쿠팡직구|rocket\s*fresh|rocket\s*global)[^\]]*\]\s*/giu, ' ')
    // 대괄호 없이 붙은 배송/직구 배지도 키에서는 제거
    .replace(/\s*(?:로켓\s*프레시|로켓프레시|로켓\s*직구|로켓직구|쿠팡\s*직구|쿠팡직구|rocket\s*fresh|rocket\s*global)\s*/giu, ' ')
    // 이모지만 남은 경우 제거
    .replace(/[❄️🌏]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function productKeyTextVariantsFromTitle(title = '') {
  const displayTitle = cleanTitleText(title || '');
  const strippedTitle = stripDeliveryBadgeForKey(displayTitle);
  const loose = looseIdentityFromParts(strippedTitle, '');
  const baseTitle = loose.title || strippedTitle;
  const baseKey = normKey(baseTitle);
  const rawKey = normKey(strippedTitle);
  return uniqueStrings([
    baseKey ? `TXT:${baseKey}` : '',
    rawKey ? `TXT:${rawKey}` : '',
    baseKey ? `TXT:${baseKey}로켓프레시` : '',
    baseKey ? `TXT:${baseKey}로켓직구` : '',
    baseKey ? `TXT:${baseKey}쿠팡직구` : '',
    rawKey ? `TXT:${rawKey}로켓프레시` : '',
    rawKey ? `TXT:${rawKey}로켓직구` : '',
    rawKey ? `TXT:${rawKey}쿠팡직구` : '',
  ]);
}

function dedupeKey(a) {
  const product = [s(a.productId, 80), s(a.itemId, 80), s(a.vendorItemId, 80)].filter(Boolean).join('|');
  const titleKey = normKey(stripDeliveryBadgeForKey(cleanTitleText(a.title || '')));
  // [서버] v055:
  // alert dedupe도 observation/stats와 같은 canonical option key를 쓴다.
  // 예: "1개, 850g" / "850g, 1개"가 서로 다른 알림키로 갈라지는 문제 차단.
  const opt = normalizeOptionForKey(a.option || a.optionKey || '');
  const price = n(a.price || a.payPrice);
  if (product) return `PID:${product}:OPT:${opt}:PRICE:${price}`;
  // v023: 텔레그램 재가공/직접전송은 공백·이모지·문장부호가 조금씩 달라져도 같은 상품/옵션/가격이면 중복으로 본다.
  return crypto.createHash('sha1').update(`${titleKey}|${opt}|${price}`).digest('hex');
}

function normalizeAlert(body) {
  const price = alertPriceFromBody(body);
  const avg = n(body.avg || body.avgPrice || body.baselineAvg);
  const title = cleanTitleText(body.title);
  const option = cleanOptionText(body.option || body.optionKey);
  const dropPct = f(body.dropPct || body.avgDrop || (avg > 0 && price > 0 ? ((avg - price) / avg) * 100 : 0));
  return {
    id: s(body.id) || id('alert'),
    dedupeKey: s(body.dedupeKey) || dedupeKey({ ...body, title, option, price }),
    source: s(body.source || 'unknown', 80),
    section: s(body.section || '핫딜', 80),
    title,
    option,
    price,
    avg,
    low: n(body.low || body.lowPrice || body.baselineLow),
    dropPct,
    appDiscount: f(body.appDiscount || body.discount),
    forceBigDeal: body.forceBigDeal === true || String(body.forceBigDeal || '').toLowerCase() === 'true',
    manualGrade: s(body.manualGrade, 40),
    priceLabelMode: s(body.priceLabelMode, 40),
    manualToolMode: s(body.manualToolMode, 40),
    cardText: cleanCardText(body.cardText || body.cardBestInfo, title, option),
    cardDiscountPct: f(body.cardDiscountPct || body.cardPct || body.cardRate || 0),
    hasFresh: !!(body.hasFresh || body.isFresh || /로켓\s*프레시|rocket\s*fresh/i.test(String(body.deliveryBadge || body.badge || body.raw?.deliveryBadge || ''))),
    hasJikgu: !!(body.hasJikgu || body.isJikgu || /로켓\s*직구|쿠팡\s*직구|rocket\s*global/i.test(String(body.deliveryBadge || body.badge || body.raw?.deliveryBadge || ''))),
    deliveryBadge: deliveryBadgeForDisplay(body.deliveryBadge, body.badge, body, body.raw),
    // 구매 링크는 파트너스/제휴 링크를 최우선으로 저장합니다.
    url: s(body.partnerUrl || body.coupangPartnerUrl || body.affiliateUrl || body.shortUrl || body.deepLink || body.url || body.productUrl, 1000),
    originalUrl: s(body.originalUrl || body.productUrl || body.url, 1000),
    productId: s(body.productId, 80),
    itemId: s(body.itemId, 80),
    vendorItemId: s(body.vendorItemId, 80),
    createdAt: n(body.createdAt) || now(),
    raw: body
  };
}

function won(v) { return `${n(v).toLocaleString('ko-KR')}원`; }

function stripDealPrefix(text) {
  let t = s(text, 500);
  for (let i = 0; i < 4; i += 1) {
    const before = t;
    t = t
      .replace(/^✨\s*/u, '')
      .replace(/^(?:🔥\s*){1,3}대박(?:딜|알림)?\s*[:：\-·|]*/u, '')
      .replace(/^🔥🔥대박(?:딜|알림)?\s*[:：\-·|]*/u, '')
      .replace(/^대박(?:딜|알림)?\s*[:：\-·|]*/u, '')
      .replace(/^실시간\s*핫딜\s*[:：\-·|]*/u, '')
      .replace(/^핫딜\s*[:：\-·|]*/u, '')
      .replace(/^가격\s*하락\s*[:：\-·|]*/u, '')
      .replace(/^가격하락\s*[:：\-·|]*/u, '')
      .replace(/^인기\s*[:：\-·|]*/u, '')
      .trim();
    if (t === before) break;
  }
  return t;
}

function cleanTitleText(text) {
  let t = stripDealPrefix(text);
  // 상품명 끝에 붙는 가격 꼬리 제거.
  // 예: "도브 바디워시 (7,450원)", "도브 바디워시 (7,450?)"
  // 단순 용량(500g, 1kg)은 건드리지 않도록 콤마가 있는 금액 또는 "원" 포함 금액만 제거한다.
  t = t
    .replace(/\s*[（(]\s*\d{1,3}(?:,\d{3})+(?:\s*원|[^\d)]*)?\s*[）)]\s*$/u, '')
    .replace(/\s*\d{1,3}(?:,\d{3})+\s*원\s*$/u, '')
    .replace(/\s*\d{4,}\s*원\s*$/u, '')
    // 쿠팡 옵션 찌꺼기가 상품명 끝에 붙는 경우 정리: "상품명, FREE" / "상품명 FREE"
    .replace(/\s*,\s*(?:FREE|free|Free|프리)\s*$/u, '')
    .replace(/\s+(?:FREE|free|Free|프리)\s*$/u, '')
    .trim();
  return t || s(text, 300);
}

function normKey(text) {
  return String(text || '').toLowerCase().replace(/[^0-9a-z가-힣]+/gi, '');
}


function normalizeOptionForKey(text) {
  const cleaned = stripDeliveryBadgeForKey(cleanOptionText(text || ''));
  // v044: 옵션 순서가 달라도 같은 상품으로 묶는다.
  // 예: "110g, 8개" == "8개, 110g" → optionKey=110g8개
  return canonicalOptionMatchKey(cleaned) || normKey(cleaned);
}

function canonicalProductIdentity(a = {}) {
  const productId = s(a.productId, 80);
  const itemId = s(a.itemId, 80);
  const vendorItemId = s(a.vendorItemId, 80);
  const idKey = [productId, itemId, vendorItemId].filter(Boolean).join('|');

  // v042: PC 스크랩이 title에 "신라면 120g, 5개"처럼 옵션을 붙여 보내도
  // 서버 저장/조회 키는 title="신라면", option="120g, 5개"로 정규화한다.
  // 단, 로켓프레시/로켓직구 배지는 여전히 키에서만 제거한다.
  const rawTitle = cleanTitleText(a.title || '');
  const rawOption = cleanOptionText(stripDeliveryBadgeForKey(a.option || a.optionText || a.optionKey || ''));
  const loose = looseIdentityFromParts(rawTitle, rawOption);
  const title = loose.title || stripDeliveryBadgeForKey(rawTitle);
  const titleKey = normKey(title);
  const option = cleanOptionText(loose.option || rawOption || '');
  const optionKey = normalizeOptionForKey(option);
  const productKey = idKey ? `ID:${idKey}` : `TXT:${titleKey}`;
  return { productKey, title, titleKey, option, optionKey, productId, itemId, vendorItemId };
}

function observationBucket(ts = now()) {
  return Math.floor(n(ts) / OBS_BUCKET_MS);
}

function observationDedupeKey(obs) {
  // 파트너스 링크/원본 링크는 절대 중복 기준에 넣지 않는다.
  return crypto.createHash('sha1').update(`${obs.productKey}|${obs.optionKey}|${n(obs.price)}|${observationBucket(obs.collectedAt)}`).digest('hex');
}

function alertDedupeFromObservation(obs) {
  // 앱 알림 중복은 24시간 안에 같은 상품/옵션/현재가면 차단한다. 링크 제외.
  return crypto.createHash('sha1').update(`${obs.productKey}|${obs.optionKey}|${n(obs.price)}`).digest('hex');
}

function normalizeObservation(body = {}) {
  const ids = canonicalProductIdentity(body);
  const price = observationPriceFromBody(body);
  const collectedAt = n(body.collectedAt) || now();
  const obs = {
    id: s(body.id) || id('obs'),
    productKey: ids.productKey,
    title: ids.title,
    titleKey: ids.titleKey,
    option: ids.option,
    optionKey: ids.optionKey,
    price,
    cardDiscountPct: f(body.cardDiscountPct || body.cardPct || body.cardRate || 0),
    cardText: cleanCardText(body.cardText || body.cardBestInfo || body.cardInfo || '', ids.title, ids.option),
    hasFresh: !!(body.hasFresh || body.isFresh || /로켓\s*프레시|rocket\s*fresh/i.test(String(body.deliveryBadge || body.badge || body.raw?.deliveryBadge || ''))),
    hasJikgu: !!(body.hasJikgu || body.isJikgu || /로켓\s*직구|쿠팡\s*직구|rocket\s*global/i.test(String(body.deliveryBadge || body.badge || body.raw?.deliveryBadge || ''))),
    deliveryBadge: deliveryBadgeForDisplay(body.deliveryBadge, body.badge, body, body.raw),
    url: s(body.url || body.productUrl || body.originalUrl || '', 1000),
    partnerUrl: s(body.partnerUrl || body.coupangPartnerUrl || body.affiliateUrl || body.shortUrl || body.deepLink || '', 1000),
    productId: ids.productId,
    itemId: ids.itemId,
    vendorItemId: ids.vendorItemId,
    category: s(body.category || body.cat || '', 120),
    workerId: s(body.workerId || body.collectorId || body.pcId || 'unknown-worker', 120),
    source: s(body.source || 'collector_observe', 80),
    collectedAt,
    createdAt: now(),
    raw: body
  };
  obs.obsKey = s(body.obsKey) || observationDedupeKey(obs);
  if (!obs.title || !obs.price) throw new Error('EMPTY_OBSERVATION_REQUIRED_FIELD');
  return obs;
}

function cleanOptionText(rawOption) {
  const raw = s(stripDeliveryBadgeForKey(rawOption), 300);
  if (!raw) return '';
  const parts = raw
    .replace(/\s*[·|/]\s*/g, ', ')
    .split(/\s*,\s*/g)
    .map(x => x.trim())
    .filter(Boolean);

  const kept = [];
  const seen = new Set();
  for (const part of parts) {
    const key = normKey(part);
    // FREE/FREE사이즈는 색상·수량 옵션보다 식별력이 낮아서 중복키/DB키에서 제외한다.
    if (key === 'free' || key === '프리') continue;
    if (!key || seen.has(key)) continue;

    const mult = part.split(/\s*[xX×*]\s*/).map(x => x.trim()).filter(Boolean);
    if (mult.length >= 2) {
      const existing = new Set(kept.map(normKey));
      if (mult.every(x => existing.has(normKey(x)))) continue;
    }

    seen.add(key);
    kept.push(part);
  }
  return kept.join(', ');
}

function cleanCardText(rawCard, title = '', option = '') {
  let t = s(rawCard, 220);
  if (!t) return '';
  t = t
    .replace(/^💳\s*/u, '')
    .replace(/^카드\s*할인\s*[:：]?\s*/u, '')
    .replace(/^카드가\s*[:：]?\s*/u, '')
    .trim();

  const key = normKey(t);
  const titleKey = normKey(title);
  const optionKey = normKey(option);
  if (!key) return '';
  if (titleKey && (key === titleKey || key.includes(titleKey.slice(0, Math.min(12, titleKey.length))))) return '';
  if (optionKey && (key === optionKey || key.includes(optionKey))) return '';

  const looksCard = /(카드|결제|즉시|할인|청구|신한|kb|국민|삼성|현대|롯데|하나|우리|bc|nh|농협|카카오|토스|씨티|기업|ibk)/i.test(t);
  if (!looksCard) return '';
  return t;
}

function splitCompactTitleOption(text) {
  const cleaned = cleanTitleText(text);
  const parts = cleaned.split(/\s+·\s+/u).map(x => x.trim()).filter(Boolean);
  if (parts.length >= 2) return { title: parts[0], option: parts.slice(1).join(' · ') };
  return { title: cleaned, option: '' };
}

function isCategoryOnlyLine(line) {
  return /^(?:🔥\s*){0,3}(?:대박|대박딜|대박\s*알림)$|^🔥🔥대박$|^실시간\s*핫딜$|^인기$/u.test(s(line).replace(/[:：\-·|]+$/u, '').trim());
}

function isBigText(text) {
  return /(?:🔥\s*){1,3}대박|대박딜|대박\s*알림/u.test(String(text || ''));
}

function isBigAlert(alert) {
  const section = s(alert.section || '', 120).replace(/\s+/g, '');
  const raw = alert.raw || {};
  const rawKind = s(raw.kind || raw.type || raw.level || alert.kind || alert.type || '', 80).toLowerCase();
  return (
    section.includes('대박') ||
    section.includes('긴급') ||
    rawKind.includes('대박') ||
    rawKind.includes('big') ||
    rawKind.includes('urgent') ||
    f(alert.dropPct) >= 30 ||
    f(alert.appDiscount) >= 30
  );
}

function pushMessage(a) {
  return `${a.title || '상품'} (${won(a.price)})`;
}

function pushTitle(a) {
  return isBigAlert(a) ? '🔥🔥대박' : '🔥인기';
}

function pushCompactText(a) {
  return `${pushTitle(a)}\n${pushMessage(a)}`;
}

function formatPct1(v) {
  const x = Math.round(f(v) * 10) / 10;
  return Number.isInteger(x) ? String(x) : x.toFixed(1);
}

function parseRawJsonSafe(raw) {
  if (!raw) return {};
  if (typeof raw === 'object') return raw;
  try { return JSON.parse(String(raw)); } catch (_) { return {}; }
}

function rawAppDiscountPct(raw) {
  const r = parseRawJsonSafe(raw);
  return Math.max(
    0,
    Math.abs(f(r.appDiscount || r.discount || 0)),
    Math.abs(f(r.raw && (r.raw.appDiscount || r.raw.discount) || 0)),
    Math.abs(f(r.observation && r.observation.raw && (r.observation.raw.appDiscount || r.observation.raw.discount) || 0))
  );
}

function estimateAvgFromAppDiscount(price, pct) {
  const p = n(price);
  const d = Math.abs(f(pct));
  if (p <= 0 || d <= 0 || d >= 95) return 0;
  return Math.round(p / (1 - d / 100));
}

function bigDealThresholdForPrice(price = 0) {
  const p = n(price);
  return p > 0 && p <= 10000 ? 40 : 35;
}

function isManualBigDealForced(obj = {}) {
  const raw = obj && typeof obj.raw === 'object' ? obj.raw : {};
  const nested = raw && typeof raw.raw === 'object' ? raw.raw : {};
  const vals = [
    obj.forceBigDeal, raw.forceBigDeal, nested.forceBigDeal,
    obj.manualGrade, raw.manualGrade, nested.manualGrade,
    obj.priceLabelMode, raw.priceLabelMode, nested.priceLabelMode,
    obj.manualToolMode, raw.manualToolMode, nested.manualToolMode,
    obj.section, raw.section, nested.section
  ].map(v => String(v ?? '').trim().toLowerCase());
  return vals.includes('true') || vals.includes('big') || vals.includes('bigdeal') || vals.includes('daebak') || vals.some(v => v.includes('대박'));
}


function deliveryBadgeForDisplay(...vals) {
  const blob = vals
    .flatMap(v => {
      if (!v) return [];
      if (typeof v === 'object') return [v.deliveryBadge, v.delivery_badge, v.badge, v.hasFresh ? '로켓프레시' : '', v.hasJikgu ? '로켓직구' : '', v.raw];
      return [v];
    })
    .map(v => {
      if (!v) return '';
      if (typeof v === 'object') return JSON.stringify(v);
      return String(v);
    })
    .join(' ')
    .replace(/\s+/g, '');
  if (/로켓프레시|rocketfresh/i.test(blob)) return ' [로켓프레시❄️]';
  if (/로켓직구|쿠팡직구|rocketglobal/i.test(blob)) return ' [로켓직구🌏]';
  return '';
}

function titleWithDisplayDeliveryBadge(title, badge) {
  const clean = stripDeliveryBadgeForKey(title || '').trim() || s(title || '', 500).trim();
  const b = String(badge || '').trim();
  if (!b) return clean;
  if (/로켓프레시|로켓직구|쿠팡직구/i.test(clean)) return clean;
  return `${clean}${b}`.trim();
}

function dealGradeLine(dropPct, appPct = 0, price = 0) {
  const d = Math.max(Math.abs(f(dropPct)), Math.abs(f(appPct)));
  const bigNeed = bigDealThresholdForPrice(price);
  if (d >= 45) return `🚨 초대박딜 · 🔻${formatPct1(d)}%`;
  if (d >= bigNeed) return `🔥 대박딜 · 🔻${formatPct1(d)}%`;
  if (d >= 25) return `✨ 핫딜 · 🔻${formatPct1(d)}%`;
  if (d >= 15) return `📉 가격하락 · 🔻${formatPct1(d)}%`;
  return '';
}

function formatCollectorFullTemplate(a) {
  const raw = a.raw || {};
  const obs = raw.observation || {};
  const obsRaw = obs.raw || {};
  const stats = raw.stats || {};
  const decision = raw.decision || {};
  const rawTitleForDisplay = s(a.title || obs.title || '상품', 500);
  const displayBadge = deliveryBadgeForDisplay(
    a.deliveryBadge, a.badge, a.hasFresh ? '로켓프레시' : '', a.hasJikgu ? '로켓직구' : '',
    raw.deliveryBadge, raw.badge, raw.hasFresh ? '로켓프레시' : '', raw.hasJikgu ? '로켓직구' : '',
    obs.deliveryBadge, obs.badge, obs.hasFresh ? '로켓프레시' : '', obs.hasJikgu ? '로켓직구' : '',
    obsRaw.deliveryBadge, obsRaw.badge, obsRaw.hasFresh ? '로켓프레시' : '', obsRaw.hasJikgu ? '로켓직구' : '', obsRaw.raw
  );
  const title = titleWithDisplayDeliveryBadge(rawTitleForDisplay, displayBadge);
  const option = s(a.option || obs.option || '', 300);
  const price = n(a.price || obs.price);
  const avg = n(a.avg || stats.avg);
  const low = n(a.low || stats.low);
  const appFallbackPct = f(stats.appDiscountFallbackPct || obsRaw.appDiscount || obsRaw.raw?.appDiscount || a.appDiscount || 0);
  const avgDrop = f(a.dropPct || decision.avgDropPct || (avg > 0 && price > 0 ? ((avg - price) / avg) * 100 : 0) || appFallbackPct);
  const lowDrop = f(decision.lowDropPct || (low > 0 && price > 0 ? ((low - price) / low) * 100 : 0));
  const avgDiff = avg > 0 && price > 0 ? Math.max(0, avg - price) : 0;
  const lowDiff = low > 0 && price > 0 ? Math.max(0, low - price) : 0;
  const url = s(a.url || a.partnerUrl || obs.partnerUrl || obs.url || '', 1000);
  // v063: 배송배지는 DB/중복키에서는 제거하지만 출력 템플릿에는 보존한다.
  // title 변수에 이미 displayBadge를 붙였으므로 여기서는 추가하지 않는다.
  const badge = '';
  const bigPriceLabelDrop = Math.max(Math.abs(f(avgDrop)), Math.abs(f(appFallbackPct)));
  const forcedBigDeal = isManualBigDealForced(a) || isManualBigDealForced(raw) || isManualBigDealForced(obsRaw);
  // v054: PC 대박툴 수동 클릭은 자동 할인율 계산보다 우선한다.
  // v041: 가격하락/핫딜/대박딜 등급 문구는 제목 위에 출력하지 않지만,
  // 대박 기준(1만원 이하 40% 이상, 그 외 35% 이상)을 넘으면 가격 라벨만 대박으로 표시한다.
  const label = forcedBigDeal || bigPriceLabelDrop >= bigDealThresholdForPrice(price) ? '🔥대박🔥 최종 혜택가 :' : '💰 최종 혜택가 :';
  const lines = [];
  lines.push('※ 파트너스활동으로 수수료를 제공받습니다.');
  lines.push(`✨ ${title}${badge}`);
  if (option) lines.push(`└ ${option}`);
  lines.push('');
  lines.push(`${label} ${won(price)}`);
  if (a.cardText) lines.push(`💳 (${a.cardText})`);
  lines.push('');
  // v069: 평균/최저가 현재가와 같거나 할인액이 0원인 경우에는 표시하지 않는다.
  // 예: 평균 11,400원 · 0% (0원) 같은 무의미한 줄은 텔레그램 출력에서 생략.
  const showAvgLine = avg > 0 && price > 0 && avg > price && avgDiff > 0 && avgDrop > 0.05;
  const showLowLine = low > 0 && price > 0 && low > price && lowDiff > 0 && lowDrop > 0.05;
  if (showAvgLine) lines.push(`📉 평균 ${won(avg)} · 🔻${formatPct1(avgDrop)}% (${won(avgDiff)})`);
  if (showLowLine && avg !== low) {
    lines.push(`🏆 최저 ${won(low)} · 🔻${formatPct1(lowDrop)}% (${won(lowDiff)})`);
  }
  lines.push('');
  lines.push('🔗 상세보기 및 구매하기');
  if (url) lines.push(url);
  return lines.join('\n').trim();
}

function firstUrl(text) {
  const m = String(text || '').match(/https?:\/\/\S+/i);
  return m ? m[0].replace(/[)\]\s]+$/g, '') : '';
}


function firstPercent(text) {
  const m = String(text || '').match(/(\d+(?:\.\d+)?)\s*%/u);
  return m ? f(m[1]) : 0;
}

function firstCardLine(text) {
  const lines = String(text || '').split(/\r?\n/).map(x => x.trim()).filter(Boolean);
  return lines.find(x => /카드|결제|즉시할인|청구할인|신한|농협|국민|삼성|현대|롯데|하나|우리|BC|NH/i.test(x)) || '';
}

function firstWon(text) {
  const t = String(text || '');
  // 정상 한국어 문구: 7,450원
  let m = t.match(/([0-9][0-9,]*)\s*원/u);
  if (m) return n(m[1].replace(/,/g, ''));

  // PowerShell/콘솔 인코딩 문제로 "원"이 깨져도 괄호 안 금액은 잡는다.
  // 예: 도브 바디워시 (7,450?)
  m = t.match(/[（(]\s*([0-9][0-9,]*)\s*(?:원|[^\d)]*)?[）)]/u);
  if (m) return n(m[1].replace(/,/g, ''));

  // 최종 혜택가 : 7,450 처럼 원 글자가 빠진 경우의 마지막 방어.
  m = t.match(/(?:가격|혜택가|최종|핫딜|대박딜)[^0-9]*([0-9][0-9,]*)/u);
  return m ? n(m[1].replace(/,/g, '')) : 0;
}


function detectSectionFromText(text, body = {}) {
  const explicit = s(body.section || body.category || body.kind || body.type || '', 80);
  if (explicit) return isBigText(explicit) ? '대박' : (explicit.includes('인기') ? '인기' : explicit);
  const compact = String(text || '').replace(/\s+/g, '');
  if (/(?:🔥){0,3}대박|대박딜|핫딜대박|초대박|역대급/u.test(compact)) return '대박';
  if (/인기|실시간핫딜|핫딜/u.test(compact)) return '인기';
  return '인기';
}

function looksNoiseTitleLine(line) {
  const t = s(line, 300);
  if (!t) return true;
  if (t.startsWith('※') || t.startsWith('└') || /^https?:\/\//i.test(t)) return true;
  if (isCategoryOnlyLine(t)) return true;
  if (/^쿠팡을\s*추천합니다\.?$/u.test(t)) return true;
  if (/^상세보기|^구매하기|^링크\s*열기/u.test(t)) return true;
  if (/파트너스활동|수수료를\s*제공/u.test(t)) return true;
  return false;
}

function parseTelegramText(text, body = {}) {
  const lines = String(text || '').split(/\r?\n/).map(x => x.trim()).filter(Boolean);

  // 사람이 텔레그램방에 직접 넣은 추천 문구/봇이 재가공한 문구를 모두 받아낸다.
  // 상품명 후보는 ✨ 라인을 우선하고, 없으면 노이즈/URL/가격 라인을 제외한 첫 줄을 쓴다.
  const titleLine =
    lines.find(x => x.startsWith('✨')) ||
    lines.find(x => !looksNoiseTitleLine(x) && !/[0-9][0-9,]*\s*원/u.test(x) && !x.includes('평균') && !x.includes('최저') && !x.includes('최종')) ||
    '';

  const optionLine = lines.find(x => x.startsWith('└')) || '';
  const priceLine =
    lines.find(x => x.includes('최종') || x.includes('혜택가') || x.includes('가격')) ||
    lines.find(x => /[0-9][0-9,]*\s*원/u.test(x)) ||
    lines.find(x => /[（(]\s*[0-9][0-9,]*/u.test(x)) ||
    '';
  const avgLine = lines.find(x => x.includes('평균')) || '';
  const lowLine = lines.find(x => x.includes('최저')) || '';
  const url = s(body.partnerUrl || body.url || body.link || firstUrl(text), 1000);
  const cardLine = firstCardLine(text);
  const price = lockedFallcentPrice(body) || n(body.price || firstWon(priceLine || titleLine));
  const compact = splitCompactTitleOption(body.title || titleLine);
  const title = cleanTitleText(compact.title || body.message || '텔레그램 핫딜');
  const option = cleanOptionText(body.option || optionLine.replace(/^└\s*/, '') || compact.option);
  const section = detectSectionFromText(text, body);
  // v052: PC가 body.avg=현재가(의미 없는 0% 기준)를 같이 보내도,
  // 텔레그램 원문에 📉 평균 줄이 있으면 그 원문 값을 최우선으로 사용한다.
  // 기존 body.avg 우선순위 때문에 원문 평균 12,894원이 있어도 avg=8,510으로 저장되는 문제가 있었다.
  const avgFromLine = n(firstWon(avgLine));
  const lowFromLine = n(firstWon(lowLine));
  const bodyAvg = n(body.avg || body.avgPrice || body.baselineAvg);
  const bodyLow = n(body.low || body.lowPrice || body.baselineLow);
  const avg = avgFromLine > 0 ? avgFromLine : bodyAvg;
  const low = lowFromLine > 0 ? lowFromLine : bodyLow;
  const dropPct = f(body.dropPct || body.avgDrop || (avg > 0 && price > 0 ? ((avg - price) / avg) * 100 : 0));

  return normalizeAlert({
    // v034: text 경로에서도 수동 버튼/키위 재요청 dedupeKey를 보존한다.
    dedupeKey: s(body.dedupeKey, 240),
    source: body.source || 'telegram_bridge',
    section,
    title,
    option,
    price,
    avg,
    low,
    dropPct,
    appDiscount: f(body.appDiscount || body.discount || 0),
    cardText: body.cardText || cardLine,
    cardDiscountPct: f(body.cardDiscountPct || firstPercent(cardLine)),
    partnerUrl: url,
    url,
    originalUrl: body.originalUrl || body.productUrl || url,
    raw: { telegramText: text, ...body }
  });
}

function pickTelegramFullText(alert, overrideText = '') {
  const direct = String(overrideText || '').trim();
  if (direct) return direct;

  const raw = alert && typeof alert.raw === 'object' ? alert.raw : {};
  const nested = raw.observation?.raw || {};

  // v048: 텔레그램/키위 수동 재전송을 stats로 보강한 뒤에는 원문을 그대로 보내면 안 된다.
  // 원문이 평균/최저 없는 템플릿이면 다시 평균 없이 나가므로, 보강된 alert 값으로 무조건 재렌더한다.
  if (alert.source === 'collector_observe' || raw.observation || raw.telegramIngestStatsEnriched) {
    return formatCollectorFullTemplate(alert);
  }

  const rawText = String(
    raw.telegramText || raw.text || raw.message || raw.caption ||
    nested.telegramText || nested.telegramReply || nested.text || nested.message || nested.caption ||
    alert.text || alert.message || alert.caption || ''
  ).trim();

  // 보강 대상이 아닌 완성 템플릿은 기존처럼 그대로 보낸다.
  if (rawText && (
    rawText.includes('최종 혜택가') ||
    rawText.includes('상세보기 및 구매하기') ||
    rawText.includes('파트너스활동') ||
    rawText.includes('📉 평균') ||
    rawText.includes('🏆 최저')
  )) {
    return rawText;
  }

  return '';
}

async function sendTelegram(alert, overrideText = '') {
  const token = process.env.TELEGRAM_BOT_TOKEN || '';
  const chatId = process.env.TELEGRAM_CHAT_ID || '';
  if (!token || !chatId) return { sent: false, skipped: true };

  const fullText = pickTelegramFullText(alert, overrideText);
  const text = (fullText || `${pushCompactText(alert)}
${alert.url || ''}`).trim();

  try {
    const r = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text, disable_web_page_preview: false })
    });
    return { sent: r.ok, status: r.status, fullTemplate: !!fullText };
  } catch (e) {
    return { sent: false, error: String(e.message || e) };
  }
}





function partnersCacheKey(url = '', subId = '') {
  return `${cleanCoupangShareUrlServer(url)}::${String(subId || '').trim()}`;
}

function getPartnersCache(url = '', subId = '') {
  const key = partnersCacheKey(url, subId);
  const hit = partnersDeeplinkCache.get(key);
  if (hit && hit.expiresAt > Date.now() && hit.partnerUrl) return hit;
  if (hit) partnersDeeplinkCache.delete(key);
  return null;
}

function setPartnersCache(url = '', subId = '', partnerUrl = '', meta = {}) {
  const key = partnersCacheKey(url, subId);
  if (!partnerUrl) return;
  partnersDeeplinkCache.set(key, { partnerUrl, expiresAt: Date.now() + PARTNERS_DEEPLINK_CACHE_MS, ...meta });
}

function getPartnersFailCache(url = '', subId = '') {
  const key = partnersCacheKey(url, subId);
  const hit = partnersDeeplinkFailCache.get(key);
  if (hit && hit.expiresAt > Date.now()) return hit;
  if (hit) partnersDeeplinkFailCache.delete(key);
  return null;
}

function setPartnersFailCache(url = '', subId = '', error = 'DEEPLINK_FAILED', meta = {}) {
  const key = partnersCacheKey(url, subId);
  partnersDeeplinkFailCache.set(key, { error, expiresAt: Date.now() + PARTNERS_DEEPLINK_FAIL_CACHE_MS, ...meta });
}

function isCoupangRateLimitError(msg = '') {
  const t = String(msg || '');
  return /사용 횟수|1분당\s*100회|초과|rate.?limit|too many/i.test(t);
}

function originalPartnersFallback(url = '', reason = 'DEEPLINK_FALLBACK_ORIGINAL', extra = {}) {
  const finalUrl = cleanCoupangShareUrlServer(url) || String(url || '').trim();
  // v073: 서버는 파트너스 변환을 절대 하지 않는다.
  // 다만 기존 PC/에뮬/브릿지가 /partners/deeplink 응답 필드명을 다르게 기대해도
  // 링크가 빈 값으로 빠지지 않도록 모든 호환 alias에 원본 링크를 채운다.
  return {
    ok: true,
    partnerOk: true,
    passThrough: true,
    serverPartnersConversionDisabled: true,
    fallbackOriginal: true,
    reason,
    partnerUrl: finalUrl,
    partnersUrl: finalUrl,
    finalUrl,
    url: finalUrl,
    link: finalUrl,
    deeplink: finalUrl,
    deepLink: finalUrl,
    convertedUrl: finalUrl,
    originalUrl: finalUrl,
    usedUrl: finalUrl,
    shortenUrl: finalUrl,
    landingUrl: finalUrl,
    ...extra
  };
}

function coupangSignedDate() {
  // Coupang Partners Open API signature uses YYMMDDTHHMMSSZ.
  // v069 used full year and query-style subId, which can make server deeplink fail.
  const d = new Date();
  const pad = (x) => String(x).padStart(2, '0');
  const yy = String(d.getUTCFullYear()).slice(-2);
  return `${yy}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}T${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}Z`;
}

function isCoupangUrl(url) {
  try {
    const u = new URL(String(url || ''));
    return /(^|\.)coupang\.com$/i.test(u.hostname) || /(^|\.)link\.coupang\.com$/i.test(u.hostname);
  } catch (_) {
    return false;
  }
}

function cleanCoupangShareUrlServer(rawUrl) {
  const raw = String(rawUrl || '').trim();
  if (!raw) return '';
  try {
    const u = new URL(raw);
    const drop = new Set(['abtestinfo', 'q', 'src', 'spec', 'addtag', 'ctag']);
    for (const k of [...u.searchParams.keys()]) {
      if (drop.has(String(k).toLowerCase())) u.searchParams.delete(k);
    }
    u.hash = '';
    return u.toString();
  } catch (_) {
    return raw;
  }
}

async function resolveCoupangShortUrlServer(rawUrl) {
  const url = cleanCoupangShareUrlServer(rawUrl);
  if (!url) return '';
  try {
    const u = new URL(url);
    if (!/(^|\.)link\.coupang\.com$/i.test(u.hostname)) return url;
  } catch (_) {
    return url;
  }

  const headers = {
    'User-Agent': 'Mozilla/5.0 (Linux; Android 13; SM-G991N) AppleWebKit/537.36 Mobile Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Referer': 'https://www.coupang.com/'
  };

  for (const method of ['HEAD', 'GET']) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 7000);
    try {
      const r = await fetch(url, { method, headers, redirect: 'follow', signal: controller.signal });
      const finalUrl = cleanCoupangShareUrlServer(r.url || '');
      if (finalUrl && /coupang\.com/i.test(finalUrl) && !/link\.coupang\.com/i.test(finalUrl)) return finalUrl;
    } catch (_) {
      // try next method
    } finally {
      clearTimeout(timer);
    }
  }
  return url;
}

async function coupangDeeplinkApiCallServer(url, subId = '') {
  const accessKey = String(COUPANG_PARTNERS_ACCESS_KEY || '').trim();
  const secretKey = String(COUPANG_PARTNERS_SECRET_KEY || '').trim();
  if (!accessKey || !secretKey) return { ok: false, skipped: true, error: 'COUPANG_PARTNERS_ENV_MISSING' };

  const apiPath = '/v2/providers/affiliate_open_api/apis/openapi/v1/deeplink';
  const signedDate = coupangSignedDate();
  const message = signedDate + 'POST' + apiPath;
  const signature = crypto.createHmac('sha256', secretKey).update(message).digest('hex');
  const authorization = `CEA algorithm=HmacSHA256, access-key=${accessKey}, signed-date=${signedDate}, signature=${signature}`;

  const body = { coupangUrls: [url] };
  const sid = String(subId || '').trim();
  if (/^[A-Za-z0-9._-]{1,64}$/.test(sid)) body.subId = sid;

  const r = await fetch(`https://api-gateway.coupang.com${apiPath}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json;charset=UTF-8', Authorization: authorization },
    body: JSON.stringify(body)
  });
  const data = await r.json().catch(() => null);
  if (!r.ok || !data) throw new Error(`DEEPLINK_HTTP_${r.status}`);
  if ((data.rCode === '0' || data.rCode === '0000') && Array.isArray(data.data) && data.data.length) {
    const item = data.data[0] || {};
    const partnerUrl = item.shortenUrl || item.landingUrl || '';
    if (partnerUrl) return { ok: true, partnerUrl, shortenUrl: item.shortenUrl || '', landingUrl: item.landingUrl || '', raw: data };
  }
  throw new Error(data.message || data.rMessage || data.rCode || 'DEEPLINK_API_ERROR');
}

async function createCoupangDeeplinkServer(originalUrl, requestedSubId = '') {
  const requested = String(requestedSubId || '').trim();
  const safeRequested = /^[A-Za-z0-9._-]{1,64}$/.test(requested) ? requested : '';
  const subId = safeRequested || String(COUPANG_PARTNERS_SUB_ID || '').trim();
  const inputUrl = String(originalUrl || '').trim();
  if (!inputUrl) throw new Error('EMPTY_URL');
  if (!isCoupangUrl(inputUrl)) throw new Error('NOT_COUPANG_URL');

  const cleaned = cleanCoupangShareUrlServer(inputUrl);
  const attempts = [];

  // v072: 서버에서는 Coupang Partners API를 절대 호출하지 않는다.
  // 원래 구조처럼 PC/에뮬/브릿지가 이미 가진 링크를 그대로 서버가 보존만 한다.
  // /partners/deeplink, /partner_link 는 호환용 echo 엔드포인트로만 유지한다.
  return originalPartnersFallback(cleaned, 'SERVER_PARTNERS_CONVERSION_DISABLED', {
    inputUrl,
    cleanedUrl: cleaned,
    usedSubId: false,
    serverPartnersConversionDisabled: true, serverPartnersPassThroughCompat: true,
    attempts: [{ label: 'server_echo_only', ok: true, usedUrl: cleaned }]
  });

  const goodCache = getPartnersCache(cleaned, subId);
  if (goodCache) {
    return { ok: true, partnerOk: true, cacheHit: true, partnerUrl: goodCache.partnerUrl, finalUrl: goodCache.partnerUrl, inputUrl, usedUrl: goodCache.usedUrl || cleaned, usedSubId: !!subId, attempts: [{ label: 'cache', ok: true, usedUrl: goodCache.usedUrl || cleaned }] };
  }

  if (PARTNERS_DEEPLINK_DISABLE_API) {
    return originalPartnersFallback(cleaned, 'DEEPLINK_API_DISABLED', { inputUrl, cleanedUrl: cleaned, usedSubId: !!subId, attempts: [{ label: 'disabled', ok: false, usedUrl: cleaned }] });
  }

  if (partnersDeeplinkCircuitOpenUntil > Date.now()) {
    return originalPartnersFallback(cleaned, 'DEEPLINK_RATE_LIMIT_CIRCUIT_OPEN', { inputUrl, cleanedUrl: cleaned, circuitOpenUntil: partnersDeeplinkCircuitOpenUntil, usedSubId: !!subId, attempts: [{ label: 'circuit_open', ok: false, usedUrl: cleaned }] });
  }

  const failCache = getPartnersFailCache(cleaned, subId);
  if (failCache) {
    return originalPartnersFallback(cleaned, failCache.error || 'DEEPLINK_FAIL_CACHE', { inputUrl, cleanedUrl: cleaned, failCacheHit: true, usedSubId: !!subId, attempts: [{ label: 'fail_cache', ok: false, usedUrl: cleaned, error: failCache.error || 'DEEPLINK_FAIL_CACHE' }] });
  }

  const resolved = await resolveCoupangShortUrlServer(cleaned);
  const candidates = [];
  if (resolved) candidates.push({ label: resolved !== cleaned ? 'resolved_short_url' : 'cleaned_url', url: resolved });
  if (cleaned && cleaned !== resolved) candidates.push({ label: 'cleaned_original', url: cleaned });

  const seen = new Set();
  for (const c of candidates) {
    const u = String(c.url || '').trim();
    if (!u || seen.has(u)) continue;
    seen.add(u);
    try {
      const result = await coupangDeeplinkApiCallServer(u, subId);
      attempts.push({ label: c.label, ok: !!result.ok, usedUrl: u });
      if (result.ok) {
        setPartnersCache(cleaned, subId, result.partnerUrl, { usedUrl: u });
        return { ...result, partnerOk: true, finalUrl: result.partnerUrl, inputUrl, usedUrl: u, usedSubId: !!subId, attempts };
      }
      attempts.push({ label: c.label, ok: false, usedUrl: u, error: result.error || 'NOT_OK' });
    } catch (e) {
      const errText = String(e.message || e);
      attempts.push({ label: c.label, ok: false, usedUrl: u, error: errText.slice(0, 220) });
      if (isCoupangRateLimitError(errText)) {
        partnersDeeplinkCircuitOpenUntil = Date.now() + PARTNERS_DEEPLINK_CIRCUIT_MS;
        setPartnersFailCache(cleaned, subId, 'DEEPLINK_RATE_LIMITED_FALLBACK_ORIGINAL', { attempts });
        return originalPartnersFallback(cleaned, 'DEEPLINK_RATE_LIMITED_FALLBACK_ORIGINAL', { inputUrl, cleanedUrl: cleaned, resolvedUrl: resolved, circuitOpenUntil: partnersDeeplinkCircuitOpenUntil, usedSubId: !!subId, attempts });
      }
    }
  }

  setPartnersFailCache(cleaned, subId, 'DEEPLINK_FAILED_FALLBACK_ORIGINAL', { attempts });
  return originalPartnersFallback(cleaned, 'DEEPLINK_FAILED_FALLBACK_ORIGINAL', {
    inputUrl,
    cleanedUrl: cleaned,
    resolvedUrl: resolved,
    usedSubId: !!subId,
    env: {
      accessKey: !!String(COUPANG_PARTNERS_ACCESS_KEY || '').trim(),
      secretKey: !!String(COUPANG_PARTNERS_SECRET_KEY || '').trim(),
      subId: !!subId
    },
    attempts
  });

  return {
    ok: false,
    error: 'DEEPLINK_ALL_ATTEMPTS_FAILED',
    inputUrl,
    cleanedUrl: cleaned,
    resolvedUrl: resolved,
    usedSubId: !!subId,
    env: {
      accessKey: !!String(COUPANG_PARTNERS_ACCESS_KEY || '').trim(),
      secretKey: !!String(COUPANG_PARTNERS_SECRET_KEY || '').trim(),
      subId: !!subId
    },
    attempts
  };
}

async function initDb() {
  if (!pool) return;
  await pool.query(`CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    device_name TEXT,
    platform TEXT,
    expo_push_token TEXT,
    settings JSONB DEFAULT '{}'::jsonb,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS alerts (
    id TEXT PRIMARY KEY,
    dedupe_key TEXT UNIQUE NOT NULL,
    source TEXT,
    section TEXT,
    title TEXT NOT NULL,
    option_text TEXT,
    price INTEGER NOT NULL,
    avg_price INTEGER DEFAULT 0,
    low_price INTEGER DEFAULT 0,
    drop_pct DOUBLE PRECISION DEFAULT 0,
    app_discount DOUBLE PRECISION DEFAULT 0,
    card_text TEXT,
    card_discount_pct DOUBLE PRECISION DEFAULT 0,
    url TEXT,
    original_url TEXT,
    product_id TEXT,
    item_id TEXT,
    vendor_item_id TEXT,
    raw JSONB DEFAULT '{}'::jsonb,
    created_at BIGINT NOT NULL
  )`);
  if (BOOT_SCHEMA_INDEXES) {
    try { await pool.query(`CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts(created_at DESC)`); }
    catch (e) { console.warn('[initDb] index skipped idx_alerts_created_at:', String(e?.message || e)); }
  } else {
    console.log('[initDb] boot index skipped: idx_alerts_created_at');
  }
  // 기존 DB에서 통합 서버로 교체해도 누락 컬럼이 있으면 자동 보강한다.
  await pool.query(`ALTER TABLE alerts ADD COLUMN IF NOT EXISTS card_discount_pct DOUBLE PRECISION DEFAULT 0`);
  await pool.query(`ALTER TABLE alerts ADD COLUMN IF NOT EXISTS original_url TEXT`);
  await pool.query(`ALTER TABLE alerts ADD COLUMN IF NOT EXISTS product_id TEXT`);
  await pool.query(`ALTER TABLE alerts ADD COLUMN IF NOT EXISTS item_id TEXT`);
  await pool.query(`ALTER TABLE alerts ADD COLUMN IF NOT EXISTS vendor_item_id TEXT`);
  await pool.query(`CREATE TABLE IF NOT EXISTS alert_opens (
    id TEXT PRIMARY KEY,
    alert_id TEXT,
    device_id TEXT,
    url TEXT,
    opened_at BIGINT NOT NULL
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS price_observations (
    id TEXT PRIMARY KEY,
    obs_key TEXT UNIQUE NOT NULL,
    product_key TEXT NOT NULL,
    title TEXT NOT NULL,
    title_key TEXT,
    option_text TEXT,
    option_key TEXT,
    price INTEGER NOT NULL,
    card_discount_pct DOUBLE PRECISION DEFAULT 0,
    card_text TEXT,
    url TEXT,
    partner_url TEXT,
    product_id TEXT,
    item_id TEXT,
    vendor_item_id TEXT,
    category TEXT,
    worker_id TEXT,
    source TEXT,
    raw JSONB DEFAULT '{}'::jsonb,
    collected_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL
  )`);
  if (BOOT_SCHEMA_INDEXES) {
    const obsIndexes = [
      [`idx_obs_product_option_time`, `CREATE INDEX IF NOT EXISTS idx_obs_product_option_time ON price_observations(product_key, option_key, collected_at DESC)`],
      [`idx_obs_created_at`, `CREATE INDEX IF NOT EXISTS idx_obs_created_at ON price_observations(created_at DESC)`],
      [`idx_obs_product_option_time_price`, `CREATE INDEX IF NOT EXISTS idx_obs_product_option_time_price ON price_observations(product_key, option_key, collected_at DESC) WHERE price > 0`],
      [`idx_obs_title_option_time_price`, `CREATE INDEX IF NOT EXISTS idx_obs_title_option_time_price ON price_observations(title_key, option_key, collected_at DESC) WHERE price > 0`],
      [`idx_obs_collected_time_price`, `CREATE INDEX IF NOT EXISTS idx_obs_collected_time_price ON price_observations(collected_at DESC) WHERE price > 0`],
    ];
    for (const [name, sql] of obsIndexes) {
      try { await pool.query(sql); }
      catch (e) { console.warn(`[initDb] index skipped ${name}:`, String(e?.message || e)); }
    }
    console.log('[initDb] observation indexes create attempted');
  } else {
    console.log('[initDb] boot observation indexes skipped by BOOT_SCHEMA_INDEXES=false');
  }
  await pool.query(`CREATE TABLE IF NOT EXISTS emul_price_stats (
    id TEXT PRIMARY KEY,
    stat_key TEXT UNIQUE NOT NULL,
    product_key TEXT NOT NULL,
    title TEXT NOT NULL,
    title_key TEXT,
    option_text TEXT,
    option_key TEXT,
    count INTEGER DEFAULT 0,
    avg_price INTEGER DEFAULT 0,
    low_price INTEGER DEFAULT 0,
    high_price INTEGER DEFAULT 0,
    source TEXT,
    worker_id TEXT,
    raw JSONB DEFAULT '{}'::jsonb,
    first_seen_at BIGINT DEFAULT 0,
    last_seen_at BIGINT DEFAULT 0,
    updated_at BIGINT NOT NULL
  )`);
  if (BOOT_SCHEMA_INDEXES) {
    const emulIndexes = [
      [`idx_emul_stats_product_option`, `CREATE INDEX IF NOT EXISTS idx_emul_stats_product_option ON emul_price_stats(product_key, option_key, updated_at DESC)`],
      [`idx_emul_stats_title_option`, `CREATE INDEX IF NOT EXISTS idx_emul_stats_title_option ON emul_price_stats(title_key, option_key, updated_at DESC)`],
      [`idx_emul_stats_last_seen`, `CREATE INDEX IF NOT EXISTS idx_emul_stats_last_seen ON emul_price_stats(last_seen_at DESC)`],
      [`idx_emul_stats_product_option_seen`, `CREATE INDEX IF NOT EXISTS idx_emul_stats_product_option_seen ON emul_price_stats(product_key, option_key, last_seen_at DESC) WHERE count > 0 AND avg_price > 0`],
      [`idx_emul_stats_title_option_seen`, `CREATE INDEX IF NOT EXISTS idx_emul_stats_title_option_seen ON emul_price_stats(title_key, option_key, last_seen_at DESC) WHERE count > 0 AND avg_price > 0`],
    ];
    for (const [name, sql] of emulIndexes) {
      try { await pool.query(sql); }
      catch (e) { console.warn(`[initDb] index skipped ${name}:`, String(e?.message || e)); }
    }
    console.log('[initDb] emul stats indexes create attempted');
  } else {
    console.log('[initDb] boot emul stats indexes skipped by BOOT_SCHEMA_INDEXES=false');
  }
  await pool.query(`CREATE TABLE IF NOT EXISTS collector_workers (
    worker_id TEXT PRIMARY KEY,
    name TEXT,
    last_seen_at BIGINT NOT NULL,
    app_version TEXT,
    ip TEXT,
    raw JSONB DEFAULT '{}'::jsonb
  )`);
}

function allowIngest(req, res) {
  if (!INGEST_KEY) return true;
  const got = req.headers['x-ingest-key'] || req.body?.ingestKey || '';
  if (got === INGEST_KEY) return true;
  res.status(403).json({ ok: false, error: 'BAD_INGEST_KEY' });
  return false;
}


async function pruneOldAlerts() {
  const cutoff = now() - ALERT_RETENTION_MS;
  if (!pool) {
    memory.alerts = memory.alerts.filter(a => n(a.createdAt) >= cutoff);
    return 0;
  }
  const r = await pool.query(`DELETE FROM alerts WHERE created_at < $1`, [cutoff]);
  return r.rowCount || 0;
}

async function pruneOldOpens() {
  const cutoff = now() - ALERT_RETENTION_MS;
  if (!pool) {
    memory.opens = memory.opens.filter(o => n(o.openedAt) >= cutoff);
    return 0;
  }
  const r = await pool.query(`DELETE FROM alert_opens WHERE opened_at < $1`, [cutoff]);
  return r.rowCount || 0;
}

async function pruneOldObservations() {
  const cutoff = now() - PRICE_RETENTION_MS;
  if (!pool) {
    memory.observations = memory.observations.filter(o => n(o.collectedAt || o.createdAt) >= cutoff);
    return 0;
  }
  const r = await pool.query(`DELETE FROM price_observations WHERE collected_at < $1`, [cutoff]);
  return r.rowCount || 0;
}

async function pruneOldData() {
  try {
    const alerts = await pruneOldAlerts();
    const opens = await pruneOldOpens();
    const observations = await pruneOldObservations();
    return { alerts, opens, observations, alertCutoff: now() - ALERT_RETENTION_MS, observationCutoff: now() - PRICE_RETENTION_MS };
  } catch (e) {
    return { alerts: 0, opens: 0, observations: 0, error: String(e.message || e) };
  }
}

async function pruneOldDataThrottled(force = false) {
  const ts = now();
  if (!force && lastPruneResult && ts - lastPruneAt < PRUNE_INTERVAL_MS) {
    return { ...lastPruneResult, throttled: true, nextPruneInMs: Math.max(0, PRUNE_INTERVAL_MS - (ts - lastPruneAt)) };
  }
  if (pruneInFlight) return pruneInFlight;
  pruneInFlight = pruneOldData()
    .then((result) => {
      lastPruneAt = now();
      lastPruneResult = result;
      return { ...result, throttled: false };
    })
    .finally(() => { pruneInFlight = null; });
  return pruneInFlight;
}

async function registerDevice(body) {
  const deviceId = s(body.deviceId, 120);
  if (!deviceId) throw new Error('EMPTY_DEVICE_ID');
  const row = {
    deviceId,
    deviceName: s(body.deviceName, 120),
    platform: s(body.platform, 40),
    expoPushToken: s(body.expoPushToken, 300),
    settings: body.settings || {},
    ts: now()
  };
  if (!pool) {
    if (row.expoPushToken) {
      for (const [key, value] of memory.devices.entries()) {
        if (key !== deviceId && value.expoPushToken === row.expoPushToken) memory.devices.delete(key);
      }
    }
    memory.devices.set(deviceId, row);
    return row;
  }
  await pool.query(`INSERT INTO devices (device_id, device_name, platform, expo_push_token, settings, created_at, updated_at)
    VALUES ($1,$2,$3,$4,$5,$6,$6)
    ON CONFLICT (device_id) DO UPDATE SET device_name=$2, platform=$3, expo_push_token=$4, settings=$5, updated_at=$6`,
    [row.deviceId, row.deviceName, row.platform, row.expoPushToken, JSON.stringify(row.settings), row.ts]);
  if (row.expoPushToken) {
    await pool.query(`DELETE FROM devices WHERE expo_push_token=$1 AND device_id<>$2`, [row.expoPushToken, row.deviceId]);
  }
  return row;
}

function uniqueDevicesByPushToken(devices) {
  const byToken = new Map();
  for (const d of devices || []) {
    const token = String(d.expoPushToken || '').trim();
    if (!token || byToken.has(token)) continue;
    byToken.set(token, d);
  }
  return Array.from(byToken.values());
}

async function listDevices() {
  if (!pool) return uniqueDevicesByPushToken(Array.from(memory.devices.values()));
  const { rows } = await pool.query(`SELECT device_id AS "deviceId", expo_push_token AS "expoPushToken", settings FROM devices WHERE expo_push_token <> '' ORDER BY updated_at DESC`);
  return uniqueDevicesByPushToken(rows);
}

async function insertAlert(alert) {
  if (!alert.title || !alert.price || !alert.url) throw new Error('EMPTY_ALERT_REQUIRED_FIELD');
  await pruneOldDataThrottled();

  if (!pool) {
    const exists = memory.alerts.find(x => x.dedupeKey === alert.dedupeKey);
    if (exists) return { inserted: false, alert: exists };
    memory.alerts.unshift(alert);
    memory.alerts = memory.alerts.slice(0, 1000);
    return { inserted: true, alert };
  }

  const r = await pool.query(`INSERT INTO alerts (
    id,dedupe_key,source,section,title,option_text,price,avg_price,low_price,drop_pct,app_discount,card_text,card_discount_pct,url,original_url,product_id,item_id,vendor_item_id,raw,created_at
  ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
  ON CONFLICT (dedupe_key) DO NOTHING RETURNING id`,
  [alert.id, alert.dedupeKey, alert.source, alert.section, alert.title, alert.option, alert.price, alert.avg, alert.low, alert.dropPct, alert.appDiscount, alert.cardText, alert.cardDiscountPct, alert.url, alert.originalUrl, alert.productId, alert.itemId, alert.vendorItemId, JSON.stringify(alert.raw), alert.createdAt]);

  return { inserted: r.rowCount > 0, alert };
}

function rowToAlert(r) {
  const raw = r.raw || {};
  const partnerUrl = raw.partnerUrl || raw.coupangPartnerUrl || raw.affiliateUrl || raw.shortUrl || raw.deepLink || '';
  return {
    id: r.id,
    dedupeKey: r.dedupe_key || r.dedupeKey,
    source: r.source,
    section: r.section,
    title: r.title,
    option: r.option_text || r.option,
    price: n(r.price),
    avg: n(r.avg_price || r.avg),
    low: n(r.low_price || r.low),
    dropPct: f(r.drop_pct || r.dropPct),
    appDiscount: f(r.app_discount || r.appDiscount),
    cardText: r.card_text || r.cardText || '',
    cardDiscountPct: f(r.card_discount_pct || r.cardDiscountPct || 0),
    url: partnerUrl || r.url,
    originalUrl: r.original_url || raw.productUrl || r.originalUrl || '',
    productId: r.product_id || r.productId || '',
    itemId: r.item_id || r.itemId || '',
    vendorItemId: r.vendor_item_id || r.vendorItemId || '',
    createdAt: n(r.created_at || r.createdAt)
  };
}

async function getAlerts(limit = 100) {
  await pruneOldDataThrottled();
  if (!pool) return memory.alerts.slice(0, limit);
  const { rows } = await pool.query(`SELECT * FROM alerts ORDER BY created_at DESC LIMIT $1`, [Math.min(Math.max(n(limit), 1), 300)]);
  return rows.map(rowToAlert);
}


function allowCollector(req, res) {
  if (!COLLECTOR_KEY) return true;
  const got = req.headers['x-collector-key'] || req.headers['x-ingest-key'] || req.body?.collectorKey || req.body?.ingestKey || '';
  if (got === COLLECTOR_KEY) return true;
  res.status(403).json({ ok: false, error: 'BAD_COLLECTOR_KEY' });
  return false;
}

async function touchWorker(obs, req) {
  const workerId = s(obs.workerId, 120);
  if (!workerId) return;
  if (!pool) return;
  await pool.query(`INSERT INTO collector_workers (worker_id, name, last_seen_at, app_version, ip, raw)
    VALUES ($1,$2,$3,$4,$5,$6)
    ON CONFLICT (worker_id) DO UPDATE SET name=$2, last_seen_at=$3, app_version=$4, ip=$5, raw=$6`,
    [workerId, s(obs.raw.workerName || obs.raw.name || workerId, 120), now(), s(obs.raw.appVersion || obs.raw.version || '', 80), s(req.ip || '', 80), JSON.stringify(obs.raw || {})]);
}

function kstDayRangeMs(ts = now()) {
  const KST_OFFSET_MS = 9 * 60 * 60 * 1000;
  const base = n(ts) || now();
  const shifted = base + KST_OFFSET_MS;
  const dayStartShifted = Math.floor(shifted / (24 * 60 * 60 * 1000)) * (24 * 60 * 60 * 1000);
  const start = dayStartShifted - KST_OFFSET_MS;
  return { start, end: start + 24 * 60 * 60 * 1000 };
}

function observationMatchesSameProductOption(obs, row = {}) {
  const variants = productKeyLookupVariants(obs);
  const wanted = looseIdentityFromParts(obs.title, obs.option);
  const wantedTitleKey = wanted.titleKey || obs.titleKey;
  const wantedOptionMatchKey = wanted.optionMatchKey || canonicalOptionMatchKey(obs.option || obs.optionKey || '');
  const rowId = looseIdentityFromRow(row);
  const productMatch = variants.includes(String(row.product_key || row.productKey || ''));
  const titleMatch = wantedTitleKey && rowId.titleKey === wantedTitleKey;
  const optionMatch = wantedOptionMatchKey
    ? rowId.optionMatchKey === wantedOptionMatchKey
    : !rowId.optionMatchKey;
  return Boolean((productMatch || titleMatch) && optionMatch);
}

async function serverDailySavePolicy(obs) {
  const price = n(obs.price);
  if (price <= 0) return { allow: false, reason: 'INVALID_PRICE', dayPrices: [] };

  const day = kstDayRangeMs(obs.collectedAt || obs.createdAt || now());
  const variants = productKeyLookupVariants(obs);
  const wanted = looseIdentityFromParts(obs.title, obs.option);
  const wantedTitleKey = wanted.titleKey || obs.titleKey || '';

  const sameProductRowsFromMemory = () => memory.observations.filter(o => {
    const ts = n(o.collectedAt || o.createdAt);
    if (ts < day.start || ts >= day.end || n(o.price) <= 0) return false;
    return observationMatchesSameProductOption(obs, {
      product_key: o.productKey,
      title: o.title,
      option_text: o.option,
      option_key: o.optionKey,
      price: o.price
    });
  });

  let rows = [];
  if (!pool) {
    rows = sameProductRowsFromMemory();
  } else {
    const params = [day.start, day.end, variants, wantedTitleKey];
    let where = `collected_at >= $1 AND collected_at < $2 AND price > 0 AND (product_key = ANY($3::text[]) OR title_key = $4`;
    if (DAILY_SAVE_ENABLE_TITLE_ILIKE && wanted.title && wanted.title.length >= 2) {
      params.push(`%${wanted.title}%`);
      where += ` OR title ILIKE $5`;
    }
    where += `)`;

    const q = await pool.query(`SELECT product_key, title, title_key, option_text, option_key, price, collected_at
      FROM price_observations
      WHERE ${where}
      ORDER BY collected_at DESC
      LIMIT 5000`, params);
    rows = q.rows.filter(row => observationMatchesSameProductOption(obs, row));
  }

  const prices = rows.map(r => n(r.price)).filter(x => x > 0);
  if (!prices.length) return { allow: true, reason: 'SAVE_FIRST_TODAY', dayPrices: [], dayStart: day.start, dayEnd: day.end };

  if (prices.includes(price)) {
    return { allow: false, reason: 'SKIP_SAME_PRICE_TODAY', dayPrices: prices, dayMin: Math.min(...prices), dayStart: day.start, dayEnd: day.end };
  }

  const dayMin = Math.min(...prices);
  if (price < dayMin) {
    return { allow: true, reason: 'SAVE_NEW_LOW_TODAY', dayPrices: prices, dayMin, dayStart: day.start, dayEnd: day.end };
  }

  return { allow: false, reason: 'SKIP_HIGHER_THAN_DAY_MIN', dayPrices: prices, dayMin, dayStart: day.start, dayEnd: day.end };
}

async function insertObservation(obs, req) {
  const perf = makePerf('insertObservation', { title: obs?.title, option: obs?.option, price: obs?.price });
  await pruneOldDataThrottled();
  perf.step('prune');
  await touchWorker(obs, req);
  perf.step('touchWorker');

  const policy = await serverDailySavePolicy(obs);
  perf.step('dailySavePolicy', { policy: policy?.reason });
  if (!policy.allow) {
    const timing = perf.done({ inserted: false, reason: policy.reason });
    return { inserted: false, observation: obs, skipped: true, reason: policy.reason, policy, timing };
  }

  if (!pool) {
    const exists = memory.observations.find(x => x.obsKey === obs.obsKey);
    if (exists) {
      const timing = perf.done({ inserted: false, reason: 'OBS_KEY_DUPLICATE' });
      return { inserted: false, observation: exists, skipped: true, reason: 'OBS_KEY_DUPLICATE', policy, timing };
    }
    memory.observations.unshift(obs);
    memory.observations = memory.observations.slice(0, 200000);
    const timing = perf.done({ inserted: true, reason: policy.reason, memory: true });
    return { inserted: true, observation: obs, reason: policy.reason, policy, timing };
  }
  const r = await pool.query(`INSERT INTO price_observations (
    id,obs_key,product_key,title,title_key,option_text,option_key,price,card_discount_pct,card_text,url,partner_url,product_id,item_id,vendor_item_id,category,worker_id,source,raw,collected_at,created_at
  ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
  ON CONFLICT (obs_key) DO NOTHING RETURNING id`,
  [obs.id, obs.obsKey, obs.productKey, obs.title, obs.titleKey, obs.option, obs.optionKey, obs.price, obs.cardDiscountPct, obs.cardText, obs.url, obs.partnerUrl, obs.productId, obs.itemId, obs.vendorItemId, obs.category, obs.workerId, obs.source, JSON.stringify(obs.raw), obs.collectedAt, obs.createdAt]);
  perf.step('insertPriceObservation', { rowCount: r.rowCount });
  const timing = perf.done({ inserted: r.rowCount > 0, reason: r.rowCount > 0 ? policy.reason : 'OBS_KEY_DUPLICATE' });
  return { inserted: r.rowCount > 0, observation: obs, skipped: r.rowCount <= 0, reason: r.rowCount > 0 ? policy.reason : 'OBS_KEY_DUPLICATE', policy, timing };
}

function uniqueStrings(list) {
  const out = [];
  const seen = new Set();
  for (const x of list || []) {
    const v = String(x || '').trim();
    if (!v || seen.has(v)) continue;
    seen.add(v);
    out.push(v);
  }
  return out;
}

function productKeyLookupVariants(obs = {}) {
  const productId = s(obs.productId, 80);
  const itemId = s(obs.itemId, 80);
  const vendorItemId = s(obs.vendorItemId, 80);

  // v3.184: ID 조합 + 텍스트키 + 기존에 잘못 갈라졌던 배지 포함 텍스트키까지 같이 조회한다.
  // 예: TXT:한돈등갈비냉장, TXT:한돈등갈비냉장로켓프레시 모두 같은 상품으로 흡수.
  const textVariants = productKeyTextVariantsFromTitle(obs.title || '');

  return uniqueStrings([
    obs.productKey,
    ...textVariants,
    productId && vendorItemId ? `ID:${productId}|${vendorItemId}` : '',
    productId && itemId && vendorItemId ? `ID:${productId}|${itemId}|${vendorItemId}` : '',
    productId && itemId ? `ID:${productId}|${itemId}` : '',
    productId ? `ID:${productId}` : ''
  ]);
}
function summarizeObservationPrices(items, cutoff) {
  const prices = (items || []).map(o => n(o.price)).filter(x => x > 0);
  const count = prices.length;
  const avg = count ? Math.round(prices.reduce((a,b) => a + b, 0) / count) : 0;
  const low = count ? Math.min(...prices) : 0;
  const high = count ? Math.max(...prices) : 0;
  return { count, avg, low, high, cutoff };
}


function stripPriceNoiseForMatch(text) {
  let t = s(stripDeliveryBadgeForKey(text), 700);
  if (!t) return '';
  t = t
    .replace(/(\d{1,3})\s*,\s*(\d{3})/g, '$1,$2')
    .replace(/\s+/g, ' ')
    .replace(/[（(][^）)]*(?:당|원)[^）)]*[）)]/gu, '')
    .replace(/\s*\d{1,3}(?:,\d{3})+\s*원\s*\/\s*\d{1,3}(?:,\d{3})+\s*원.*$/u, '')
    .replace(/\s*\d{1,3}(?:,\d{3})+\s*원.*$/u, '')
    .replace(/\s*\d{4,}\s*원.*$/u, '')
    .replace(/\s*\d{1,3}\s*%\s*$/u, '')
    .replace(/\s*(?:할인|와우)\s*$/u, '')
    .trim();
  return t;
}

function looksOptionPartForMatch(part) {
  const t = stripPriceNoiseForMatch(part).replace(/\s+/g, ' ').trim();
  if (!t) return false;
  if (/원|당|할인|쿠폰|카드|즉시|청구|적립|와우/u.test(t)) return false;
  return /^(?:\d+(?:\.\d+)?\s*(?:kg|g|mg|l|L|ml|mL|cm|mm|GB|TB|gb|tb|개|개입|입|매|장|팩|봉|병|캔|롤|세트|회분|포|p|P|매입|통|박스|구|정|알|캡슐)|\d+\s*[xX×]\s*\d+|\d+개\s*입)$/u.test(t);
}

function cleanOptionPartForMatch(part) {
  let t = stripPriceNoiseForMatch(part)
    .replace(/(\d{1,3})\s*,\s*(\d{3})/g, '$1,$2')
    .replace(/[()（）]/g, '')
    .replace(/\s+/g, '')
    .trim();
  if (!t) return '';
  if (/원|당|할인|쿠폰|카드|즉시|청구|적립|와우/u.test(t)) return '';
  return t;
}

function popEmbeddedTailOptionForMatch(titleText) {
  const t = stripPriceNoiseForMatch(titleText || '').replace(/\s+/g, ' ').trim();
  if (!t) return { title: '', option: '' };

  // 예: "신라면 120g" -> title "신라면", option "120g"
  // 예: "QCY 이어폰 블랙" 같은 색상/모델명은 여기서 건드리지 않는다.
  const m = t.match(/^(.*?)(?:\s+)(\d+(?:\.\d+)?\s*(?:kg|g|mg|l|L|ml|mL|cm|mm|GB|TB|gb|tb|개|개입|입|매|장|팩|봉|병|캔|롤|세트|회분|포|p|P|매입|통|박스|구|정|알|캡슐))$/u);
  if (!m) return { title: t, option: '' };

  const base = stripPriceNoiseForMatch(m[1] || '').trim();
  const opt = cleanOptionPartForMatch(m[2] || '');
  if (!base || !opt) return { title: t, option: '' };
  if (normKey(base).length < 2) return { title: t, option: '' };
  return { title: base, option: opt };
}

function splitLooseTitleOption(text) {
  const cleaned = stripPriceNoiseForMatch(stripDeliveryBadgeForKey(cleanTitleText(text || '')));
  if (!cleaned) return { title: '', option: '' };
  const parts = cleaned.split(/\s*,\s*/g).map(x => x.trim()).filter(Boolean);

  if (parts.length <= 1) {
    const embedded = popEmbeddedTailOptionForMatch(cleaned);
    return { title: embedded.title || cleaned, option: embedded.option || '' };
  }

  const optionParts = [];
  while (parts.length > 1 && looksOptionPartForMatch(parts[parts.length - 1])) {
    optionParts.unshift(cleanOptionPartForMatch(parts.pop()));
  }

  let title = stripPriceNoiseForMatch(parts.join(', '));
  const embedded = popEmbeddedTailOptionForMatch(title);
  if (embedded.option) {
    title = embedded.title;
    optionParts.unshift(embedded.option);
  }

  return {
    title: title || cleaned,
    option: optionParts.filter(Boolean).join(', ')
  };
}

function optionPartSortRank(part) {
  const k = normKey(part);
  if (!k) return 90;
  // 용량/중량/크기/저장용량은 앞쪽: 110g, 600ml, 400ml, 2kg ...
  if (/^\d+(?:\.\d+)?(?:kg|g|mg|l|ml|ml|cm|mm|gb|tb)$/i.test(k)) return 10;
  // 2x3, 2×3 같은 구성 표기는 용량 뒤, 색상/맛 앞
  if (/^\d+[x×*]\d+$/i.test(k)) return 15;
  // 색상/맛/호수 같은 텍스트 옵션은 수량보다 앞쪽
  if (!/^\d/.test(k)) return 20;
  if (/호$|번$/u.test(k)) return 20;
  // 수량/개수는 뒤쪽: 8개, 20개, 6입, 15매 ...
  if (/^\d+(?:개|개입|입|매|장|팩|봉|병|캔|롤|세트|회분|포|p|매입|통|박스|구|정|알|캡슐)$/iu.test(k)) return 30;
  return 40;
}

function canonicalOptionPartsForMatch(option) {
  const raw = String(option || '').trim();
  if (!raw) return [];
  const seen = new Set();
  const out = [];
  for (const part of raw.replace(/\s*[·|/]\s*/g, ', ').split(/\s*,\s*/g)) {
    const cleaned = cleanOptionPartForMatch(part);
    if (!cleaned) continue;
    const key = normKey(cleaned);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push({ key, rank: optionPartSortRank(cleaned) });
  }
  out.sort((a, b) => (a.rank - b.rank) || a.key.localeCompare(b.key, 'ko-KR'));
  return out.map(x => x.key);
}

function canonicalOptionMatchKey(option) {
  return canonicalOptionPartsForMatch(option).join('');
}

function looseIdentityFromParts(title, option) {
  const t = splitLooseTitleOption(title || '');
  const opt = cleanOptionText([t.option, option || ''].filter(Boolean).join(', '));
  const baseTitle = stripDeliveryBadgeForKey(stripPriceNoiseForMatch(t.title || title || ''));
  return {
    title: baseTitle,
    titleKey: normKey(baseTitle),
    option: opt,
    optionMatchKey: canonicalOptionMatchKey(opt),
    optionKey: normalizeOptionForKey(opt)
  };
}

function looseIdentityFromRow(row = {}) {
  return looseIdentityFromParts(row.title || '', row.option_text || row.option || '');
}


function clientFallbackStatsFromRaw(raw = {}, currentPrice = 0) {
  const candidates = [
    raw.clientFallbackStats,
    raw.fallbackStats,
    raw.localFallbackStats,
    raw.bridgeFallbackStats,
    raw.raw && raw.raw.clientFallbackStats,
    raw.raw && raw.raw.fallbackStats
  ].filter(Boolean);

  for (const src of candidates) {
    const count = n(src.count || src.n || src.historyCount || 0);
    const avg = n(src.avg || src.ref || src.baselineAvg || src.mean || 0);
    const low = n(src.low || src.min || src.baselineLow || 0);
    const high = n(src.high || src.max || src.baselineHigh || avg || 0);
    const source = s(src.source || src.avgSource || 'client_local_bridge_fallback', 80);
    if (count > 0 && avg > 0) {
      return { count, avg, low, high, source };
    }
  }

  const rootCount = n(raw.fallbackCount || raw.baselineCount || raw.localCount || 0);
  const rootAvg = n(raw.fallbackAvg || raw.baselineAvg || raw.localAvg || 0);
  const rootLow = n(raw.fallbackLow || raw.baselineLow || raw.localLow || 0);
  const rootHigh = n(raw.fallbackHigh || raw.baselineHigh || raw.localHigh || rootAvg || 0);
  if (rootCount > 0 && rootAvg > 0) {
    return { count: rootCount, avg: rootAvg, low: rootLow, high: rootHigh, source: 'client_local_bridge_fallback' };
  }

  return null;
}

function applyClientFallbackStats(out, raw = {}, currentPrice = 0) {
  const fb = clientFallbackStatsFromRaw(raw, currentPrice);
  if (!fb) return out;

  const price = n(currentPrice);
  const serverCount = n(out.count);
  const fallbackAvg = n(fb.avg);
  const fallbackCount = n(fb.count);
  const fallbackLow = n(fb.low);
  const fallbackHigh = n(fb.high);
  const fallbackDrop = fallbackAvg > 0 && price > 0 ? ((fallbackAvg - price) / fallbackAvg) * 100 : 0;

  // v043: 서버 DB 이력이 3건 이하이면 PC/에뮬 로컬 bridge 평균을 우선 표시/판정에 사용한다.
  // 서버는 사용자의 PC/에뮬 SQLite에 직접 접근할 수 없으므로, PC가 /avg 조회 결과를 payload에 넣어 보내야 한다.
  if (serverCount <= 3 && fallbackCount > 0 && fallbackAvg > 0 && (!price || fallbackAvg > price) && fallbackDrop > 0.5) {
    return {
      ...out,
      count: Math.max(serverCount, fallbackCount),
      avg: fallbackAvg,
      low: fallbackLow > 0 ? fallbackLow : 0,
      high: Math.max(n(out.high), fallbackHigh, fallbackAvg),
      clientFallbackStats: fb,
      avgSource: fb.source || 'client_local_bridge_fallback',
      match: `${out.match || 'none'}_client_local_bridge_fallback`
    };
  }

  return out;
}

function summarizeRowsForStats(rows, cutoff, match, variants, currentPrice = 0) {
  const prices = (rows || []).map(r => n(r.price)).filter(x => x > 0);
  const count = prices.length;
  const dbAvg = count ? Math.round(prices.reduce((a, b) => a + b, 0) / count) : 0;
  const dbLow = count ? Math.min(...prices) : 0;
  const dbHigh = count ? Math.max(...prices) : 0;

  const appDiscountFallbackPct = Math.max(0, ...(rows || []).map(r => rawAppDiscountPct(r.raw)).filter(x => x > 0));
  const appDiscountFallbackAvg = estimateAvgFromAppDiscount(currentPrice, appDiscountFallbackPct);
  const dbDrop = dbAvg > 0 && currentPrice > 0 ? ((dbAvg - currentPrice) / dbAvg) * 100 : 0;
  const dbMeaningful = count >= MIN_HISTORY_COUNT && dbAvg > currentPrice && dbDrop > 0.5;

  const out = {
    count,
    avg: dbAvg,
    low: dbLow,
    high: dbHigh,
    dbAvg,
    dbLow,
    dbHigh,
    appDiscountFallbackPct,
    appDiscountFallbackAvg,
    avgSource: 'server_db',
    cutoff,
    match,
    productKeyVariants: variants
  };

  // v038: 서버DB에 의미 있는 평균/최저 이력이 아직 없으면,
  // 에뮬/키위/PC가 저장해 둔 앱 표시 할인율(raw.appDiscount)을 평균 기준가로 역산해서 표시한다.
  // 첫 관측 1건만 있어서 avg == 현재가로 0.0%가 찍히는 케이스를 막는다.
  if (!dbMeaningful && appDiscountFallbackAvg > currentPrice) {
    out.avg = appDiscountFallbackAvg;
    out.low = dbLow > currentPrice ? dbLow : 0;
    out.high = Math.max(dbHigh, appDiscountFallbackAvg);
    out.avgSource = 'app_discount_fallback';
    out.match = `${match || 'none'}_app_discount_fallback`;
  }

  return out;
}

function normalizeEmulStatsItem(body = {}) {
  const optionRaw = body.option || body.optionText || body.optionKey || '';
  const ids = canonicalProductIdentity({
    title: body.title || '',
    option: optionRaw,
    productId: body.productId || '',
    itemId: body.itemId || '',
    vendorItemId: body.vendorItemId || ''
  });
  const count = n(body.count || body.n || body.historyCount || 0);
  const avg = n(body.avg || body.ref || body.mean || body.avgPrice || 0);
  const low = n(body.low || body.min || body.lowPrice || 0);
  const high = n(body.high || body.max || body.highPrice || avg || 0);
  const source = s(body.source || body.avgSource || 'emulator_server_stats_cache', 80);
  const workerId = s(body.workerId || body.collectorId || 'emul-stats-sync', 120);
  const firstSeenAt = n(body.firstSeenAt || body.first_seen_at || body.from || 0);
  const lastSeenAt = n(body.lastSeenAt || body.last_seen_at || body.until || body.collectedAt || now());
  const updatedAt = now();
  if (!ids.title || !ids.optionKey || count <= 0 || avg <= 0) throw new Error('EMPTY_EMUL_STATS_REQUIRED_FIELD');
  const statKey = crypto.createHash('sha1').update(`${ids.productKey}|${ids.optionKey}|${source}`).digest('hex');
  return {
    id: s(body.id) || `emulstat_${statKey}`,
    statKey,
    productKey: ids.productKey,
    title: ids.title,
    titleKey: ids.titleKey,
    option: ids.option,
    optionKey: ids.optionKey,
    count,
    avg,
    low,
    high,
    source,
    workerId,
    firstSeenAt,
    lastSeenAt,
    updatedAt,
    raw: body
  };
}

async function upsertEmulStats(stat) {
  if (!pool) {
    const idx = memory.emulStats.findIndex(x => x.statKey === stat.statKey);
    if (idx >= 0) memory.emulStats[idx] = stat;
    else memory.emulStats.unshift(stat);
    memory.emulStats = memory.emulStats.slice(0, 50000);
    return { upserted: true, stat };
  }
  await pool.query(`INSERT INTO emul_price_stats (
    id, stat_key, product_key, title, title_key, option_text, option_key, count, avg_price, low_price, high_price, source, worker_id, raw, first_seen_at, last_seen_at, updated_at
  ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
  ON CONFLICT (stat_key) DO UPDATE SET
    product_key=$3,
    title=$4,
    title_key=$5,
    option_text=$6,
    option_key=$7,
    count=$8,
    avg_price=$9,
    low_price=$10,
    high_price=$11,
    source=$12,
    worker_id=$13,
    raw=$14,
    first_seen_at=$15,
    last_seen_at=$16,
    updated_at=$17`,
    [stat.id, stat.statKey, stat.productKey, stat.title, stat.titleKey, stat.option, stat.optionKey, stat.count, stat.avg, stat.low, stat.high, stat.source, stat.workerId, JSON.stringify(stat.raw), stat.firstSeenAt, stat.lastSeenAt, stat.updatedAt]);
  return { upserted: true, stat };
}

function summarizeEmulStatsRows(rows = [], cutoff = 0) {
  const valid = (rows || []).map(r => ({
    count: n(r.count || r.count_price || r.n || 0),
    avg: n(r.avg_price || r.avg || 0),
    low: n(r.low_price || r.low || 0),
    high: n(r.high_price || r.high || 0),
    source: s(r.source || 'emulator_server_stats_cache', 80),
    lastSeenAt: n(r.last_seen_at || r.lastSeenAt || 0),
  })).filter(x => x.count > 0 && x.avg > 0);
  if (!valid.length) return null;
  const total = valid.reduce((a, b) => a + b.count, 0);
  const avg = total ? Math.round(valid.reduce((a, b) => a + b.avg * b.count, 0) / total) : 0;
  const lows = valid.map(x => x.low).filter(x => x > 0);
  const highs = valid.map(x => x.high).filter(x => x > 0);
  return {
    count: total,
    avg,
    low: lows.length ? Math.min(...lows) : 0,
    high: highs.length ? Math.max(...highs, avg) : avg,
    source: uniqueStrings(valid.map(x => x.source)).join('+') || 'emulator_server_stats_cache',
    cutoff,
    lastSeenAt: Math.max(...valid.map(x => x.lastSeenAt || 0))
  };
}

async function getEmulStatsCacheForObservation(obs, variants, wantedTitleKey, wantedOptionMatchKey, cutoff) {
  const wanted = looseIdentityFromParts(obs.title, obs.option);
  function rowMatches(row) {
    const rowId = looseIdentityFromRow(row);
    const productMatch = variants.includes(String(row.product_key || row.productKey || ''));
    const titleMatch = wantedTitleKey && rowId.titleKey === wantedTitleKey;
    const optionMatch = wantedOptionMatchKey
      ? rowId.optionMatchKey === wantedOptionMatchKey
      : !rowId.optionMatchKey;
    return Boolean((productMatch || titleMatch) && optionMatch);
  }

  if (!pool) {
    const rows = memory.emulStats.filter(r => n(r.lastSeenAt || r.updatedAt) >= cutoff && rowMatches({
      product_key: r.productKey,
      title: r.title,
      option_text: r.option,
      option_key: r.optionKey,
      count: r.count,
      avg_price: r.avg,
      low_price: r.low,
      high_price: r.high,
      source: r.source,
      last_seen_at: r.lastSeenAt,
    }));
    return summarizeEmulStatsRows(rows, cutoff);
  }

  const params = [variants, wantedTitleKey || '', cutoff];
  let where = `(product_key = ANY($1::text[]) OR title_key = $2`;
  if (STATS_ENABLE_TITLE_ILIKE && wanted.title && wanted.title.length >= 2) {
    params.push(`%${wanted.title}%`);
    where += ` OR title ILIKE $4`;
  }
  where += `) AND last_seen_at >= $3`;

  const { rows } = await pool.query(`SELECT product_key, title, title_key, option_text, option_key, count, avg_price, low_price, high_price, source, worker_id, last_seen_at, updated_at
    FROM emul_price_stats
    WHERE ${where}
    ORDER BY updated_at DESC
    LIMIT 1000`, params);
  return summarizeEmulStatsRows(rows.filter(rowMatches), cutoff);
}

async function applyEmulServerStatsCache(out, obs, variants, wantedTitleKey, wantedOptionMatchKey, cutoff) {
  const serverCount = n(out.count);
  const price = n(obs.price);
  const serverAvg = n(out.avg);
  const serverLow = n(out.low);
  const serverDrop = serverAvg > 0 && price > 0 ? ((serverAvg - price) / serverAvg) * 100 : 0;
  const avgSource = String(out.avgSource || '');

  // v050: 기존 조건(serverCount <= 3) 때문에 app_discount_fallback 값이 4건 이상처럼 보이면
  // 에뮬 통계 캐시가 있어도 무시되는 문제가 있었다.
  // 서버 DB가 진짜 의미 있는 server_db 평균일 때만 유지하고,
  // app_discount_fallback / low=0 / 현재가만 쌓인 DB / 약한 DB는 에뮬 조회값을 우선 사용한다.
  const keepServerDb = Boolean(
    avgSource === 'server_db' &&
    serverCount > 3 &&
    serverAvg > price &&
    serverDrop > 0.5 &&
    serverLow > 0
  );
  if (keepServerDb) return out;

  const emul = await getEmulStatsCacheForObservation(obs, variants, wantedTitleKey, wantedOptionMatchKey, cutoff);
  if (!emul || n(emul.count) <= 0 || n(emul.avg) <= 0) return out;
  const emulAvg = n(emul.avg);
  const emulLow = n(emul.low);
  const emulHigh = n(emul.high);
  const drop = emulAvg > 0 && price > 0 ? ((emulAvg - price) / emulAvg) * 100 : 0;
  if (price > 0 && emulAvg <= price && drop <= 0.5) return out;
  return {
    ...out,
    count: n(emul.count),
    avg: emulAvg,
    low: emulLow,
    high: Math.max(n(out.high), emulHigh, emulAvg),
    emulStatsCache: emul,
    avgSource: 'emulator_server_stats_cache',
    match: `${out.match || 'none'}_emul_server_stats_cache`
  };
}

async function getObservationStats(obs) {
  const cutoff = now() - PRICE_RETENTION_MS;
  const variants = productKeyLookupVariants(obs);
  const wanted = looseIdentityFromParts(obs.title, obs.option);
  const wantedTitleKey = wanted.titleKey || obs.titleKey;
  const wantedOptionMatchKey = wanted.optionMatchKey || canonicalOptionMatchKey(obs.option || obs.optionKey || '');

  if (!wantedTitleKey && !wantedOptionMatchKey) {
    return { count: 0, avg: 0, low: 0, high: 0, cutoff, match: 'none', productKeyVariants: variants };
  }

  // v062: 에뮬 통계 캐시가 있으면 느린 price_observations 대량 스캔 전에 먼저 사용한다.
  // 평균/최저는 그대로 나오면서 반환 시간이 크게 줄어든다.
  try {
    const emulFast = await getEmulStatsCacheForObservation(obs, variants, wantedTitleKey, wantedOptionMatchKey, cutoff);
    if (emulFast && n(emulFast.count) > 0 && n(emulFast.avg) > 0) {
      const fastStats = applyClientFallbackStats({
        count: n(emulFast.count),
        avg: n(emulFast.avg),
        low: n(emulFast.low),
        high: Math.max(n(emulFast.high), n(emulFast.avg)),
        dbAvg: 0,
        dbLow: 0,
        dbHigh: 0,
        emulStatsCache: emulFast,
        avgSource: 'emulator_server_stats_cache_fast',
        cutoff,
        match: 'fast_emul_stats_cache',
        productKeyVariants: variants
      }, obs.raw, n(obs.price));
      return fastStats;
    }
  } catch (e) {
    console.warn('[stats-fast-emul-cache] skipped', String(e?.message || e));
  }

  function rowMatches(row) {
    const rowId = looseIdentityFromRow(row);
    const productMatch = variants.includes(String(row.product_key || row.productKey || ''));
    const titleMatch = wantedTitleKey && rowId.titleKey === wantedTitleKey;
    const optionMatch = wantedOptionMatchKey
      ? rowId.optionMatchKey === wantedOptionMatchKey
      : !rowId.optionMatchKey;
    return Boolean((productMatch || titleMatch) && optionMatch);
  }

  if (!pool) {
    const items = memory.observations.filter(o => n(o.collectedAt || o.createdAt) >= cutoff && rowMatches({
      product_key: o.productKey,
      title: o.title,
      option_text: o.option,
      price: o.price,
      raw: o.raw
    }));
    {
      const baseStats = summarizeRowsForStats(items, cutoff, items.length ? 'smart_memory' : 'none', variants, n(obs.price));
      const emulStats = await applyEmulServerStatsCache(baseStats, obs, variants, wantedTitleKey, wantedOptionMatchKey, cutoff);
      return applyClientFallbackStats(emulStats, obs.raw, n(obs.price));
    }
  }

  // v064: 기존 smart scan은 후보 5000행을 읽어 JS에서 필터링해서 Render/Postgres에서 timeout이 잦았다.
  // 기본은 index-friendly exact aggregate로 바로 내려가고, 꼭 필요할 때만 STATS_SMART_SCAN_ENABLE=true로 켠다.
  if (STATS_SMART_SCAN_ENABLE) {
    const params = [cutoff, variants, wantedTitleKey || ''];
    let where = `collected_at >= $1 AND price > 0 AND (product_key = ANY($2::text[]) OR title_key = $3`;
    if (STATS_ENABLE_TITLE_ILIKE && wanted.title && wanted.title.length >= 2) {
      params.push(`%${wanted.title}%`);
      where += ` OR title ILIKE $4`;
    }
    where += `)`;

    const { rows } = await pool.query(`SELECT product_key, title, title_key, option_text, option_key, price, collected_at, source, raw
      FROM price_observations
      WHERE ${where}
      ORDER BY collected_at DESC
      LIMIT 5000`, params);

    const matched = rows.filter(rowMatches);

    if (matched.length) {
      const baseStats = summarizeRowsForStats(matched, cutoff, 'smart_canonical', variants, n(obs.price));
      const emulStats = await applyEmulServerStatsCache(baseStats, obs, variants, wantedTitleKey, wantedOptionMatchKey, cutoff);
      return applyClientFallbackStats(emulStats, obs.raw, n(obs.price));
    }
  }

  // v064 기본 경로: 정확 product/title key + option key aggregate. 평균/최저 기능은 유지하되 DB 반환량을 크게 줄인다.
  async function queryByProductKeys(keys, optionKey) {
    if (!keys.length) return { count: 0, avg: 0, low: 0, high: 0 };
    const { rows } = await pool.query(`SELECT COUNT(*)::int AS count, ROUND(AVG(price))::int AS avg, MIN(price)::int AS low, MAX(price)::int AS high
      FROM price_observations
      WHERE product_key = ANY($1::text[]) AND option_key=$2 AND collected_at >= $3 AND price > 0`,
      [keys, obs.optionKey, cutoff]);
    return rows[0] || {};
  }

  async function queryByTitleKey(titleKey, optionKey) {
    if (!titleKey) return { count: 0, avg: 0, low: 0, high: 0 };
    const { rows } = await pool.query(`SELECT COUNT(*)::int AS count, ROUND(AVG(price))::int AS avg, MIN(price)::int AS low, MAX(price)::int AS high
      FROM price_observations
      WHERE title_key=$1 AND option_key=$2 AND collected_at >= $3 AND price > 0`,
      [titleKey, optionKey, cutoff]);
    return rows[0] || {};
  }

  let r = await queryByProductKeys(variants, obs.optionKey);
  let match = 'product_key';

  if (n(r.count) <= 0 && obs.titleKey) {
    r = await queryByTitleKey(obs.titleKey, obs.optionKey);
    match = 'title_key';
  }

  const baseStats = {
    count: n(r.count),
    avg: n(r.avg),
    low: n(r.low),
    high: n(r.high),
    dbAvg: n(r.avg),
    dbLow: n(r.low),
    dbHigh: n(r.high),
    avgSource: 'server_db',
    cutoff,
    match: n(r.count) > 0 ? match : 'none',
    productKeyVariants: variants
  };
  const emulStats = await applyEmulServerStatsCache(baseStats, obs, variants, wantedTitleKey, wantedOptionMatchKey, cutoff);
  return applyClientFallbackStats(emulStats, obs.raw, n(obs.price));
}

async function getObservationStatsSafe(obs, ms = STATS_TIMEOUT_MS) {
  const perf = makePerf('getObservationStatsSafe', { title: obs?.title, option: obs?.option, price: obs?.price, timeoutMs: ms });
  const cached = getCachedObservationStats(obs);
  if (cached) {
    perf.step('memoryCacheHit', { match: cached.match, count: n(cached.count) });
    const timing = perf.done({ cached: true, match: cached.match, count: n(cached.count) });
    try { return { ...applyClientFallbackStats(cached, obs?.raw || {}, n(obs?.price)), timing }; }
    catch { return { ...cached, timing }; }
  }
  try {
    const stats = await withTimeout(getObservationStats(obs), ms, 'STATS_TIMEOUT');
    perf.step('getObservationStats', { match: stats?.match, count: n(stats?.count), avg: n(stats?.avg), low: n(stats?.low) });
    setCachedObservationStats(obs, stats);
    const timing = perf.done({ timeout: false, match: stats?.match, count: n(stats?.count), avg: n(stats?.avg), low: n(stats?.low) });
    return { ...stats, timing };
  } catch (e) {
    timedOutStatsRequests += 1;
    perf.step('statsTimeout', { error: String(e?.message || e) });
    const fallback = emptyStatsFallback(obs, String(e?.message || e || 'stats_timeout').toLowerCase());
    setCachedObservationStats(obs, fallback);
    const timing = perf.done({ timeout: true, match: fallback.match, error: String(e?.message || e) });
    return { ...fallback, timing };
  }
}

function shouldCreateAlertFromObservation(obs, stats) {
  const price = n(obs.price);
  const avg = n(stats.avg);
  const low = n(stats.low);
  const count = n(stats.count);
  const avgDropPct = avg > 0 && price > 0 ? ((avg - price) / avg) * 100 : 0;
  const lowDropPct = low > 0 && price > 0 ? ((low - price) / low) * 100 : 0;
  const enoughHistory = count >= MIN_HISTORY_COUNT;
  const avgOk = avgDropPct >= ALERT_MIN_AVG_DROP_PCT;
  const lowOk = !ALERT_REQUIRE_LOW_MATCH || low <= 0 || price <= low;
  return {
    create: Boolean(enoughHistory && avgOk && lowOk),
    reason: !enoughHistory ? 'NOT_ENOUGH_HISTORY' : (!avgOk ? 'AVG_DROP_TOO_LOW' : (!lowOk ? 'HIGHER_THAN_LOW' : 'OK')),
    avgDropPct,
    lowDropPct,
    count,
    avg,
    low
  };
}

function alertFromObservation(obs, stats, decision) {
  const appPct = rawAppDiscountPct(obs.raw);
  const effectiveDrop = Math.max(f(decision.avgDropPct), f(decision.lowDropPct), appPct);
  const section = effectiveDrop >= bigDealThresholdForPrice(obs.price) ? '대박' : (effectiveDrop >= 25 ? '핫딜' : '인기');
  return normalizeAlert({
    id: id('alert'),
    dedupeKey: alertDedupeFromObservation(obs),
    source: 'collector_observe',
    section,
    title: obs.title,
    option: obs.option,
    price: obs.price,
    avg: stats.avg,
    low: stats.low,
    dropPct: decision.avgDropPct,
    appDiscount: appPct,
    cardText: obs.cardText,
    cardDiscountPct: obs.cardDiscountPct,
    partnerUrl: obs.partnerUrl || obs.url,
    url: obs.partnerUrl || obs.url,
    originalUrl: obs.url,
    productId: obs.productId,
    itemId: obs.itemId,
    vendorItemId: obs.vendorItemId,
    raw: { observation: obs, stats, decision }
  });
}

function deviceAllows(device, alert) {
  const st = device.settings || {};
  const cats = st.categories || {};
  const section = String(alert.section || '');
  const compactSection = section.replace(/\s+/g, '');
  const allowBigDeal = cats.bigDeal !== false && cats.urgent !== false;
  const allowNormal = cats.realtimeTrend !== false && cats.realtime !== false;
  const big = isBigAlert(alert);

  if (big && !allowBigDeal) return false;
  if (!big && !allowNormal) return false;
  if (compactSection.includes('골드') || compactSection.includes('골드박스')) return false;

  const kw = st.keywords || {};
  const text = `${alert.title} ${alert.option}`.toLowerCase();
  const include = String(kw.include || '').split(',').map(x => x.trim()).filter(Boolean);
  const exclude = String(kw.exclude || '').split(',').map(x => x.trim()).filter(Boolean);
  if (exclude.some(k => text.includes(k.toLowerCase()))) return false;
  if (include.length && !include.some(k => text.includes(k.toLowerCase()))) return false;
  return true;
}

async function sendPush(alert) {
  const devices = await listDevices();
  const messages = [];
  for (const d of devices) {
    const token = d.expoPushToken;
    if (!token || !Expo.isExpoPushToken(token)) continue;
    if (!deviceAllows(d, alert)) continue;
    messages.push({
      to: token,
      sound: 'default',
      title: pushTitle(alert),
      body: pushMessage(alert),
      data: { alertId: alert.id, url: alert.url, kind: isBigAlert(alert) ? 'big' : 'hotdeal', message: pushCompactText(alert) },
      channelId: 'hotdeal',
      priority: 'high'
    });
  }
  const tickets = [];
  for (const chunk of expo.chunkPushNotifications(messages)) {
    const sent = await expo.sendPushNotificationsAsync(chunk);
    tickets.push(...sent);
  }
  return { sent: messages.length, tickets };
}

app.get('/health', (req, res) => {
  res.json({ ok: true, service: 'KUHOT_UNIFIED_CENTRAL', app: 'KUHOT', version: SERVER_VERSION, deployMarker: 'SERVER_V073_20260702_NO_SERVER_PARTNERS_PASS_THROUGH', mode: pool ? 'postgres' : 'memory', time: now(), uptimeMs: now() - startedAt, activeHeavyRequests, rejectedHeavyRequests, timedOutStatsRequests, heavyMaxActive: HEAVY_MAX_ACTIVE, heavyRetryAfterMs: HEAVY_RETRY_AFTER_MS, statsTimeoutMs: STATS_TIMEOUT_MS, observeStatsTimeoutMs: OBSERVE_STATS_TIMEOUT_MS, dbQueryTimeoutMs: DB_QUERY_TIMEOUT_MS, dbConnectTimeoutMs: DB_CONNECT_TIMEOUT_MS, statsCacheTtlMs: STATS_CACHE_TTL_MS, statsCacheSize: statsMemoryCache.size, statsEnableTitleIlike: STATS_ENABLE_TITLE_ILIKE, dailySaveEnableTitleIlike: DAILY_SAVE_ENABLE_TITLE_ILIKE, statsSmartScanEnable: STATS_SMART_SCAN_ENABLE, dbTimeoutFastIndexes: true, deliveryBadgePreserved: true, zeroPctStatsOmit: true,
    partnersDeeplinkFix: false, partnersFailOpenRateGuard: false, serverPartnersConversionDisabled: true, serverPartnersPassThroughCompat: true, partnersCircuitOpenUntil: 0, partnersCacheSize: 0, partnersFailCacheSize: 0, perfTimingDiagnose: true, perfLogEnabled: PERF_LOG_ENABLED, perfSlowMs: PERF_SLOW_MS, perfDebugResponse: PERF_DEBUG_RESPONSE, bootFastIndexes: BOOT_FAST_INDEXES, bootSchemaIndexes: BOOT_SCHEMA_INDEXES, pruneIntervalMs: PRUNE_INTERVAL_MS, lastPruneAt, fastNotifyResponse: FAST_NOTIFY_RESPONSE, skipSilentObserveStats: SKIP_SILENT_OBSERVE_STATS, backgroundTelegramQueued, backgroundTelegramSent, backgroundTelegramFailed, backgroundPushQueued, backgroundPushSent, backgroundPushFailed, alertRetentionMs: ALERT_RETENTION_MS, priceRetentionMs: PRICE_RETENTION_MS });
});

app.post('/devices/register', async (req, res) => {
  try { res.json({ ok: true, device: await registerDevice(req.body || {}) }); }
  catch (e) { res.status(400).json({ ok: false, error: String(e.message || e) }); }
});

app.get('/debug/latest', async (req, res) => {
  try {
    const alerts = await getAlerts(10);
    res.json({ ok: true, count: alerts.length, latest: alerts[0] || null, items: alerts.map(a => ({ id: a.id, section: a.section, title: a.title, price: a.price, source: a.source, createdAt: a.createdAt })) });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});




// v072 호환용: 서버는 파트너스 변환을 하지 않고 받은 쿠팡 링크를 그대로 돌려준다
app.post('/partners/deeplink', async (req, res) => {
  try {
    const url = req.body?.url || req.body?.originalUrl || req.body?.coupangUrl || req.body?.productUrl || '';
    const requestedSubId = req.body?.subId || req.body?.cpSubId || req.body?.partnerSubId || '';
    const result = await createCoupangDeeplinkServer(url, requestedSubId);
    if (!result.ok) return res.status(400).json(result);
    res.json(result);
  } catch (e) {
    res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

// v072 호환용 별칭: 기존 에뮬/PC가 /partner_link를 호출해도 원본 링크를 그대로 반환한다.
app.post('/partner_link', async (req, res) => {
  try {
    const url = req.body?.url || req.body?.originalUrl || req.body?.coupangUrl || req.body?.productUrl || '';
    const requestedSubId = req.body?.subId || req.body?.cpSubId || req.body?.partnerSubId || '';
    const result = await createCoupangDeeplinkServer(url, requestedSubId);
    if (!result.ok) return res.status(400).json({ ...result, partnerOk: true, finalUrl: url });
    res.json({ ...result, partnerOk: true, finalUrl: result.finalUrl || result.partnerUrl || url });
  } catch (e) {
    res.status(400).json({ ok: false, partnerOk: false, error: String(e.message || e) });
  }
});

// 백필 전용: 기존 WowDrop SQLite DB의 최근 N일 가격기록을 서버 DB에 심는 용도.
// 알림/푸시/텔레그램을 만들지 않고 price_observations에만 저장한다.
app.post('/collector/backfill-batch', async (req, res) => {
  try {
    if (!allowCollector(req, res)) return;
    const items = Array.isArray(req.body?.items) ? req.body.items : (Array.isArray(req.body) ? req.body : []);
    if (!items.length) return res.status(400).json({ ok: false, error: 'EMPTY_ITEMS' });

    const common = req.body?.common && typeof req.body.common === 'object' ? req.body.common : {};
    const results = [];
    let inserted = 0;
    let skipped = 0;
    let failed = 0;

    for (const item of items.slice(0, OBSERVE_BATCH_LIMIT)) {
      try {
        const merged = { ...common, ...item, source: item.source || common.source || 'wowdrop_sqlite_backfill' };
        const obs = normalizeObservation(merged);
        const obsResult = await insertObservation(obs, req);
        if (obsResult.inserted) inserted += 1;
        else skipped += 1;
        results.push({ ok: true, inserted: obsResult.inserted, reason: obsResult.reason || '', policy: obsResult.policy || null, title: obs.title, option: obs.option, price: obs.price, collectedAt: obs.collectedAt });
      } catch (e) {
        failed += 1;
        results.push({ ok: false, error: String(e.message || e), raw: item });
      }
    }

    res.json({ ok: true, mode: 'backfill_only_no_alert_no_push', count: results.length, inserted, skipped, failed, results });
  } catch (e) {
    res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});



// [서버] v046: 에뮬 로컬 DB 원본 행을 올리는 것이 아니라,
// 에뮬 DB에서 조회/계산한 평균·최저 통계값만 서버 캐시에 올린다.
// 이후 /collector/stats, /collector/observe는 서버 DB count <= 3이면 이 캐시를 fallback으로 사용한다.
app.post('/collector/emul-stats-batch', async (req, res) => {
  try {
    if (!allowCollector(req, res)) return;
    const items = Array.isArray(req.body?.items) ? req.body.items : (Array.isArray(req.body) ? req.body : []);
    if (!items.length) return res.status(400).json({ ok: false, error: 'EMPTY_ITEMS' });
    const common = req.body?.common && typeof req.body.common === 'object' ? req.body.common : {};
    const results = [];
    let upserted = 0;
    let failed = 0;
    for (const item of items.slice(0, EMUL_STATS_BATCH_LIMIT)) {
      try {
        const merged = { ...common, ...item, source: item.source || common.source || 'emulator_server_stats_cache' };
        const stat = normalizeEmulStatsItem(merged);
        await upsertEmulStats(stat);
        upserted += 1;
        results.push({ ok: true, productKey: stat.productKey, optionKey: stat.optionKey, title: stat.title, option: stat.option, count: stat.count, avg: stat.avg, low: stat.low, high: stat.high, source: stat.source });
      } catch (e) {
        failed += 1;
        results.push({ ok: false, error: String(e.message || e), raw: item });
      }
    }
    res.json({ ok: true, mode: 'emul_stats_cache_only_no_raw_observations', count: results.length, upserted, failed, results });
  } catch (e) {
    res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

app.get('/collector/emul-stats', async (req, res) => {
  try {
    const obs = normalizeObservation({
      title: req.query.title || req.query.q || '',
      option: req.query.option || '',
      price: req.query.price || 1,
      productId: req.query.productId || '',
      itemId: req.query.itemId || '',
      vendorItemId: req.query.vendorItemId || ''
    });
    const cutoff = now() - PRICE_RETENTION_MS;
    const variants = productKeyLookupVariants(obs);
    const wanted = looseIdentityFromParts(obs.title, obs.option);
    const stats = await getEmulStatsCacheForObservation(obs, variants, wanted.titleKey || obs.titleKey, wanted.optionMatchKey || canonicalOptionMatchKey(obs.option || obs.optionKey || ''), cutoff);
    res.json({ ok: true, productKey: obs.productKey, optionKey: obs.optionKey, title: obs.title, option: obs.option, stats: stats || { count: 0, avg: 0, low: 0, high: 0, avgSource: 'emulator_server_stats_cache', match: 'none' } });
  } catch (e) {
    res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});


app.post('/collector/observe-batch', async (req, res) => {
  try {
    if (!allowCollector(req, res)) return;
    const items = Array.isArray(req.body?.items) ? req.body.items : (Array.isArray(req.body) ? req.body : []);
    if (!items.length) return res.status(400).json({ ok: false, error: 'EMPTY_ITEMS' });
    const results = [];
    let alertsCreated = 0;
    let pushed = 0;
    for (const item of items.slice(0, OBSERVE_BATCH_LIMIT)) {
      try {
        const merged = { ...(req.body.common || {}), ...item };
        const obs = normalizeObservation(merged);
        const obsResult = await insertObservation(obs, req);
        const silentCollector = !!(merged?.muteAlert || merged?.noAlert || merged?.silent || merged?.noTelegram || merged?.collectorOnly || req.body?.common?.muteAlert || req.body?.common?.noTelegram);
        const stats = (silentCollector && SKIP_SILENT_OBSERVE_STATS)
          ? emptyStatsFallback(obs, 'silent_observe_stats_skipped')
          : await getObservationStatsSafe(obs, OBSERVE_STATS_TIMEOUT_MS);
        const decision = silentCollector
          ? { create: false, reason: 'collector_only_mute_alert' }
          : shouldCreateAlertFromObservation(obs, stats);
        let alertResult = { created: false, duplicate: false, reason: decision.reason };
        let push = { sent: 0, skipped: true };
        let telegram = { sent: false, skipped: true };
        if (decision.create) {
          const alert = alertFromObservation(obs, stats, decision);
          const inserted = await insertAlert(alert);
          alertResult = { created: inserted.inserted, duplicate: !inserted.inserted, alert: inserted.alert };
          if (inserted.inserted) {
            const sent = await sendTelegramPushForResponse(alert);
            telegram = sent.telegram;
            push = sent.push;
            alertsCreated += 1;
            pushed += n(push.sent || 0);
          }
        }
        results.push({ ok: true, observationInserted: obsResult.inserted, observationReason: obsResult.reason || '', observationPolicy: obsResult.policy || null, title: obs.title, option: obs.option, price: obs.price, stats, decision, alert: alertResult, push, telegram });
      } catch (e) {
        results.push({ ok: false, error: String(e.message || e), raw: item });
      }
    }
    res.json({ ok: true, count: results.length, alertsCreated, pushed, results });
  } catch (e) {
    res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

app.post('/collector/observe', async (req, res) => {
  const perf = makePerf('/collector/observe');
  try {
    if (!allowCollector(req, res)) return;
    const body = req.body || {};
    const obs = normalizeObservation(body);
    perf.step('normalizeObservation');
    const obsResult = await insertObservation(obs, req);
    perf.step('insertObservation', { obsReason: obsResult?.reason, obsInserted: !!obsResult?.inserted, obsTiming: obsResult?.timing });
    const silentCollector = !!(body?.muteAlert || body?.noAlert || body?.silent || body?.noTelegram || body?.collectorOnly);
    const stats = (silentCollector && SKIP_SILENT_OBSERVE_STATS)
      ? emptyStatsFallback(obs, 'silent_observe_stats_skipped')
      : await getObservationStatsSafe(obs, OBSERVE_STATS_TIMEOUT_MS);
    perf.step('getObservationStatsSafe', { match: stats?.match, count: n(stats?.count), avg: n(stats?.avg), low: n(stats?.low), statsTiming: stats?.timing });
    const decision = silentCollector
      ? { create: false, reason: 'collector_only_mute_alert' }
      : shouldCreateAlertFromObservation(obs, stats);
    perf.step('decision', { reason: decision?.reason, create: !!decision?.create });
    let alertResult = { created: false, duplicate: false, reason: decision.reason };
    let push = { sent: 0, skipped: true };
    let telegram = { sent: false, skipped: true };
    if (decision.create) {
      const alert = alertFromObservation(obs, stats, decision);
      const inserted = await insertAlert(alert);
      perf.step('insertAlert', { inserted: !!inserted?.inserted });
      alertResult = { created: inserted.inserted, duplicate: !inserted.inserted, alert: inserted.alert };
      if (inserted.inserted) {
        const sent = await sendTelegramPushForResponse(alert);
        perf.step('sendTelegramPushForResponse');
        telegram = sent.telegram;
        push = sent.push;
      }
    }
    const timing = perf.done({ title: obs.title, price: obs.price, match: stats?.match, decision: decision?.reason });
    res.json({ ok: true, observationInserted: obsResult.inserted, observationReason: obsResult.reason || '', observationPolicy: obsResult.policy || null, observation: obs, stats, decision, alert: alertResult, push, telegram, timing: PERF_DEBUG_RESPONSE ? timing : undefined });
  } catch (e) {
    const timing = perf.done({ error: String(e.message || e) });
    res.status(400).json({ ok: false, error: String(e.message || e), timing: PERF_DEBUG_RESPONSE ? timing : undefined });
  }
});

app.get('/collector/stats', async (req, res) => {
  try {
    const obs = normalizeObservation({
      title: req.query.title || req.query.q || '',
      option: req.query.option || '',
      price: req.query.price || 1,
      productId: req.query.productId || '',
      itemId: req.query.itemId || '',
      vendorItemId: req.query.vendorItemId || ''
    });
    const stats = await getObservationStatsSafe(obs);
    res.json({ ok: true, productKey: obs.productKey, optionKey: obs.optionKey, title: obs.title, option: obs.option, stats });
  } catch (e) {
    res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

app.get('/collector/observations', async (req, res) => {
  try {
    await pruneOldData();
    const limit = Math.min(Math.max(n(req.query.limit || 50), 1), 300);
    if (!pool) return res.json({ ok: true, observations: memory.observations.slice(0, limit) });
    const { rows } = await pool.query(`SELECT id, product_key AS "productKey", title, option_text AS "option", price, card_discount_pct AS "cardDiscountPct", card_text AS "cardText", worker_id AS "workerId", source, collected_at AS "collectedAt", created_at AS "createdAt" FROM price_observations ORDER BY collected_at DESC LIMIT $1`, [limit]);
    res.json({ ok: true, observations: rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});


// TEMP DEBUG: DB에 실제로 어떤 title/option/product_key로 쌓였는지 그룹별 확인
// 사용 후 필요 없으면 제거 가능. COLLECTOR_KEY가 설정돼 있으면 x-collector-key 필요.
app.get('/collector/debug-groups', async (req, res) => {
  try {
    if (!allowCollector(req, res)) return;
    await pruneOldData();
    if (!pool) return res.json({ ok: true, mode: 'memory', groups: [] });

    const limit = Math.min(Math.max(n(req.query.limit || 80), 1), 300);
    const days = Math.min(Math.max(n(req.query.days || Math.ceil(PRICE_RETENTION_MS / (24 * 60 * 60 * 1000))), 1), 30);
    const cutoff = now() - days * 24 * 60 * 60 * 1000;

    const rawTitle = s(req.query.title || req.query.q || '', 300);
    const rawOption = s(req.query.option || '', 300);
    const titleKey = normKey(stripDeliveryBadgeForKey(cleanTitleText(rawTitle)));
    const optionKey = normalizeOptionForKey(rawOption);
    const titleLike = rawTitle ? `%${rawTitle.replace(/[\\%_]/g, '\\$&')}%` : '';
    const titleKeyLike = titleKey ? `%${titleKey}%` : '';

    const where = ['collected_at >= $1', 'price > 0'];
    const params = [cutoff];
    let idx = params.length;

    if (rawTitle || titleKey) {
      where.push(`(title_key = $${++idx} OR title_key LIKE $${++idx} OR title ILIKE $${++idx})`);
      params.push(titleKey, titleKeyLike, titleLike);
    }
    if (rawOption || optionKey) {
      where.push(`option_key = $${++idx}`);
      params.push(optionKey);
    }

    params.push(limit);
    const limitParam = params.length;

    const { rows } = await pool.query(`
      SELECT
        product_key AS "productKey",
        title,
        title_key AS "titleKey",
        COALESCE(option_text, '') AS "option",
        COALESCE(option_key, '') AS "optionKey",
        COUNT(*)::int AS count,
        ROUND(AVG(price))::int AS avg,
        MIN(price)::int AS low,
        MAX(price)::int AS high,
        MIN(collected_at)::bigint AS "firstSeenAt",
        MAX(collected_at)::bigint AS "lastSeenAt",
        ARRAY_REMOVE(ARRAY_AGG(DISTINCT source), NULL) AS sources,
        ARRAY_REMOVE(ARRAY_AGG(DISTINCT worker_id), NULL) AS workers
      FROM price_observations
      WHERE ${where.join(' AND ')}
      GROUP BY product_key, title, title_key, option_text, option_key
      ORDER BY count DESC, "lastSeenAt" DESC
      LIMIT $${limitParam}
    `, params);

    res.json({ ok: true, days, cutoff, query: { title: rawTitle, titleKey, option: rawOption, optionKey }, count: rows.length, groups: rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

app.get('/collector/workers', async (req, res) => {
  try {
    if (!pool) return res.json({ ok: true, workers: [] });
    const { rows } = await pool.query(`SELECT worker_id AS "workerId", name, last_seen_at AS "lastSeenAt", app_version AS "appVersion", ip FROM collector_workers ORDER BY last_seen_at DESC LIMIT 100`);
    res.json({ ok: true, workers: rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});


app.get('/collector/summary', async (req, res) => {
  try {
    await pruneOldData();
    if (!pool) {
      const products = new Set(memory.observations.map(o => `${o.productKey}|${o.optionKey}`));
      return res.json({ ok: true, mode: 'memory', observations: memory.observations.length, products: products.size, alerts: memory.alerts.length });
    }
    const obs = await pool.query(`SELECT COUNT(*)::int AS observations, COUNT(DISTINCT product_key || '|' || option_key)::int AS products FROM price_observations`);
    const alerts = await pool.query(`SELECT COUNT(*)::int AS alerts FROM alerts`);
    const workers = await pool.query(`SELECT COUNT(*)::int AS workers FROM collector_workers`);
    res.json({ ok: true, mode: 'postgres', observations: n(obs.rows[0]?.observations), products: n(obs.rows[0]?.products), alerts: n(alerts.rows[0]?.alerts), workers: n(workers.rows[0]?.workers), priceRetentionMs: PRICE_RETENTION_MS, alertRetentionMs: ALERT_RETENTION_MS });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

app.get('/alerts', async (req, res) => {
  try {
    res.set('Cache-Control', 'no-store');
    res.json({ ok: true, alerts: await getAlerts(req.query.limit || 100) }); }
  catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
});



app.post('/maintenance/prune', async (req, res) => {
  try {
    const deleted = await pruneOldData();
    res.json({ ok: true, alertRetentionMs: ALERT_RETENTION_MS, priceRetentionMs: PRICE_RETENTION_MS, deleted });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

app.get('/alerts/:id', async (req, res) => {
  try {
    const alertId = s(req.params.id, 120);
    const alerts = await getAlerts(300);
    const found = alerts.find(a => String(a.id) === alertId);
    if (!found) return res.status(404).json({ ok: false, error: 'ALERT_NOT_FOUND' });
    res.json({ ok: true, alert: found });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

function isStatsEnrichableTelegramAlert(alert = {}) {
  // v051: Coupangautobot/PC/키위 재전송이 source명을 telegram/kiwi/manual로 안 보내도
  // title + price + url만 있으면 무조건 서버 stats/emul-stats 보강을 태운다.
  // 이전 조건은 source 문자열에 의존해서 avg/low가 0인 채 그대로 발송되는 경로가 남았다.
  return Boolean(alert.title && n(alert.price) > 0 && (alert.url || alert.originalUrl));
}

async function enrichTelegramAlertWithServerStats(alert, req) {
  const perf = makePerf('enrichTelegramAlertWithServerStats', { title: alert?.title, option: alert?.option, price: alert?.price, source: alert?.source });
  // v047: kiwi_telegram_manual_reply / telegram ingest가 바로 alerts를 만들면
  // 서버 DB·에뮬 통계 캐시를 타지 않아 평균/최저가 빠졌다.
  // 여기서만 collector 관측 흐름과 같은 stats 조회를 태우고, 템플릿은 서버가 다시 렌더한다.
  if (!isStatsEnrichableTelegramAlert(alert)) return { alert, enriched: false, obs: null, stats: null, decision: null, obsResult: null };

  const obsInput = {
    ...alert.raw,
    source: alert.source || 'telegram_bridge',
    title: alert.title,
    option: alert.option,
    price: alert.price,
    lockedPrice: lockedFallcentPrice(alert.raw || {}) || alert.price,
    fallcentPrice: lockedFallcentPrice(alert.raw || {}) || alert.price,
    priceSource: (alert.raw && (alert.raw.priceSource || alert.raw.lockedBy || alert.raw.source)) || (isFallcentLockedPayload(alert.raw || {}) ? 'fallcent_locked' : ''),
    noCoupangPriceOverride: isFallcentLockedPayload(alert.raw || {}),
    linkOnlyVerified: isFallcentLockedPayload(alert.raw || {}),
    avg: alert.avg,
    low: alert.low,
    dropPct: alert.dropPct,
    appDiscount: alert.appDiscount,
    cardText: alert.cardText,
    cardDiscountPct: alert.cardDiscountPct,
    url: alert.originalUrl || alert.url,
    partnerUrl: alert.url || alert.originalUrl,
    originalUrl: alert.originalUrl || alert.url,
    productId: alert.productId,
    itemId: alert.itemId,
    vendorItemId: alert.vendorItemId,
    workerId: alert.raw?.workerId || alert.raw?.collectorId || alert.source || 'telegram-ingest',
    collectedAt: n(alert.createdAt) || now(),
    noTelegram: true,
    muteAlert: true,
    rawTelegramIngest: true
  };

  const obs = normalizeObservation(obsInput);
  perf.step('normalizeObservation');
  let obsResult = { inserted: false, reason: 'not_attempted' };
  try {
    obsResult = await insertObservation(obs, req);
  } catch (e) {
    obsResult = { inserted: false, reason: 'insert_observation_failed', error: String(e.message || e) };
  }
  perf.step('insertObservation', { obsReason: obsResult?.reason, obsInserted: !!obsResult?.inserted, obsTiming: obsResult?.timing });

  const serverStats = await getObservationStatsSafe(obs);
  perf.step('getObservationStatsSafe', { match: serverStats?.match, count: n(serverStats?.count), avg: n(serverStats?.avg), low: n(serverStats?.low), statsTiming: serverStats?.timing });
  const textAvg = n(alert.avg);
  const textLow = n(alert.low);
  const textDrop = f(alert.dropPct || (textAvg > 0 && obs.price > 0 ? ((textAvg - obs.price) / textAvg) * 100 : 0));

  const serverAvg = n(serverStats.avg);
  const serverCount = n(serverStats.count);
  const textLooksUseful = textAvg > 0 && obs.price > 0 && textAvg > obs.price;
  const serverLooksWeak = serverAvg <= 0 || serverAvg <= obs.price || serverCount <= 3;
  const preferTextStats = Boolean(textLooksUseful && (serverLooksWeak || textAvg > serverAvg * 1.05));

  const chosenAvg = preferTextStats ? textAvg : (serverAvg > 0 ? serverAvg : textAvg);
  const chosenLow = preferTextStats ? (textLow > 0 ? textLow : n(serverStats.low)) : (n(serverStats.low) > 0 ? n(serverStats.low) : textLow);

  const stats = {
    ...serverStats,
    count: Math.max(serverCount, textAvg > 0 ? MIN_HISTORY_COUNT : 0),
    avg: chosenAvg,
    low: chosenLow,
    high: Math.max(n(serverStats.high), serverAvg, textAvg, textLow),
    avgSource: preferTextStats ? 'telegram_text_avg_line' : (serverAvg > 0 ? serverStats.avgSource : (textAvg > 0 ? 'telegram_text_avg_line' : serverStats.avgSource)),
    match: preferTextStats ? `${serverStats.match || 'none'}_telegram_text_avg_line` : (serverAvg > 0 ? serverStats.match : (textAvg > 0 ? `${serverStats.match || 'none'}_telegram_text_avg_line` : serverStats.match))
  };

  let decision = shouldCreateAlertFromObservation(obs, stats);
  if (!decision.create && textAvg > 0 && textDrop >= ALERT_MIN_AVG_DROP_PCT) {
    decision = {
      ...decision,
      create: true,
      reason: 'OK_TELEGRAM_TEXT_AVG_LINE',
      avgDropPct: textDrop,
      lowDropPct: textLow > 0 && obs.price > 0 ? ((textLow - obs.price) / textLow) * 100 : 0,
      count: stats.count,
      avg: stats.avg,
      low: stats.low
    };
  }

  const appPct = rawAppDiscountPct(obs.raw);
  const effectiveDrop = Math.max(f(decision.avgDropPct), f(decision.lowDropPct), f(alert.dropPct), appPct);
  const section = effectiveDrop >= bigDealThresholdForPrice(obs.price) ? '대박' : (effectiveDrop >= 25 ? '핫딜' : (alert.section || '인기'));

  const enriched = normalizeAlert({
    ...alert,
    source: alert.source || 'telegram_bridge',
    section,
    title: obs.title,
    option: obs.option,
    price: obs.price,
    avg: stats.avg,
    low: stats.low,
    dropPct: f(decision.avgDropPct || alert.dropPct),
    appDiscount: appPct || alert.appDiscount,
    forceBigDeal: alert.forceBigDeal || alert.raw?.forceBigDeal || false,
    manualGrade: alert.manualGrade || alert.raw?.manualGrade || '',
    priceLabelMode: alert.priceLabelMode || alert.raw?.priceLabelMode || '',
    manualToolMode: alert.manualToolMode || alert.raw?.manualToolMode || '',
    cardText: obs.cardText || alert.cardText,
    cardDiscountPct: obs.cardDiscountPct || alert.cardDiscountPct,
    partnerUrl: obs.partnerUrl || alert.url,
    url: obs.partnerUrl || alert.url,
    originalUrl: obs.url || alert.originalUrl || alert.url,
    productId: obs.productId || alert.productId,
    itemId: obs.itemId || alert.itemId,
    vendorItemId: obs.vendorItemId || alert.vendorItemId
  });

  // [서버] v055:
  // normalizeAlert()가 내부에서 자동 생성한 예전 dedupeKey는 옵션 순서가 반영되어
  // "1개, 850g" / "850g, 1개"가 갈라질 수 있었다.
  // 사용자가/클라이언트가 명시적으로 보낸 dedupeKey만 보존하고,
  // 명시값이 없으면 normalizeObservation()이 만든 canonical productKey+optionKey 기준으로 통일한다.
  const explicitDedupeKey = s(alert.raw?.dedupeKey || alert.raw?.raw?.dedupeKey || '', 240);
  enriched.dedupeKey = explicitDedupeKey || alertDedupeFromObservation(obs) || enriched.dedupeKey;
  const timing = perf.done({ match: stats?.match, count: n(stats?.count), avg: n(stats?.avg), low: n(stats?.low), decision: decision?.reason });
  enriched.raw = {
    ...(alert.raw || {}),
    observation: obs,
    stats,
    decision,
    obsResult,
    telegramIngestStatsEnriched: true,
    originalAlert: alert,
    timing
  };
  return { alert: enriched, enriched: true, obs, stats, decision, obsResult, timing };
}

app.post(['/telegram/ingest', '/telegram-ingest'], async (req, res) => {
  const perf = makePerf('/telegram/ingest');
  try {
    if (!allowIngest(req, res)) return;
    const body = req.body || {};
    const rawObj = (body && typeof body.raw === 'object') ? body.raw : {};
    const text = body.text || body.message || body.caption ||
      body.telegramText || body.telegramReply || body.originalText || body.rawText || body.fullText ||
      body.sourceText || body.inputText || body.originalMessage || body.originalMessageText ||
      rawObj.telegramText || rawObj.telegramReply || rawObj.originalText || rawObj.rawText || rawObj.fullText || '';
    const parsed = text ? parseTelegramText(text, { ...body, text }) : normalizeAlert({ ...body, source: body.source || 'telegram_bridge' });
    perf.step('parse', { title: parsed?.title, price: parsed?.price });
    const enriched = await enrichTelegramAlertWithServerStats(parsed, req);
    perf.step('enrichTelegramAlertWithServerStats', { enriched: !!enriched?.enriched, enrichTiming: enriched?.timing });
    const alert = enriched.alert;
    const result = await insertAlert(alert);
    perf.step('insertAlert', { inserted: !!result?.inserted });
    // v061: 응답 지연 방지. 텔레그램/푸시는 백그라운드 전송하고 HTTP 응답은 즉시 반환한다.
    const sent = result.inserted ? await sendTelegramPushForResponse(alert) : { telegram: { sent: false, duplicate: true }, push: { sent: 0, duplicate: true } };
    perf.step('sendTelegramPushForResponse');
    const telegram = sent.telegram;
    const push = sent.push;
    const timing = perf.done({ title: alert?.title, price: alert?.price, inserted: !!result?.inserted, match: enriched?.stats?.match });
    res.json({ ok: true, bridge: 'telegram', inserted: result.inserted, duplicate: !result.inserted, enriched: enriched.enriched, obsResult: enriched.obsResult, stats: enriched.stats, decision: enriched.decision, alert, telegram, push, timing: PERF_DEBUG_RESPONSE ? timing : undefined });
  } catch (e) {
    const timing = perf.done({ error: String(e.message || e) });
    res.status(400).json({ ok: false, error: String(e.message || e), timing: PERF_DEBUG_RESPONSE ? timing : undefined });
  }
});

app.post('/ingest', async (req, res) => {
  const perf = makePerf('/ingest');
  try {
    if (!allowIngest(req, res)) return;
    const parsed = normalizeAlert(req.body || {});
    perf.step('normalizeAlert', { title: parsed?.title, price: parsed?.price });
    const enriched = await enrichTelegramAlertWithServerStats(parsed, req);
    perf.step('enrichTelegramAlertWithServerStats', { enriched: !!enriched?.enriched, enrichTiming: enriched?.timing });
    const alert = enriched.alert;
    const result = await insertAlert(alert);
    perf.step('insertAlert', { inserted: !!result?.inserted });
    // v061: 응답 지연 방지. 텔레그램/푸시는 백그라운드 전송하고 HTTP 응답은 즉시 반환한다.
    const sent = result.inserted ? await sendTelegramPushForResponse(alert) : { telegram: { sent: false, duplicate: true }, push: { sent: 0, duplicate: true } };
    perf.step('sendTelegramPushForResponse');
    const telegram = sent.telegram;
    const push = sent.push;
    const timing = perf.done({ title: alert?.title, price: alert?.price, inserted: !!result?.inserted, match: enriched?.stats?.match });
    res.json({ ok: true, inserted: result.inserted, duplicate: !result.inserted, enriched: enriched.enriched, obsResult: enriched.obsResult, stats: enriched.stats, decision: enriched.decision, alert, telegram, push, timing: PERF_DEBUG_RESPONSE ? timing : undefined });
  } catch (e) {
    const timing = perf.done({ error: String(e.message || e) });
    res.status(400).json({ ok: false, error: String(e.message || e), timing: PERF_DEBUG_RESPONSE ? timing : undefined });
  }
});

app.post('/alert-open', async (req, res) => {
  try {
    const row = { id: id('open'), alertId: s(req.body.alertId, 120), deviceId: s(req.body.deviceId, 120), url: s(req.body.url, 1000), openedAt: now() };
    if (!pool) memory.opens.unshift(row);
    else await pool.query(`INSERT INTO alert_opens (id, alert_id, device_id, url, opened_at) VALUES ($1,$2,$3,$4,$5)`, [row.id, row.alertId, row.deviceId, row.url, row.openedAt]);
    res.json({ ok: true, open: row });
  } catch (e) {
    res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

app.post('/push/test', async (req, res) => {
  try {
    const alert = normalizeAlert({
      source: 'test',
      section: req.body.section || '실시간인기',
      title: req.body.title || '쿠핫 테스트 알림',
      option: req.body.option || '푸시 테스트',
      price: req.body.price || 7450,
      avg: req.body.avg || 10375,
      low: req.body.low || 8600,
      appDiscount: req.body.appDiscount || 31,
      url: req.body.url || 'https://link.coupang.com/'
    });
    const push = await sendPush(alert);
    res.json({ ok: true, alert, push });
  } catch (e) {
    res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

app.use((req, res) => {
  res.status(404).json({
    ok: false,
    error: 'NOT_FOUND',
    path: req.path,
    service: 'WOWDROP_CENTRAL',
    marker: 'WOWDROP_JSON_404_FALLBACK'
  });
});

app.use((err, req, res, next) => {
  console.error('[wowdrop-central] error', err);
  res.status(500).json({
    ok: false,
    error: String(err?.message || err),
    service: 'WOWDROP_CENTRAL'
  });
});

await initDb();
app.listen(PORT, () => { console.log('############ KUHOT SERVER V073 BOOT MARKER - NO SERVER PARTNERS PASS THROUGH ############'); console.log(`[wowdrop-central] listening :${PORT} mode=${pool ? 'postgres' : 'memory'} version=${SERVER_VERSION} heavyMax=${HEAVY_MAX_ACTIVE}`); });
