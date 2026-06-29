
import express from 'express';
import cors from 'cors';
import crypto from 'crypto';
import pg from 'pg';
import { Expo } from 'expo-server-sdk';

const { Pool } = pg;
const app = express();
const expo = new Expo();

const PORT = Number(process.env.PORT || 8787);
const DATABASE_URL = process.env.DATABASE_URL || '';
const INGEST_KEY = process.env.INGEST_KEY || '';
const ALERT_RETENTION_MS = Number(process.env.ALERT_RETENTION_MS || 24 * 60 * 60 * 1000); // 앱 알림함 기본 24시간 보관
const PRICE_RETENTION_MS = Number(process.env.PRICE_RETENTION_MS || 7 * 24 * 60 * 60 * 1000); // 가격 관측 DB 기본 7일 보관
const COLLECTOR_KEY = process.env.COLLECTOR_KEY || INGEST_KEY || '';
const OBS_BUCKET_MS = Number(process.env.OBS_BUCKET_MS || 60 * 60 * 1000); // 같은 상품/옵션/가격은 기본 1시간 1건만 관측 저장
const ALERT_MIN_AVG_DROP_PCT = Number(process.env.ALERT_MIN_AVG_DROP_PCT || 20);
const ALERT_REQUIRE_LOW_MATCH = String(process.env.ALERT_REQUIRE_LOW_MATCH || 'true').toLowerCase() !== 'false';
const MIN_HISTORY_COUNT = Number(process.env.MIN_HISTORY_COUNT || 2);
const COUPANG_PARTNERS_ACCESS_KEY = process.env.COUPANG_PARTNERS_ACCESS_KEY || process.env.CP_ACCESS_KEY || '';
const COUPANG_PARTNERS_SECRET_KEY = process.env.COUPANG_PARTNERS_SECRET_KEY || process.env.CP_SECRET_KEY || '';
const COUPANG_PARTNERS_SUB_ID = process.env.COUPANG_PARTNERS_SUB_ID || process.env.CP_SUB_ID || '';


app.use(cors());
app.use(express.json({ limit: '2mb' }));

let pool = null;
const memory = { devices: new Map(), alerts: [], opens: [], observations: [] };

if (DATABASE_URL) {
  pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false }
  });
}

function now() { return Date.now(); }
function id(prefix) { return `${prefix}_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`; }
function s(v, max = 500) { return String(v ?? '').trim().slice(0, max); }
function n(v) { const x = Number(v || 0); return Number.isFinite(x) ? Math.round(x) : 0; }
function f(v) { const x = Number(v || 0); return Number.isFinite(x) ? x : 0; }


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
  const opt = normKey(stripDeliveryBadgeForKey(cleanOptionText(a.option || a.optionKey || '')));
  const price = n(a.price || a.payPrice);
  if (product) return `PID:${product}:OPT:${opt}:PRICE:${price}`;
  // v023: 텔레그램 재가공/직접전송은 공백·이모지·문장부호가 조금씩 달라져도 같은 상품/옵션/가격이면 중복으로 본다.
  return crypto.createHash('sha1').update(`${titleKey}|${opt}|${price}`).digest('hex');
}

function normalizeAlert(body) {
  const price = n(body.price || body.payPrice);
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
    cardText: cleanCardText(body.cardText || body.cardBestInfo, title, option),
    cardDiscountPct: f(body.cardDiscountPct || body.cardPct || body.cardRate || 0),
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
  return normKey(stripDeliveryBadgeForKey(cleanOptionText(text || '')));
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
  const price = n(body.price || body.payPrice || body.finalPrice);
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
  const title = stripDeliveryBadgeForKey(rawTitleForDisplay) || rawTitleForDisplay;
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
  const hasFresh = !!(obsRaw.hasFresh || obsRaw.raw?.hasFresh);
  const hasJikgu = !!(obsRaw.hasJikgu || obsRaw.raw?.hasJikgu);
  const badge = hasFresh ? ' [로켓프레시❄️]' : (hasJikgu ? ' [로켓직구🌏]' : '');
  const bigPriceLabelDrop = Math.max(Math.abs(f(avgDrop)), Math.abs(f(appFallbackPct)));
  // v041: 가격하락/핫딜/대박딜 등급 문구는 제목 위에 출력하지 않지만,
  // 대박 기준(1만원 이하 40% 이상, 그 외 35% 이상)을 넘으면 가격 라벨만 대박으로 표시한다.
  const label = bigPriceLabelDrop >= bigDealThresholdForPrice(price) ? '🔥대박🔥 최종 혜택가 :' : '💰 최종 혜택가 :';
  const lines = [];
  lines.push('※ 파트너스활동으로 수수료를 제공받습니다.');
  lines.push(`✨ ${title}${badge}`);
  if (option) lines.push(`└ ${option}`);
  lines.push('');
  lines.push(`${label} ${won(price)}`);
  if (a.cardText) lines.push(`💳 (${a.cardText})`);
  lines.push('');
  if (avg > 0) lines.push(`📉 평균 ${won(avg)} · 🔻${formatPct1(avgDrop)}% (${won(avgDiff)})`);
  if (low > 0 && avg !== low) {
    if (price > 0 && Math.round(price) === Math.round(low)) {
      lines.push('🏆 기존 최저가와 동일');
    } else if (price > 0 && price < low) {
      lines.push(`🏆 최저 ${won(low)} · 🔻${formatPct1(lowDrop)}% (${won(lowDiff)})`);
    }
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
  const price = n(body.price || firstWon(priceLine || titleLine));
  const compact = splitCompactTitleOption(body.title || titleLine);
  const title = cleanTitleText(compact.title || body.message || '텔레그램 핫딜');
  const option = cleanOptionText(body.option || optionLine.replace(/^└\s*/, '') || compact.option);
  const section = detectSectionFromText(text, body);
  const avg = n(body.avg || firstWon(avgLine));
  const low = n(body.low || firstWon(lowLine));
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
  const rawText = String(
    raw.telegramText || raw.text || raw.message || raw.caption ||
    nested.telegramText || nested.telegramReply || nested.text || nested.message || nested.caption ||
    alert.text || alert.message || alert.caption || ''
  ).trim();

  // 완성 템플릿이면 서버가 줄이지 않고 그대로 보낸다.
  if (rawText && (
    rawText.includes('최종 혜택가') ||
    rawText.includes('상세보기 및 구매하기') ||
    rawText.includes('파트너스활동') ||
    rawText.includes('📉 평균') ||
    rawText.includes('🏆 최저')
  )) {
    return rawText;
  }

  // collector 경로는 서버가 평균/최저/할인율 판단 후 풀 템플릿을 직접 만든다.
  if (alert.source === 'collector_observe' || raw.observation) {
    return formatCollectorFullTemplate(alert);
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




function coupangSignedDate() {
  const d = new Date();
  const pad = (x) => String(x).padStart(2, '0');
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}T${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}Z`;
}

function isCoupangUrl(url) {
  try {
    const u = new URL(String(url || ''));
    return /(^|\.)coupang\.com$/i.test(u.hostname) || /(^|\.)link\.coupang\.com$/i.test(u.hostname);
  } catch (_) {
    return false;
  }
}

async function createCoupangDeeplinkServer(originalUrl, requestedSubId = '') {
  const accessKey = String(COUPANG_PARTNERS_ACCESS_KEY || '').trim();
  const secretKey = String(COUPANG_PARTNERS_SECRET_KEY || '').trim();
  const requested = String(requestedSubId || '').trim();
  const safeRequested = /^[A-Za-z0-9._-]{1,64}$/.test(requested) ? requested : '';
  const subId = safeRequested || String(COUPANG_PARTNERS_SUB_ID || '').trim();
  const url = String(originalUrl || '').trim();
  if (!url) throw new Error('EMPTY_URL');
  if (!isCoupangUrl(url)) throw new Error('NOT_COUPANG_URL');
  if (!accessKey || !secretKey || !subId) return { ok: false, skipped: true, error: 'COUPANG_PARTNERS_ENV_MISSING' };

  const apiPath = '/v2/providers/affiliate_open_api/apis/openapi/v1/deeplink';
  const query = `_E_=${encodeURIComponent(subId)}`;
  const signedDate = coupangSignedDate();
  const message = signedDate + 'POST' + apiPath + query;
  const signature = crypto.createHmac('sha256', secretKey).update(message).digest('hex');
  const authorization = `CEA algorithm=HmacSHA256, access-key=${accessKey}, signed-date=${signedDate}, signature=${signature}`;

  const r = await fetch(`https://api-gateway.coupang.com${apiPath}?${query}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: authorization },
    body: JSON.stringify({ coupangUrls: [url] })
  });
  const data = await r.json().catch(() => null);
  if (!r.ok || !data) throw new Error(`DEEPLINK_HTTP_${r.status}`);
  if ((data.rCode === '0' || data.rCode === '0000') && Array.isArray(data.data) && data.data.length) {
    const item = data.data[0] || {};
    return { ok: true, partnerUrl: item.shortenUrl || item.landingUrl || '', shortenUrl: item.shortenUrl || '', landingUrl: item.landingUrl || '', raw: data };
  }
  throw new Error(data.message || data.rMessage || 'DEEPLINK_API_ERROR');
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
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts(created_at DESC)`);
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
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_obs_product_option_time ON price_observations(product_key, option_key, collected_at DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_obs_created_at ON price_observations(created_at DESC)`);
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
  await pruneOldData();

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
  await pruneOldData();
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
    if (wanted.title && wanted.title.length >= 2) {
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
  await pruneOldData();
  await touchWorker(obs, req);

  const policy = await serverDailySavePolicy(obs);
  if (!policy.allow) {
    return { inserted: false, observation: obs, skipped: true, reason: policy.reason, policy };
  }

  if (!pool) {
    const exists = memory.observations.find(x => x.obsKey === obs.obsKey);
    if (exists) return { inserted: false, observation: exists, skipped: true, reason: 'OBS_KEY_DUPLICATE', policy };
    memory.observations.unshift(obs);
    memory.observations = memory.observations.slice(0, 200000);
    return { inserted: true, observation: obs, reason: policy.reason, policy };
  }
  const r = await pool.query(`INSERT INTO price_observations (
    id,obs_key,product_key,title,title_key,option_text,option_key,price,card_discount_pct,card_text,url,partner_url,product_id,item_id,vendor_item_id,category,worker_id,source,raw,collected_at,created_at
  ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
  ON CONFLICT (obs_key) DO NOTHING RETURNING id`,
  [obs.id, obs.obsKey, obs.productKey, obs.title, obs.titleKey, obs.option, obs.optionKey, obs.price, obs.cardDiscountPct, obs.cardText, obs.url, obs.partnerUrl, obs.productId, obs.itemId, obs.vendorItemId, obs.category, obs.workerId, obs.source, JSON.stringify(obs.raw), obs.collectedAt, obs.createdAt]);
  return { inserted: r.rowCount > 0, observation: obs, skipped: r.rowCount <= 0, reason: r.rowCount > 0 ? policy.reason : 'OBS_KEY_DUPLICATE', policy };
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

function canonicalOptionMatchKey(option) {
  const raw = String(option || '').trim();
  if (!raw) return '';
  const parts = raw
    .replace(/\s*[·|/]\s*/g, ', ')
    .split(/\s*,\s*/g)
    .map(cleanOptionPartForMatch)
    .filter(Boolean)
    .map(normKey)
    .filter(Boolean);
  return uniqueStrings(parts).sort().join('');
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

async function getObservationStats(obs) {
  const cutoff = now() - PRICE_RETENTION_MS;
  const variants = productKeyLookupVariants(obs);
  const wanted = looseIdentityFromParts(obs.title, obs.option);
  const wantedTitleKey = wanted.titleKey || obs.titleKey;
  const wantedOptionMatchKey = wanted.optionMatchKey || canonicalOptionMatchKey(obs.option || obs.optionKey || '');

  if (!wantedTitleKey && !wantedOptionMatchKey) {
    return { count: 0, avg: 0, low: 0, high: 0, cutoff, match: 'none', productKeyVariants: variants };
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
    return summarizeRowsForStats(items, cutoff, items.length ? 'smart_memory' : 'none', variants, n(obs.price));
  }

  const params = [cutoff, variants, wantedTitleKey || ''];
  let where = `collected_at >= $1 AND price > 0 AND (product_key = ANY($2::text[]) OR title_key = $3`;
  if (wanted.title && wanted.title.length >= 2) {
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
    return summarizeRowsForStats(matched, cutoff, 'smart_canonical', variants, n(obs.price));
  }

  // 마지막 방어: 기존 정확매칭 방식. smart 매칭 실패 시에만 사용한다.
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

  return {
    count: n(r.count),
    avg: n(r.avg),
    low: n(r.low),
    high: n(r.high),
    cutoff,
    match: n(r.count) > 0 ? match : 'none',
    productKeyVariants: variants
  };
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
  res.json({ ok: true, service: 'KUHOT_UNIFIED_CENTRAL', app: 'KUHOT', version: 'v042-unit-option-canonical-template-exact', mode: pool ? 'postgres' : 'memory', time: now(), alertRetentionMs: ALERT_RETENTION_MS, priceRetentionMs: PRICE_RETENTION_MS });
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




// 배포용 확장프로그램: 파트너스 키를 확장 안에 넣지 않고 서버 환경변수로 딥링크 생성
app.post('/partners/deeplink', async (req, res) => {
  try {
    const url = req.body?.url || req.body?.originalUrl || req.body?.coupangUrl || '';
    const requestedSubId = req.body?.subId || req.body?.cpSubId || req.body?.partnerSubId || '';
    const result = await createCoupangDeeplinkServer(url, requestedSubId);
    if (!result.ok) return res.status(400).json(result);
    res.json(result);
  } catch (e) {
    res.status(400).json({ ok: false, error: String(e.message || e) });
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

    for (const item of items.slice(0, 200)) {
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



app.post('/collector/observe-batch', async (req, res) => {
  try {
    if (!allowCollector(req, res)) return;
    const items = Array.isArray(req.body?.items) ? req.body.items : (Array.isArray(req.body) ? req.body : []);
    if (!items.length) return res.status(400).json({ ok: false, error: 'EMPTY_ITEMS' });
    const results = [];
    let alertsCreated = 0;
    let pushed = 0;
    for (const item of items.slice(0, 200)) {
      try {
        const merged = { ...(req.body.common || {}), ...item };
        const obs = normalizeObservation(merged);
        const obsResult = await insertObservation(obs, req);
        const stats = await getObservationStats(obs);
        const silentCollector = !!(merged?.muteAlert || merged?.noAlert || merged?.silent || merged?.noTelegram || merged?.collectorOnly || req.body?.common?.muteAlert || req.body?.common?.noTelegram);
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
            push = await sendPush(alert);
            telegram = await sendTelegram(alert);
            alertsCreated += 1;
            pushed += n(push.sent);
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
  try {
    if (!allowCollector(req, res)) return;
    const obs = normalizeObservation(req.body || {});
    const obsResult = await insertObservation(obs, req);
    const stats = await getObservationStats(obs);
    const silentCollector = !!(req.body?.muteAlert || req.body?.noAlert || req.body?.silent || req.body?.noTelegram || req.body?.collectorOnly);
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
        push = await sendPush(alert);
        telegram = await sendTelegram(alert);
      }
    }
    res.json({ ok: true, observationInserted: obsResult.inserted, observationReason: obsResult.reason || '', observationPolicy: obsResult.policy || null, observation: obs, stats, decision, alert: alertResult, push, telegram });
  } catch (e) {
    res.status(400).json({ ok: false, error: String(e.message || e) });
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
    const stats = await getObservationStats(obs);
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

app.post(['/telegram/ingest', '/telegram-ingest'], async (req, res) => {
  try {
    if (!allowIngest(req, res)) return;
    const body = req.body || {};
    const text = body.text || body.message || body.caption || '';
    const alert = text ? parseTelegramText(text, body) : normalizeAlert({ ...body, source: body.source || 'telegram_bridge' });
    const result = await insertAlert(alert);
    const push = result.inserted ? await sendPush(alert) : { sent: 0, duplicate: true };
    const telegram = result.inserted ? await sendTelegram(alert, text) : { sent: false, duplicate: true };
    res.json({ ok: true, bridge: 'telegram', inserted: result.inserted, duplicate: !result.inserted, alert, push, telegram });
  } catch (e) {
    res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

app.post('/ingest', async (req, res) => {
  try {
    if (!allowIngest(req, res)) return;
    const alert = normalizeAlert(req.body || {});
    const result = await insertAlert(alert);
    const push = result.inserted ? await sendPush(alert) : { sent: 0, duplicate: true };
    const telegram = result.inserted ? await sendTelegram(alert) : { sent: false, duplicate: true };
    res.json({ ok: true, inserted: result.inserted, duplicate: !result.inserted, alert, push, telegram });
  } catch (e) {
    res.status(400).json({ ok: false, error: String(e.message || e) });
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
app.listen(PORT, () => console.log(`[wowdrop-central] listening :${PORT} mode=${pool ? 'postgres' : 'memory'}`));
