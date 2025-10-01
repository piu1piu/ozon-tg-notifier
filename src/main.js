/**
 * Ozon → Telegram notifier (Node.js) — v3.1
 * Фокус: БД-хранилище всех товаров + быстрый дифф размеров + ЛОГИРОВАНИЕ КАЖДОГО API-ЗАПРОСА
 *
 * Новое в v3.1:
 * - Надёжный парсер булевых .env (trim + yes/1/on/true)
 * - Стартовый лог в консоль с эффективными флагами логирования
 * - Heartbeat-логи начала/конца скана и количества товаров
 */

import 'dotenv/config';
import axios from 'axios';
import { Telegraf } from 'telegraf';
import Database from 'better-sqlite3';
import crypto from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import pLimit from 'p-limit';
import fs from 'node:fs';

// ================== Helpers ==================
const parseBool = (v, def = false) => {
  if (v == null) return def;
  const s = String(v).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(s)) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(s)) return false;
  return def;
};

// ================== Config ==================
const API_BASE = 'https://api-seller.ozon.ru';
const HEADERS = {
  'Client-Id': process.env.OZON_CLIENT_ID || '',
  'Api-Key': process.env.OZON_API_KEY || '',
  'Content-Type': 'application/json',
};

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
if (!TELEGRAM_BOT_TOKEN) throw new Error('TELEGRAM_BOT_TOKEN is required');
const POLL_INTERVAL_SECONDS = Number(
  (process.env.POLL_INTERVAL_SECONDS || '60').trim(),
);
const TRACK_OFFER_IDS = (process.env.TRACK_OFFER_IDS || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
const SIZE_TRACKING_MODE = (process.env.SIZE_TRACKING_MODE || 'DIMENSIONS')
  .trim()
  .toUpperCase(); // DIMENSIONS|ATTRIBUTE|BOTH
const SIZE_ATTR_PATTERNS = (
  process.env.SIZE_ATTRIBUTE_PATTERNS ||
  'размер,российский размер,размер производителя,size'
)
  .split(',')
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);
const DB_PATH = (process.env.DB_PATH || 'ozon_notifier.db').trim();

// ================== Logging ==================
const LOG_API = parseBool(process.env.LOG_API, true);
const LOG_TO_FILE = parseBool(process.env.LOG_TO_FILE, false);
const LOG_TO_CONSOLE = parseBool(process.env.LOG_TO_CONSOLE, true);
const LOG_FILE = (process.env.LOG_FILE || 'ozon_api.log').trim();
const LOG_REQ_BODY = parseBool(process.env.LOG_REQ_BODY, false);
const LOG_RES_BODY = parseBool(process.env.LOG_RES_BODY, false);
const LOG_MAX_BODY_CHARS = Number(
  (process.env.LOG_MAX_BODY_CHARS || '2000').trim(),
);

let logStream = null;
if (LOG_TO_FILE) {
  try {
    logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });
  } catch {
    /* ignore */
  }
}

const redactKeys = new Set(['api-key', 'authorization', 'password', 'token']);
const redact = (obj) => {
  if (!obj || typeof obj !== 'object') return obj;
  if (Array.isArray(obj)) return obj.map(redact);
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (redactKeys.has(k.toLowerCase())) out[k] = '[REDACTED]';
    else if (v && typeof v === 'object') out[k] = redact(v);
    else out[k] = v;
  }
  return out;
};
const trunc = (str) => {
  if (str == null) return str;
  const s = typeof str === 'string' ? str : JSON.stringify(str);
  if (s.length <= LOG_MAX_BODY_CHARS) return s;
  return (
    s.slice(0, LOG_MAX_BODY_CHARS) +
    `… <truncated ${s.length - LOG_MAX_BODY_CHARS} chars>`
  );
};
const writeLog = (record) => {
  const line = JSON.stringify({ ts: new Date().toISOString(), ...record });
  if (LOG_TO_CONSOLE) console.log(line);
  if (logStream) logStream.write(line + '\n'); // <-- было line + ''
};

// ================== DB ==================
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

db.exec(`
CREATE TABLE IF NOT EXISTS chats (
  chat_id INTEGER PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS products (
  offer_id TEXT PRIMARY KEY,
  product_id INTEGER,
  name TEXT,
  updated_at TEXT,
  dim_hash TEXT,
  depth_mm REAL,
  width_mm REAL,
  height_mm REAL,
  weight_g REAL,
  last_seen_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_products_updated_at ON products(updated_at);
`);

const stmtInsertChat = db.prepare(
  'INSERT OR IGNORE INTO chats(chat_id) VALUES (?)',
);
const stmtAllChats = db.prepare('SELECT chat_id FROM chats');

const stmtGetProd = db.prepare(
  'SELECT offer_id, updated_at, dim_hash, depth_mm, width_mm, height_mm, weight_g FROM products WHERE offer_id = ?',
);
const stmtUpsertProd = db.prepare(`
INSERT INTO products(offer_id, product_id, name, updated_at, dim_hash, depth_mm, width_mm, height_mm, weight_g, last_seen_at)
VALUES(@offer_id,@product_id,@name,@updated_at,@dim_hash,@depth_mm,@width_mm,@height_mm,@weight_g,datetime('now'))
ON CONFLICT(offer_id) DO UPDATE SET
  product_id = excluded.product_id,
  name       = excluded.name,
  updated_at = excluded.updated_at,
  dim_hash   = excluded.dim_hash,
  depth_mm   = excluded.depth_mm,
  width_mm   = excluded.width_mm,
  height_mm  = excluded.height_mm,
  weight_g   = excluded.weight_g,
  last_seen_at = excluded.last_seen_at
`);

const txUpsertMany = db.transaction((rows) => {
  for (const r of rows) stmtUpsertProd.run(r);
});

// ================== Telegram ==================
const bot = new Telegraf(TELEGRAM_BOT_TOKEN);

bot.start(async (ctx) => {
  stmtInsertChat.run(ctx.chat.id);
  await ctx.reply('👋 Готово! Отслеживаю изменения размеров на Ozon.');
});

bot.command('status', async (ctx) => {
  const count = db.prepare('SELECT COUNT(1) AS c FROM products').get().c;
  await ctx.reply(`📦 Товаров в БД: ${count}
Режим: ${SIZE_TRACKING_MODE}
Интервал: ${POLL_INTERVAL_SECONDS}s`);
});

const getChats = () => stmtAllChats.all().map((r) => r.chat_id);
const notifyAll = async (html) => {
  const chats = getChats();
  for (const chatId of chats) {
    try {
      await bot.telegram.sendMessage(chatId, html, {
        parse_mode: 'HTML',
        disable_web_page_preview: true,
      });
    } catch {}
  }
};

// ================== Ozon API ==================
const ozonPost = async (path, body) => {
  const url = `${API_BASE}${path}`;
  const started = Date.now();
  const reqBodyToLog = LOG_REQ_BODY ? trunc(redact(body)) : undefined;
  try {
    const { data, status, headers } = await axios.post(url, body, {
      headers: HEADERS,
      timeout: 30_000,
    });
    const duration = Date.now() - started;
    if (LOG_API) {
      writeLog({
        kind: 'ozon_api',
        event: 'success',
        method: 'POST',
        path,
        status,
        ms: duration,
        ratelimit: {
          rl_limit: headers?.['x-ratelimit-limit'],
          rl_rem: headers?.['x-ratelimit-remaining'],
          rl_reset: headers?.['x-ratelimit-reset'],
        },
        request: { body: reqBodyToLog },
        response: { body: LOG_RES_BODY ? trunc(data) : undefined },
      });
    }
    return data;
  } catch (e) {
    const duration = Date.now() - started;
    const status = e?.response?.status;
    const respData = e?.response?.data;
    if (LOG_API) {
      writeLog({
        kind: 'ozon_api',
        event: 'error',
        method: 'POST',
        path,
        status,
        ms: duration,
        request: { body: reqBodyToLog },
        response: { body: LOG_RES_BODY ? trunc(respData) : undefined },
        error: e?.message,
      });
    }
    throw e;
  }
};

async function* iterOffers() {
  let last_id = '';
  while (true) {
    const body = { limit: 1000, last_id, filter: { visibility: 'ALL' } };
    if (TRACK_OFFER_IDS.length) body.filter.offer_id = TRACK_OFFER_IDS;
    const data = await ozonPost('/v3/product/list', body);
    const items = data?.result?.items ?? data?.result ?? data?.items ?? [];
    if (!items.length) break;
    for (const it of items) {
      if (it?.offer_id)
        yield { offer_id: it.offer_id, product_id: it.product_id };
    }
    last_id = data?.result?.last_id || data?.last_id || '';
    if (!last_id || TRACK_OFFER_IDS.length) break;
  }
}

const fetchInfoList = async (offerIds) => {
  if (!offerIds.length) return [];
  const body = { offer_id: offerIds };
  const data = await ozonPost('/v3/product/info/list', body);
  return data?.items || [];
};

const fetchInfoV2 = async (offerIds) => {
  if (!offerIds.length) return [];
  const body = { offer_id: offerIds };
  const data = await ozonPost('/v2/product/info', body);
  return data?.items || data?.result || [];
};

const fetchAttributesV4 = async (offerIds) => {
  if (!offerIds.length) return [];
  const body = {
    filter: {
      product_id: [],
      offer_id: offerIds,
      sku: [],
      visibility: 'ALL',
    },
    limit: 1000,
    sort_dir: 'ASC',
  };
  const data = await ozonPost('/v4/product/info/attributes', body);
  // console.log('data data', data, 'data data');
  return data?.result || [];
};

// ================== Размеры: нормализация ==================
const mm = (val, unit) => {
  if (val == null) return null;
  const x = Number(val);
  if (!isFinite(x)) return null;
  const u = (unit || '').toLowerCase();
  if (u === 'mm' || u === '') return +x.toFixed(2);
  if (u === 'cm') return +(x * 10).toFixed(2);
  if (u === 'm') return +(x * 1000).toFixed(2);
  return +x;
};
const grams = (val, unit) => {
  if (val == null) return null;
  const x = Number(val);
  if (!isFinite(x)) return null;
  const u = (unit || '').toLowerCase();
  if (u === 'g' || u === '') return +x.toFixed(1);
  if (u === 'kg') return +(x * 1000).toFixed(1);
  return +x;
};

const extractDimsFromAttrsRoot = (item) => {
  const dimension_unit =
    item?.dimension_unit || item?.length_unit || item?.unit || 'mm';
  const weight_unit = item?.weight_unit || 'g';
  return {
    depth_mm: mm(item?.depth ?? item?.length ?? item?.long, dimension_unit),
    width_mm: mm(item?.width, dimension_unit),
    height_mm: mm(item?.height, dimension_unit),
    weight_g: grams(item?.weight, weight_unit),
  };
};

const extractDimsFromInfo = (it) => {
  const direct = {
    depth: it.depth,
    width: it.width,
    height: it.height,
    dimension_unit: it.dimension_unit,
    weight: it.weight,
    weight_unit: it.weight_unit,
  };
  const dims = it.dimensions || it.dimension || it.package_dimensions || {};
  const maybe = (obj) => ({
    depth_mm: mm(
      obj?.depth ?? obj?.length ?? obj?.long,
      obj?.dimension_unit || obj?.length_unit || obj?.unit,
    ),
    width_mm: mm(
      obj?.width,
      obj?.dimension_unit || obj?.width_unit || obj?.unit,
    ),
    height_mm: mm(
      obj?.height,
      obj?.dimension_unit || obj?.height_unit || obj?.unit,
    ),
    weight_g: grams(obj?.weight, obj?.weight_unit),
  });
  const c1 = maybe(direct);
  const c2 = maybe(dims);
  const vals1 = [c1.depth_mm, c1.width_mm, c1.height_mm, c1.weight_g].filter(
    (v) => v != null,
  );
  if (vals1.length) return c1;
  return c2;
};

// const extractDims = (it) => {
//   const direct = {
//     depth: it.depth,
//     width: it.width,
//     height: it.height,
//     dimension_unit: it.dimension_unit,
//     weight: it.weight,
//     weight_unit: it.weight_unit,
//   };
//   const dims = it.dimensions || it.dimension || it.package_dimensions || {};
//   const maybe = (obj) => ({
//     depth_mm: mm(
//       obj?.depth ?? obj?.length ?? obj?.long,
//       obj?.dimension_unit || obj?.length_unit || obj?.unit,
//     ),
//     width_mm: mm(
//       obj?.width,
//       obj?.dimension_unit || obj?.width_unit || obj?.unit,
//     ),
//     height_mm: mm(
//       obj?.height,
//       obj?.dimension_unit || obj?.height_unit || obj?.unit,
//     ),
//     weight_g: grams(obj?.weight, obj?.weight_unit),
//   });
//   const c1 = maybe(direct);
//   const c2 = maybe(dims);
//   const vals1 = [c1.depth_mm, c1.width_mm, c1.height_mm, c1.weight_g].filter(
//     (v) => v != null,
//   );
//   if (vals1.length) return c1;
//   return c2;
// };

const sizeFingerprint = (s) => {
  const str = JSON.stringify({
    d: s.depth_mm ?? null,
    w: s.width_mm ?? null,
    h: s.height_mm ?? null,
    wg: s.weight_g ?? null,
  });
  return crypto.createHash('sha256').update(str).digest('hex');
};

const pickSizeAttributes = (attrsItem) => {
  console.log(attrsItem, 'test23131412');
  const out = [];
  const list = attrsItem?.attributes || [];
  for (const a of list) {
    console.log(a);
    const name = (a.name || '').toString().toLowerCase();
    // console.log(name);
    if (SIZE_ATTR_PATTERNS.some((p) => name.includes(p))) {
      const values = (a.values || [])
        .map((v) => v?.value ?? v?.text ?? v?.dictionary_value_id)
        .filter(Boolean);
      out.push({ name: a.name, attribute_id: a.attribute_id, values });
      console.log(values);
    }
  }
  return out;
};

const attrFingerprint = (arr) => {
  const norm = arr.map((a) => ({
    n: a.name,
    id: a.attribute_id,
    v: a.values.slice().sort(),
  }));
  const str = JSON.stringify(norm);
  return crypto.createHash('sha256').update(str).digest('hex');
};

// ================== Сообщения ==================
const fmt = (v, unit) => (v == null ? '—' : `${v}${unit || ''}`);
const dimDiffMessage = (offer_id, it, oldDims, newDims) => {
  const title = it.name || offer_id;
  const lines = [];
  if ((oldDims.depth_mm ?? null) !== (newDims.depth_mm ?? null))
    lines.push(
      `• Длина/Глубина: <code>${fmt(
        oldDims.depth_mm,
        ' мм',
      )}</code> → <code>${fmt(newDims.depth_mm, ' мм')}</code>`,
    );
  if ((oldDims.width_mm ?? null) !== (newDims.width_mm ?? null))
    lines.push(
      `• Ширина: <code>${fmt(oldDims.width_mm, ' мм')}</code> → <code>${fmt(
        newDims.width_mm,
        ' мм',
      )}</code>`,
    );
  if ((oldDims.height_mm ?? null) !== (newDims.height_mm ?? null))
    lines.push(
      `• Высота: <code>${fmt(oldDims.height_mm, ' мм')}</code> → <code>${fmt(
        newDims.height_mm,
        ' мм',
      )}</code>`,
    );
  if ((oldDims.weight_g ?? null) !== (newDims.weight_g ?? null))
    lines.push(
      `• Вес: <code>${fmt(oldDims.weight_g, ' г')}</code> → <code>${fmt(
        newDims.weight_g,
        ' г',
      )}</code>`,
    );
  const updated = it.updated_at || it.updatedAt || '';
  return `<b>Изменение размеров</b> — <code>${offer_id}</code>
${title}
Обновлено: <code>${updated}</code>

${lines.join('\n')}`;
};

const attrDiffMessage = (offer_id, it, oldArr, newArr) => {
  const title = it.name || offer_id;
  const toLine = (arr) =>
    arr.map((a) => `${a.name}: ${a.values.join(', ')}`).join('; ');
  return `<b>Изменение атрибутов размера</b> — <code>${offer_id}</code>
${title}

${toLine(oldArr)}
→
${toLine(newArr)}`;
};

// ================== Основной скан ==================
const chunk = (arr, n) =>
  arr.reduce((acc, _, i) => (i % n ? acc : [...acc, arr.slice(i, i + n)]), []);

const processBatch = async (offerIds) => {
  // 1) тянем основную инфо
  let infoItems = [];
  try {
    infoItems = await fetchInfoList(offerIds);
  } catch (e) {
    // фолбэк меньшими пачками
    for (const bb of chunk(offerIds, 100)) {
      try {
        infoItems.push(...(await fetchInfoV2(bb)));
      } catch {}
    }
  }
  const infoByOffer = new Map(infoItems.map((x) => [x.offer_id, x]));

  // 2) опционально тянем атрибуты
  let attrsByOffer = new Map();

  if (SIZE_TRACKING_MODE === 'ATTRIBUTE' || SIZE_TRACKING_MODE === 'BOTH') {
    // 1) Ждём запрос
    const attrItems = await fetchAttributesV4(offerIds);

    attrsByOffer = new Map(attrItems.map((x) => [x.offer_id, x]));
    // console.log(attrsByOffer);
  }

  // 3) сравнение с БД
  const upserts = [];
  for (const offer_id of offerIds) {
    const info = infoByOffer.get(offer_id) || { offer_id };
    const attrs = attrsByOffer.get(offer_id) || {};

    // приоритет — размеры из v4 (корневые поля)
    let dims = extractDimsFromAttrsRoot(attrs);
    const hasAny = [
      dims.depth_mm,
      dims.width_mm,
      dims.height_mm,
      dims.weight_g,
    ].some((v) => v != null);
    if (!hasAny) {
      // фолбэк на инфо
      if (info && Object.keys(info).length) dims = extractDimsFromInfo(info);
    }

    const newHash = sizeFingerprint(dims);
    const prev = stmtGetProd.get(offer_id);

    if (prev && prev.dim_hash && prev.dim_hash !== newHash) {
      const oldDims = {
        depth_mm: prev.depth_mm,
        width_mm: prev.width_mm,
        height_mm: prev.height_mm,
        weight_g: prev.weight_g,
      };
      await notifyAll(dimDiffMessage(offer_id, info, oldDims, dims));
    }

    upserts.push({
      offer_id,
      product_id: info?.id || info?.product_id || 0,
      name: info?.name || '',
      updated_at: info?.updated_at || info?.updatedAt || '',
      dim_hash: newHash,
      depth_mm: dims.depth_mm ?? null,
      width_mm: dims.width_mm ?? null,
      height_mm: dims.height_mm ?? null,
      weight_g: dims.weight_g ?? null,
    });
  }

  if (upserts.length) txUpsertMany(upserts);
};

const scanOnce = async () => {
  writeLog({ kind: 'scan', event: 'start' });
  const offers = [];
  for await (const rec of iterOffers()) offers.push(rec);
  writeLog({ kind: 'scan', event: 'offers_loaded', count: offers.length });
  if (!offers.length) return;

  const offerIds = offers.map((o) => o.offer_id);
  const batches = chunk(offerIds, 1000);

  const limit = pLimit(2);
  await Promise.all(batches.map((b) => limit(() => processBatch(b))));
  writeLog({
    kind: 'scan',
    event: 'end',
    batches: Math.ceil(offers.length / 1000),
  });
};

// ================== Цикл ==================
// ===== СКЕДУЛЕР СКАНА =====
let isScanning = false;
let scanTimer = null;

async function tick() {
  if (isScanning) {
    writeLog({ kind: 'scan', event: 'skip_overlap' });
    return;
  }
  isScanning = true;
  writeLog({ kind: 'scan', event: 'tick_start' });

  try {
    await scanOnce();
  } catch (e) {
    const msg = e?.response?.data?.message || e?.message || String(e);
    writeLog({ kind: 'scan', event: 'error', message: msg });
    try {
      await notifyAll(`⚠️ Ошибка мониторинга: <code>${msg}</code>`);
    } catch {
      /* глушим, чтобы не уронить планировщик */
    }
  } finally {
    isScanning = false;
    writeLog({ kind: 'scan', event: 'tick_end' });
  }
}

// ================== Start ==================
const run = async () => {
  if (!HEADERS['Client-Id'] || !HEADERS['Api-Key']) {
    throw new Error('OZON_CLIENT_ID и OZON_API_KEY обязательны');
  }

  writeLog({
    kind: 'app',
    event: 'startup',
    LOG_API,
    LOG_TO_CONSOLE,
    LOG_TO_FILE,
    LOG_REQ_BODY,
    LOG_RES_BODY,
    POLL_INTERVAL_SECONDS,
    SIZE_TRACKING_MODE,
    DB_PATH,
    track_offer_ids_count: TRACK_OFFER_IDS.length,
  });

  // ✅ 1) Запускаем бота с onLaunch-колбэком (не ждём промис, он может не резолвиться)
  //    В Telegraf ≥4.16 можно передать колбэк вторым аргументом
  bot
    .launch(() => {
      console.log('🤖 Telegram bot started');
    })
    .catch((e) => {
      console.error('Failed to launch bot:', e);
      process.exit(1);
    });

  // 2) Мгновенный первый прогон
  await tick();

  // 3) Периодический тикер с защитой от наложений
  scanTimer = setInterval(() => {
    // не await — чтобы интервал не блокировался; ошибки ловим внутри tick()
    tick().catch(() => {});
  }, POLL_INTERVAL_SECONDS * 1000);

  // 4) Корректная остановка
  process.once('SIGINT', () => {
    clearInterval(scanTimer);
    bot.stop('SIGINT');
  });
  process.once('SIGTERM', () => {
    clearInterval(scanTimer);
    bot.stop('SIGTERM');
  });
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});

process.once('SIGINT', () => bot.stop('SIGINT'));
process.once('SIGTERM', () => bot.stop('SIGTERM'));
