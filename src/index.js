import 'dotenv/config';
import axios from 'axios';
import { Telegraf } from 'telegraf';
import Database from 'better-sqlite3';
import crypto from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import pLimit from 'p-limit';

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
  process.env.POLL_INTERVAL_SECONDS || '300',
);
const TRACK_OFFER_IDS = (process.env.TRACK_OFFER_IDS || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
const SIZE_TRACKING_MODE = (
  process.env.SIZE_TRACKING_MODE || 'DIMENSIONS'
).toUpperCase(); // DIMENSIONS|ATTRIBUTE|BOTH
const SIZE_ATTR_PATTERNS = (
  process.env.SIZE_ATTRIBUTE_PATTERNS ||
  'размер,российский размер,размер производителя,size'
)
  .split(',')
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);
const DB_PATH = process.env.DB_PATH || 'ozon_notifier.db';

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
attr_hash TEXT,
last_seen_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_products_updated_at ON products(updated_at);
`);

const stmtInsertChat = db.prepare(
  'INSERT OR IGNORE INTO chats(chat_id) VALUES (?)',
);
const stmtAllChats = db.prepare('SELECT chat_id FROM chats');

const stmtGetProd = db.prepare(
  'SELECT offer_id, updated_at, dim_hash, depth_mm, width_mm, height_mm, weight_g, attr_hash FROM products WHERE offer_id = ?',
);
const stmtUpsertProd = db.prepare(`
INSERT INTO products(offer_id, product_id, name, updated_at, dim_hash, depth_mm, width_mm, height_mm, weight_g, attr_hash, last_seen_at)
VALUES(@offer_id,@product_id,@name,@updated_at,@dim_hash,@depth_mm,@width_mm,@height_mm,@weight_g,@attr_hash,datetime('now'))
ON CONFLICT(offer_id) DO UPDATE SET
product_id = excluded.product_id,
name = excluded.name,
updated_at = excluded.updated_at,
dim_hash = excluded.dim_hash,
depth_mm = excluded.depth_mm,
width_mm = excluded.width_mm,
height_mm = excluded.height_mm,
weight_g = excluded.weight_g,
attr_hash = excluded.attr_hash,
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
  const { data } = await axios.post(`${API_BASE}${path}`, body, {
    headers: HEADERS,
    timeout: 30_000,
  });
  return data;
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
  const body = { offer_id: offerIds };
  const data = await ozonPost('/v4/product/info/attributes', body);
  return data?.items || [];
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

const extractDims = (it) => {
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
  const out = [];
  const list = attrsItem?.attributes || [];
  for (const a of list) {
    const name = (a.name || '').toString().toLowerCase();
    if (SIZE_ATTR_PATTERNS.some((p) => name.includes(p))) {
      const values = (a.values || [])
        .map((v) => v?.value ?? v?.text ?? v?.dictionary_value_id)
        .filter(Boolean);
      out.push({ name: a.name, attribute_id: a.attribute_id, values });
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
${lines.join('')}`;
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
  // 1) тянем основную инфу
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
  // 2) опционально тянем атрибуты
  let attrsByOffer = new Map();
  if (SIZE_TRACKING_MODE === 'ATTRIBUTE' || SIZE_TRACKING_MODE === 'BOTH') {
    const attrItems = [];
    for (const bb of chunk(offerIds, 100)) {
      try {
        attrItems.push(...(await fetchAttributesV4(bb)));
      } catch {}
    }
    attrsByOffer = new Map(attrItems.map((x) => [x.offer_id, x]));
  }

  // 3) сравнение с БД
  const upserts = [];
  for (const it of infoItems) {
    const offer_id = it.offer_id || it.offerId || it.offer;
    if (!offer_id) continue;

    const name = it.name || '';
    const updated_at = it.updated_at || it.updatedAt || '';

    const prev = stmtGetProd.get(offer_id);

    const NOTIFY_ON_NEW =
      (process.env.NOTIFY_ON_NEW_PRODUCT || 'false').toLowerCase() === 'true';

    if (!prev) {
      // baseline расчёт
      const dims = extractDims(it);
      const dimHash = sizeFingerprint(dims);

      // опциональный алерт о новом товаре
      if (NOTIFY_ON_NEW) {
        await notifyAll(
          `<b>Новый товар</b> — <code>${offer_id}</code>\n${it.name || ''}\n` +
            `Размеры: Д=${dims.depth_mm ?? '—'} мм, Ш=${
              dims.width_mm ?? '—'
            } мм, В=${dims.height_mm ?? '—'} мм, ` +
            `Вес=${dims.weight_g ?? '—'} г`,
        );
      }

      upserts.push({
        offer_id,
        product_id: it.id || it.product_id || 0,
        name: it.name || '',
        updated_at: it.updated_at || it.updatedAt || '',
        dim_hash: dimHash,
        depth_mm: dims.depth_mm ?? null,
        width_mm: dims.width_mm ?? null,
        height_mm: dims.height_mm ?? null,
        weight_g: dims.weight_g ?? null,
        attr_hash: null,
      });
      return; // переходим к следующему товару
    }

    // Ранний выход: если updated_at не поменялся — шанс, что ничего не менялось (ускоряет обработку крупных каталогов)
    if (
      prev &&
      prev.updated_at &&
      updated_at &&
      prev.updated_at === updated_at &&
      SIZE_TRACKING_MODE === 'DIMENSIONS'
    ) {
      // но всё же обновим last_seen_at
      upserts.push({
        offer_id,
        product_id: it.id || it.product_id || 0,
        name,
        updated_at,
        dim_hash: prev.dim_hash,
        depth_mm: prev.depth_mm,
        width_mm: prev.width_mm,
        height_mm: prev.height_mm,
        weight_g: prev.weight_g,
        attr_hash: prev.attr_hash,
      });
      continue;
    }

    // Извлекаем габариты и считаем хэш
    const dims = extractDims(it);
    const dimHash = sizeFingerprint(dims);

    let attrHash = prev?.attr_hash || null;
    let oldPicked = [];
    if (SIZE_TRACKING_MODE === 'ATTRIBUTE' || SIZE_TRACKING_MODE === 'BOTH') {
      const attrsItem = attrsByOffer.get(offer_id);
      const picked = attrsItem ? pickSizeAttributes(attrsItem) : [];
      attrHash = attrFingerprint(picked);
      oldPicked = prev?.attr_hash ? oldPicked : [];
    }

    // Нотификации
    if (prev) {
      // DIMENSIONS
      if (
        SIZE_TRACKING_MODE === 'DIMENSIONS' ||
        SIZE_TRACKING_MODE === 'BOTH'
      ) {
        if (prev.dim_hash && prev.dim_hash !== dimHash) {
          const oldDims = {
            depth_mm: prev.depth_mm,
            width_mm: prev.width_mm,
            height_mm: prev.height_mm,
            weight_g: prev.weight_g,
          };
          await notifyAll(dimDiffMessage(offer_id, it, oldDims, dims));
        }
      }
      // ATTRIBUTES — если включен режим
      if (
        (SIZE_TRACKING_MODE === 'ATTRIBUTE' || SIZE_TRACKING_MODE === 'BOTH') &&
        prev.attr_hash &&
        prev.attr_hash !== attrHash
      ) {
        await notifyAll(attrDiffMessage(offer_id, it, [], [])); // при желании можно хранить и старые picked
      }
    }

    // upsert
    upserts.push({
      offer_id,
      product_id: it.id || it.product_id || 0,
      name,
      updated_at,
      dim_hash: dimHash,
      depth_mm: dims.depth_mm ?? null,
      width_mm: dims.width_mm ?? null,
      height_mm: dims.height_mm ?? null,
      weight_g: dims.weight_g ?? null,
      attr_hash: attrHash ?? null,
    });
  }

  // 4) транзакционный upsert
  if (upserts.length) txUpsertMany(upserts);
};

const scanOnce = async () => {
  // Собираем офферы
  const offers = [];
  for await (const rec of iterOffers()) offers.push(rec);
  if (!offers.length) return;

  const offerIds = offers.map((o) => o.offer_id);
  const batches = chunk(offerIds, 1000);

  // ограничим конкуррентность, чтобы не ловить 429
  const limit = pLimit(2);
  await Promise.all(batches.map((b) => limit(() => processBatch(b))));
};

// ================== Цикл ==================
const loop = async () => {
  while (true) {
    try {
      await scanOnce();
    } catch (e) {
      const msg = e?.response?.data?.message || e?.message || String(e);
      await notifyAll(`⚠️ Ошибка мониторинга: <code>${msg}</code>`);
    }
    await sleep(POLL_INTERVAL_SECONDS * 1000);
  }
};

// ================== Start ==================
const run = async () => {
  if (!HEADERS['Client-Id'] || !HEADERS['Api-Key'])
    throw new Error('OZON_CLIENT_ID и OZON_API_KEY обязательны');
  await bot.launch();
  console.log('Telegram bot started');
  loop();
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});

process.once('SIGINT', () => bot.stop('SIGINT'));
process.once('SIGTERM', () => bot.stop('SIGTERM'));
