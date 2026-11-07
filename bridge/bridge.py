#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GGsel (Digiseller) <-> FazerCards: мост автоматической выдачи.

Поток:
 1. Опрос продаж Digiseller (GGsel работает на его API).
 2. Проверка оплаты по уникальному коду.
 3. Заказ у FazerCards с идемпотентным ключом (ключ выводится из кода продажи,
    поэтому повторный запуск сервиса не создаст второй заказ).
 4. Ожидание выполнения заказа, извлечение контента (код карты / ключ / ссылка).
 5. Выдача контента покупателю и отметка доставки в Digiseller.
 6. Всё состояние хранится в SQLite: сервис можно рестартовать без потерь.
"""

import argparse
import hashlib
import json
import logging
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path

import requests

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):
        pass

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# ----------------------------- конфигурация ---------------------------------

DS_SELLER_ID = int(os.getenv("DS_SELLER_ID", "0"))
DS_API_KEY = os.getenv("DS_API_KEY", "")
DS_SECRET = os.getenv("DS_SECRET", "") or DS_API_KEY  # для подписи seller-sells
FZ_API_KEY = os.getenv("FZ_API_KEY", "")
DB_PATH = Path(os.getenv("DB_PATH", BASE_DIR / "bridge.db"))
MAPPING_PATH = Path(os.getenv("MAPPING_PATH", BASE_DIR / "mapping.json"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))
ORDER_WAIT_TTL = int(os.getenv("ORDER_WAIT_TTL", "1800"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "20"))
TG_TOKEN = os.getenv("TG_TOKEN", "")
TG_CHAT = os.getenv("TG_CHAT", "")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

DS_API = "https://api.digiseller.com/api"
DS_API_SALES = "https://api.digiseller.ru/api"  # seller-sells живёт на этом хосте
FZ_API = "https://api.fzr.cards/api/v2"

try:
    MAPPING = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))
except FileNotFoundError:
    MAPPING = {}

log = logging.getLogger("bridge")


class BridgeError(Exception):
    pass


class DigisellerError(BridgeError):
    pass


class FazerError(BridgeError):
    pass


# ------------------------------ база данных ----------------------------------

def db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS orders (
                unique_code    TEXT PRIMARY KEY,
                invoice_id     INTEGER,
                product_id     INTEGER,
                quantity       INTEGER DEFAULT 1,
                status         TEXT DEFAULT 'new',
                fazer_order_id TEXT,
                content        TEXT,
                attempts       INTEGER DEFAULT 0,
                last_error     TEXT,
                created_at     INTEGER,
                updated_at     INTEGER
            );
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
            """
        )


def get_meta(key, default=None):
    with db_conn() as conn:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def set_meta(key, value):
    with db_conn() as conn:
        conn.execute("INSERT INTO meta(key, value) VALUES(?, ?) "
                     "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                     (key, str(value)))


def upsert_sale(invoice_id, product_id, unique_code, quantity=1):
    if not unique_code:
        return None
    now = int(time.time())
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO orders(unique_code, invoice_id, product_id, quantity, "
            "status, created_at, updated_at) VALUES(?,?,?,?, 'new', ?, ?) "
            "ON CONFLICT(unique_code) DO UPDATE SET "
            "invoice_id=excluded.invoice_id, updated_at=excluded.updated_at",
            (unique_code, invoice_id, product_id, quantity, now, now))
    return unique_code


def fetch_pending():
    with db_conn() as conn:
        return conn.execute(
            "SELECT * FROM orders WHERE status IN ('new','ordered','waiting') "
            "ORDER BY created_at").fetchall()


def update_order(code, **fields):
    fields["updated_at"] = int(time.time())
    cols = ", ".join(f"{k}=?" for k in fields)
    with db_conn() as conn:
        conn.execute(f"UPDATE orders SET {cols} WHERE unique_code=?",
                     (*fields.values(), code))


def bump_attempts(code, err):
    with db_conn() as conn:
        row = conn.execute("SELECT attempts FROM orders WHERE unique_code=?",
                           (code,)).fetchone()
        attempts = (row["attempts"] if row else 0) + 1
        status = "error" if attempts >= MAX_ATTEMPTS else None
        if status:
            conn.execute(
                "UPDATE orders SET attempts=?, last_error=?, status=?, updated_at=? "
                "WHERE unique_code=?",
                (attempts, str(err)[:500], status, int(time.time()), code))
        else:
            conn.execute(
                "UPDATE orders SET attempts=?, last_error=?, updated_at=? "
                "WHERE unique_code=?",
                (attempts, str(err)[:500], int(time.time()), code))
    return attempts, status


# ------------------------------- Digiseller ----------------------------------

_ds_token_cache = {"value": None, "ts": 0.0}


def ds_token(force=False):
    if not force and _ds_token_cache["value"] and time.time() - _ds_token_cache["ts"] < 100 * 60:
        return _ds_token_cache["value"]
    ts = int(time.time())
    sign = hashlib.sha256((DS_API_KEY + str(ts)).encode()).hexdigest()
    r = requests.post(f"{DS_API}/apilogin",
                      json={"seller_id": DS_SELLER_ID, "timestamp": ts, "sign": sign},
                      headers={"Accept": "application/json"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("retval") != 0:
        raise DigisellerError(f"apilogin: retval={data.get('retval')} {data.get('desc')}")
    _ds_token_cache.update(value=data["token"], ts=time.time())
    log.info("Получен новый токен Digiseller (действует 2 часа)")
    return _ds_token_cache["value"]


def ds_request(method, path, token=None, json_body=None, base=DS_API, _retried=False):
    headers = {"Accept": "application/json"}
    url = f"{base}{path}"
    if token:
        headers["Token"] = token
    last_exc = None
    for attempt in range(4):
        try:
            r = requests.request(method, url, headers=headers, json=json_body, timeout=60)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", "5") or 5) + 1)
                continue
            if r.status_code == 401 and not _retried:
                ds_token(force=True)
                return ds_request(method, path, token=ds_token(), json_body=json_body,
                                  base=base, _retried=True)
            if r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r.json()
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            time.sleep(2 ** attempt)
    raise DigisellerError(f"Сетевая ошибка {method} {url}: {last_exc}")


def seller_sells(date_start, date_finish, page=1, rows=500):
    """Список продаж. Подпись: sha256(id_seller+product_ids+date_start+
    date_finish+returned+page+rows+secret), lowercase."""
    ids = sorted(int(pid) for pid in MAPPING)
    ids_str = "".join(str(i) for i in ids)
    src = f"{DS_SELLER_ID}{ids_str}{date_start}{date_finish}1{page}{rows}{DS_SECRET}"
    sign = hashlib.sha256(src.encode()).hexdigest()
    body = {
        "id_seller": DS_SELLER_ID,
        "product_ids": ids or None,
        "date_start": date_start,
        "date_finish": date_finish,
        "returned": 1,      # без возвратов
        "page": page,
        "rows": rows,
        "sign": sign,
    }
    data = ds_request("POST", "/seller-sells", json_body=body, base=DS_API_SALES)
    if data.get("retval") not in (0, None):
        raise DigisellerError(f"seller-sells: {data}")
    rows_out = data.get("rows") or (data.get("content") or {}).get("rows") or []
    return rows_out


def check_code(token, unique_code):
    """Проверка оплаты по уникальному коду (этот же вызов верифицирует код)."""
    data = ds_request("GET", f"/purchases/unique-code/{unique_code}", token=token)
    if data.get("retval") != 0:
        raise DigisellerError(f"check {unique_code}: {data.get('retdesc') or data}")
    return data


def deliver_code(token, unique_code, message):
    """Отметка 'товар выдан'. retval 0 - ок, 4 - сообщение уже было отправлено."""
    data = ds_request("PUT", f"/purchases/unique-code/{unique_code}/deliver?token={token}",
                      json_body={"message": message})
    retval = data.get("retval")
    if retval == 0 or retval == 4:
        return data
    if retval == 3:  # код ещё не верифицирован — верифицируем и повторяем
        check_code(ds_token(), unique_code)
        token = ds_token()
        data = ds_request("PUT", f"/purchases/unique-code/{unique_code}/deliver?token={token}",
                          json_body={"message": message})
        if data.get("retval") in (0, 4):
            return data
    raise DigisellerError(f"deliver {unique_code}: retval={retval} {data.get('retdesc')}")


def extract_options(info):
    """Параметры товара, введённые покупателем (id игрока, ссылка и т.п.)."""
    opts = {}
    raw = info.get("options")
    if isinstance(raw, dict):  # вариант ответа без списка
        for key, val in raw.items():
            opts[str(key)] = val
    elif isinstance(raw, list):
        for item in raw:
            name = str(item.get("id") or item.get("name") or "")
            opts[name] = item.get("value") or item.get("user_data")
    return opts


# ------------------------------- FazerCards ----------------------------------

def fz_request(method, path, json_body=None, extra_headers=None):
    headers = {"X-API-Key": FZ_API_KEY, "Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    url = f"{FZ_API}{path}"
    last_exc = None
    for attempt in range(4):
        try:
            r = requests.request(method, url, headers=headers, json=json_body, timeout=60)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", "5") or 5) + 1)
                continue
            if r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("ok") is False:
                raise FazerError(data.get("error") or data.get("code") or "неизвестная ошибка")
            return data
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            time.sleep(2 ** attempt)
    raise FazerError(f"Сетевая ошибка {method} {url}: {last_exc}")


def build_order_body(cfg, quantity, field_value):
    fam = cfg.get("family")
    if fam == "topups":
        return "/topups/order", {
            "category_id": cfg["category_id"],
            "offer_id": cfg["offer_id"],
            "fields": {cfg.get("field_key", "player_id"): field_value or ""},
        }
    if fam == "giftcards":
        return "/giftcards/order", {
            "category_id": cfg["category_id"],
            "card_id": cfg["card_id"],
            "quantity": quantity,
        }
    if fam == "gamekeys":
        return "/gamekeys/order", {
            "game_id": cfg["game_id"],
            "key_id": cfg["key_id"],
            "quantity": quantity,
        }
    if fam == "steam-gifts":
        return "/steam-gifts/order", {
            "invite_url": field_value or cfg.get("invite_url"),
            "sub_id": cfg.get("sub_id"),
            "app_id": cfg.get("app_id"),
            "region": cfg.get("region", "RU"),
        }
    if fam == "manual-services":
        body = {"manual_service_id": cfg["manual_service_id"],
                "product_id": cfg["product_id"]}
        if field_value:
            body["fields"] = {cfg.get("field_key", "comment"): field_value}
        return "/manual-services/order", body
    if fam == "custom":  # резерв для нестандартных эндпоинтов
        return cfg["path"], cfg.get("body", {})
    raise BridgeError(f"Неизвестное семейство товара: {fam}")


def fazer_create_order(cfg, idem_key, quantity, field_value):
    path, body = build_order_body(cfg, quantity, field_value)
    data = fz_request("POST", path, json_body=body,
                      extra_headers={"Idempotency-Key": idem_key})
    order = data.get("order") or data
    order_id = order.get("id") or order.get("order_id")
    if not order_id:
        raise FazerError(f"В ответе Fazer нет id заказа: {data}")
    return order_id, order.get("status", "processing")


def fazer_get_order(order_id):
    data = fz_request("GET", f"/orders/{order_id}")
    return data.get("order") or {}


def validate_player_id(category_id, fields):
    """Для пополнений: проверить ID игрока до заказа (если игра это поддерживает)."""
    try:
        data = fz_request("POST", "/topups/validate-id",
                          json_body={"category_id": category_id, "fields": fields})
        return bool(data.get("valid"))
    except FazerError as exc:
        log.warning("validate-id недоступен: %s", exc)
        return True  # не блокируем заказ, если валидатор не работает


def extract_content(order):
    """Достаём контент из ответа FazerCards. Формат payload зависит от вида товара,
    поэтому пробуем типовые поля, иначе возвращаем JSON целиком."""
    payload = order.get("payload") or order
    for key in ("cards", "codes", "keys", "results"):
        val = payload.get(key)
        if isinstance(val, list) and val:
            lines = []
            for item in val:
                if isinstance(item, str):
                    lines.append(item)
                elif isinstance(item, dict):
                    for sub in ("code", "pin", "key", "value", "url", "result", "text"):
                        if item.get(sub):
                            lines.append(str(item[sub]))
                            break
            if lines:
                return "\n".join(lines)
    for key in ("delivery_url", "result", "code", "key", "pin", "text", "message"):
        if payload.get(key):
            return str(payload[key])
    return json.dumps(payload, ensure_ascii=False, indent=2)


# ------------------------------- обработка -----------------------------------

def notify(text):
    if TG_TOKEN and TG_CHAT:
        try:
            requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                          json={"chat_id": TG_CHAT, "text": text[:3500]}, timeout=15)
        except Exception:
            pass
    log.warning(text)


def verify_sale(token, row):
    """Проверка оплаты + сверка товара и суммы. Возвращает (info, cfg, field_value)."""
    code = row["unique_code"]
    info = check_code(token, code)
    state = (info.get("unique_code_state") or {}).get("state")
    if state in (2, 3):  # товар уже выдан
        update_order(code, status="delivered", content="(выдан ранее)")
        return None, None, None
    ds_product = info.get("id_goods")
    if ds_product and int(ds_product) != row["product_id"]:
        raise DigisellerError(f"Товар в ответе ({ds_product}) не совпадает с продажей")
    cfg = MAPPING.get(str(row["product_id"]))
    if not cfg:
        raise BridgeError(f"Нет маппинга для товара {row['product_id']}")
    expected = cfg.get("price_rub")
    amount = float(info.get("amount") or 0)
    if amount <= 0:
        raise DigisellerError(f"Нулевая/неизвестная сумма заказа {info.get('inv')}")
    if expected and abs(amount - float(expected)) > max(1.0, float(expected) * 0.02):
        raise BridgeError(
            f"Сумма продажи ({amount}) сильно отличается от ожидаемой ({expected})")
    options = extract_options(info)
    field_key = cfg.get("field_key")
    field_value = None
    if field_key:
        field_value = options.get(field_key) or options.get(str(field_key))
        if not field_value:
            raise BridgeError(f"Покупатель не передал параметр '{field_key}' "
                              f"(options={options})")
    return info, cfg, field_value


def ensure_order(row, cfg, field_value):
    code = row["unique_code"]
    if row["fazer_order_id"]:
        return row["fazer_order_id"]
    if cfg["family"] == "topups" and cfg.get("validate") and cfg.get("field_key"):
        ok = validate_player_id(cfg["category_id"],
                                {cfg["field_key"]: field_value or ""})
        if not ok:
            raise FazerError("ID игрока не прошёл валидацию FazerCards")
    order_id, _ = fazer_create_order(cfg, f"ggsel-{code}",
                                     row["quantity"] or 1, field_value)
    update_order(code, fazer_order_id=order_id, status="ordered")
    log.info("[%s] создан заказ FazerCards %s", code, order_id)
    return order_id


def poll_and_deliver(row):
    code = row["unique_code"]
    order_id = row["fazer_order_id"]
    order = fazer_get_order(order_id)
    status = order.get("status")
    if status in ("failed", "refunded", "cancelled"):
        raise FazerError(f"Заказ {order_id} в статусе {status}")
    if status != "completed":
        waited = int(time.time()) - row["updated_at"]
        if waited < ORDER_WAIT_TTL:
            return False  # ждём следующего цикла
        raise FazerError(f"Заказ {order_id} не выполнен за {ORDER_WAIT_TTL} сек")
    content = extract_content(order)
    update_order(code, content=content, status="ordered")  # контент сохраняем ДО выдачи
    deliver_code(ds_token(), code, content)
    update_order(code, status="delivered")
    log.info("[%s] выдано, заказ Fazer %s", code, order_id)
    notify(f"Выдача завершена: товар {row['product_id']}, заказ {order_id}")
    return True


def handle(row):
    token = ds_token()
    code = row["unique_code"]
    if row["status"] == "new":
        info, cfg, field_value = verify_sale(token, row)
        if info is None:
            return  # уже выдан на стороне Digiseller
        ensure_order(row, cfg, field_value)
        row = {**dict(row), "status": "ordered"}
    if row["status"] in ("ordered", "waiting"):
        poll_and_deliver(row)


# --------------------------------- циклы -------------------------------------

def poll_sales_cycle():
    """Опрос новых продаж и запись их в базу."""
    now = int(time.time())
    start = int(get_meta("last_poll", now - 3600))
    fmt = lambda ts: time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts))
    page = 1
    total_new = 0
    while True:
        rows = seller_sells(fmt(start - 300), fmt(now), page=page, rows=500)
        if not rows:
            break
        for r in rows:
            if upsert_sale(r.get("invoice_id"), r.get("id_goods"),
                           r.get("unique_code"), int(r.get("cnt_goods") or 1)):
                total_new += 1
        if len(rows) < 500:
            break
        page += 1
    if total_new:
        log.info("Новых продаж: %s", total_new)
    set_meta("last_poll", now)


def process_pending():
    token = ds_token()
    for row in fetch_pending():
        code = row["unique_code"]
        if row["status"] == "waiting":
            continue  # для waiting достаточно того, что заказ уже попал в цикл выше
        try:
            handle(row)
        except BridgeError as exc:
            attempts, final = bump_attempts(code, exc)
            log.error("[%s] ошибка (попытка %s): %s", code, attempts, exc)
            if final:
                notify(f"ЗАКАЗ ЗАСТРЯЛ: {code} — {exc}")
        except Exception as exc:  # непредвиденное — не убиваем цикл
            attempts, final = bump_attempts(code, exc)
            log.exception("[%s] непредвиденная ошибка", code)
            if final:
                notify(f"ЗАКАЗ ЗАСТРЯЛ: {code} — {exc}")


_stop = {"flag": False}


def _signal_handler(signum, frame):
    _stop["flag"] = True
    log.info("Получен сигнал остановки, завершаюсь после текущего цикла")


def run():
    init_db()
    if not (DS_SELLER_ID and DS_API_KEY and FZ_API_KEY):
        print("Не заполнены DS_SELLER_ID / DS_API_KEY / FZ_API_KEY в .env", file=sys.stderr)
        sys.exit(1)
    if not MAPPING:
        print("mapping.json пуст или не найден — заполните маппинг товаров",
              file=sys.stderr)
        sys.exit(1)
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    log.info("Мост запущен. Товаров в маппинге: %s, интервал опроса: %s c",
             len(MAPPING), POLL_INTERVAL)
    while not _stop["flag"]:
        try:
            poll_sales_cycle()
            process_pending()
        except Exception:
            log.exception("Ошибка в основном цикле")
        time.sleep(POLL_INTERVAL)
    log.info("Остановлено")


# ------------------------------- CLI-утилиты ---------------------------------

def cmd_check(code):
    init_db()
    info = check_code(ds_token(), code)
    print(json.dumps(info, ensure_ascii=False, indent=2))


def cmd_deliver(code, text):
    init_db()
    deliver_code(ds_token(), code, text)
    print("Выдано.")


def cmd_status(order_id):
    print(json.dumps(fazer_get_order(order_id), ensure_ascii=False, indent=2))


def main():
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(BASE_DIR / "bridge.log", encoding="utf-8")])
    parser = argparse.ArgumentParser(description="GGsel <-> FazerCards bridge")
    parser.add_argument("command", nargs="?", default="run",
                        choices=["run", "check", "deliver", "status"])
    parser.add_argument("arg1", nargs="?")
    parser.add_argument("arg2", nargs="?")
    args = parser.parse_args()
    if args.command == "run":
        run()
    elif args.command == "check":
        cmd_check(args.arg1)
    elif args.command == "deliver":
        cmd_deliver(args.arg1, args.arg2)
    elif args.command == "status":
        cmd_status(args.arg1)


if __name__ == "__main__":
    main()
