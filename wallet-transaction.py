# -*- coding: utf-8 -*-
"""
Author: SnowBar0v0
GitHub: https://github.com/SnowBar0v0

简化说明：
这是一个把 Telegram 交易消息转发到微信并做简单汇总与持久化的脚本。

注意（重要）：
    - 本文件包含若干“配置项”（在 CONFIG 和 TG_BOT_TOKEN）需要在你的环境中填写真实值后才能正常运行。
    - 为了安全，关键凭证（如 bot token）不应提交到公共仓库；开发时请在本地/私有环境中填写。

我已把关键信息保留为占位（空值或 None）。部署前请手动编辑下方 CONFIG 和 TG_BOT_TOKEN。
为方便初学者，文件中已添加或整理了若干注释，解释每个主要变量和函数的目的与输入/输出。
"""

import re
import pyperclip
import time
import logging
import threading
import requests
import sys
import json
import os
import random  # 微信发送节奏随机化，降低机器人嫌疑
from collections import defaultdict, deque
import datetime
from pywinauto.application import Application
from pywinauto.keyboard import send_keys
import pandas as pd  # 地址库

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ========== 配置（部署前请填写） ==========
# 说明：为了安全，本文件把关键信息保留为空/占位。
# 在你的私有环境中，请填写这些项：wechat_pid、source_channel_ids、address_book_path、TG_BOT_TOKEN 等。
CONFIG = {
    # 微信进程 PID：若不填写（None），脚本会尝试自动查找正在运行的微信进程
    "wechat_pid": ,

    # 微信窗口与控件名
    "window_title": " ",
    "message_list_name": "消息",
    "message_list_type": "List",

    # 需要监听的 Telegram 频道 ID（例如 [-1004541546]）。
    # 留空列表 [] 表示不过滤（接收来自所有频道的消息），上线前请根据需求填写。
    "source_channel_ids": [],

    # 地址库路径
    "address_book_path": "",
}

SHOW_EVM_ADDR_IN_SUMMARY = True
# 消息长度限制：微信单条消息一般有长度上限，设置为一个保守值以避免被截断
MAX_WECHAT_MSG_LEN = 2500
MAX_TG_MSG_LEN = 3900  # 保守设置，避免接近 TG 平台的单条上限

# ========== 敏感/关键配置（部署前填写） ==========
# Telegram Bot Token：在部署前把实际 token 填入此处，例如 "123456:ABC-DEF"。
# 出于安全考虑，这里留空；填写后脚本才能正常调用 Telegram API。
TG_BOT_TOKEN = ""
TG_API_URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}"  # 若为空字符串，API 调用会失败

CACHE = deque()
TIME_RE = re.compile(r"(\d{2}-\d{2}\s*\d{2}:\d{2}:\d{2})")  # 11-11 10:37:34

# ===== 地址/域名正则 =====
ADDRESS_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,48}\b")   # Solana Base58
EVM_ADDRESS_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")         # EVM
BRACKET_RE = re.compile(r"\[(.*?)\]")

GROUP_CMD = re.compile(r"^/\s*(jak|bz|house|tst|bsc)\s*组\b", re.IGNORECASE)
KEY_CMD = re.compile(r"^/\s*(\S+)\b", re.IGNORECASE)

# 时间段命令 & 24 小时命令
RANGE_CMD = re.compile(r"^/(交易|转账|汇总)\s+(\d{1,2})-(\d{1,2})\s*$")
LAST24_CMD = re.compile(r"^/(交易|转账|汇总)\s*24\s*$")

PAUSE_START = datetime.time(hour=3, minute=0, second=0)
PAUSE_END = datetime.time(hour=3, minute=0, second=10)

JSON_PATH = "forward_records_jak.json"
RANGES_PATH = "last_ranges_jak.json"   # 新增：持久化最近一次时间段
CACHE_DAYS = 14

# ===== 全局状态 =====
WECHAT_WIN = None
WECHAT_AVAILABLE = False       # 是否已经成功连接并可用
FORWARD_ENABLED = False        # TG -> 微信实时转发开关（/启动 /关闭）

LAST_TRADE_RANGE = None        # 最近一次 /交易 H1-H2 或 /汇总 H1-H2
LAST_TRANSFER_RANGE = None     # 最近一次 /转账 H1-H2 或 /汇总 H1-H2

# ===== 地址库结构 =====
ADDRESS_BOOK = {}          # norm_full_addr -> display label（含emoji或短码）
HEADTAIL_INDEX = {}        # (head5, tail6) -> norm_full_addr
ROUTER_ADDRS = set()       # 从《路由》表读取的完整地址（含 EVM/SOL）
ROUTER_NAMES = set()       # 从《路由》表读取的名称
SCAN_HOSTS = {
    "solscan.io", "basescan.org", "etherscan.io", "bscscan.com", "arbiscan.io",
    "polygonscan.com", "ftmscan.com", "snowtrace.io", "tronscan.org",
    "solana.com", "debridge.finance"
}

ADDR_NORMALIZER = lambda s: s.strip().lower() if s and s.startswith("0x") else (s.strip() if s else s)

ADDR_COL_CANDIDATES = {"address", "地址", "钱包地址", "solana地址", "合约", "合约地址", "CA", "addr"}
NAME_COL_CANDIDATES = {"name", "名称", "备注", "label", "标签", "昵称", "别名"}
EMOJI_COL_CANDIDATES = {"emoji", "符号", "表情"}


def _pick_col(cols, candidates):
    low = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in low:
            return low[cand]
    for c in cols:
        cl = c.lower()
        for cand in candidates:
            if cand in cl:
                return c
    return None


def _mk_shortcode(addr: str) -> str:
    if not addr:
        return ""
    a = addr[2:] if addr.startswith("0x") else addr
    if len(a) < 4:
        return a.upper()
    return (a[:2] + a[-2:]).upper()


def _mk_headtail(addr: str):
    if not addr:
        return None
    a = addr[2:] if addr.startswith("0x") else addr
    if len(a) < 11:
        return None
    return (a[:5], a[-6:])


def _is_valid_full_addr(s: str) -> bool:
    if not s:
        return False
    return bool(ADDRESS_RE.fullmatch(s) or EVM_ADDRESS_RE.fullmatch(s))


def _norm_addr(s: str) -> str:
    if not s:
        return s
    return s.lower() if s.startswith("0x") else s


def load_address_book(force: bool = False):
    """读取 Excel 地址库。
    force=False：若已加载则直接返回；force=True：无条件重读（用于 /刷新地址）。"""
    global ADDRESS_BOOK, HEADTAIL_INDEX, ROUTER_ADDRS, ROUTER_NAMES

    if not force and ADDRESS_BOOK:
        return

    ADDRESS_BOOK = {}
    HEADTAIL_INDEX = {}
    ROUTER_ADDRS = set()
    ROUTER_NAMES = set()

    path = CONFIG.get("address_book_path") or "solana_add.xlsx"
    tried = [path]
    if not os.path.exists(path):
        alt = os.path.join(os.path.dirname(os.path.abspath(__file__)), "solana_add.xlsx")
        tried.append(alt)
        path = alt
    if not os.path.exists(path):
        logging.error(f"地址库未找到，尝试路径: {tried}")
        return

    try:
        xls = pd.ExcelFile(path)
    except Exception as e:
        logging.error(f"读取地址库失败: {e}")
        return

    cnt = 0
    for sheet in xls.sheet_names:
        try:
            df = xls.parse(sheet)
        except Exception:
            continue
        if df is None or df.empty:
            continue

        addr_col = _pick_col(df.columns, ADDR_COL_CANDIDATES)
        name_col = _pick_col(df.columns, NAME_COL_CANDIDATES)
        emoji_col = _pick_col(df.columns, EMOJI_COL_CANDIDATES)

        is_router_sheet = (str(sheet).strip() == "路由")

        if not addr_col:
            # 没地址列也可能是纯名称路由表
            if is_router_sheet and name_col:
                for _, row in df.iterrows():
                    label = str(row.get(name_col) or "").strip()
                    if label:
                        ROUTER_NAMES.add(label)
            continue

        for _, row in df.iterrows():
            addr = str(row.get(addr_col) or "").strip()
            if not addr:
                continue
            label = str(row.get(name_col) or "").strip() if name_col else ""
            emoji = str(row.get(emoji_col) or "").strip() if emoji_col else ""
            if not label:
                label = _mk_shortcode(addr)
            display = f"{emoji} {label}".strip() if emoji and emoji != "nan" else label

            norm = ADDR_NORMALIZER(addr)
            ADDRESS_BOOK[norm] = display

            ht = _mk_headtail(addr)
            if ht:
                HEADTAIL_INDEX[ht] = norm
            cnt += 1

            if is_router_sheet:
                if _is_valid_full_addr(addr):
                    ROUTER_ADDRS.add(_norm_addr(addr))
                if label:
                    ROUTER_NAMES.add(label.strip())

    # 补充常见路由/域名关键字
    ROUTER_NAMES.update({
        "交易路由", "Relay: Solver", "Jitotip", "Trojan Fees",
        "Jupiter Aggregator Authority", "FixedFloat Exchange",
        "Fireblocks Custody", "Fireblocks Custody 充值中转",
    })
    ROUTER_NAMES.update(SCAN_HOSTS)

    logging.info(f"地址库加载完成：{cnt} 条；路由地址 {len(ROUTER_ADDRS)} 条，路由名 {len(ROUTER_NAMES)} 个")


# ===== RANGES 持久化（新增） =====
def load_last_ranges():
    """从 RANGES_PATH 加载 LAST_TRADE_RANGE / LAST_TRANSFER_RANGE（若存在）"""
    global LAST_TRADE_RANGE, LAST_TRANSFER_RANGE
    if not os.path.exists(RANGES_PATH):
        logging.info("last ranges 文件不存在，使用默认（None）")
        return
    try:
        with open(RANGES_PATH, "r", encoding="utf-8") as f:
            j = json.load(f)
        tr = j.get("LAST_TRADE_RANGE")
        tf = j.get("LAST_TRANSFER_RANGE")
        LAST_TRADE_RANGE = tuple(tr) if isinstance(tr, (list, tuple)) else None
        LAST_TRANSFER_RANGE = tuple(tf) if isinstance(tf, (list, tuple)) else None
        logging.info(f"已加载最近时间段：LAST_TRADE_RANGE={LAST_TRADE_RANGE}, LAST_TRANSFER_RANGE={LAST_TRANSFER_RANGE}")
    except Exception as e:
        logging.error(f"加载 last ranges 失败: {e}")


def save_last_ranges():
    """把当前 LAST_TRADE_RANGE / LAST_TRANSFER_RANGE 持久化到文件"""
    try:
        j = {
            "LAST_TRADE_RANGE": list(LAST_TRADE_RANGE) if LAST_TRADE_RANGE else None,
            "LAST_TRANSFER_RANGE": list(LAST_TRANSFER_RANGE) if LAST_TRANSFER_RANGE else None
        }
        with open(RANGES_PATH, "w", encoding="utf-8") as f:
            json.dump(j, f, ensure_ascii=False, indent=2)
        logging.info(f"已保存最近时间段到 {RANGES_PATH}")
    except Exception as e:
        logging.error(f"保存 last ranges 失败: {e}")


# ===== JSON 持久化 =====
def load_forward_records():
    """读取 JSON 记录，保留最近 CACHE_DAYS 天。
    如果文件损坏无法解析，会把原文件备份成 .broken_ 时间戳 再重新开始。
    """
    if not os.path.exists(JSON_PATH):
        return []

    try:
        with open(JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        # 读取失败：先把原文件备份，再从空列表开始
        try:
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup = f"{JSON_PATH}.broken_{ts}"
            os.replace(JSON_PATH, backup)
            logging.error(
                f"读取缓存文件失败，已将原文件备份到 {backup}：{e}"
            )
        except Exception as e2:
            logging.error(
                f"读取缓存文件失败，且备份原文件也失败：{e2}"
            )
        return []

    now = datetime.datetime.now()
    cutoff = now - datetime.timedelta(days=CACHE_DAYS)
    cleaned = []
    dropped = 0

    for r in data:
        try:
            ts = datetime.datetime.strptime(
                r.get("ts", ""), "%Y-%m-%d %H:%M:%S"
            )
        except Exception:
            dropped += 1
            continue
        if ts >= cutoff:
            cleaned.append(r)
        else:
            dropped += 1

    if dropped:
        logging.info(
            f"JSON 载入 {len(data)} 条，剔除过期/无效 {dropped} 条，保留 {len(cleaned)} 条"
        )
    return cleaned

def save_forward_record(text: str, status="forwarded", raw_text=None):
    """所有 TG 消息必须先写入 JSON，再考虑发微信。"""
    data = load_forward_records()
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = {"ts": now, "text": text, "status": status}
    if raw_text is not None:
        entry["raw_text"] = raw_text
    data.append(entry)
    try:
        with open(JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"写入缓存文件失败: {e}")


# ===== 基础工具 =====
def is_pause_period() -> bool:
    now = datetime.datetime.now().time()
    return PAUSE_START <= now < PAUSE_END


def _parse_dt_from_raw(raw_text: str, ts_fallback: datetime.datetime) -> datetime.datetime:
    """从 raw_text 提取 'MM-DD HH:MM:SS'；若只出现 HH:MM:SS 则用 ts_fallback 的年月日；都无则回落 ts_fallback。"""
    if not raw_text:
        return ts_fallback
    m = re.search(r"(\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", raw_text)
    if m:
        try:
            return datetime.datetime.strptime(f"{ts_fallback.year}-{m.group(1)}", "%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    m2 = re.search(r"\b(\d{2}:\d{2}:\d{2})\b", raw_text)
    if m2:
        try:
            d = ts_fallback.date().isoformat()
            return datetime.datetime.strptime(f"{d} {m2.group(1)}", "%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    return ts_fallback


def extract_timestamp(text: str) -> datetime.datetime:
    m = TIME_RE.search(text)
    now = datetime.datetime.now()
    if m:
        try:
            return datetime.datetime.strptime(f"{now.year}-{m.group(1)}", "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return now


def clean_cache():
    cutoff = datetime.datetime.now() - datetime.timedelta(hours=36)
    while CACHE and CACHE[0][0] < cutoff:
        CACHE.popleft()


def reconstruct_with_entities(post: dict) -> str:
    """把 entities/caption_entities 里的 text_link 展开成 '文字 (URL)'，仅用于 raw_text 保存与解析。"""
    txt = post.get("text") or post.get("caption") or ""
    entities = post.get("entities") or post.get("caption_entities") or []
    if not txt or not entities:
        return txt
    out = []
    last = 0
    for ent in sorted(entities, key=lambda e: e.get("offset", 0)):
        off = ent.get("offset", 0)
        length = ent.get("length", 0)
        off = max(0, min(off, len(txt)))
        end = min(off + length, len(txt))
        if last < off:
            out.append(txt[last:off])
        seg = txt[off:end]
        etype = ent.get("type")
        url = ent.get("url")
        if etype == "text_link" and url:
            out.append(f"{seg} ({url})")
        else:
            out.append(seg)
        last = end
    if last < len(txt):
        out.append(txt[last:])
    return "".join(out)


# ===== 地址提取 =====
def extract_addresses_all(text: str):
    sol = ADDRESS_RE.findall(text) or []
    evm = EVM_ADDRESS_RE.findall(text) or []

    def uniq(seq):
        s, out = set(), []
        for x in seq:
            if x not in s:
                out.append(x)
                s.add(x)
        return out

    return uniq(sol), uniq(evm)


# ===== 交易/转账分类（基于首行方括号标题） =====
def _header_label(txt: str) -> str:
    first = txt.strip().splitlines()[0] if txt.strip().splitlines() else ""
    m = BRACKET_RE.search(first)
    return (m.group(1) if m else first).strip()


def _is_buy_trade_header(label: str) -> bool:
    # 只把“买入”视为交易；无视“卖出”
    return "买入" in (label or "")


def _is_buy_trade_post(txt: str) -> bool:
    return _is_buy_trade_header(_header_label(txt))


# ===== Sender/To 抽取与替换（基于 raw_text URL） =====

# 归一化 raw_text 中被链接插入的 "T (url) o:" 之类拆断情况
GARBLED_TO_RE = re.compile(r"T\s*\((https?://[^\)]*?/address/[^\)]+)\)\s*o\s*:", re.IGNORECASE)


def _normalize_garbled_sender_to(raw: str) -> str:
    if not raw:
        return raw
    return GARBLED_TO_RE.sub(r"To: (\1)", raw)


URL_ADDR_RE = re.compile(
    r"https?://[^\s\)]+/address/([1-9A-HJ-NP-Za-km-z]{32,48}|0x[a-fA-F0-9]{40})"
)


def _scan_full_addrs_after_markers(raw_text: str, kinds=("Sender", "Token Sender", "To")):
    """从 raw_text 中按顺序扫描 kinds 关键字，取各自后面最近的 /address/<full_addr>。"""
    res = {k: [] for k in kinds}
    if not raw_text:
        return res
    s = _normalize_garbled_sender_to(raw_text)
    for k in kinds:
        for m in re.finditer(rf"{re.escape(k)}\s*:", s, flags=re.IGNORECASE):
            pos = m.end()
            window = s[pos:pos + 600]
            um = URL_ADDR_RE.search(window)
            if um:
                res[k].append(um.group(1))
            else:
                res[k].append(None)
    return res


def _lookup_label_by_fulladdr(addr: str) -> str | None:
    if not addr:
        return None
    return ADDRESS_BOOK.get(ADDR_NORMALIZER(addr))


def _lookup_label_by_headtail(short_text: str) -> str | None:
    if not short_text or "..." not in short_text:
        return None
    parts = short_text.split("...")
    head = re.sub(r'[^A-Za-z0-9]', '', parts[0])[-5:]
    tail = re.sub(r'[^A-Za-z0-9]', '', parts[-1])[:6]
    if len(head) < 5 or len(tail) < 6:
        return None
    key = (head, tail)
    full_norm = HEADTAIL_INDEX.get(key)
    if not full_norm:
        return None
    return ADDRESS_BOOK.get(full_norm)


def replace_sender_to_with_names(display_text: str, raw_text: str) -> str:
    """
    把 display_text 中的 Sender/To/Token Sender 替换成：
      先用 raw_text 中 URL 提取完整地址 → 地址库名称 → 未命中则显示完整地址；
    不会回落为 solscan.io 之类域名。
    display_text：保持 Telegram 聊天窗口看到的样子（不含 URL 展开）
    raw_text：reconstruct_with_entities 生成的富文本（含 URL），仅用于解析。
    """
    if not display_text:
        return display_text

    url_seq = _scan_full_addrs_after_markers(raw_text or display_text)
    sender_urls = list(url_seq.get("Sender", []))
    token_sender_urls = list(url_seq.get("Token Sender", []))
    to_urls = list(url_seq.get("To", []))

    lines = display_text.splitlines()
    idx_used = {"Sender": 0, "Token Sender": 0, "To": 0}

    def resolve_value(kind: str, short_line_value: str) -> str:
        url_list = sender_urls if kind == "Sender" else (token_sender_urls if kind == "Token Sender" else to_urls)
        full_addr = None
        if idx_used[kind] < len(url_list):
            full_addr = url_list[idx_used[kind]]
        idx_used[kind] += 1

        label = None
        if full_addr and _is_valid_full_addr(full_addr):
            label = _lookup_label_by_fulladdr(full_addr) or full_addr
        else:
            short = short_line_value.strip()
            if short in SCAN_HOSTS:
                short = ""
            if short:
                by_ht = _lookup_label_by_headtail(short)
                if by_ht:
                    label = by_ht

        return label or (full_addr if full_addr else short_line_value.strip())

    for i, ln in enumerate(lines):
        lns = ln.strip()
        if lns.startswith("Sender:") or lns.startswith("To:") or lns.startswith("Token Sender:"):
            if lns.startswith("Sender:"):
                kind = "Sender"
                short_val = lns.split(":", 1)[1].strip()
                new_val = resolve_value(kind, short_val)
                lines[i] = f"Sender: {new_val}"
            elif lns.startswith("Token Sender:"):
                kind = "Token Sender"
                short_val = lns.split(":", 1)[1].strip()
                new_val = resolve_value(kind, short_val)
                lines[i] = f"Sender: {new_val}"
            else:
                kind = "To"
                short_val = lns.split(":", 1)[1].strip()
                new_val = resolve_value(kind, short_val)
                lines[i] = f"To: {new_val}"

    return "\n".join(lines)


# ===== 微信 / TG 发送 =====
def split_wechat_message(text: str, max_len: int = MAX_WECHAT_MSG_LEN) -> list:
    if not text:
        return []
    if len(text) <= max_len:
        return [text]
    paragraphs = text.split("\n\n")
    chunks, cur = [], ""
    for pi, p in enumerate(paragraphs):
        seg = p if pi == len(paragraphs) - 1 else p + "\n\n"
        if len(seg) > max_len:
            for line in seg.splitlines(keepends=True):
                if len(cur) + len(line) > max_len:
                    if cur:
                        chunks.append(cur.rstrip())
                        cur = ""
                    if len(line) > max_len:
                        start = 0
                        while start < len(line):
                            chunks.append(line[start:start + max_len])
                            start += max_len
                    else:
                        cur = line
                else:
                    cur += line
        else:
            if len(cur) + len(seg) > max_len:
                if cur:
                    chunks.append(cur.rstrip())
                cur = seg
            else:
                cur += seg
    if cur:
        chunks.append(cur.rstrip())
    if len(chunks) > 1:
        total = len(chunks)
        return [f"({i}/{total})\n{c}" for i, c in enumerate(chunks, 1)]
    return chunks


def split_telegram_message(text: str, max_len: int = MAX_TG_MSG_LEN) -> list:
    if not text:
        return []
    if len(text) <= max_len:
        return [text]
    paragraphs = text.split("\n\n")
    chunks, cur = [], ""
    for pi, p in enumerate(paragraphs):
        seg = p if pi == len(paragraphs) - 1 else p + "\n\n"
        if len(seg) > max_len:
            for line in seg.splitlines(keepends=True):
                if len(cur) + len(line) > max_len:
                    if cur:
                        chunks.append(cur.rstrip())
                        cur = ""
                    if len(line) > max_len:
                        start = 0
                        while start < len(line):
                            chunks.append(line[start:start + max_len])
                            start += max_len
                    else:
                        cur = line
                else:
                    cur += line
        else:
            if len(cur) + len(seg) > max_len:
                if cur:
                    chunks.append(cur.rstrip())
                cur = seg
            else:
                cur += seg
    if cur:
        chunks.append(cur.rstrip())
    if len(chunks) > 1:
        total = len(chunks)
        return [f"({i}/{total})\n{c}" for i, c in enumerate(chunks, 1)]
    return chunks


def send_to_telegram(text: str):
    if not text:
        return
    for part in split_telegram_message(text):
        try:
            requests.post(
                f"{TG_API_URL}/sendMessage",
                data={"chat_id": "-1002390497380", "text": part},
                timeout=10
            )
        except Exception:
            pass


def send_to_wechat(text: str):
    """
    Ctrl+V + Enter 发送，微信挂掉不退出脚本。

    细节：
    - 使用 MAX_WECHAT_MSG_LEN 分段；
    - 若总段数 > 8，则显著减速发送，并在每段间加入随机等待，降低被判定为机器人的风险。
    """
    global WECHAT_WIN, WECHAT_AVAILABLE

    if not text:
        return

    if is_pause_period():
        logging.info(f"暂停窗口，跳过发送: {text[:80]}...")
        return

    if not WECHAT_AVAILABLE or WECHAT_WIN is None:
        logging.warning("微信当前不可用，跳过发送（脚本继续运行）。")
        return

    parts = split_wechat_message(text, max_len=MAX_WECHAT_MSG_LEN)
    total = len(parts)
    if total == 0:
        return

    for idx, part in enumerate(parts, 1):
        if not part:
            continue
        pyperclip.copy(part)
        try:
            box = WECHAT_WIN.child_window(auto_id="chat_input_field", control_type="Edit")
            if not box.exists(timeout=2):
                box = WECHAT_WIN.child_window(control_type="Edit")
                if not box.exists(timeout=2):
                    raise RuntimeError("微信输入框不存在，可能已掉线")
            box.click_input()
            box.type_keys("^v{ENTER}")
        except Exception as e:
            WECHAT_AVAILABLE = False
            WECHAT_WIN = None
            warning = "⚠️ 微信发送失败，可能已掉线（脚本继续运行，仅停止发送）。"
            send_to_telegram(warning)
            logging.error(f"{warning} 详细：{e}")
            break

        # 根据总段数控制节奏
        if total <= 3:
            sleep_sec = 0.8 + random.uniform(0.2, 0.6)
        elif total <= 8:
            sleep_sec = 1.5 + random.uniform(0.5, 1.0)
        else:
            # 大于 8 段：明显减速（3~5 秒）
            sleep_sec = 3.0 + random.uniform(1.0, 2.0)

        logging.info(f"微信发送第 {idx}/{total} 段，休眠 {sleep_sec:.2f}s")
        time.sleep(sleep_sec)


# ===== 金额解析 =====
AMOUNT_LINE_RE = re.compile(
    r'^\s*(?:[🟢🔴]\s*)?([+\-])\s*([<\s]*\d*\.?\d+(?:\s*[万亿KMBkmb])?)\s*([A-Za-z0-9._-]+)\b'
)


def _parse_amount_lines(text: str):
    items = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        m = AMOUNT_LINE_RE.match(line)
        if not m:
            continue
        sign = m.group(1)
        amount_raw = m.group(2).strip()
        token = m.group(3).strip()
        items.append({"sign": sign, "amount_raw": amount_raw, "token": token, "raw_line": line})
    return items


def _amount_to_float(amount_raw: str):
    try:
        s = amount_raw.replace("<", "").replace(" ", "")
        m = re.match(r'^(\d*\.?\d+)(万|亿|[KkMmBb])?$', s)
        if not m:
            return float(s)
        val = float(m.group(1))
        unit = m.group(2)
        if not unit:
            return val
        factor = {
            '万': 1e4,
            '亿': 1e8,
            'K': 1e3,
            'k': 1e3,
            'M': 1e6,
            'm': 1e6,
            'B': 1e9,
            'b': 1e9
        }[unit]
        return val * factor
    except Exception:
        return None


def _fmt_num(x):
    try:
        s = f"{float(x):,.6f}"
        return s.rstrip("0").rstrip(".")
    except Exception:
        return str(x)


def _is_micro_sol_item(it) -> bool:
    if it["token"].upper() != "SOL":
        return False
    raw = it["amount_raw"]
    if "<" in raw:
        return True
    try:
        val = float(raw.replace(" ", ""))
        return abs(val - 0.0075) < 1e-9
    except Exception:
        return False


def _should_ignore_entire_transfer(items) -> bool:
    if not items:
        return True
    return all(_is_micro_sol_item(it) for it in items)


# ===== Token/MCap 辅助 =====
def _find_mcap_in_text(full_text: str):
    if not full_text:
        return None
    t = re.sub(r'0\{(\d+)\}', lambda m: '0' * int(m.group(1)), full_text)
    m = re.search(r'(?:MCap|市值)[:：]\s*([0-9\.,]+(?:[KkMmBb]|万|亿)?)', t, re.IGNORECASE)
    return m.group(1) if m else None


def _extract_token_from_label(label_parts):
    try:
        idx = label_parts.index('买入')
        return label_parts[idx + 1] if idx + 1 < len(label_parts) else None
    except ValueError:
        return None


# ===== 路由判定 =====
def _is_router_name(name: str) -> bool:
    if not name:
        return False
    nm = name.strip()
    if nm in ROUTER_NAMES:
        return True
    if any(nm.endswith(h) or nm == h for h in SCAN_HOSTS):
        return True
    return False


def _is_router_addr(addr: str) -> bool:
    if not addr:
        return False
    return _norm_addr(addr) in ROUTER_ADDRS


# ===== 汇总核心收集函数（统一基于 raw_text） =====
def _collect_group_buys(key: str, records, time_filter_fn):
    """从 JSON 记录中收集某组的买入记录：
    - 解析 / 过滤使用 raw_text（base）
    - 展示（sender 名、标题等）使用 text（display，不含 URL）
    """
    token_buys = defaultdict(list)
    addr_map = defaultdict(lambda: defaultdict(set))

    for r in records:
        try:
            ts_raw = datetime.datetime.strptime(r["ts"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        raw = r.get("raw_text") or ""
        display = r.get("text") or ""          # TG 里看到的样子，不带 URL
        base = raw or display                  # 解析基准（优先 raw_text）

        if not base and not display:
            continue

        # 分组 / 买入判断依然基于 raw_text
        if not re.search(fr"\b{key}\b", base, re.IGNORECASE):
            continue
        if not _is_buy_trade_post(base):
            continue

        # 时间基于 raw_text + ts_fallback
        ts = _parse_dt_from_raw(raw, ts_raw)
        if not time_filter_fn(ts):
            continue

        # ===== 标题 & sender 全部用“展示文本”的首行来算，避免 URL =====
        header_for_label = display if display.strip() else base
        label_disp = _header_label(header_for_label)  # 这里不再用 raw_text
        parts_disp = label_disp.split()

        # token 名优先从“买入 XXX”里抓（用展示标题，不带 URL）
        token_name = _extract_token_from_label(parts_disp)
        if not token_name:
            # 兜底：在展示标题首行里找第一个字母数字单词
            first_words = re.findall(r"[A-Za-z0-9_\-]{2,}", label_disp)
            token_name = first_words[0] if first_words else None
        if not token_name:
            continue

        # MCap 仍然用 raw_text 抓（有 0{5} 这种展开）
        mcap_raw = _find_mcap_in_text(base)

        # ===== CA 地址：仍然用 display（不带 URL） =====
        lines = (display or base).splitlines()
        sol_addr = None
        evm_addr = None
        for i, line in enumerate(lines):
            if '🟢' in line and token_name in line:
                for j in range(i + 1, min(i + 6, len(lines))):
                    found_sol = ADDRESS_RE.findall(lines[j]) or []
                    found_evm = EVM_ADDRESS_RE.findall(lines[j]) or []
                    if found_sol and not sol_addr:
                        sol_addr = found_sol[0]
                    if found_evm and not evm_addr:
                        evm_addr = found_evm[0]
                    if sol_addr and evm_addr:
                        break
                if sol_addr or evm_addr:
                    break
        if not sol_addr or (SHOW_EVM_ADDR_IN_SUMMARY and not evm_addr):
            all_sol, all_evm = extract_addresses_all(display or base)
            if not sol_addr and all_sol:
                sol_addr = all_sol[0]
            if SHOW_EVM_ADDR_IN_SUMMARY and not evm_addr and all_evm:
                evm_addr = all_evm[0]

        # ===== sender 只用展示标题分词，彻底摆脱 URL =====
        try:
            idx = parts_disp.index('买入')
            # 保持你原来的「从第 3 个词开始」的习惯
            found_sender = ' '.join(parts_disp[2:idx]) if idx > 2 else (parts_disp[0] if parts_disp else "未知")
        except Exception:
            found_sender = parts_disp[0] if parts_disp else "未知"

        # 把 sender 写进 addr_map，用于“🤑 包含地址：”
        for a in [sol_addr, evm_addr]:
            if a:
                addr_map[token_name][a].add(found_sender)

        token_buys[token_name].append({
            "ts": ts,
            "sender": found_sender,
            "sol_addr": sol_addr,
            "evm_addr": evm_addr,
            "mcap": mcap_raw
        })

    return token_buys, addr_map


def _collect_group_transfers(key: str, records, time_filter_fn):
    """从 JSON 记录中收集某组的转账记录（严格基于 raw_text 时间，但展示用 text）。"""
    grouped = defaultdict(list)

    for r in records:
        try:
            ts_raw = datetime.datetime.strptime(r["ts"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        raw = r.get("raw_text") or ""
        text_saved = r.get("text") or ""
        base = raw or text_saved

        if not base and not text_saved:
            continue

        # 用 raw/base 检索分组关键字（里面才有完整 JAK/BZ 信息）
        if not re.search(fr"\b{key}\b", base, re.IGNORECASE):
            continue

        # 用“展示文本”的首行当 header，避免 URL
        header_for_label = text_saved or base
        label = _header_label(header_for_label)

        # 首行带“买入/卖出”的直接排除（这是交易，不是转账）
        if ("买入" in label) or ("卖出" in label):
            continue

        # 必须包含 Sender/To/Token Sender 关键字
        if not any(hint in base for hint in ("Sender:", "To:", "Token Sender:")):
            continue

        # 金额行解析基于 base（raw_text），便于识别微额 SOL gas
        items = _parse_amount_lines(base)
        if not items:
            continue
        if _should_ignore_entire_transfer(items):
            continue

        # 时间基于 raw_text + ts_fallback
        ts = _parse_dt_from_raw(raw, ts_raw)
        if not time_filter_fn(ts):
            continue

        # 展示文本：已经是“TG 样子”的 text，没有 URL 展开
        display = text_saved or base
        txt = replace_sender_to_with_names(display, raw or display)

        sender_val = None
        to_val = None
        for line in txt.splitlines():
            ls = line.strip()
            if ls.startswith("Sender:"):
                sender_val = ls.split(":", 1)[1].strip()
            elif ls.startswith("To:"):
                to_val = ls.split(":", 1)[1].strip()
            elif ls.startswith("Token Sender:"):
                sender_val = ls.split(":", 1)[1].strip()

        # raw_text 中的完整地址，用于路由过滤
        url_seq = _scan_full_addrs_after_markers(raw or base)
        sender_full = (url_seq.get("Sender") or [None])[0]
        if not sender_full:
            sender_full = (url_seq.get("Token Sender") or [None])[0]
        to_full = (url_seq.get("To") or [None])[0]

        # 路由过滤
        if _is_router_name(sender_val) or _is_router_addr(sender_full):
            continue
        if _is_router_name(to_val) or _is_router_addr(to_full):
            continue

        # 标题优先用“展示文本”的首行（无 URL），再兜底原 label
        display_label = _header_label(display) if display.strip() else ""
        title = display_label or label or "未命名"

        def _pretty_item(it):
            icon = "🟢" if it["sign"] == "+" else "🔴"
            return f"{icon} {it['sign']} {it['amount_raw']} {it['token']}"

        item_str = " · ".join(_pretty_item(it) for it in items if not _is_micro_sol_item(it))

        grouped[title].append({
            "dt": ts,
            "item_str": item_str,
            "sender": sender_val,
            "to": to_val
        })

    return grouped


# ===== 汇总：交易（仅买入） =====
def summarize_for_group_with_hours(key: str, hours: int) -> str:
    records = load_forward_records()
    now = datetime.datetime.now()
    cutoff = now - datetime.timedelta(hours=hours)

    def _time_filter(ts: datetime.datetime) -> bool:
        return ts >= cutoff

    token_buys, addr_map = _collect_group_buys(key, records, _time_filter)

    ts_str = now.strftime('%Y-%m-%d %H:%M:%S')
    grp = key.upper() + "组"
    if not token_buys:
        return f"[{ts_str}] ⚠️ 过去 {hours} 小时未发现 {grp} 买入 CA。"

    separator = "💎" * 12
    blocks = []

    def last_time(tk):
        return sorted(token_buys[tk], key=lambda b: b["ts"])[-1]["ts"]

    # 最后一次买入时间越早，排越前
    for token in sorted(token_buys.keys(), key=lambda x: last_time(x)):
        buys = sorted(token_buys[token], key=lambda b: b["ts"])
        first = buys[0]
        lastb = buys[-1]
        first_time_txt = first['ts'].strftime('%H:%M')
        last_time_txt = lastb['ts'].strftime('%H:%M')
        first_sender_txt = first['sender']
        last_sender_txt = lastb['sender']
        first_mcap_txt = first.get('mcap')
        last_mcap_txt = lastb.get('mcap')

        token_lines = [f"📈 {token} 📈\n"]
        if len(buys) == 1:
            line_first = f"▶️ {first_sender_txt} / ⏰ {first_time_txt}"
            if first_mcap_txt:
                line_first += f" / 💵 {first_mcap_txt}"
            token_lines.append(line_first)
        else:
            line_first = f"▶️ {first_sender_txt} / ⏰ {first_time_txt}"
            if first_mcap_txt:
                line_first += f" / 💵 {first_mcap_txt}"
            token_lines.append(line_first)

            line_last = f"⏸️ {last_sender_txt} / ⏰ {last_time_txt}"
            if last_mcap_txt:
                line_last += f" / 💵 {last_mcap_txt}"
            token_lines.append(line_last)

        if first.get("sol_addr") or (SHOW_EVM_ADDR_IN_SUMMARY and first.get("evm_addr")):
            token_lines.append("")
            if first.get("sol_addr"):
                token_lines.append(f"📮 SOL: {first['sol_addr']}")
            if SHOW_EVM_ADDR_IN_SUMMARY and first.get("evm_addr"):
                token_lines.append(f"📮 EVM: {first['evm_addr']}")

        if addr_map.get(token):
            senders_flat = []
            for a, sset in addr_map[token].items():
                for s in sorted(sset):
                    senders_flat.append(f"🤑 {s}")
            token_lines.append("")
            token_lines.append("🤑 包含地址：" + " / ".join(senders_flat))
        else:
            token_lines.append("")
            token_lines.append("🤑 包含地址： 无")

        blocks.append("\n".join(token_lines))

    combined = ("\n\n" + separator + "\n\n").join(blocks)
    header = f"[{ts_str}] 📑 {grp} 过去 {hours} 小时买入 CA：\n\n"
    return header + combined


def summarize_for_group_in_range(key: str, start_hour: int, end_hour: int) -> str:
    """当日 H1–H2 买入汇总，例如 当日 07:00–11:00 买入 CA："""
    records = load_forward_records()
    now = datetime.datetime.now()
    today = now.date()

    def _time_filter(ts: datetime.datetime) -> bool:
        return ts.date() == today and start_hour <= ts.hour < end_hour

    token_buys, addr_map = _collect_group_buys(key, records, _time_filter)

    ts_str = now.strftime('%Y-%m-%d %H:%M:%S')
    grp = key.upper() + "组"
    label_range = f"当日 {start_hour:02d}:00–{end_hour:02d}:00 "

    if not token_buys:
        return f"[{ts_str}] ⚠️ {grp} {label_range}未发现 买入 CA。"

    separator = "💎" * 12
    blocks = []

    def last_time(tk):
        return sorted(token_buys[tk], key=lambda b: b["ts"])[-1]["ts"]

    for token in sorted(token_buys.keys(), key=lambda x: last_time(x)):
        buys = sorted(token_buys[token], key=lambda b: b["ts"])
        first = buys[0]
        lastb = buys[-1]
        first_time_txt = first['ts'].strftime('%H:%M')
        last_time_txt = lastb['ts'].strftime('%H:%M')
        first_sender_txt = first['sender']
        last_sender_txt = lastb['sender']
        first_mcap_txt = first.get('mcap')
        last_mcap_txt = lastb.get('mcap')

        token_lines = [f"📈 {token} 📈\n"]
        if len(buys) == 1:
            line_first = f"▶️ {first_sender_txt} / ⏰ {first_time_txt}"
            if first_mcap_txt:
                line_first += f" / 💵 {first_mcap_txt}"
            token_lines.append(line_first)
        else:
            line_first = f"▶️ {first_sender_txt} / ⏰ {first_time_txt}"
            if first_mcap_txt:
                line_first += f" / 💵 {first_mcap_txt}"
            token_lines.append(line_first)

            line_last = f"⏸️ {last_sender_txt} / ⏰ {last_time_txt}"
            if last_mcap_txt:
                line_last += f" / 💵 {last_mcap_txt}"
            token_lines.append(line_last)

        if first.get("sol_addr") or (SHOW_EVM_ADDR_IN_SUMMARY and first.get("evm_addr")):
            token_lines.append("")
            if first.get("sol_addr"):
                token_lines.append(f"📮 SOL: {first['sol_addr']}")
            if SHOW_EVM_ADDR_IN_SUMMARY and first.get("evm_addr"):
                token_lines.append(f"📮 EVM: {first['evm_addr']}")

        if addr_map.get(token):
            senders_flat = []
            for a, sset in addr_map[token].items():
                for s in sorted(sset):
                    senders_flat.append(f"🤑 {s}")
            token_lines.append("")
            token_lines.append("🤑 包含地址：" + " / ".join(senders_flat))
        else:
            token_lines.append("")
            token_lines.append("🤑 包含地址： 无")

        blocks.append("\n".join(token_lines))

    combined = ("\n\n" + separator + "\n\n").join(blocks)
    header = f"[{ts_str}] 📑 {grp} {label_range}买入 CA：\n\n"
    return header + combined


def summarize_for_key(key: str) -> str:
    """按 key 粗略罗列最近 8 小时 CA（仅买入），地址从 raw_text 提取。"""
    clean_cache()
    cutoff = datetime.datetime.now() - datetime.timedelta(hours=8)
    addrs = []

    for r in load_forward_records():
        try:
            ts_raw = datetime.datetime.strptime(r["ts"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        raw = r.get("raw_text") or ""
        base = raw or (r.get("text") or "")
        if not base:
            continue
        ts = _parse_dt_from_raw(raw, ts_raw)
        if ts < cutoff:
            continue
        if not re.search(fr"\b{key}\b", base, re.IGNORECASE):
            continue
        if not _is_buy_trade_post(base):
            continue
        sol, evm = extract_addresses_all(raw or base)
        addrs.extend(sol)
        if SHOW_EVM_ADDR_IN_SUMMARY:
            addrs.extend(evm)

    seen, ordered = set(), []
    for a in addrs:
        if a not in seen:
            ordered.append(a)
            seen.add(a)

    ts_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    lbl = key.upper()
    if not ordered:
        return f"[{ts_str}] ⚠️ 过去 8 小时未发现 {lbl} 买入 CA。"
    return f"[{ts_str}] 📑 {lbl} 过去 8 小时买入 CA：\n\n" + "\n\n".join(ordered)


# ===== 转账汇总（模板②） =====
def summarize_transfers_for_group_with_hours(key: str, hours: int) -> str:
    records = load_forward_records()
    now = datetime.datetime.now()
    cutoff = now - datetime.timedelta(hours=hours)

    def _time_filter(ts: datetime.datetime) -> bool:
        return ts >= cutoff

    grouped = _collect_group_transfers(key, records, _time_filter)

    ts_str = now.strftime('%Y-%m-%d %H:%M:%S')
    grp = key.upper() + "组"
    header = f"[{ts_str}] 📑 {grp} 过去 {hours} 小时转账汇总（已剔除微额 SOL 扣款 & 路由地址）\n\n"
    if not grouped:
        return header + "（无记录）"

    out_blocks = []
    sep = "🏦" * 12
    for title in sorted(grouped.keys(), key=lambda s: s):
        rows = sorted(grouped[title], key=lambda x: x["dt"])
        if not rows:
            continue
        lines = [sep, "", title]
        for idx, row in enumerate(rows, 1):
            t = row["dt"].strftime("%H:%M")
            segs = [f"{idx}/⏰ {t} | {row['item_str']}"]
            if row["sender"]:
                segs.append(f"Sender: {row['sender']}")
            if row["to"]:
                segs.append(f"To: {row['to']}")
            merged = " | ".join(segs)
            lines.append(merged)
        out_blocks.append("\n".join(lines))

    return header + "\n\n".join(out_blocks)


def summarize_transfers_for_group_in_range(key: str, start_hour: int, end_hour: int) -> str:
    """当日 H1–H2 转账汇总（模板②）。"""
    records = load_forward_records()
    now = datetime.datetime.now()
    today = now.date()

    def _time_filter(ts: datetime.datetime) -> bool:
        return ts.date() == today and start_hour <= ts.hour < end_hour

    grouped = _collect_group_transfers(key, records, _time_filter)

    ts_str = now.strftime('%Y-%m-%d %H:%M:%S')
    grp = key.upper() + "组"
    header = f"[{ts_str}] 📑 {grp} 当日 {start_hour:02d}:00–{end_hour:02d}:00 转账汇总（已剔除微额 SOL 扣款 & 路由地址）\n\n"
    if not grouped:
        return header + "（无记录）"

    out_blocks = []
    sep = "🏦" * 12
    for title in sorted(grouped.keys(), key=lambda s: s):
        rows = sorted(grouped[title], key=lambda x: x["dt"])
        if not rows:
            continue
        lines = [sep, "", title]
        for idx, row in enumerate(rows, 1):
            t = row["dt"].strftime("%H:%M")
            segs = [f"{idx}/⏰ {t} | {row['item_str']}"]
            if row["sender"]:
                segs.append(f"Sender: {row['sender']}")
            if row["to"]:
                segs.append(f"To: {row['to']}")
            merged = " | ".join(segs)
            lines.append(merged)
        out_blocks.append("\n".join(lines))

    return header + "\n\n".join(out_blocks)


# =================== 新增：基于明确时间区间的汇总函数 ===================
def summarize_for_group_between(key: str, start_dt: datetime.datetime, end_dt: datetime.datetime) -> str:
    """在明确的 start_dt <= ts < end_dt 区间内汇总买入（只统计买入）。"""
    records = load_forward_records()

    def _time_filter(ts: datetime.datetime) -> bool:
        return (ts >= start_dt) and (ts < end_dt)

    token_buys, addr_map = _collect_group_buys(key, records, _time_filter)

    ts_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    grp = key.upper() + "组"
    hours_span = f"{start_dt.strftime('%Y-%m-%d %H:%M')}–{(end_dt - datetime.timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M')}"
    if not token_buys:
        return f"[{ts_str}] ⚠️ {grp} {hours_span} 未发现 买入 CA。"

    separator = "💎" * 12
    blocks = []

    def last_time(tk):
        return sorted(token_buys[tk], key=lambda b: b["ts"])[-1]["ts"]

    for token in sorted(token_buys.keys(), key=lambda x: last_time(x)):
        buys = sorted(token_buys[token], key=lambda b: b["ts"])
        first = buys[0]
        lastb = buys[-1]
        first_time_txt = first['ts'].strftime('%H:%M')
        last_time_txt = lastb['ts'].strftime('%H:%M')
        first_sender_txt = first['sender']
        last_sender_txt = lastb['sender']
        first_mcap_txt = first.get('mcap')
        last_mcap_txt = lastb.get('mcap')

        token_lines = [f"📈 {token} 📈\n"]
        if len(buys) == 1:
            line_first = f"▶️ {first_sender_txt} / ⏰ {first_time_txt}"
            if first_mcap_txt:
                line_first += f" / 💵 {first_mcap_txt}"
            token_lines.append(line_first)
        else:
            line_first = f"▶️ {first_sender_txt} / ⏰ {first_time_txt}"
            if first_mcap_txt:
                line_first += f" / 💵 {first_mcap_txt}"
            token_lines.append(line_first)

            line_last = f"⏸️ {last_sender_txt} / ⏰ {last_time_txt}"
            if last_mcap_txt:
                line_last += f" / 💵 {last_mcap_txt}"
            token_lines.append(line_last)

        if first.get("sol_addr") or (SHOW_EVM_ADDR_IN_SUMMARY and first.get("evm_addr")):
            token_lines.append("")
            if first.get("sol_addr"):
                token_lines.append(f"📮 SOL: {first['sol_addr']}")
            if SHOW_EVM_ADDR_IN_SUMMARY and first.get("evm_addr"):
                token_lines.append(f"📮 EVM: {first['evm_addr']}")

        if addr_map.get(token):
            senders_flat = []
            for a, sset in addr_map[token].items():
                for s in sorted(sset):
                    senders_flat.append(f"🤑 {s}")
            token_lines.append("")
            token_lines.append("🤑 包含地址：" + " / ".join(senders_flat))
        else:
            token_lines.append("")
            token_lines.append("🤑 包含地址： 无")

        blocks.append("\n".join(token_lines))

    combined = ("\n\n" + separator + "\n\n").join(blocks)
    header = f"[{ts_str}] 📑 {grp} {hours_span} 买入 CA：\n\n"
    return header + combined


def summarize_transfers_for_group_between(key: str, start_dt: datetime.datetime, end_dt: datetime.datetime) -> str:
    """在明确时间窗口内汇总转账（剔除微额 SOL & 路由）。"""
    records = load_forward_records()

    def _time_filter(ts: datetime.datetime) -> bool:
        return (ts >= start_dt) and (ts < end_dt)

    grouped = _collect_group_transfers(key, records, _time_filter)

    ts_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    grp = key.upper() + "组"
    header = f"[{ts_str}] 📑 {grp} {start_dt.strftime('%Y-%m-%d %H:%M')}–{(end_dt - datetime.timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M')} 转账汇总（已剔除微额 SOL 扣款 & 路由地址）\n\n"
    if not grouped:
        return header + "（无记录）"

    out_blocks = []
    sep = "🏦" * 12
    for title in sorted(grouped.keys(), key=lambda s: s):
        rows = sorted(grouped[title], key=lambda x: x["dt"])
        if not rows:
            continue
        lines = [sep, "", title]
        for idx, row in enumerate(rows, 1):
            t = row["dt"].strftime("%H:%M")
            segs = [f"{idx}/⏰ {t} | {row['item_str']}"]
            if row["sender"]:
                segs.append(f"Sender: {row['sender']}")
            if row["to"]:
                segs.append(f"To: {row['to']}")
            merged = " | ".join(segs)
            lines.append(merged)
        out_blocks.append("\n".join(lines))

    return header + "\n\n".join(out_blocks)
# ======================================================================


# ===== Telegram 转发线程 =====
class TelegramForwarder(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.offset = None
        self.missed = []  # (display_text, raw_full)
        self.prev_pause = is_pause_period()
        self.allowed_chats = set(CONFIG.get("source_channel_ids") or [])

    def run(self):
        global FORWARD_ENABLED, LAST_TRADE_RANGE, LAST_TRANSFER_RANGE

        logging.info("启动 Telegram 转发线程")
        while True:
            try:
                params = {"timeout": 30}
                if self.offset is not None:
                    params["offset"] = self.offset
                resp = requests.get(f"{TG_API_URL}/getUpdates", params=params, timeout=40)
                curr_pause = is_pause_period()

                # 刚结束暂停，补发缓冲（不再重复写 JSON）
                if self.prev_pause and not curr_pause and self.missed:
                    for display_text, raw_full in self.missed:
                        if FORWARD_ENABLED:
                            send_to_wechat(display_text)
                        base_for_flag = raw_full or display_text
                        if _is_buy_trade_post(base_for_flag):
                            try:
                                CACHE.append((extract_timestamp(base_for_flag), display_text))
                            except Exception:
                                CACHE.append((datetime.datetime.now(), display_text))
                    self.missed.clear()
                self.prev_pause = curr_pause

                for upd in resp.json().get("result", []):
                    self.offset = upd["update_id"] + 1
                    post = upd.get("channel_post") or upd.get("message") or {}
                    text = (post.get("text") or post.get("caption") or "").strip()
                    if not text:
                        continue

                    display_text = text
                    raw_full = reconstruct_with_entities(post)

                    chat_id = None
                    try:
                        chat_id = post.get("chat", {}).get("id")
                    except Exception:
                        pass
                    is_command = display_text.startswith("/")

                    # 白名单过滤（命令不过滤）
                    if (not is_command) and self.allowed_chats:
                        if chat_id not in self.allowed_chats:
                            continue

                    # ===== 开关指令：/启动 /关闭 =====
                    if display_text == "/启动":
                        if not FORWARD_ENABLED:
                            FORWARD_ENABLED = True
                            msg = "✅ 已开启 TG→微信实时转发。"
                        else:
                            msg = "ℹ️ 实时转发已处于开启状态。"
                        send_to_telegram(msg)
                        send_to_wechat(msg)
                        continue

                    if display_text == "/关闭":
                        if FORWARD_ENABLED:
                            FORWARD_ENABLED = False
                            msg = "⏹ 已关闭 TG→微信实时转发（仍记录 JSON & 响应指令）。"
                        else:
                            msg = "ℹ️ 实时转发本就处于关闭状态。"
                        send_to_telegram(msg)
                        send_to_wechat(msg)
                        continue

                    # ===== 时间段 & 24h 指令（优先处理） =====
                    m_range = RANGE_CMD.match(display_text)
                    if m_range:
                        cmd_type = m_range.group(1)
                        h1 = int(m_range.group(2))
                        h2 = int(m_range.group(3))
                        if not (0 <= h1 < h2 <= 24):
                            msg = "⚠️ 时间段格式错误，应为 0-24 且前小后大。"
                            send_to_telegram(msg)
                            send_to_wechat(msg)
                        else:
                            # 记录最近一次时间段（并持久化）
                            if cmd_type == "交易":
                                LAST_TRADE_RANGE = (h1, h2)
                            elif cmd_type == "转账":
                                LAST_TRANSFER_RANGE = (h1, h2)
                            else:  # 汇总
                                LAST_TRADE_RANGE = (h1, h2)
                                LAST_TRANSFER_RANGE = (h1, h2)
                            save_last_ranges()  # <- 新增：保存到文件
                            logging.info(f"已设置最近时间段：LAST_TRADE_RANGE={LAST_TRADE_RANGE}, LAST_TRANSFER_RANGE={LAST_TRANSFER_RANGE}")

                            for key in ['jak', 'bz', 'house', 'tst', 'bsc']:
                                if cmd_type == "交易":
                                    s = summarize_for_group_in_range(key, h1, h2)
                                    send_to_telegram(s)
                                    send_to_wechat(s)
                                elif cmd_type == "转账":
                                    s = summarize_transfers_for_group_in_range(key, h1, h2)
                                    send_to_telegram(s)
                                    send_to_wechat(s)
                                else:
                                    s1 = summarize_for_group_in_range(key, h1, h2)
                                    send_to_telegram(s1)
                                    send_to_wechat(s1)
                                    s2 = summarize_transfers_for_group_in_range(key, h1, h2)
                                    send_to_telegram(s2)
                                    send_to_wechat(s2)
                        continue

                    m24 = LAST24_CMD.match(display_text)
                    if m24:
                        cmd_type = m24.group(1)
                        for key in ['jak', 'bz', 'house', 'tst', 'bsc']:
                            if cmd_type == "交易":
                                s = summarize_for_group_with_hours(key, 24)
                                send_to_telegram(s)
                                send_to_wechat(s)
                            elif cmd_type == "转账":
                                s = summarize_transfers_for_group_with_hours(key, 24)
                                send_to_telegram(s)
                                send_to_wechat(s)
                            else:
                                s1 = summarize_for_group_with_hours(key, 24)
                                send_to_telegram(s1)
                                send_to_wechat(s1)
                                s2 = summarize_transfers_for_group_with_hours(key, 24)
                                send_to_telegram(s2)
                                send_to_wechat(s2)
                        continue

                    # 旧命令：固定 8 小时窗口
                    if display_text in ("/交易", "/汇总"):
                        for key in ['jak', 'bz', 'house', 'tst', 'bsc']:
                            s = summarize_for_group_with_hours(key, 8)
                            send_to_telegram(s)
                            send_to_wechat(s)
                        continue
                    if display_text == "/转账":
                        for key in ['jak', 'bz', 'house', 'tst', 'bsc']:
                            s = summarize_transfers_for_group_with_hours(key, 8)
                            send_to_telegram(s)
                            send_to_wechat(s)
                        continue
                    if display_text == "/刷新地址":
                        load_address_book(force=True)
                        msg = "✅ 地址库已重载。"
                        send_to_telegram(msg)
                        send_to_wechat(msg)
                        continue

                    m1 = GROUP_CMD.match(display_text)
                    if m1:
                        k = m1.group(1).lower()
                        s = summarize_for_group_with_hours(k, 8)
                        send_to_telegram(s)
                        send_to_wechat(s)
                        continue

                    m2 = KEY_CMD.match(display_text)
                    if m2:
                        k = m2.group(1).lower()
                        s = summarize_for_key(k)
                        send_to_telegram(s)
                        send_to_wechat(s)
                        continue

                    # ===== 正常消息：先做地址替换，再写 JSON，再根据开关决定是否发微信 =====
                    text_after_replace = replace_sender_to_with_names(display_text, raw_full or display_text)

                    if curr_pause:
                        logging.info(f"暂停期间接收，加入缓冲: {text_after_replace[:80]}...")
                        self.missed.append((text_after_replace, raw_full))
                        save_forward_record(text_after_replace, "pending", raw_text=raw_full)
                    else:
                        save_forward_record(text_after_replace, "forwarded", raw_text=raw_full)
                        if FORWARD_ENABLED:
                            send_to_wechat(text_after_replace)

                    base_for_flag = raw_full or display_text
                    if _is_buy_trade_post(base_for_flag):
                        try:
                            CACHE.append((extract_timestamp(base_for_flag), text_after_replace))
                        except Exception:
                            CACHE.append((datetime.datetime.now(), text_after_replace))

            except Exception as e:
                logging.error(f"Telegram 转发异常: {e}")
                time.sleep(5)


# ===== 微信连接 / 拉取消息 =====
def connect_wechat():
    """尝试连接微信窗口，失败则返回 None，不退出进程。"""
    global WECHAT_WIN, WECHAT_AVAILABLE
    try:
        app = Application(backend="uia").connect(process=CONFIG['wechat_pid'])
        win = app.window(title=CONFIG['window_title'])
        win.set_focus()
        time.sleep(1)
        WECHAT_WIN = win
        WECHAT_AVAILABLE = True
        logging.info("微信窗口连接成功。")
        return win
    except Exception as e:
        warning = f"⚠️ 检测不到微信窗口（PID={CONFIG['wechat_pid']}，标题={CONFIG['window_title']}）"
        send_to_telegram(warning)
        logging.error(warning + f" 详细错误：{e}")
        WECHAT_WIN = None
        WECHAT_AVAILABLE = False
        return None


def get_all_wechat_messages(win):
    """获取当前会话窗口中的全部消息文本列表。"""
    try:
        lst = win.child_window(auto_id="chat_message_list", control_type="List")
        if not lst.exists(timeout=3):
            lst = win.child_window(title=CONFIG['message_list_name'], control_type=CONFIG['message_list_type'])
            lst.wait("visible", timeout=10)
        return [c.window_text().strip() for c in lst.children()]
    except Exception:
        return []


# ===== 定时汇总（02:50/10:30/17:30=8h；00:00=动态窗口） =====
def auto_trigger_group_summary():
    global LAST_TRADE_RANGE, LAST_TRANSFER_RANGE

    schedule_times = [
        datetime.time(hour=2, minute=50, second=0),
        datetime.time(hour=10, minute=30, second=0),
        datetime.time(hour=17, minute=30, second=0),
        datetime.time(hour=0, minute=0, second=0),
    ]
    groups = ['jak', 'bz', 'house', 'tst', 'bsc']

    while True:
        now = datetime.datetime.now()
        today = now.date()
        candidates = []
        for t in schedule_times:
            candidate = datetime.datetime.combine(today, t)
            if candidate <= now:
                candidate += datetime.timedelta(days=1)
            candidates.append(candidate)
        next_dt = min(candidates)
        sleep_seconds = (next_dt - now).total_seconds()
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        try:
            is_midnight = (next_dt.time() == datetime.time(hour=0, minute=0, second=0))

            if is_midnight:
                # === 00:00：按“最后一次时间段指令”计算窗口（改为用明确 datetime 窗口，避免相对 hours 导致的歧义） ===
                # next_dt 为即将到来的午夜（通常是明日 00:00），我们要汇总“刚刚结束的一日”
                yesterday = (next_dt - datetime.timedelta(days=1)).date()

                # 交易部分：若有 LAST_TRADE_RANGE=(h1,h2)，按 h2-1 开始；否则整天
                if LAST_TRADE_RANGE:
                    _, h2 = LAST_TRADE_RANGE
                    start_hour = max(h2 - 1, 0)
                else:
                    start_hour = 0  # 整天

                trade_start_dt = datetime.datetime.combine(yesterday, datetime.time(hour=start_hour, minute=0, second=0))
                trade_end_dt = datetime.datetime.combine(yesterday + datetime.timedelta(days=1), datetime.time(hour=0, minute=0, second=0))

                # 转账部分
                if LAST_TRANSFER_RANGE:
                    _, h2_t = LAST_TRANSFER_RANGE
                    start_hour_t = max(h2_t - 1, 0)
                else:
                    start_hour_t = 0

                transfer_start_dt = datetime.datetime.combine(yesterday, datetime.time(hour=start_hour_t, minute=0, second=0))
                transfer_end_dt = datetime.datetime.combine(yesterday + datetime.timedelta(days=1), datetime.time(hour=0, minute=0, second=0))

                logging.info(f"midnight summary windows -> trade: {trade_start_dt} - {trade_end_dt}, transfer: {transfer_start_dt} - {transfer_end_dt}")

                for key in groups:
                    s1 = summarize_for_group_between(key, trade_start_dt, trade_end_dt)
                    send_to_telegram(s1)
                    send_to_wechat(s1)
                    s2 = summarize_transfers_for_group_between(key, transfer_start_dt, transfer_end_dt)
                    send_to_telegram(s2)
                    send_to_wechat(s2)
            else:
                # 其余三个时间仍然是“过去 8 小时”
                for key in groups:
                    s1 = summarize_for_group_with_hours(key, 8)
                    send_to_telegram(s1)
                    send_to_wechat(s1)
                    s2 = summarize_transfers_for_group_with_hours(key, 8)
                    send_to_telegram(s2)
                    send_to_wechat(s2)

        except Exception as e:
            logging.error(f"定时汇总发送时出错: {e}")


# ===== 主程序 =====
def main():
    global WECHAT_WIN, WECHAT_AVAILABLE, FORWARD_ENABLED, LAST_TRADE_RANGE, LAST_TRANSFER_RANGE

    # 1) 地址库：启动时强制加载
    load_address_book(force=True)

    # 1.1) 载入上次保存的时间段（如果有）
    load_last_ranges()

    # 2) 从 JSON 恢复近 36 小时“买入”到 CACHE
    try:
        json_records = load_forward_records()
        cutoff = datetime.datetime.now() - datetime.timedelta(hours=36)
        restored = 0
        for r in json_records:
            try:
                ts = datetime.datetime.strptime(r["ts"], "%Y-%m-%d %H:%M:%S")
                raw = r.get("raw_text") or ""
                base = raw or (r.get("text") or "")
                if ts >= cutoff and _is_buy_trade_post(base):
                    CACHE.append((ts, base))
                    restored += 1
            except Exception:
                continue
        if restored:
            logging.info(f"从 JSON 恢复 {restored} 条近36小时的“买入”记录到 CACHE")
    except Exception as e:
        logging.error(f"启动时恢复 JSON 记录出错: {e}")

    # 3) 启动时尝试连接微信（仅一次）
    WECHAT_WIN = connect_wechat()
    start_msg = "启动转发脚本：仅买入汇总 + 转账模板② + 时间段/24h 指令；默认关闭 TG→微信实时转发，可用 /启动 开启。"
    send_to_telegram(start_msg)
    if WECHAT_AVAILABLE:
        send_to_wechat(start_msg)

    # 4) 启动线程：TG 转发 + 定时汇总
    forwarder = TelegramForwarder()
    forwarder.start()
    threading.Thread(target=auto_trigger_group_summary, daemon=True).start()

    # 5) 监听微信命令（微信掉线后不再重连，仅保持 TG 指令）
    last = None
    while True:
        if not WECHAT_AVAILABLE or WECHAT_WIN is None:
            time.sleep(5)
            continue

        try:
            msgs = get_all_wechat_messages(WECHAT_WIN)
        except Exception as e:
            warning = "⚠️ 微信已掉线（获取消息失败），脚本继续运行，将仅监听 Telegram 命令。"
            send_to_telegram(warning)
            logging.error(f"{warning} 详细：{e}")
            WECHAT_AVAILABLE = False
            WECHAT_WIN = None
            time.sleep(5)
            continue

        if msgs is None:
            warning = "⚠️ 微信已掉线（返回 None），脚本继续运行，将仅监听 Telegram 命令。"
            send_to_telegram(warning)
            logging.error(warning)
            WECHAT_AVAILABLE = False
            WECHAT_WIN = None
            time.sleep(5)
            continue

        if msgs and msgs[-1] != last:
            last = msgs[-1].strip()

            # 开关指令
            if last == "/启动":
                if not FORWARD_ENABLED:
                    FORWARD_ENABLED = True
                    msg = "✅ 已开启 TG→微信实时转发。"
                else:
                    msg = "ℹ️ 实时转发已处于开启状态。"
                send_to_telegram(msg)
                send_to_wechat(msg)
                time.sleep(2)
                continue

            if last == "/关闭":
                if FORWARD_ENABLED:
                    FORWARD_ENABLED = False
                    msg = "⏹ 已关闭 TG→微信实时转发（仍记录 JSON & 响应指令）。"
                else:
                    msg = "ℹ️ 实时转发本就处于关闭状态。"
                send_to_telegram(msg)
                send_to_wechat(msg)
                time.sleep(2)
                continue

            # 时间段命令
            m_range = RANGE_CMD.match(last)
            if m_range:
                cmd_type = m_range.group(1)
                h1 = int(m_range.group(2))
                h2 = int(m_range.group(3))
                if not (0 <= h1 < h2 <= 24):
                    msg = "⚠️ 时间段格式错误，应为 0-24 且前小后大。"
                    send_to_telegram(msg)
                    send_to_wechat(msg)
                else:
                    # 记录最近一次时间段（并持久化）
                    if cmd_type == "交易":
                        LAST_TRADE_RANGE = (h1, h2)
                    elif cmd_type == "转账":
                        LAST_TRANSFER_RANGE = (h1, h2)
                    else:
                        LAST_TRADE_RANGE = (h1, h2)
                        LAST_TRANSFER_RANGE = (h1, h2)
                    save_last_ranges()
                    logging.info(f"已设置最近时间段（微信命令）：LAST_TRADE_RANGE={LAST_TRADE_RANGE}, LAST_TRANSFER_RANGE={LAST_TRANSFER_RANGE}")

                    for key in ['jak', 'bz', 'house', 'tst', 'bsc']:
                        if cmd_type == "交易":
                            s = summarize_for_group_in_range(key, h1, h2)
                            send_to_telegram(s)
                            send_to_wechat(s)
                        elif cmd_type == "转账":
                            s = summarize_transfers_for_group_in_range(key, h1, h2)
                            send_to_telegram(s)
                            send_to_wechat(s)
                        else:
                            s1 = summarize_for_group_in_range(key, h1, h2)
                            send_to_telegram(s1)
                            send_to_wechat(s1)
                            s2 = summarize_transfers_for_group_in_range(key, h1, h2)
                            send_to_telegram(s2)
                            send_to_wechat(s2)
                time.sleep(2)
                continue

            # 24 小时命令
            m24 = LAST24_CMD.match(last)
            if m24:
                cmd_type = m24.group(1)
                for key in ['jak', 'bz', 'house', 'tst', 'bsc']:
                    if cmd_type == "交易":
                        s = summarize_for_group_with_hours(key, 24)
                        send_to_telegram(s)
                        send_to_wechat(s)
                    elif cmd_type == "转账":
                        s = summarize_transfers_for_group_with_hours(key, 24)
                        send_to_telegram(s)
                        send_to_wechat(s)
                    else:
                        s1 = summarize_for_group_with_hours(key, 24)
                        send_to_telegram(s1)
                        send_to_wechat(s1)
                        s2 = summarize_transfers_for_group_with_hours(key, 24)
                        send_to_telegram(s2)
                        send_to_wechat(s2)
                time.sleep(2)
                continue

            # 旧命令
            if last in ("/交易", "/汇总"):
                for key in ['jak', 'bz', 'house', 'tst', 'bsc']:
                    s = summarize_for_group_with_hours(key, 8)
                    send_to_telegram(s)
                    send_to_wechat(s)
            elif last == "/转账":
                for key in ['jak', 'bz', 'house', 'tst', 'bsc']:
                    s = summarize_transfers_for_group_with_hours(key, 8)
                    send_to_telegram(s)
                    send_to_wechat(s)
            elif last == "/刷新地址":
                load_address_book(force=True)
                msg = "✅ 地址库已重载。"
                send_to_telegram(msg)
                send_to_wechat(msg)
            elif GROUP_CMD.match(last):
                k = GROUP_CMD.match(last).group(1).lower()
                s = summarize_for_group_with_hours(k, 8)
                send_to_telegram(s)
                send_to_wechat(s)
            elif KEY_CMD.match(last):
                k = KEY_CMD.match(last).group(1).lower()
                s = summarize_for_key(k)
                send_to_telegram(s)
                send_to_wechat(s)

        time.sleep(2)


if __name__ == "__main__":
    main()
