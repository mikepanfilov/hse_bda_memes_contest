import os
import re
import sys
import asyncio
from typing import Iterable
from collections import defaultdict

import streamlit as st
from telethon import TelegramClient, functions, types
from dotenv import load_dotenv

# ---------- загрузка .env ----------
load_dotenv()

# ---------- константы ----------
PAGE = 100  # лимит телеги на страницу
SESSION_FILE = "tg_topic_stats_session"  # локальная сессия, чтобы не логиниться каждый раз

# ---------- утилиты ----------
def coerce_int(v, name):
    if v is None or v == "":
        raise RuntimeError(f"{name} не задан")
    try:
        return int(v)
    except Exception:
        raise RuntimeError(f"{name} должен быть числом")

def parse_topic_link(link: str):
    """
    Принимает:
      https://t.me/c/3015720678/1152/1153  или  https://t.me/publicname/1152/1153
      https://t.me/c/3015720678/1152
    Возвращает (peer_hint, top_message_id)
    """
    m = re.search(r"https?://t\.me/(?:c/)?([^/]+)/(\d+)(?:/(\d+))?", link.strip())
    if not m:
        raise ValueError("Не смог распарсить ссылку. Дай полный URL на пост в топике.")
    group_part, first_num, _ = m.group(1), m.group(2), m.group(3)
    peer_hint = int("-100" + group_part) if group_part.isdigit() else group_part
    top_message_id = int(first_num)  # именно ПЕРВОЕ число — стартовый пост темы
    return peer_hint, top_message_id

def _internal_c_id(peer_id: int) -> int:
    # -1001234567890 -> 1234567890
    raw = str(abs(peer_id))
    return int(raw[3:]) if raw.startswith("100") else int(raw)

async def build_message_link(client, peer, msg_id: int, top_msg_id: int | None = None) -> str:
    # 1) пробуем официальный экспорт (красиво и надёжно)
    try:
        resp = await client(functions.messages.ExportMessageLinkRequest(
            peer=peer, id=msg_id, grouped=False, thread=top_msg_id or 0
        ))
        if resp and getattr(resp, "link", None):
            return resp.link
    except Exception:
        pass
    # 2) собираем вручную
    username = getattr(peer, "username", None)
    if username:
        return f"https://t.me/{username}/{top_msg_id}/{msg_id}" if top_msg_id else f"https://t.me/{username}/{msg_id}"
    try:
        cid = _internal_c_id(peer.id)
        return f"https://t.me/c/{cid}/{top_msg_id}/{msg_id}" if top_msg_id else f"https://t.me/c/{cid}/{msg_id}"
    except Exception:
        return f"(msg id: {msg_id})"

def build_user_link(user) -> str:
    return f"https://t.me/{user.username}" if getattr(user, "username", None) else f"tg://user?id={user.id}"

async def fetch_topic_messages_via_replies(client, peer, top_message_id: int) -> list[types.Message]:
    """
    Тянем ВСЕ сообщения темы через messages.GetReplies по ID стартового сообщения.
    """
    all_msgs: list[types.Message] = []
    offset_id = 0
    while True:
        resp = await client(functions.messages.GetRepliesRequest(
            peer=peer, msg_id=top_message_id, offset_id=offset_id,
            offset_date=None, add_offset=0, limit=PAGE, max_id=0, min_id=0, hash=0
        ))
        msgs = [m for m in resp.messages if isinstance(m, types.Message)]
        # убираем стартовый пост темы
        msgs = [m for m in msgs if m.id != top_message_id]
        if not msgs:
            break
        all_msgs.extend(msgs)
        offset_id = msgs[-1].id
        if len(msgs) < PAGE:
            break
    return all_msgs

async def count_reactions_in_message(msg: types.Message, like_emojis: Iterable[str] | None) -> int:
    """
    Если like_emojis пустые/None — считаем любые реакции (включая кастомные).
    Иначе — только заданные эмодзи.
    """
    if not getattr(msg, "reactions", None):
        return 0
    total = 0
    for r in msg.reactions.results:
        if isinstance(r.reaction, types.ReactionEmoji):
            if not like_emojis or r.reaction.emoticon in like_emojis:
                total += r.count
        else:
            # кастомные — только если нет фильтра
            if not like_emojis:
                total += r.count
    return total

async def iter_reactors_for_message(client, peer, message_id: int, like_emojis: Iterable[str] | None):
    """
    Итератор по юзерам, которые поставили подходящие реакции.
    offset — строка из resp.next_offset.
    """
    offset = ""
    while True:
        resp = await client(functions.messages.GetMessageReactionsListRequest(
            peer=peer, id=message_id, reaction=None, offset=offset, limit=PAGE
        ))
        if not resp.reactions:
            break
        users_by_id = {u.id: u for u in resp.users}

        for item in resp.reactions:
            ok = False
            if isinstance(item.reaction, types.ReactionEmoji):
                ok = (not like_emojis) or (item.reaction.emoticon in like_emojis)
            else:
                ok = not like_emojis  # кастом считаем только при отсутствии фильтра

            if ok:
                uid = getattr(item.peer_id, "user_id", None)
                if uid in users_by_id:
                    yield users_by_id[uid]

        if not resp.next_offset:
            break
        offset = resp.next_offset

async def analyze_topic(topic_link: str, top_n: int, like_emojis: set[str] | None,
                        api_id: int, api_hash: str, phone: str | None):
    # Windows loop fix
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    client = TelegramClient(SESSION_FILE, api_id, api_hash)
    await client.start(phone=phone)

    peer_hint, top_msg_id = parse_topic_link(topic_link)
    peer = await client.get_entity(peer_hint)

    # sanity
    top_msg = await client.get_messages(peer, ids=top_msg_id)
    if not top_msg:
        await client.disconnect()
        return {"error": "Не вижу стартовое сообщение темы. Проверь ссылку/историю."}

    msgs = await fetch_topic_messages_via_replies(client, peer, top_msg_id)

    likes_by_msg: dict[int, int] = {}
    for m in msgs:
        likes_by_msg[m.id] = await count_reactions_in_message(m, like_emojis)

    # сортировка и топ
    top_msgs = sorted(msgs, key=lambda m: (-likes_by_msg.get(m.id, 0), -m.id))[:top_n]

    # строим ссылки и превью
    top_rows = []
    rank = 1
    for m in top_msgs:
        link = await build_message_link(client, peer, m.id, top_msg_id)
        preview = (m.message or "").strip().replace("\n", " ")
        if len(preview) > 140:
            preview = preview[:137] + "..."
        top_rows.append({
            "rank": rank,
            "reactions": likes_by_msg[m.id],
            "link": link,
            "text": preview
        })
        rank += 1

    # топ-реактор
    liker_counter = defaultdict(int)
    liker_name = {}

    for m in msgs:
        if getattr(m, "reactions", None):
            async for user in iter_reactors_for_message(client, peer, m.id, like_emojis):
                liker_counter[user.id] += 1
                if user.id not in liker_name:
                    name = (user.first_name or "") + (" " + user.last_name if user.last_name else "")
                    name = name.strip() or ("@" + user.username if user.username else f"user_{user.id}")
                    liker_name[user.id] = name

    top_liker = None
    if liker_counter:
        top_liker_id, top_likes = max(liker_counter.items(), key=lambda kv: kv[1])
        top_user = await client.get_entity(top_liker_id)
        top_liker = {
            "name": liker_name.get(top_liker_id, top_liker_id),
            "profile": build_user_link(top_user),
            "count": top_likes
        }

    await client.disconnect()
    return {"top": top_rows, "top_liker": top_liker, "total_msgs": len(msgs)}

# ---------- UI ----------
st.set_page_config(page_title="Telegram Topic Stats", page_icon="🔥", layout="centered")

st.title("Telegram Topic Stats")
st.caption("Да, считаем мемы и лайки. Без спама, без банов, только хардкор.")

with st.sidebar:
    st.subheader("Auth")
    api_id = st.text_input("TG_API_ID", value=os.getenv("TG_API_ID", ""), type="password")
    api_hash = st.text_input("TG_API_HASH", value=os.getenv("TG_API_HASH", ""), type="password")
    phone = st.text_input("TG_PHONE (для первого логина)", value=os.getenv("TG_PHONE", ""))

st.write("Введи ссылку на сообщение в топике, например: `https://t.me/c/3015720678/1152/1153`")
topic_link = st.text_input("Topic link", value="", placeholder="https://t.me/c/<internal>/top_msg_id/<msg_id>")

col1, col2 = st.columns(2)
with col1:
    top_n = st.number_input("Top-N мемов", min_value=1, max_value=50, value=3, step=1)
with col2:
    emojis_raw = st.text_input("Фильтр эмодзи (через запятую). Оставь пустым, чтобы считать все реакции.", value="")

if st.button("Посчитать", type="primary"):
    try:
        api_id_int = coerce_int(api_id, "TG_API_ID")
        like_emojis = None
        if emojis_raw.strip():
            like_emojis = {e.strip() for e in emojis_raw.split(",") if e.strip()}

        with st.spinner("Считаю... не дергай Telegram, он нервный."):
            result = asyncio.run(analyze_topic(
                topic_link=topic_link,
                top_n=int(top_n),
                like_emojis=like_emojis,
                api_id=api_id_int,
                api_hash=api_hash.strip(),
                phone=(phone.strip() or None),
            ))

        if "error" in result:
            st.error(result["error"])
        else:
            st.success(f"Сообщений в теме: {result['total_msgs']}")

            top = result["top"]
            if not top:
                st.warning("Топ пуст. Либо реакций нет, либо тема такая же живая, как проект после демо.")
            else:
                # выводим таблицу
                import pandas as pd
                df = pd.DataFrame(top)[["rank", "reactions", "link", "text"]]
                # кликабельные ссылки
                df["link"] = df["link"].apply(lambda x: f"[open]({x})")
                st.markdown("### Top мемы")
                st.dataframe(df, use_container_width=True)

            # топ-лайкер
            if result["top_liker"]:
                tl = result["top_liker"]
                st.markdown("### Самый активный лайкер")
                st.markdown(f"**{tl['name']}** — {tl['count']} реакций  •  [профиль]({tl['profile']})")
            else:
                st.info("Самого активного лайкера нет. Видимо, все заняты рефакторингом души.")

    except Exception as e:
        st.error(f"Ошибка: {e}")
        st.stop()

st.markdown("---")
st.caption("P.S. Ключи не палим, ToS не ломаем. Автоспам не включаем, даже если очень хочется.")

