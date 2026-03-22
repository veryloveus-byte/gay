import asyncio
import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from html import escape
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MessageEntity,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, TimedOut
from telegram.request import HTTPXRequest
from telegram.ext import (
    AIORateLimiter,
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram._utils.entities import parse_message_entities


load_dotenv()

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("autoposter_bot")

DB_PATH = os.getenv("DB_PATH", "bot_data.sqlite3")
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DEFAULT_TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "Europe/Moscow").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)
WIZARD_KEY = "wizard"


@dataclass
class PostTask:
    id: int
    owner_id: int
    target_chat_id: int
    target_chat_title: str
    topic_id: int | None
    source_chat_id: int | None
    source_message_id: int | None
    photo_file_id: str
    buttons_json: str
    message_text: str
    message_entities_json: str
    message_html: str
    post_time: str
    interval_minutes: int | None
    timezone: str
    is_active: bool
    created_at: str
    updated_at: str

    @property
    def buttons(self) -> list[dict[str, str]]:
        if not self.buttons_json:
            return []
        return json.loads(self.buttons_json)

    @property
    def message_entities(self) -> list[MessageEntity]:
        if not self.message_entities_json:
            return []
        return [
            MessageEntity.de_json(item, bot=None)
            for item in json.loads(self.message_entities_json)
        ]


class Database:
    def __init__(self, path: str):
        self.path = path
        self._init_db()

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS post_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_id INTEGER NOT NULL,
                    target_chat_id INTEGER NOT NULL,
                    target_chat_title TEXT NOT NULL,
                    topic_id INTEGER,
                    source_chat_id INTEGER,
                    source_message_id INTEGER,
                    photo_file_id TEXT NOT NULL DEFAULT '',
                    buttons_json TEXT NOT NULL DEFAULT '',
                    message_text TEXT NOT NULL,
                    message_entities_json TEXT NOT NULL DEFAULT '',
                    message_html TEXT NOT NULL DEFAULT '',
                    post_time TEXT NOT NULL,
                    interval_minutes INTEGER,
                    timezone TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(post_tasks)").fetchall()
            }
            if "topic_id" not in columns:
                conn.execute("ALTER TABLE post_tasks ADD COLUMN topic_id INTEGER")
            if "source_chat_id" not in columns:
                conn.execute("ALTER TABLE post_tasks ADD COLUMN source_chat_id INTEGER")
            if "source_message_id" not in columns:
                conn.execute("ALTER TABLE post_tasks ADD COLUMN source_message_id INTEGER")
            if "photo_file_id" not in columns:
                conn.execute(
                    "ALTER TABLE post_tasks ADD COLUMN photo_file_id TEXT NOT NULL DEFAULT ''"
                )
            if "buttons_json" not in columns:
                conn.execute(
                    "ALTER TABLE post_tasks ADD COLUMN buttons_json TEXT NOT NULL DEFAULT ''"
                )
            if "message_entities_json" not in columns:
                conn.execute(
                    "ALTER TABLE post_tasks ADD COLUMN message_entities_json TEXT NOT NULL DEFAULT ''"
                )
            if "message_html" not in columns:
                conn.execute(
                    "ALTER TABLE post_tasks ADD COLUMN message_html TEXT NOT NULL DEFAULT ''"
                )
            if "interval_minutes" not in columns:
                conn.execute("ALTER TABLE post_tasks ADD COLUMN interval_minutes INTEGER")
            conn.execute(
                "UPDATE post_tasks SET topic_id = NULL WHERE topic_id IS NOT NULL AND topic_id <= 0"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS allowed_users (
                    user_id INTEGER PRIMARY KEY,
                    label TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    added_by INTEGER NOT NULL
                )
                """
            )
            if OWNER_ID:
                timestamp = datetime.utcnow().isoformat()
                conn.execute(
                    """
                    INSERT OR IGNORE INTO allowed_users (user_id, label, created_at, added_by)
                    VALUES (?, ?, ?, ?)
                    """,
                    (OWNER_ID, "Owner", timestamp, OWNER_ID),
                )

    def create_task(
        self,
        owner_id: int,
        target_chat_id: int,
        target_chat_title: str,
        topic_id: int | None,
        source_chat_id: int | None,
        source_message_id: int | None,
        photo_file_id: str,
        buttons_json: str,
        message_text: str,
        message_entities_json: str,
        message_html: str,
        post_time: str,
        interval_minutes: int | None,
        timezone: str,
    ) -> int:
        timestamp = datetime.utcnow().isoformat()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO post_tasks (
                    owner_id, target_chat_id, target_chat_title, topic_id, source_chat_id, source_message_id, photo_file_id, buttons_json,
                    message_text, message_entities_json, message_html, post_time, interval_minutes, timezone, is_active, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    owner_id,
                    target_chat_id,
                    target_chat_title,
                    topic_id,
                    source_chat_id,
                    source_message_id,
                    photo_file_id,
                    buttons_json,
                    message_text,
                    message_entities_json,
                    message_html,
                    post_time,
                    interval_minutes,
                    timezone,
                    timestamp,
                    timestamp,
                ),
            )
            return int(cursor.lastrowid)

    def list_tasks(self, owner_id: int | None = None) -> list[PostTask]:
        query = "SELECT * FROM post_tasks"
        params: tuple[int, ...] = ()
        if owner_id is not None:
            query += " WHERE owner_id = ?"
            params = (owner_id,)
        query += " ORDER BY is_active DESC, id DESC"
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_task(row) for row in rows]

    def get_task(self, task_id: int, owner_id: int | None = None) -> PostTask | None:
        query = "SELECT * FROM post_tasks WHERE id = ?"
        params: tuple[int, ...] | tuple[int, int]
        params = (task_id,)
        if owner_id is not None:
            query += " AND owner_id = ?"
            params = (task_id, owner_id)
        with self.connect() as conn:
            row = conn.execute(query, params).fetchone()
        return self._row_to_task(row) if row else None

    def update_task_message(
        self,
        task_id: int,
        owner_id: int | None,
        message_text: str,
        message_entities_json: str,
        message_html: str,
        source_chat_id: int | None,
        source_message_id: int | None,
    ) -> bool:
        timestamp = datetime.utcnow().isoformat()
        query = """
            UPDATE post_tasks
            SET message_text = ?, message_entities_json = ?, message_html = ?, source_chat_id = ?, source_message_id = ?, updated_at = ?
            WHERE id = ?
        """
        params: tuple[str, str, str, int | None, int | None, str, int] | tuple[str, str, str, int | None, int | None, str, int, int] = (
            message_text,
            message_entities_json,
            message_html,
            source_chat_id,
            source_message_id,
            timestamp,
            task_id,
        )
        if owner_id is not None:
            query += " AND owner_id = ?"
            params = (
                message_text,
                message_entities_json,
                message_html,
                source_chat_id,
                source_message_id,
                timestamp,
                task_id,
                owner_id,
            )
        with self.connect() as conn:
            cursor = conn.execute(query, params)
            return cursor.rowcount > 0

    def update_task_full(
        self,
        task_id: int,
        owner_id: int | None,
        topic_id: int | None,
        photo_file_id: str,
        buttons_json: str,
        message_text: str,
        message_entities_json: str,
        message_html: str,
        source_chat_id: int | None,
        source_message_id: int | None,
        interval_minutes: int | None,
    ) -> bool:
        timestamp = datetime.utcnow().isoformat()
        query = """
            UPDATE post_tasks
            SET topic_id = ?, source_chat_id = ?, source_message_id = ?, photo_file_id = ?, buttons_json = ?, message_text = ?, message_entities_json = ?, message_html = ?, interval_minutes = ?, updated_at = ?
            WHERE id = ?
        """
        params: tuple[int | None, int | None, int | None, str, str, str, str, str, int | None, str, int] | tuple[
            int | None, int | None, int | None, str, str, str, str, str, int | None, str, int, int
        ] = (
            topic_id,
            source_chat_id,
            source_message_id,
            photo_file_id,
            buttons_json,
            message_text,
            message_entities_json,
            message_html,
            interval_minutes,
            timestamp,
            task_id,
        )
        if owner_id is not None:
            query += " AND owner_id = ?"
            params = (
                topic_id,
                source_chat_id,
                source_message_id,
                photo_file_id,
                buttons_json,
                message_text,
                message_entities_json,
                message_html,
                interval_minutes,
                timestamp,
                task_id,
                owner_id,
            )
        with self.connect() as conn:
            cursor = conn.execute(query, params)
            return cursor.rowcount > 0

    def set_task_active(self, task_id: int, owner_id: int | None, is_active: bool) -> bool:
        timestamp = datetime.utcnow().isoformat()
        query = """
            UPDATE post_tasks
            SET is_active = ?, updated_at = ?
            WHERE id = ?
        """
        params: tuple[int, str, int] | tuple[int, str, int, int] = (
            1 if is_active else 0,
            timestamp,
            task_id,
        )
        if owner_id is not None:
            query += " AND owner_id = ?"
            params = (1 if is_active else 0, timestamp, task_id, owner_id)
        with self.connect() as conn:
            cursor = conn.execute(query, params)
            return cursor.rowcount > 0

    def delete_task(self, task_id: int, owner_id: int | None) -> bool:
        query = "DELETE FROM post_tasks WHERE id = ?"
        params: tuple[int] | tuple[int, int] = (task_id,)
        if owner_id is not None:
            query += " AND owner_id = ?"
            params = (task_id, owner_id)
        with self.connect() as conn:
            cursor = conn.execute(query, params)
            return cursor.rowcount > 0

    def add_allowed_user(self, user_id: int, label: str, added_by: int) -> None:
        timestamp = datetime.utcnow().isoformat()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO allowed_users (
                    user_id, label, created_at, added_by
                ) VALUES (?, ?, ?, ?)
                """,
                (user_id, label, timestamp, added_by),
            )

    def is_allowed_user(self, user_id: int) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM allowed_users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
        return bool(row)

    def list_allowed_users(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT user_id, label, created_at, added_by
                FROM allowed_users
                ORDER BY user_id
                """,
            ).fetchall()

    def remove_allowed_user(self, user_id: int) -> bool:
        with self.connect() as conn:
            cursor = conn.execute(
                "DELETE FROM allowed_users WHERE user_id = ?",
                (user_id,),
            )
            return cursor.rowcount > 0

    @staticmethod
    def _row_to_task(row: sqlite3.Row) -> PostTask:
        return PostTask(
            id=row["id"],
            owner_id=row["owner_id"],
            target_chat_id=row["target_chat_id"],
            target_chat_title=row["target_chat_title"],
            topic_id=row["topic_id"],
            source_chat_id=row["source_chat_id"],
            source_message_id=row["source_message_id"],
            photo_file_id=row["photo_file_id"] or "",
            buttons_json=row["buttons_json"] or "",
            message_text=row["message_text"],
            message_entities_json=row["message_entities_json"] or "",
            message_html=row["message_html"] or "",
            post_time=row["post_time"],
            interval_minutes=row["interval_minutes"],
            timezone=row["timezone"],
            is_active=bool(row["is_active"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


db = Database(DB_PATH)


def is_allowed_user(user_id: int) -> bool:
    if OWNER_ID == 0:
        return True
    return db.is_allowed_user(user_id)


def require_owner(update: Update) -> bool:
    user = update.effective_user
    return bool(user and is_allowed_user(user.id))


def is_superadmin(user_id: int) -> bool:
    return is_allowed_user(user_id)


def parse_addpost_args(text: str) -> tuple[str, str, str]:
    parts = [part.strip() for part in text.split("|", 2)]
    if len(parts) != 3 or not all(parts):
        raise ValueError
    return parts[0], parts[1], parts[2]


def validate_time(value: str) -> str:
    datetime.strptime(value, "%H:%M")
    return value


def validate_timezone(value: str) -> str:
    ZoneInfo(value)
    return value


def parse_optional_int(value: str) -> int | None:
    cleaned = value.strip()
    if cleaned in {"", "-", "none", "None", "null"}:
        return None
    return int(cleaned)


def parse_optional_topic_id(value: str) -> int | None:
    topic_id = parse_optional_int(value)
    if topic_id is None:
        return None
    if topic_id <= 0:
        raise ValueError
    return topic_id


def normalize_text(value: str) -> str:
    return value.replace("\\n", "\n").strip()


def serialize_entities(entities: list[MessageEntity] | tuple[MessageEntity, ...] | None) -> str:
    if not entities:
        return ""
    return json.dumps([entity.to_dict() for entity in entities], ensure_ascii=False)


def extract_text_payload(message) -> tuple[str, str, str]:
    text = message.text or ""
    entities = list(message.entities or [])
    html = message.text_html_urled if text else ""
    if entities:
        return text, serialize_entities(entities), html
    return normalize_text(text), "", normalize_text(text)


def extract_source_payload(message) -> tuple[int | None, int | None]:
    if not message or not message.chat:
        return None, None
    return message.chat.id, message.message_id


def render_task_html(task: PostTask) -> str:
    if task.message_html:
        return task.message_html
    if task.message_text and task.message_entities:
        entities_map = parse_message_entities(task.message_text, task.message_entities)
        return Message._parse_html(task.message_text, entities_map, urled=True) or task.message_text
    return task.message_text


def build_photo_caption_kwargs(task: PostTask, rendered_html: str) -> dict:
    if task.message_entities:
        return {
            "caption": task.message_text,
            "caption_entities": task.message_entities,
        }
    if rendered_html:
        return {
            "caption": rendered_html,
            "parse_mode": ParseMode.HTML,
        }
    return {"caption": task.message_text}


def task_uses_custom_emoji(task: PostTask) -> bool:
    return any(getattr(entity, "type", "") == MessageEntity.CUSTOM_EMOJI for entity in task.message_entities)


async def maybe_warn_custom_emoji_limit(message, task: PostTask) -> None:
    if not task_uses_custom_emoji(task):
        return
    await safe_reply(
        message,
        "Важно: custom emoji сохранён, но Telegram отправляет premium custom emoji только от Premium-пользователя. "
        "Обычный bot token обычно заменяет его на обычный emoji. Если нужен именно premium-вид, "
        "нужна отправка с Premium-аккаунта, а не от бота.",
    )


def parse_interval(value: str) -> int:
    cleaned = value.strip().lower().replace(" ", "")
    if not cleaned:
        raise ValueError
    if cleaned.endswith("h"):
        hours = int(cleaned[:-1])
        if hours <= 0:
            raise ValueError
        return hours * 60
    if cleaned.endswith("m"):
        minutes = int(cleaned[:-1])
        if minutes <= 0:
            raise ValueError
        return minutes
    minutes = int(cleaned)
    if minutes <= 0:
        raise ValueError
    return minutes


def format_interval(interval_minutes: int | None, post_time: str) -> str:
    if not interval_minutes:
        return f"Каждый день в {post_time}"
    return f"Каждые {interval_minutes} мин."


def parse_buttons(value: str) -> str:
    cleaned = value.strip()
    if cleaned in {"", "-"}:
        return ""

    buttons: list[dict[str, str]] = []
    for item in cleaned.split(";"):
        chunk = item.strip()
        if not chunk:
            continue
        if "=" in chunk:
            text_part, url_part = chunk.split("=", 1)
        elif "," in chunk:
            text_part, url_part = chunk.split(",", 1)
        else:
            raise ValueError
        text_value = text_part.strip()
        url_value = url_part.strip()
        if not text_value or not url_value:
            raise ValueError
        if not url_value.startswith(("http://", "https://", "tg://")):
            raise ValueError
        buttons.append({"text": text_value, "url": url_value})

    return json.dumps(buttons, ensure_ascii=False) if buttons else ""


def build_post_keyboard(buttons_json: str) -> InlineKeyboardMarkup | None:
    if not buttons_json:
        return None
    buttons = json.loads(buttons_json)
    keyboard = [
        [InlineKeyboardButton(text=button["text"], url=button["url"])]
        for button in buttons
    ]
    return InlineKeyboardMarkup(keyboard)


def parse_addpost_extended_args(text: str) -> tuple[str, str, int | None, str, str]:
    parts = [part.strip() for part in text.split("|", 4)]
    if len(parts) != 5 or not parts[0] or not parts[1] or not parts[4]:
        raise ValueError
    return (
        parts[0],
        parts[1],
        parse_optional_topic_id(parts[2]),
        parse_buttons(parts[3]),
        normalize_text(parts[4]),
    )


def parse_editpost_full_args(text: str) -> tuple[int, str, int | None, str, str]:
    parts = [part.strip() for part in text.split("|", 4)]
    if len(parts) != 5 or not parts[0] or not parts[1] or not parts[4]:
        raise ValueError
    return (
        int(parts[0]),
        parts[1],
        parse_optional_topic_id(parts[2]),
        parse_buttons(parts[3]),
        normalize_text(parts[4]),
    )


def main_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Создать пост", callback_data="ui:create"),
                InlineKeyboardButton("Мои задачи", callback_data="ui:list"),
            ],
            [
                InlineKeyboardButton("Доступы", callback_data="ui:access"),
                InlineKeyboardButton("Как получить ID", callback_data="ui:ids"),
            ],
            [InlineKeyboardButton("Помощь", callback_data="ui:help")],
        ]
    )


def wizard_cancel_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Отмена", callback_data="ui:cancel")]]
    )


def wizard_optional_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Пропустить", callback_data="ui:skip"),
                InlineKeyboardButton("Отмена", callback_data="ui:cancel"),
            ]
        ]
    )


def wizard_buttons_choice_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Без кнопок", callback_data="ui:no_buttons"),
                InlineKeyboardButton("Добавить кнопку", callback_data="ui:add_buttons"),
            ],
            [InlineKeyboardButton("Отмена", callback_data="ui:cancel")],
        ]
    )


def wizard_photo_choice_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Без фото", callback_data="ui:no_photo"),
                InlineKeyboardButton("Добавить фото", callback_data="ui:add_photo"),
            ],
            [InlineKeyboardButton("Отмена", callback_data="ui:cancel")],
        ]
    )


def tasks_list_markup(tasks: list[PostTask]) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                f"#{task.id} [{'ON' if task.is_active else 'OFF'}] {task.target_chat_title[:18]}",
                callback_data=f"task:view:{task.id}",
            )
        ]
        for task in tasks[:20]
    ]
    rows.append([InlineKeyboardButton("Назад", callback_data="ui:home")])
    return InlineKeyboardMarkup(rows)


def task_actions_markup(task: PostTask) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Предпросмотр", callback_data=f"task:preview:{task.id}")],
            [InlineKeyboardButton("Отправить сейчас", callback_data=f"task:send_now:{task.id}")],
            [InlineKeyboardButton("Изменить текст", callback_data=f"task:edit_text:{task.id}")],
            [InlineKeyboardButton("Изменить все", callback_data=f"task:edit_full:{task.id}")],
            [
                InlineKeyboardButton("Поставить на паузу", callback_data=f"task:pause:{task.id}"),
                InlineKeyboardButton("Возобновить", callback_data=f"task:resume:{task.id}"),
            ],
            [InlineKeyboardButton("Удалить", callback_data=f"task:delete:{task.id}")],
            [InlineKeyboardButton("К списку", callback_data="ui:list")],
        ]
    )


def access_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Показать доступы", callback_data="access:list")],
            [InlineKeyboardButton("Добавить аккаунт", callback_data="access:add")],
            [InlineKeyboardButton("Удалить аккаунт", callback_data="access:remove")],
            [InlineKeyboardButton("Назад", callback_data="ui:home")],
        ]
    )


def format_task_card(task: PostTask) -> str:
    status = "Активна" if task.is_active else "На паузе"
    topic = str(task.topic_id) if task.topic_id is not None else "нет"
    buttons = "есть" if task.buttons_json else "нет"
    photo = "есть" if task.photo_file_id else "нет"
    formatting = "есть" if task.message_entities_json else "нет"
    preview = escape(task.message_text[:600])
    title = escape(task.target_chat_title)
    schedule = format_interval(task.interval_minutes, task.post_time)
    return (
        f"<b>Задача #{task.id}</b>\n"
        f"<b>Статус:</b> {status}\n"
        f"<b>Создал:</b> {task.owner_id}\n"
        f"<b>Чат:</b> {title} ({task.target_chat_id})\n"
        f"<b>Период:</b> {schedule} ({task.timezone})\n"
        f"<b>Тема:</b> {topic}\n"
        f"<b>Фото:</b> {photo}\n"
        f"<b>Кнопки:</b> {buttons}\n"
        f"<b>Форматирование Telegram:</b> {formatting}\n\n"
        f"<b>Текст:</b>\n{preview}"
    )


def set_wizard(context: ContextTypes.DEFAULT_TYPE, data: dict) -> None:
    context.user_data[WIZARD_KEY] = data


def get_wizard(context: ContextTypes.DEFAULT_TYPE) -> dict | None:
    return context.user_data.get(WIZARD_KEY)


def clear_wizard(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(WIZARD_KEY, None)


async def safe_reply(message, text: str, **kwargs) -> None:
    for attempt in range(2):
        try:
            await message.reply_text(text, **kwargs)
            return
        except TimedOut:
            if attempt == 1:
                raise
            await asyncio.sleep(1)


async def safe_bot_send(bot, **kwargs) -> None:
    for attempt in range(2):
        try:
            await bot.send_message(**kwargs)
            return
        except TimedOut:
            if attempt == 1:
                raise
            await asyncio.sleep(1)


async def safe_answer_callback(query) -> None:
    try:
        await query.answer()
    except BadRequest as exc:
        if "Query is too old" in str(exc) or "query id is invalid" in str(exc):
            logger.warning("Ignoring expired callback query answer")
            return
        raise


async def send_task_preview(message, task: PostTask) -> None:
    rendered_html = render_task_html(task)
    if task.source_chat_id and task.source_message_id and not task.photo_file_id:
        for attempt in range(2):
            try:
                await message.get_bot().copy_message(
                    chat_id=message.chat_id,
                    from_chat_id=task.source_chat_id,
                    message_id=task.source_message_id,
                    reply_markup=build_post_keyboard(task.buttons_json),
                )
                return
            except TimedOut:
                if attempt == 1:
                    raise
                await asyncio.sleep(1)

    if rendered_html and not task.photo_file_id:
        for attempt in range(2):
            try:
                await message.reply_text(
                    rendered_html,
                    parse_mode=ParseMode.HTML,
                    reply_markup=build_post_keyboard(task.buttons_json),
                )
                return
            except TimedOut:
                if attempt == 1:
                    raise
                await asyncio.sleep(1)

    if task.photo_file_id:
        photo_kwargs = build_photo_caption_kwargs(task, rendered_html)
        for attempt in range(2):
            try:
                await message.reply_photo(
                    photo=task.photo_file_id,
                    **photo_kwargs,
                    reply_markup=build_post_keyboard(task.buttons_json),
                )
                return
            except TimedOut:
                if attempt == 1:
                    raise
                await asyncio.sleep(1)
        return

    for attempt in range(2):
        try:
            await message.reply_text(
                task.message_text,
                entities=task.message_entities,
                reply_markup=build_post_keyboard(task.buttons_json),
            )
            return
        except TimedOut:
            if attempt == 1:
                raise
            await asyncio.sleep(1)


async def send_access_denied(message) -> None:
    user_id = getattr(getattr(message, "from_user", None), "id", None)
    suffix = f"\nВаш user_id: {user_id}" if user_id is not None else ""
    await safe_reply(
        message,
        "У вас нет доступа к этому боту.\n"
        "Если доступ нужен, попросите администратора добавить ваш user_id в панели доступа."
        f"{suffix}"
    )


async def send_home(update: Update, text: str | None = None) -> None:
    target = update.effective_message or update.callback_query.message
    message_text = text or (
        "Красивое меню готово.\n\n"
        "Можно создать автопост, посмотреть задачи и управлять ими кнопками."
    )
    await target.reply_text(message_text, reply_markup=main_menu_markup())


async def prompt_create_chat_id(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    set_wizard(context, {"mode": "create", "step": "chat_id"})
    await safe_reply(
        message,
        "Шаг 1 из 5.\nОтправьте <chat_id> чата или канала, куда нужен автопост.",
        reply_markup=wizard_cancel_markup(),
    )


async def prompt_edit_text(message, context: ContextTypes.DEFAULT_TYPE, task_id: int) -> None:
    set_wizard(context, {"mode": "edit_text", "step": "text", "task_id": task_id})
    await message.reply_text(
        f"Отправьте новый текст для задачи #{task_id}.",
        reply_markup=wizard_cancel_markup(),
    )


async def prompt_edit_full_topic(message, context: ContextTypes.DEFAULT_TYPE, task_id: int) -> None:
    task = db.get_task(task_id)
    if not task:
        await safe_reply(message, "Задача не найдена.", reply_markup=main_menu_markup())
        return
    set_wizard(
        context,
        {
            "mode": "edit_full",
            "step": "interval",
            "task_id": task_id,
            "interval_minutes": task.interval_minutes,
            "topic_id": task.topic_id,
            "source_chat_id": task.source_chat_id,
            "source_message_id": task.source_message_id,
            "photo_file_id": task.photo_file_id,
            "buttons_json": task.buttons_json,
        },
    )
    await safe_reply(
        message,
        f"Задача #{task_id}. Отправьте новый интервал в минутах.\nНапример: 1, 5, 10, 30",
        reply_markup=wizard_cancel_markup(),
    )


async def prompt_add_access(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    set_wizard(context, {"mode": "access_add", "step": "user_id"})
    await message.reply_text(
        "Отправьте user_id аккаунта, которому нужно дать доступ.",
        reply_markup=wizard_cancel_markup(),
    )


async def prompt_remove_access(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    set_wizard(context, {"mode": "access_remove", "step": "user_id"})
    await message.reply_text(
        "Отправьте user_id аккаунта, которому нужно убрать доступ.",
        reply_markup=wizard_cancel_markup(),
    )


async def show_access_menu(message, user_id: int) -> None:
    if not is_superadmin(user_id):
        await message.reply_text(
            "Панель доступов доступна только администраторам бота.",
            reply_markup=main_menu_markup(),
        )
        return
    await message.reply_text(
        "Панель доступа к боту.",
        reply_markup=access_menu_markup(),
    )


async def show_allowed_users(message) -> None:
    rows = db.list_allowed_users()
    if not rows:
        await message.reply_text("Список доступов пуст.", reply_markup=access_menu_markup())
        return
    lines = ["Доступ к боту:"]
    for row in rows:
        owner_mark = " [owner]" if row["user_id"] == OWNER_ID else ""
        label = f" ({row['label']})" if row["label"] else ""
        lines.append(f"- {row['user_id']}{label}{owner_mark}")
    await message.reply_text("\n".join(lines), reply_markup=access_menu_markup())


async def show_tasks_menu(message) -> None:
    tasks = db.list_tasks()
    if not tasks:
        await message.reply_text(
            "Пока нет задач. Нажмите 'Создать пост'.",
            reply_markup=main_menu_markup(),
        )
        return
    await message.reply_text(
        "Список автопостов:",
        reply_markup=tasks_list_markup(tasks),
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        clear_wizard(context)
        await send_access_denied(update.message)
        return
    clear_wizard(context)
    await update.message.reply_text(
        "Бот для автопостинга готов.\n\n"
        "Все можно делать через кнопки: создать пост, изменить текст, поставить на паузу и удалить задачу.",
        reply_markup=main_menu_markup(),
    )


async def chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await send_access_denied(update.message)
        return
    chat = update.effective_chat
    await update.message.reply_text(f"chat_id: {chat.id}")


async def topic_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await send_access_denied(update.message)
        return
    chat = update.effective_chat
    thread_id = update.effective_message.message_thread_id
    if thread_id:
        await update.message.reply_text(f"chat_id: {chat.id}\ntopic_id: {thread_id}")
        return

    await update.message.reply_text(
        f"chat_id: {chat.id}\nСейчас это не тема. В обычном чате topic_id не нужен."
    )


async def add_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await update.message.reply_text("У вас нет доступа к управлению этим ботом.")
        return

    raw_args = update.message.text.partition(" ")[2].strip()
    try:
        chat_id_str, interval_value, message_text = parse_addpost_args(raw_args)
        target_chat_id = int(chat_id_str)
        interval_minutes = parse_interval(interval_value)
        post_time = "00:00"
        topic_id = None
        buttons_json = ""
    except (ValueError, TypeError):
        try:
            chat_id_str, interval_value, topic_id, buttons_json, message_text = parse_addpost_extended_args(
                raw_args
            )
            target_chat_id = int(chat_id_str)
            interval_minutes = parse_interval(interval_value)
            post_time = "00:00"
        except (ValueError, TypeError):
            await update.message.reply_text(
                "Форматы:\n"
                "/addpost <chat_id> | <интервал> | <текст>\n\n"
                "или\n"
                "/addpost <chat_id> | <интервал> | <topic_id или -> | <кнопки или -> | <текст>\n\n"
                "Пример:\n"
                "/addpost -1001234567890 | 60 | 55 | Купить=https://site.ru | Список наших проектов"
            )
            return

    try:
        target_chat = await context.bot.get_chat(target_chat_id)
    except Exception as exc:
        logger.exception("Failed to get target chat", exc_info=exc)
        await update.message.reply_text(
            "Не удалось получить чат. Проверьте `chat_id` и убедитесь, что бот уже добавлен в этот чат."
        )
        return

    task_id = db.create_task(
        owner_id=update.effective_user.id,
        target_chat_id=target_chat_id,
        target_chat_title=target_chat.title or target_chat.full_name or str(target_chat_id),
        topic_id=topic_id,
        source_chat_id=None,
        source_message_id=None,
        photo_file_id="",
        buttons_json=buttons_json,
        message_text=message_text,
        message_entities_json="",
        message_html=message_text,
        post_time=post_time,
        interval_minutes=interval_minutes,
        timezone=DEFAULT_TIMEZONE,
    )
    schedule_post_job(task_id)

    await update.message.reply_text(
        "Автопостинг создан.\n"
        f"ID задачи: {task_id}\n"
        f"Чат: {target_chat.title or target_chat_id}\n"
        f"Период: {format_interval(interval_minutes, post_time)} ({DEFAULT_TIMEZONE})\n"
        f"Тема: {topic_id if topic_id is not None else '-'}\n"
        f"Кнопки: {'да' if buttons_json else 'нет'}"
    )


async def list_posts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await update.message.reply_text("У вас нет доступа к управлению этим ботом.")
        return

    await show_tasks_menu(update.message)


async def edit_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await update.message.reply_text("У вас нет доступа к управлению этим ботом.")
        return

    raw_args = update.message.text.partition(" ")[2].strip()
    parts = [part.strip() for part in raw_args.split("|", 1)]
    if len(parts) != 2:
        await update.message.reply_text("Формат: /editpost <id> | <новый текст>")
        return

    try:
        task_id = int(parts[0])
    except ValueError:
        await update.message.reply_text("ID задачи должен быть числом.")
        return

    plain_text = normalize_text(parts[1])
    if not db.update_task_message(task_id, None, plain_text, "", plain_text, None, None):
        await update.message.reply_text("Задача не найдена.")
        return

    await update.message.reply_text(
        f"Текст задачи #{task_id} обновлён. Следующие отправки будут уже с новым сообщением."
    )


async def edit_post_full(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await update.message.reply_text("У вас нет доступа к управлению этим ботом.")
        return

    raw_args = update.message.text.partition(" ")[2].strip()
    try:
        task_id, interval_value, topic_id, buttons_json, message_text = parse_editpost_full_args(raw_args)
        interval_minutes = parse_interval(interval_value)
    except (ValueError, TypeError):
        await update.message.reply_text(
            "Формат:\n"
            "/editpostfull <id> | <интервал> | <topic_id или -> | <кнопки или -> | <новый текст>"
        )
        return

    current_task = db.get_task(task_id)
    if not current_task:
        await update.message.reply_text("Задача не найдена.")
        return

    if not db.update_task_full(
        task_id,
        None,
        topic_id,
        current_task.photo_file_id,
        buttons_json,
        message_text,
        "",
        message_text,
        None,
        None,
        interval_minutes,
    ):
        await update.message.reply_text("Задача не найдена.")
        return

    await update.message.reply_text(
        f"Задача #{task_id} обновлена: тема, кнопки и текст сохранены."
    )


async def ui_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await safe_answer_callback(query)

    if not require_owner(update):
        await send_access_denied(query.message)
        return

    action = query.data
    if action == "ui:home":
        clear_wizard(context)
        await query.message.reply_text("Главное меню:", reply_markup=main_menu_markup())
        return

    if action == "ui:create":
        await prompt_create_chat_id(query.message, context)
        return

    if action == "ui:list":
        clear_wizard(context)
        await show_tasks_menu(query.message)
        return

    if action == "ui:ids":
        await query.message.reply_text(
            "Как получить ID:\n"
            "1. Зайдите в нужный чат или тему.\n"
            "2. Напишите /chatid или /topicid.\n"
            "3. Вставьте эти значения в мастер создания поста.",
            reply_markup=main_menu_markup(),
        )
        return

    if action == "ui:access":
        clear_wizard(context)
        await show_access_menu(query.message, query.from_user.id)
        return

    if action == "ui:help":
        await query.message.reply_text(
            "Сценарий простой:\n"
            "1. Нажмите 'Создать пост'.\n"
            "2. Бот сам спросит чат, интервал в минутах, текст, фото, кнопку и тему.\n"
            "3. Готовую задачу потом можно открыть из списка и менять кнопками.",
            reply_markup=main_menu_markup(),
        )
        return

    if action == "ui:cancel":
        clear_wizard(context)
        await query.message.reply_text(
            "Действие отменено.",
            reply_markup=main_menu_markup(),
        )
        return

    if action == "ui:skip":
        await handle_wizard_skip(query.message, context)
        return

    if action == "ui:no_buttons":
        await handle_no_buttons_choice(query.message, context)
        return

    if action == "ui:add_buttons":
        await handle_add_buttons_choice(query.message, context)
        return

    if action == "ui:no_photo":
        await handle_no_photo_choice(query.message, context)
        return

    if action == "ui:add_photo":
        await handle_add_photo_choice(query.message, context)
        return


async def task_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await safe_answer_callback(query)

    if not require_owner(update):
        await send_access_denied(query.message)
        return

    _, action, task_id_raw = query.data.split(":", 2)
    task_id = int(task_id_raw)
    task = db.get_task(task_id)
    if not task and action != "delete":
        await query.message.reply_text("Задача не найдена.", reply_markup=main_menu_markup())
        return

    if action == "view":
        await query.message.reply_text(
            format_task_card(task),
            parse_mode=ParseMode.HTML,
            reply_markup=task_actions_markup(task),
        )
        await safe_reply(query.message, "Предпросмотр сообщения:")
        await send_task_preview(query.message, task)
        return

    if action == "preview":
        await safe_reply(query.message, "Предпросмотр сообщения:")
        await send_task_preview(query.message, task)
        return

    if action == "edit_text":
        await prompt_edit_text(query.message, context, task_id)
        return

    if action == "edit_full":
        await prompt_edit_full_topic(query.message, context, task_id)
        return

    if action == "send_now":
        try:
            await send_task_message(context.bot, task)
            await query.message.reply_text(
                "Сообщение отправлено сейчас.",
                reply_markup=task_actions_markup(task),
            )
        except TimedOut:
            logger.warning("Timed out while sending task %s immediately", task.id)
            await query.message.reply_text(
                "Telegram ответил слишком поздно. Сообщение могло уже уйти в чат, проверьте его.",
                reply_markup=task_actions_markup(task),
            )
        except Exception as exc:
            logger.exception("Failed to send task immediately", exc_info=exc)
            await query.message.reply_text(
                "Не удалось отправить сообщение сейчас. Проверьте права бота в чате.",
                reply_markup=task_actions_markup(task),
            )
        return

    if action == "pause":
        if not task.is_active:
            await query.message.reply_text(
                "Задача уже стоит на паузе.",
                reply_markup=task_actions_markup(task),
            )
            return
        db.set_task_active(task_id, None, False)
        remove_post_job(task_id)
        fresh = db.get_task(task_id)
        await query.message.reply_text(
            "Пост поставлен на паузу.",
            reply_markup=task_actions_markup(fresh),
        )
        return

    if action == "resume":
        if task.is_active:
            await query.message.reply_text(
                "Задача уже активна.",
                reply_markup=task_actions_markup(task),
            )
            return
        db.set_task_active(task_id, None, True)
        schedule_post_job(task_id)
        fresh = db.get_task(task_id)
        await query.message.reply_text(
            "Пост снова активен.",
            reply_markup=task_actions_markup(fresh),
        )
        return

    if action == "delete":
        if not db.delete_task(task_id, None):
            await query.message.reply_text("Задача не найдена.", reply_markup=main_menu_markup())
            return
        remove_post_job(task_id)
        await query.message.reply_text("Задача удалена.", reply_markup=main_menu_markup())


async def access_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await safe_answer_callback(query)

    if not is_superadmin(query.from_user.id):
        await send_access_denied(query.message)
        return

    action = query.data.split(":", 1)[1]
    if action == "list":
        clear_wizard(context)
        await show_allowed_users(query.message)
        return

    if action == "add":
        await prompt_add_access(query.message, context)
        return

    if action == "remove":
        await prompt_remove_access(query.message, context)


async def handle_wizard_skip(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    wizard = get_wizard(context)
    if not wizard:
        await message.reply_text("Сейчас нечего пропускать.", reply_markup=main_menu_markup())
        return

    if wizard["mode"] == "create" and wizard["step"] == "topic_id":
        wizard["topic_id"] = None
        wizard["step"] = "done"
        set_wizard(context, wizard)
        await finalize_create_wizard(message, context, wizard)
        return

    if wizard["mode"] == "edit_full" and wizard["step"] == "topic_id":
        wizard["topic_id"] = None
        set_wizard(context, wizard)
        await finalize_edit_full_wizard(message, context, wizard)
        return

    await message.reply_text("Этот шаг нельзя пропустить.", reply_markup=wizard_cancel_markup())


async def handle_no_buttons_choice(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    wizard = get_wizard(context)
    if not wizard or wizard.get("mode") not in {"create", "edit_full"} or wizard.get("step") != "buttons_choice":
        await message.reply_text("Сейчас этот выбор неактуален.", reply_markup=main_menu_markup())
        return

    if wizard.get("mode") == "edit_full":
        wizard["buttons_json"] = ""
        wizard["step"] = "topic_id"
        set_wizard(context, wizard)
        await safe_reply(
            message,
            "Отправьте topic_id или нажмите 'Пропустить'.",
            reply_markup=wizard_optional_markup(),
        )
        return

    wizard["buttons_json"] = ""
    wizard["step"] = "topic_id"
    set_wizard(context, wizard)
    await safe_reply(
        message,
        "Шаг 6 из 6.\nЕсли нужен пост в тему, отправьте topic_id.\nЕсли тема не нужна, нажмите 'Пропустить'.",
        reply_markup=wizard_optional_markup(),
    )


async def handle_no_photo_choice(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    wizard = get_wizard(context)
    if not wizard or wizard.get("mode") != "create" or wizard.get("step") != "photo_choice":
        await message.reply_text("Сейчас этот выбор неактуален.", reply_markup=main_menu_markup())
        return
    wizard["photo_file_id"] = ""
    wizard["step"] = "buttons_choice"
    set_wizard(context, wizard)
    await safe_reply(
        message,
        "Шаг 5 из 6.\nНужны кнопки под сообщением?",
        reply_markup=wizard_buttons_choice_markup(),
    )


async def handle_add_photo_choice(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    wizard = get_wizard(context)
    if not wizard or wizard.get("mode") != "create" or wizard.get("step") != "photo_choice":
        await message.reply_text("Сейчас этот выбор неактуален.", reply_markup=main_menu_markup())
        return
    wizard["step"] = "photo"
    set_wizard(context, wizard)
    await safe_reply(
        message,
        "Шаг 5 из 6.\nОтправьте фото одним сообщением.",
        reply_markup=wizard_cancel_markup(),
    )


async def handle_add_buttons_choice(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    wizard = get_wizard(context)
    if not wizard or wizard.get("mode") not in {"create", "edit_full"} or wizard.get("step") != "buttons_choice":
        await message.reply_text("Сейчас этот выбор неактуален.", reply_markup=main_menu_markup())
        return
    wizard["step"] = "button_text"
    set_wizard(context, wizard)
    await safe_reply(
        message,
        "Отправьте текст кнопки.",
        reply_markup=wizard_cancel_markup(),
    )


async def wizard_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        return

    wizard = get_wizard(context)
    if not wizard:
        return

    if update.message.photo:
        if wizard["mode"] == "create" and wizard.get("step") == "photo":
            await process_create_photo_wizard(update, context, wizard)
            return
        await safe_reply(
            update.message,
            "Сейчас фото не ожидается. Продолжайте текущий шаг или нажмите 'Отмена'.",
            reply_markup=wizard_cancel_markup(),
        )
        return

    text = (update.message.text or "").strip()
    if not text:
        return

    try:
        if wizard["mode"] == "create":
            await process_create_wizard(update, context, wizard, text)
            return
        if wizard["mode"] == "edit_text":
            await process_edit_text_wizard(update, context, wizard, text)
            return
        if wizard["mode"] == "edit_full":
            await process_edit_full_wizard(update, context, wizard, text)
            return
        if wizard["mode"] == "access_add":
            await process_access_add_wizard(update, context, wizard, text)
            return
        if wizard["mode"] == "access_remove":
            await process_access_remove_wizard(update, context, wizard, text)
            return
    except ValueError:
        await safe_reply(
            update.message,
            "Формат не подошел. Попробуйте еще раз или нажмите 'Отмена'.",
            reply_markup=wizard_cancel_markup(),
        )
    except Exception as exc:
        logger.exception("Wizard step failed", exc_info=exc)
        await safe_reply(
            update.message,
            f"Не получилось обработать этот шаг.\nОшибка: {type(exc).__name__}\nПроверьте данные и попробуйте еще раз.",
            reply_markup=wizard_cancel_markup(),
        )


async def finalize_create_wizard(message, context: ContextTypes.DEFAULT_TYPE, wizard: dict) -> None:
    task_id = db.create_task(
        owner_id=message.from_user.id,
        target_chat_id=wizard["target_chat_id"],
        target_chat_title=wizard["target_chat_title"],
        topic_id=wizard.get("topic_id"),
        source_chat_id=wizard.get("source_chat_id"),
        source_message_id=wizard.get("source_message_id"),
        photo_file_id=wizard.get("photo_file_id", ""),
        buttons_json=wizard.get("buttons_json", ""),
        message_text=wizard["message_text"],
        message_entities_json=wizard.get("message_entities_json", ""),
        message_html=wizard.get("message_html", wizard["message_text"]),
        post_time=wizard["post_time"],
        interval_minutes=wizard.get("interval_minutes"),
        timezone=DEFAULT_TIMEZONE,
    )
    clear_wizard(context)
    schedule_post_job(task_id)
    task = db.get_task(task_id)
    await safe_reply(
        message,
        "Автопост создан.",
        reply_markup=task_actions_markup(task),
    )
    await safe_reply(
        message,
        format_task_card(task),
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_markup(),
    )
    await safe_reply(message, "Предпросмотр сообщения:")
    await send_task_preview(message, task)
    await maybe_warn_custom_emoji_limit(message, task)


async def process_create_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE, wizard: dict, text: str
) -> None:
    if wizard["step"] == "chat_id":
        target_chat_id = int(text)
        wizard["target_chat_id"] = target_chat_id
        wizard["target_chat_title"] = str(target_chat_id)
        try:
            target_chat = await context.bot.get_chat(target_chat_id)
            wizard["target_chat_title"] = (
                target_chat.title or target_chat.full_name or str(target_chat_id)
            )
        except Exception as exc:
            logger.warning(
                "Could not resolve chat %s during wizard step",
                target_chat_id,
                exc_info=exc,
            )
        wizard["step"] = "time"
        set_wizard(context, wizard)
        await safe_reply(
            update.message,
            "Шаг 2 из 5.\nОтправьте интервал в минутах.\nПримеры: 1, 5, 30, 60",
            reply_markup=wizard_cancel_markup(),
        )
        return

    if wizard["step"] == "time":
        wizard["interval_minutes"] = parse_interval(text)
        wizard["post_time"] = "00:00"
        wizard["step"] = "text"
        set_wizard(context, wizard)
        await safe_reply(
            update.message,
            "Шаг 3 из 5.\nОтправьте текст поста. Можно в несколько строк.\nИнтервал указывайте в минутах, например: 1, 5, 30, 60.",
            reply_markup=wizard_cancel_markup(),
        )
        return

    if wizard["step"] == "text":
        wizard["message_text"], wizard["message_entities_json"], wizard["message_html"] = extract_text_payload(update.message)
        wizard["source_chat_id"], wizard["source_message_id"] = extract_source_payload(update.message)
        wizard["step"] = "photo_choice"
        set_wizard(context, wizard)
        await safe_reply(
            update.message,
            "Шаг 4 из 6.\nНужна картинка к сообщению?",
            reply_markup=wizard_photo_choice_markup(),
        )
        return

    if wizard["step"] == "button_text":
        wizard["button_text"] = text.strip()
        wizard["step"] = "button_url"
        set_wizard(context, wizard)
        await safe_reply(
            update.message,
            "Теперь отправьте ссылку для этой кнопки.",
            reply_markup=wizard_cancel_markup(),
        )
        return

    if wizard["step"] == "button_url":
        button_url = text.strip()
        if not button_url.startswith(("http://", "https://", "tg://")):
            raise ValueError
        wizard["buttons_json"] = json.dumps(
            [{"text": wizard["button_text"], "url": button_url}],
            ensure_ascii=False,
        )
        wizard["step"] = "topic_id"
        set_wizard(context, wizard)
        await safe_reply(
            update.message,
            "Шаг 6 из 6.\nЕсли нужен пост в тему, отправьте topic_id.\nЕсли тема не нужна, нажмите 'Пропустить'.",
            reply_markup=wizard_optional_markup(),
        )
        return

    if wizard["step"] == "topic_id":
        wizard["topic_id"] = parse_optional_topic_id(text)
        wizard["step"] = "done"
        set_wizard(context, wizard)
        await finalize_create_wizard(update.message, context, wizard)


async def process_create_photo_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE, wizard: dict
) -> None:
    wizard["photo_file_id"] = update.message.photo[-1].file_id
    wizard["step"] = "buttons_choice"
    set_wizard(context, wizard)
    await safe_reply(
        update.message,
        "Шаг 5 из 6.\nФото сохранено.\nНужны кнопки под сообщением?",
        reply_markup=wizard_buttons_choice_markup(),
    )


async def process_edit_text_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE, wizard: dict, text: str
) -> None:
    task_id = int(wizard["task_id"])
    message_text, message_entities_json, message_html = extract_text_payload(update.message)
    source_chat_id, source_message_id = extract_source_payload(update.message)
    if not db.update_task_message(
        task_id,
        None,
        message_text,
        message_entities_json,
        message_html,
        source_chat_id,
        source_message_id,
    ):
        clear_wizard(context)
        await update.message.reply_text("Задача не найдена.", reply_markup=main_menu_markup())
        return
    clear_wizard(context)
    task = db.get_task(task_id)
    await update.message.reply_text(
        "Текст обновлен.",
        reply_markup=task_actions_markup(task),
    )
    await safe_reply(update.message, "Предпросмотр сообщения:")
    await send_task_preview(update.message, task)
    await maybe_warn_custom_emoji_limit(update.message, task)


async def finalize_edit_full_wizard(
    message, context: ContextTypes.DEFAULT_TYPE, wizard: dict
) -> None:
    task_id = int(wizard["task_id"])
    if not db.update_task_full(
        task_id,
        None,
        wizard.get("topic_id"),
        wizard.get("photo_file_id", ""),
        wizard.get("buttons_json", ""),
        wizard["message_text"],
        wizard.get("message_entities_json", ""),
        wizard.get("message_html", wizard["message_text"]),
        wizard.get("source_chat_id"),
        wizard.get("source_message_id"),
        wizard.get("interval_minutes"),
    ):
        clear_wizard(context)
        await safe_reply(message, "Задача не найдена.", reply_markup=main_menu_markup())
        return

    clear_wizard(context)
    task = db.get_task(task_id)
    await safe_reply(
        message,
        "Задача обновлена.",
        reply_markup=task_actions_markup(task),
    )
    await safe_reply(message, "Предпросмотр сообщения:")
    await send_task_preview(message, task)
    await maybe_warn_custom_emoji_limit(message, task)


async def process_edit_full_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE, wizard: dict, text: str
) -> None:
    if wizard["step"] == "interval":
        wizard["interval_minutes"] = parse_interval(text)
        wizard["step"] = "text"
        set_wizard(context, wizard)
        await safe_reply(
            update.message,
            "Отправьте новый текст поста.",
            reply_markup=wizard_cancel_markup(),
        )
        return

    if wizard["step"] == "text":
        wizard["message_text"], wizard["message_entities_json"], wizard["message_html"] = extract_text_payload(update.message)
        wizard["source_chat_id"], wizard["source_message_id"] = extract_source_payload(update.message)
        wizard["step"] = "buttons_choice"
        set_wizard(context, wizard)
        await safe_reply(
            update.message,
            "Нужны кнопки под сообщением?",
            reply_markup=wizard_buttons_choice_markup(),
        )
        return

    if wizard["step"] == "button_text":
        wizard["button_text"] = text.strip()
        wizard["step"] = "button_url"
        set_wizard(context, wizard)
        await safe_reply(
            update.message,
            "Теперь отправьте ссылку для этой кнопки.",
            reply_markup=wizard_cancel_markup(),
        )
        return

    if wizard["step"] == "button_url":
        button_url = text.strip()
        if not button_url.startswith(("http://", "https://", "tg://")):
            raise ValueError
        wizard["buttons_json"] = json.dumps(
            [{"text": wizard["button_text"], "url": button_url}],
            ensure_ascii=False,
        )
        wizard["step"] = "topic_id"
        set_wizard(context, wizard)
        await safe_reply(
            update.message,
            "Отправьте topic_id или нажмите 'Пропустить'.",
            reply_markup=wizard_optional_markup(),
        )
        return

    if wizard["step"] == "topic_id":
        wizard["topic_id"] = parse_optional_topic_id(text)
        set_wizard(context, wizard)
        await finalize_edit_full_wizard(update.message, context, wizard)


async def process_access_add_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE, wizard: dict, text: str
) -> None:
    if not is_superadmin(update.effective_user.id):
        clear_wizard(context)
        await update.message.reply_text(
            "Только администратор бота может выдавать доступ.",
            reply_markup=main_menu_markup(),
        )
        return
    user_id = int(text)
    if user_id == OWNER_ID:
        clear_wizard(context)
        await update.message.reply_text(
            "Этот user_id уже является главным владельцем.",
            reply_markup=access_menu_markup(),
        )
        return
    db.add_allowed_user(user_id, "Admin access", update.effective_user.id)
    clear_wizard(context)
    await update.message.reply_text(
        f"Доступ выдан аккаунту {user_id}.",
        reply_markup=access_menu_markup(),
    )


async def process_access_remove_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE, wizard: dict, text: str
) -> None:
    if not is_superadmin(update.effective_user.id):
        clear_wizard(context)
        await update.message.reply_text(
            "Только администратор бота может убирать доступ.",
            reply_markup=main_menu_markup(),
        )
        return
    user_id = int(text)
    if user_id == OWNER_ID:
        clear_wizard(context)
        await update.message.reply_text(
            "Нельзя убрать главного владельца из доступа.",
            reply_markup=access_menu_markup(),
        )
        return
    removed = db.remove_allowed_user(user_id)
    clear_wizard(context)
    await update.message.reply_text(
        "Доступ убран." if removed else "Такого аккаунта в списке доступа нет.",
        reply_markup=access_menu_markup(),
    )


async def pause_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await toggle_post(update, should_activate=False)


async def resume_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await toggle_post(update, should_activate=True)


async def toggle_post(update: Update, should_activate: bool) -> None:
    if not require_owner(update):
        await update.message.reply_text("У вас нет доступа к управлению этим ботом.")
        return

    args = context.args
    if len(args) != 1:
        command = "/resumepost <id>" if should_activate else "/pausepost <id>"
        await update.message.reply_text(f"Формат: {command}")
        return

    try:
        task_id = int(args[0])
    except ValueError:
        await update.message.reply_text("ID задачи должен быть числом.")
        return

    updated = db.set_task_active(task_id, None, should_activate)
    if not updated:
        await update.message.reply_text("Задача не найдена.")
        return

    if should_activate:
        schedule_post_job(task_id)
        await update.message.reply_text(f"Задача #{task_id} снова активна.")
    else:
        remove_post_job(task_id)
        await update.message.reply_text(f"Задача #{task_id} поставлена на паузу.")


async def delete_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not require_owner(update):
        await update.message.reply_text("У вас нет доступа к управлению этим ботом.")
        return

    if len(context.args) != 1:
        await update.message.reply_text("Формат: /deletepost <id>")
        return

    try:
        task_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("ID задачи должен быть числом.")
        return

    if not db.delete_task(task_id, None):
        await update.message.reply_text("Задача не найдена.")
        return

    remove_post_job(task_id)
    await update.message.reply_text(f"Задача #{task_id} полностью удалена.")


async def send_scheduled_post(context: ContextTypes.DEFAULT_TYPE) -> None:
    task_id = context.job.data["task_id"]
    task = db.get_task(task_id)
    if not task or not task.is_active:
        remove_post_job(task_id)
        return

    try:
        await send_task_message(context.bot, task)
        logger.info("Posted task %s to chat %s", task.id, task.target_chat_id)
    except TimedOut:
        logger.warning(
            "Timed out while sending scheduled task %s; Telegram may still have delivered it",
            task.id,
        )
    except Exception as exc:
        logger.exception("Failed to send scheduled post for task %s", task.id, exc_info=exc)


async def send_task_message(bot, task: PostTask) -> None:
    rendered_html = render_task_html(task)
    if task.source_chat_id and task.source_message_id and not task.photo_file_id:
        await bot.copy_message(
            chat_id=task.target_chat_id,
            from_chat_id=task.source_chat_id,
            message_id=task.source_message_id,
            message_thread_id=task.topic_id,
            reply_markup=build_post_keyboard(task.buttons_json),
        )
        return

    if rendered_html and not task.photo_file_id:
        await bot.send_message(
            chat_id=task.target_chat_id,
            text=rendered_html,
            parse_mode=ParseMode.HTML,
            message_thread_id=task.topic_id,
            reply_markup=build_post_keyboard(task.buttons_json),
        )
        return

    if task.photo_file_id:
        photo_kwargs = build_photo_caption_kwargs(task, rendered_html)
        await bot.send_photo(
            chat_id=task.target_chat_id,
            photo=task.photo_file_id,
            **photo_kwargs,
            message_thread_id=task.topic_id,
            reply_markup=build_post_keyboard(task.buttons_json),
        )
        return

    await bot.send_message(
        chat_id=task.target_chat_id,
        text=task.message_text,
        entities=task.message_entities,
        message_thread_id=task.topic_id,
        reply_markup=build_post_keyboard(task.buttons_json),
    )


def job_name(task_id: int) -> str:
    return f"scheduled_post_{task_id}"


def remove_post_job(task_id: int) -> None:
    scheduler = getattr(application, "job_queue", None)
    if scheduler is None:
        return
    for job in scheduler.get_jobs_by_name(job_name(task_id)):
        job.schedule_removal()


def schedule_post_job(task_id: int) -> None:
    task = db.get_task(task_id)
    if not task or not task.is_active:
        return

    remove_post_job(task_id)
    if task.interval_minutes:
        application.job_queue.run_repeating(
            send_scheduled_post,
            interval=task.interval_minutes * 60,
            first=task.interval_minutes * 60,
            name=job_name(task_id),
            data={"task_id": task_id},
        )
        logger.info(
            "Scheduled task %s every %s minutes",
            task_id,
            task.interval_minutes,
        )
        return

    hour, minute = map(int, task.post_time.split(":"))
    application.job_queue.run_daily(
        send_scheduled_post,
        time=datetime.strptime(task.post_time, "%H:%M").time().replace(
            tzinfo=ZoneInfo(task.timezone)
        ),
        name=job_name(task_id),
        data={"task_id": task_id},
        days=(0, 1, 2, 3, 4, 5, 6),
    )
    logger.info("Scheduled legacy daily task %s at %02d:%02d %s", task_id, hour, minute, task.timezone)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error", exc_info=context.error)


def restore_jobs() -> None:
    with db.connect() as conn:
        rows = conn.execute("SELECT id FROM post_tasks WHERE is_active = 1").fetchall()
    for row in rows:
        schedule_post_job(int(row["id"]))


if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is empty. Fill it in .env before запуском.")


request = HTTPXRequest(
    connect_timeout=90.0,
    read_timeout=90.0,
    write_timeout=90.0,
    pool_timeout=90.0,
)

application = (
    Application.builder()
    .token(BOT_TOKEN)
    .request(request)
    .rate_limiter(AIORateLimiter())
    .build()
)

application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("chatid", chat_id))
application.add_handler(CommandHandler("topicid", topic_id))
application.add_handler(CommandHandler("addpost", add_post))
application.add_handler(CommandHandler("listposts", list_posts))
application.add_handler(CommandHandler("editpost", edit_post))
application.add_handler(CommandHandler("editpostfull", edit_post_full))
application.add_handler(CommandHandler("pausepost", pause_post))
application.add_handler(CommandHandler("resumepost", resume_post))
application.add_handler(CommandHandler("deletepost", delete_post))
application.add_handler(CallbackQueryHandler(ui_callback, pattern=r"^ui:"))
application.add_handler(CallbackQueryHandler(task_callback, pattern=r"^task:"))
application.add_handler(CallbackQueryHandler(access_callback, pattern=r"^access:"))
application.add_handler(
    MessageHandler((filters.TEXT | filters.PHOTO) & ~filters.COMMAND, wizard_message_handler)
)
application.add_error_handler(error_handler)

restore_jobs()


def main() -> None:
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
