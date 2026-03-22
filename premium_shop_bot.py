import logging
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape

from dotenv import load_dotenv
from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    AIORateLimiter,
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    PreCheckoutQueryHandler,
    filters,
)


load_dotenv()

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("premium_shop_bot")

BOT_TOKEN = os.getenv("SHOP_BOT_TOKEN", os.getenv("BOT_TOKEN", "")).strip()
OWNER_ID = int(os.getenv("SHOP_OWNER_ID", os.getenv("OWNER_ID", "0")) or 0)
DB_PATH = os.getenv("SHOP_DB_PATH", "premium_shop.sqlite3").strip()
PREMIUM_PRICE_3 = int(os.getenv("PREMIUM_PRICE_3", "1000"))
PREMIUM_PRICE_6 = int(os.getenv("PREMIUM_PRICE_6", "1500"))
PREMIUM_PRICE_12 = int(os.getenv("PREMIUM_PRICE_12", "2500"))
GIFT_MARKUP_PERCENT = max(0, int(os.getenv("GIFT_MARKUP_PERCENT", "0")))

PREMIUM_PRODUCTS = {
    3: {"price_xtr": PREMIUM_PRICE_3, "cost_xtr": 1000, "title": "Telegram Premium на 3 месяца"},
    6: {"price_xtr": PREMIUM_PRICE_6, "cost_xtr": 1500, "title": "Telegram Premium на 6 месяцев"},
    12: {"price_xtr": PREMIUM_PRICE_12, "cost_xtr": 2500, "title": "Telegram Premium на 12 месяцев"},
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def calc_gift_price(star_count: int) -> int:
    if GIFT_MARKUP_PERCENT <= 0:
        return star_count
    extra = max(1, round(star_count * GIFT_MARKUP_PERCENT / 100))
    return star_count + extra


@dataclass
class Order:
    id: int
    user_id: int
    username: str
    product_type: str
    product_ref: str
    price_xtr: int
    status: str
    invoice_payload: str
    telegram_charge_id: str
    provider_charge_id: str
    error_message: str
    created_at: str
    updated_at: str


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
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    username TEXT NOT NULL DEFAULT '',
                    product_type TEXT NOT NULL,
                    product_ref TEXT NOT NULL,
                    price_xtr INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    invoice_payload TEXT NOT NULL DEFAULT '',
                    telegram_charge_id TEXT NOT NULL DEFAULT '',
                    provider_charge_id TEXT NOT NULL DEFAULT '',
                    error_message TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

    def create_order(
        self,
        *,
        user_id: int,
        username: str,
        product_type: str,
        product_ref: str,
        price_xtr: int,
    ) -> Order:
        now = utc_now_iso()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO orders (
                    user_id, username, product_type, product_ref, price_xtr, status,
                    invoice_payload, telegram_charge_id, provider_charge_id, error_message,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, 'pending_payment', '', '', '', '', ?, ?)
                """,
                (user_id, username, product_type, product_ref, price_xtr, now, now),
            )
            order_id = int(cursor.lastrowid)
            payload = f"order:{order_id}:{product_type}:{product_ref}"
            conn.execute(
                "UPDATE orders SET invoice_payload = ?, updated_at = ? WHERE id = ?",
                (payload, now, order_id),
            )
        return self.get_order(order_id)

    def get_order(self, order_id: int) -> Order:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        if row is None:
            raise KeyError(f"Order {order_id} not found")
        return Order(**dict(row))

    def get_order_by_payload(self, payload: str) -> Order | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM orders WHERE invoice_payload = ?",
                (payload,),
            ).fetchone()
        return Order(**dict(row)) if row else None

    def update_status(
        self,
        order_id: int,
        status: str,
        *,
        telegram_charge_id: str | None = None,
        provider_charge_id: str | None = None,
        error_message: str | None = None,
    ) -> None:
        fields = ["status = ?", "updated_at = ?"]
        params: list[object] = [status, utc_now_iso()]
        if telegram_charge_id is not None:
            fields.append("telegram_charge_id = ?")
            params.append(telegram_charge_id)
        if provider_charge_id is not None:
            fields.append("provider_charge_id = ?")
            params.append(provider_charge_id)
        if error_message is not None:
            fields.append("error_message = ?")
            params.append(error_message)
        params.append(order_id)
        with self.connect() as conn:
            conn.execute(f"UPDATE orders SET {', '.join(fields)} WHERE id = ?", params)

    def list_recent_orders(self, limit: int = 10) -> list[Order]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM orders ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [Order(**dict(row)) for row in rows]


db = Database(DB_PATH)


def user_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Premium", callback_data="menu:premium")],
            [InlineKeyboardButton("Подарки", callback_data="menu:gifts")],
            [InlineKeyboardButton("Помощь", callback_data="menu:help")],
        ]
    )


def premium_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for months, data in PREMIUM_PRODUCTS.items():
        rows.append(
            [
                InlineKeyboardButton(
                    f"{months} мес. - {data['price_xtr']} XTR",
                    callback_data=f"buy:premium:{months}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("Назад", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def gifts_keyboard(gifts) -> InlineKeyboardMarkup:
    rows = []
    for gift in gifts[:8]:
        emoji = getattr(gift.sticker, "emoji", None) or "🎁"
        button_text = f"{emoji} {gift.star_count} XTR"
        if GIFT_MARKUP_PERCENT > 0:
            button_text = f"{emoji} {calc_gift_price(gift.star_count)} XTR"
        rows.append([InlineKeyboardButton(button_text, callback_data=f"buy:gift:{gift.id}")])
    rows.append([InlineKeyboardButton("Обновить каталог", callback_data="menu:gifts")])
    rows.append([InlineKeyboardButton("Назад", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


async def set_commands(application: Application) -> None:
    await application.bot.set_my_commands(
        [
            BotCommand("start", "Открыть магазин"),
            BotCommand("orders", "Последние заказы"),
            BotCommand("balance", "Баланс Stars бота"),
        ]
    )


async def api_get_my_star_balance(context: ContextTypes.DEFAULT_TYPE) -> int:
    raw = await context.bot._post("getMyStarBalance")
    if isinstance(raw, int):
        return raw
    if isinstance(raw, dict):
        amount = raw.get("amount")
        if isinstance(amount, int):
            return amount
        if isinstance(amount, dict):
            return int(amount.get("amount", 0) or 0)
    return 0


async def api_gift_premium(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    user_id: int,
    months: int,
    star_count: int,
    text: str,
) -> None:
    payload = {
        "user_id": user_id,
        "month_count": months,
        "star_count": star_count,
        "text": text,
    }
    await context.bot._post("giftPremiumSubscription", payload)


async def answer_or_edit(query, text: str, reply_markup: InlineKeyboardMarkup) -> None:
    try:
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
    except TelegramError:
        await query.message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "<b>Магазин Telegram Premium и подарков</b>\n\n"
        "Оплата проходит в <b>Telegram Stars</b>.\n"
        "Premium выдается автоматически сразу после успешной оплаты.\n"
        "Подарки бот отправляет на ваш аккаунт автоматически."
    )
    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=user_main_keyboard(),
    )


async def menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    if query.data == "menu:home":
        await answer_or_edit(
            query,
            "<b>Выберите товар:</b>",
            user_main_keyboard(),
        )
        return

    if query.data == "menu:premium":
        lines = ["<b>Telegram Premium</b>", ""]
        for months, data in PREMIUM_PRODUCTS.items():
            lines.append(
                f"{months} мес.: {data['price_xtr']} XTR"
            )
        lines.append("")
        lines.append("После оплаты бот сразу дарит Premium на ваш аккаунт.")
        await answer_or_edit(query, "\n".join(lines), premium_keyboard())
        return

    if query.data == "menu:gifts":
        gifts = await context.bot.get_available_gifts()
        if not gifts.gifts:
            await answer_or_edit(
                query,
                "<b>Сейчас доступных подарков нет.</b>",
                InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="menu:home")]]),
            )
            return
        text_lines = [
            "<b>Каталог подарков</b>",
            "",
            "Показываю первые доступные позиции из каталога Telegram.",
            "После оплаты подарок отправляется автоматически на ваш аккаунт.",
        ]
        await answer_or_edit(query, "\n".join(text_lines), gifts_keyboard(gifts.gifts))
        return

    if query.data == "menu:help":
        await answer_or_edit(
            query,
            (
                "<b>Как это работает</b>\n\n"
                "1. Выбираете товар.\n"
                "2. Оплачиваете его в Stars.\n"
                "3. Бот автоматически выдает Premium или подарок.\n\n"
                "Сейчас покупки оформляются на тот аккаунт, с которого открыт бот."
            ),
            InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="menu:home")]]),
        )


async def buy_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    parts = query.data.split(":")
    if len(parts) != 3:
        return

    _, product_type, product_ref = parts
    user = query.from_user

    if product_type == "premium":
        months = int(product_ref)
        product = PREMIUM_PRODUCTS.get(months)
        if product is None:
            await query.answer("Тариф не найден", show_alert=True)
            return
        order = db.create_order(
            user_id=user.id,
            username=user.username or "",
            product_type="premium",
            product_ref=str(months),
            price_xtr=product["price_xtr"],
        )
        await context.bot.send_invoice(
            chat_id=query.message.chat_id,
            title=product["title"],
            description="Автоматическая выдача Telegram Premium после оплаты",
            payload=order.invoice_payload,
            currency="XTR",
            prices=[LabeledPrice("Premium", product["price_xtr"])],
            provider_token="",
        )
        return

    if product_type == "gift":
        gifts = await context.bot.get_available_gifts()
        selected_gift = next((gift for gift in gifts.gifts if gift.id == product_ref), None)
        if selected_gift is None:
            await query.answer("Подарок уже недоступен, обновите каталог", show_alert=True)
            return
        price_xtr = calc_gift_price(selected_gift.star_count)
        order = db.create_order(
            user_id=user.id,
            username=user.username or "",
            product_type="gift",
            product_ref=selected_gift.id,
            price_xtr=price_xtr,
        )
        title = f"{getattr(selected_gift.sticker, 'emoji', '🎁')} Telegram Gift"
        await context.bot.send_invoice(
            chat_id=query.message.chat_id,
            title=title,
            description="Автоматическая отправка подарка на ваш аккаунт",
            payload=order.invoice_payload,
            currency="XTR",
            prices=[LabeledPrice("Gift", price_xtr)],
            provider_token="",
        )


async def precheckout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.pre_checkout_query
    order = db.get_order_by_payload(query.invoice_payload)
    if order is None:
        await query.answer(ok=False, error_message="Заказ не найден, попробуйте снова.")
        return
    if query.currency != "XTR":
        await query.answer(ok=False, error_message="Поддерживается только оплата в Stars.")
        return
    await query.answer(ok=True)


async def safe_refund(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    user_id: int,
    telegram_charge_id: str,
) -> None:
    try:
        await context.bot.refund_star_payment(
            user_id=user_id,
            telegram_payment_charge_id=telegram_charge_id,
        )
    except TelegramError:
        logger.exception("Failed to refund star payment")


async def fulfill_premium(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    order: Order,
    user_id: int,
) -> str:
    months = int(order.product_ref)
    product = PREMIUM_PRODUCTS[months]
    balance = await api_get_my_star_balance(context)
    if balance < product["cost_xtr"]:
        raise RuntimeError(
            f"У бота недостаточно Stars для выдачи Premium: {balance} < {product['cost_xtr']}"
        )
    await api_gift_premium(
        context,
        user_id=user_id,
        months=months,
        star_count=product["cost_xtr"],
        text=f"Telegram Premium на {months} мес.",
    )
    return f"Premium на {months} мес. успешно выдан."


async def fulfill_gift(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    order: Order,
    user_id: int,
) -> str:
    await context.bot.send_gift(
        user_id=user_id,
        gift_id=order.product_ref,
        text="Ваш подарок доставлен автоматически.",
    )
    return "Подарок успешно отправлен."


async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    payment = update.effective_message.successful_payment
    if payment is None:
        return

    order = db.get_order_by_payload(payment.invoice_payload)
    if order is None:
        await update.effective_message.reply_text("Оплата получена, но заказ не найден.")
        return

    db.update_status(
        order.id,
        "paid",
        telegram_charge_id=payment.telegram_payment_charge_id,
        provider_charge_id=payment.provider_payment_charge_id,
    )

    try:
        if order.product_type == "premium":
            success_text = await fulfill_premium(
                context,
                order=order,
                user_id=update.effective_user.id,
            )
        elif order.product_type == "gift":
            success_text = await fulfill_gift(
                context,
                order=order,
                user_id=update.effective_user.id,
            )
        else:
            raise RuntimeError(f"Неизвестный тип заказа: {order.product_type}")
    except Exception as exc:
        logger.exception("Failed to fulfill order %s", order.id)
        db.update_status(order.id, "fulfillment_failed", error_message=str(exc))
        await safe_refund(
            context,
            user_id=update.effective_user.id,
            telegram_charge_id=payment.telegram_payment_charge_id,
        )
        db.update_status(order.id, "refunded", error_message=str(exc))
        await update.effective_message.reply_text(
            "Оплата получена, но автовыдача не удалась. Я попытался оформить возврат Stars. "
            "Если возврат не пришел сразу, проверьте историю платежей чуть позже.",
        )
        if OWNER_ID:
            await context.bot.send_message(
                OWNER_ID,
                (
                    f"Заказ #{order.id} не выдался автоматически.\n"
                    f"Пользователь: {update.effective_user.id}\n"
                    f"Ошибка: {escape(str(exc))}"
                ),
                parse_mode=ParseMode.HTML,
            )
        return

    db.update_status(order.id, "fulfilled")
    await update.effective_message.reply_text(
        f"{success_text}\n\nСпасибо за покупку.",
        reply_markup=user_main_keyboard(),
    )


async def recent_orders(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        await update.effective_message.reply_text("Команда доступна только владельцу.")
        return
    orders = db.list_recent_orders()
    if not orders:
        await update.effective_message.reply_text("Заказов пока нет.")
        return
    lines = ["<b>Последние заказы</b>"]
    for order in orders:
        username = f"@{order.username}" if order.username else str(order.user_id)
        lines.append(
            f"#{order.id} | {order.product_type}:{order.product_ref} | {order.price_xtr} XTR | {order.status} | {username}"
        )
    await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def star_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        await update.effective_message.reply_text("Команда доступна только владельцу.")
        return
    balance = await api_get_my_star_balance(context)
    await update.effective_message.reply_text(f"Баланс бота: {balance} XTR")


def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Укажите SHOP_BOT_TOKEN или BOT_TOKEN в .env")

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .rate_limiter(AIORateLimiter())
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("orders", recent_orders))
    application.add_handler(CommandHandler("balance", star_balance))
    application.add_handler(CallbackQueryHandler(menu_router, pattern=r"^menu:"))
    application.add_handler(CallbackQueryHandler(buy_router, pattern=r"^buy:"))
    application.add_handler(PreCheckoutQueryHandler(precheckout))
    application.add_handler(
        MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment)
    )
    application.post_init = set_commands

    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
