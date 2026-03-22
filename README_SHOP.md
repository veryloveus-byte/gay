# Premium Shop Bot

Этот файл описывает отдельного бота-магазин из `premium_shop_bot.py`.

Что умеет бот:

- продаёт Telegram Premium за `Telegram Stars`;
- после оплаты автоматически вызывает `giftPremiumSubscription`;
- показывает каталог доступных `Telegram Gifts`;
- автоматически отправляет купленный подарок на аккаунт покупателя;
- сохраняет заказы в SQLite и умеет делать возврат Stars, если автовыдача сорвалась.

## Настройки

Добавьте в `.env`:

```env
SHOP_BOT_TOKEN=токен_магазина
SHOP_OWNER_ID=ваш_user_id
SHOP_DB_PATH=premium_shop.sqlite3
PREMIUM_PRICE_3=1000
PREMIUM_PRICE_6=1500
PREMIUM_PRICE_12=2500
GIFT_MARKUP_PERCENT=0
```

Если `SHOP_BOT_TOKEN` не указан, бот возьмёт обычный `BOT_TOKEN`.

## Запуск

```bash
python premium_shop_bot.py
```

## Команды

- `/start` открыть магазин
- `/orders` последние заказы владельцу
- `/balance` баланс Stars бота

## Ограничения текущей версии

- покупки оформляются только на аккаунт самого покупателя;
- каталог подарков показывает первые доступные позиции;
- продажа именно "звёзд на аккаунт" в этой версии не реализована.
