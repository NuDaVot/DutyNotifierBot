import os
import datetime
import asyncio
import csv
import io
import logging

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
import aiofiles
import aiohttp

# Загрузка переменных окружения
load_dotenv()

ADMIN_ID = os.getenv("ADMIN_ID")
BOT_TOKEN = os.getenv("BOT_TOKEN")
SCHEDULE_URL = os.getenv("SCHEDULE_URL")
DUTY_PERSONNEL_URL = os.getenv("DUTY_PERSONNEL_URL")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="bot.log",
    filemode="a",
)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


async def send_admin_error(error_message: str):
    """Отправляет сообщение об ошибке админу."""
    if ADMIN_ID:
        try:
            await bot.send_message(chat_id=int(ADMIN_ID), text=f"⚠️ Ошибка: {error_message}")
        except Exception as e:
            logging.error(f"Ошибка при отправке ошибки админу: {e}")


async def chat_exists(chat_id: int, filename: str = "chats.txt") -> bool:
    try:
        async with aiofiles.open(filename, mode="r", encoding="utf-8") as file:
            async for line in file:
                parts = line.split(',')
                if parts and parts[0].strip() == str(chat_id):
                    return True
    except FileNotFoundError:
        return False
    return False


@dp.my_chat_member()
async def handle_my_chat_member(update: types.ChatMemberUpdated):
    """Добавляет чат в список, если бот добавлен в него."""
    try:
        if update.new_chat_member.status in ["member", "administrator"]:
            if not await chat_exists(update.chat.id):
                async with aiofiles.open("chats.txt", mode="a", encoding="utf-8") as file:
                    await file.write(f"{update.chat.id}, {update.chat.title}\n")
                logging.info(f"Добавлен новый чат: {update.chat.title} (ID: {update.chat.id})")
    except Exception as e:
        logging.error(f"Ошибка при добавлении чата: {e}")
        await send_admin_error(f"Ошибка при добавлении чата: {e}")


async def fetch_csv(url: str) -> list:
    """Асинхронно получает CSV-данные по URL и возвращает список словарей."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                text = await response.text()
        reader = csv.DictReader(io.StringIO(text))
        return [row for row in reader]
    except Exception as e:
        logging.error(f"Ошибка при загрузке CSV-данных: {e}")
        await send_admin_error(f"Ошибка при загрузке CSV: {e}")
        return []


async def get_duty_info(shift_label: str) -> tuple:
    """Получает имя дежурного и его телефон по смене."""
    try:
        schedule_rows = await fetch_csv(SCHEDULE_URL)
        today_str = datetime.datetime.now().strftime("%d.%m.%Y")
        duty_name = None

        for row in schedule_rows:
            if "Дата" in row:
                parts = row["Дата"].split()
                if len(parts) >= 2:
                    date_part, shift_part = parts[0], parts[1]
                    if date_part == today_str and shift_part == shift_label:
                        duty_name = row["Дежурный"].strip()
                        break

        if not duty_name:
            return None, None

        duty_rows = await fetch_csv(DUTY_PERSONNEL_URL)
        duty_phone = None
        for row in duty_rows:
            if "Имя" in row and row["Имя"].strip() == duty_name:
                duty_phone = row["Телефон"].strip()
                break

        return duty_name, duty_phone
    except Exception as e:
        logging.error(f"Ошибка при получении дежурного: {e}")
        await send_admin_error(f"Ошибка при получении дежурного: {e}")
        return None, None


async def send_scheduled_messages():
    """Отправляет сообщение в чаты в 9:30 и 21:30."""
    while True:
        now = datetime.datetime.now()

        if (now.hour == 9 and now.minute == 30) or (now.hour == 21 and now.minute == 30):
            shift_label = "День" if now.hour == 9 else "Ночь"
            duty_name, duty_phone = await get_duty_info(shift_label)
            start_message = "Сегодня с 10:00 до 22:00,\n" if shift_label == "День" else "Сегодня с 22:00 до 10:00,\n"
            if duty_name and duty_phone:
                message = (
                    start_message +
                    "в случае возникновения инцидента, пишите пожалуйста в чат,\n"
                    "если через 15-30 минут нет реакции дежурного в чате,\n"
                    f"то звоните дежурному по номеру телефона {duty_phone}, {duty_name}."
                )
                try:
                    async with aiofiles.open("chats.txt", mode="r", encoding="utf-8") as file:
                        async for line in file:
                            parts = line.split(',')
                            if parts:
                                chat_id_str = parts[0].strip()
                                try:
                                    chat_id = int(chat_id_str)
                                    await bot.send_message(chat_id=chat_id, text=message)
                                    logging.info(f"Сообщение отправлено в чат {chat_id}")
                                except ValueError:
                                    continue
                except Exception as e:
                    logging.error(f"Ошибка при отправке сообщений в чаты: {e}")
                    await send_admin_error(f"Ошибка при отправке сообщений в чаты: {e}")
            else:
                await send_admin_error("Информация о дежурном не найдена.")
                logging.warning("Информация о дежурном не найдена. Сообщение отправлено админу.")

        await asyncio.sleep(60)  # Задержка 1 минута


async def main():
    """Основная функция бота."""
    try:
        await send_scheduled_messages()
        await dp.start_polling(bot)
    except Exception as e:
        logging.critical(f"Критическая ошибка: {e}")
        await send_admin_error(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.critical(f"Ошибка при запуске бота: {e}")
