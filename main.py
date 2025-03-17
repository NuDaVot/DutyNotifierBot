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
        await send_admin_error(f"При добавлении чата: {e}")


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
        await send_admin_error(f"При загрузке CSV: {e}")
        return []


async def get_duty_info(shift_label: str) -> tuple:
    """Получает имя дежурного и его телефон по смене (день/ночь)."""
    try:
        schedule_rows = await fetch_csv(SCHEDULE_URL)
        today = datetime.datetime.now()

        weekday_mapping = {
            "Monday": "Понедельник",
            "Tuesday": "Вторник",
            "Wednesday": "Среда",
            "Thursday": "Четверг",
            "Friday": "Пятница",
            "Saturday": "Суббота",
            "Sunday": "Воскресенье",
        }
        weekday_en = today.strftime("%A")
        weekday_ru = weekday_mapping.get(weekday_en, "")
        target_shift = f"{weekday_ru} {shift_label}"  # Например, "Пятница ночь"

        duty_name = None
        current_period_valid = False

        for row in schedule_rows:
            period_cell = row["Период"].strip()
            duty_cell = row["Дежурный"].strip()

            # Если в ячейке есть символ "-", считаем, что это строка с периодом
            if "-" in period_cell:
                try:
                    period_start_str, period_end_str = period_cell.split("-")
                    period_start = datetime.datetime.strptime(period_start_str.strip(), "%d.%m.%Y")
                    period_end = datetime.datetime.strptime(period_end_str.strip(), "%d.%m.%Y")
                    current_period_valid = (period_start <= today <= period_end)
                except Exception as e:
                    logging.error(f"Ошибка при разборе периода '{period_cell}': {e}")
                    await send_admin_error(f"При разборе периода '{period_cell}': {e}")
                    current_period_valid = False
                continue

            # Если текущая строка не периодная и текущий период валиден,
            # сравниваем значение в первой колонке с требуемой сменой.
            if current_period_valid and period_cell == target_shift:
                duty_name = duty_cell
                break

        if not duty_name:
            return None, None

        # Получаем телефон дежурного
        duty_rows = await fetch_csv(DUTY_PERSONNEL_URL)
        duty_phone = None
        for row in duty_rows:
            if "Имя" in row and row["Имя"].strip() == duty_name:
                duty_phone = row["Телефон"].strip()
                break

        return duty_name, duty_phone

    except Exception as e:
        logging.error(f"Ошибка при получении дежурного: {e}")
        await send_admin_error(f"При получении дежурного: {e}")
        return None, None


async def send_scheduled_messages():
    """Отправляет сообщение в чаты в 9:30 и 21:30."""
    while True:
        now = datetime.datetime.now()

        if (now.hour == 9 and now.minute == 30) or (now.hour == 21 and now.minute == 30):
            shift_label = "день" if now.hour == 9 else "ночь"
            duty_name, duty_phone = await get_duty_info(shift_label)
            start_message = "Сегодня с 10:00 до 22:00,\n" if now.hour == 9 else "Сегодня с 22:00 до 10:00,\n"
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
                    await send_admin_error(f"При отправке сообщений в чаты: {e}")
            else:
                await send_admin_error("Информация о дежурном не найдена.")
                logging.warning("Информация о дежурном не найдена. Сообщение отправлено админу.")

        await asyncio.sleep(60)  # Задержка 1 минута


async def main():
    """Основная функция бота."""
    try:
        task1 = asyncio.create_task(send_scheduled_messages())
        task2 = asyncio.create_task(dp.start_polling(bot))

        await asyncio.gather(task1, task2)
    except Exception as e:
        logging.critical(f"Критическая ошибка: {e}")
        await send_admin_error(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.critical(f"При запуске бота: {e}")
