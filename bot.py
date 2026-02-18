"""
🎌 Угадай Аниме — Telegram бот-игра
Режимы: по картинке, по цитате, случайный
Система: XP, ранги, достижения, коллекция, топ игроков
"""
import asyncio
import logging
import random
import time
import uuid
from datetime import datetime

import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

import config
import database as db
from anime_data import (
    ANIME_LIST, ACHIEVEMENTS, RARITY_EMOJI, RARITY_NAMES, RARITY_POINTS,
    get_rank, get_next_rank, get_xp_progress, get_anime_by_id, get_anime_with_quotes,
    get_all_rarities_set, RARITY_COMMON, RARITY_RARE, RARITY_EPIC, RARITY_LEGENDARY,
)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота
bot = Bot(
    token=config.BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

# ============ ИГРОВОЕ СОСТОЯНИЕ (В ПАМЯТИ) ============
active_games: dict[str, dict] = {}      # game_id -> game_data
image_cache: dict[int, str] = {}         # mal_id -> image_url
jikan_semaphore = asyncio.Semaphore(3)   # Лимит параллельных запросов к Jikan


# ============ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ============

def cleanup_old_games():
    """Удалить устаревшие игры из памяти"""
    now = time.time()
    expired = [gid for gid, g in active_games.items() if now - g["created_at"] > 120]
    for gid in expired:
        del active_games[gid]


async def get_anime_image_url(mal_id: int) -> str | None:
    """Получить URL картинки аниме через Jikan API (с кешем)"""
    if mal_id in image_cache:
        return image_cache[mal_id]

    async with jikan_semaphore:
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{config.JIKAN_BASE_URL}/anime/{mal_id}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        img_url = data["data"]["images"]["jpg"]["large_image_url"]
                        image_cache[mal_id] = img_url
                        return img_url
                    elif resp.status == 429:
                        # Rate limited — подождём
                        await asyncio.sleep(2)
                        return await get_anime_image_url(mal_id)
        except Exception as e:
            logger.error(f"Jikan API error for mal_id={mal_id}: {e}")

    return None


def create_game(user_id: int, mode: str) -> tuple[str, dict]:
    """Создать новую игру и вернуть (game_id, game_data)"""
    cleanup_old_games()

    # Выбираем аниме
    if mode == "quote":
        pool = get_anime_with_quotes()
    else:
        pool = ANIME_LIST.copy()

    correct_anime = random.choice(pool)

    # Выбираем неправильные варианты (из той же редкости или близкой)
    wrong_pool = [a for a in ANIME_LIST if a["id"] != correct_anime["id"]]
    # Приоритет — аниме той же редкости
    same_rarity = [a for a in wrong_pool if a["rarity"] == correct_anime["rarity"]]
    if len(same_rarity) >= config.OPTIONS_COUNT - 1:
        wrong_choices = random.sample(same_rarity, config.OPTIONS_COUNT - 1)
    else:
        wrong_choices = random.sample(wrong_pool, min(config.OPTIONS_COUNT - 1, len(wrong_pool)))

    # Формируем варианты ответа
    options = wrong_choices + [correct_anime]
    random.shuffle(options)
    correct_index = next(i for i, o in enumerate(options) if o["id"] == correct_anime["id"])

    # Выбираем цитату (для режима цитат)
    quote = None
    if mode == "quote" and correct_anime.get("quotes"):
        quote = random.choice(correct_anime["quotes"])

    game_id = str(uuid.uuid4())[:8]
    game_data = {
        "user_id": user_id,
        "mode": mode,
        "correct_anime": correct_anime,
        "options": options,
        "correct_index": correct_index,
        "quote": quote,
        "created_at": time.time(),
    }
    active_games[game_id] = game_data
    return game_id, game_data


def get_options_keyboard(game_id: str, options: list) -> InlineKeyboardMarkup:
    """Клавиатура с вариантами ответов"""
    buttons = []
    for i, opt in enumerate(options):
        buttons.append([
            InlineKeyboardButton(
                text=f"{['🅰', '🅱', '🅲', '🅳'][i]} {opt['name_ru']}",
                callback_data=f"ans_{game_id}_{i}"
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_main_keyboard() -> InlineKeyboardMarkup:
    """Главное меню"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎮 Играть", callback_data="play")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="prof"),
         InlineKeyboardButton(text="🏆 Топ игроков", callback_data="top")],
        [InlineKeyboardButton(text="🎯 Достижения", callback_data="ach"),
         InlineKeyboardButton(text="📦 Коллекция", callback_data="col")],
        [InlineKeyboardButton(text="❓ Помощь", callback_data="help")],
    ])


def get_play_keyboard() -> InlineKeyboardMarkup:
    """Меню выбора режима игры"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🖼 По картинке", callback_data="gm_i")],
        [InlineKeyboardButton(text="💬 По цитате", callback_data="gm_q")],
        [InlineKeyboardButton(text="🎲 Случайный", callback_data="gm_r")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu")],
    ])


def get_play_again_keyboard(mode: str) -> InlineKeyboardMarkup:
    """Кнопки после ответа"""
    mode_map = {"image": "gm_i", "quote": "gm_q"}
    cb = mode_map.get(mode, "gm_r")
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Играть ещё", callback_data=cb)],
        [InlineKeyboardButton(text="🔀 Другой режим", callback_data="play")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")],
    ])


async def ensure_player(user: types.User):
    """Убедиться, что игрок существует в БД"""
    player = await db.get_player(user.id)
    if not player:
        await db.create_player(user.id, user.username or "", user.first_name or "Игрок")
    else:
        await db.update_player_info(user.id, user.username or "", user.first_name or "Игрок")


async def check_daily_bonus(user_id: int) -> str:
    """Проверить ежедневный бонус и вернуть текст (или пустую строку)"""
    result = await db.check_and_update_daily(user_id)
    if not result:
        return ""

    daily_streak = result["daily_streak"]
    bonus_xp = config.DAILY_BONUS_XP

    # Бонус за серию дней
    streak_bonus = ""
    if daily_streak >= 7:
        bonus_xp += 50
        streak_bonus = "\n🎁 Бонус за 7+ дней: +50 XP"
    elif daily_streak >= 3:
        bonus_xp += 15
        streak_bonus = "\n🎁 Бонус за 3+ дней: +15 XP"

    await db.add_xp(user_id, bonus_xp)

    return config.TEXTS["daily_bonus"].format(
        xp=bonus_xp,
        daily_streak=daily_streak,
        streak_bonus=streak_bonus
    )


async def check_achievements(user_id: int, extra: dict = None) -> list:
    """Проверить и разблокировать достижения. Возвращает список новых."""
    player = await db.get_player(user_id)
    if not player:
        return []

    new_achievements = []
    extra = extra or {}

    # Карта проверок
    checks = {
        "first_win": player["correct_answers"] >= 1,
        "correct_10": player["correct_answers"] >= 10,
        "correct_50": player["correct_answers"] >= 50,
        "correct_100": player["correct_answers"] >= 100,
        "correct_200": player["correct_answers"] >= 200,
        "streak_5": player["max_streak"] >= 5,
        "streak_10": player["max_streak"] >= 10,
        "streak_20": player["max_streak"] >= 20,
        "games_10": player["games_played"] >= 10,
        "games_100": player["games_played"] >= 100,
        "games_500": player["games_played"] >= 500,
        "image_25": player["correct_by_image"] >= 25,
        "image_50": player["correct_by_image"] >= 50,
        "quote_25": player["correct_by_quote"] >= 25,
        "quote_50": player["correct_by_quote"] >= 50,
        "daily_3": player["daily_streak"] >= 3,
        "daily_7": player["daily_streak"] >= 7,
        "daily_30": player["daily_streak"] >= 30,
        "speed_demon": extra.get("speed_answer", False),
        "legendary_guess": extra.get("guessed_legendary", False),
    }

    # Проверка коллекции
    collection_count = await db.get_collection_count(user_id)
    checks["collect_10"] = collection_count >= 10
    checks["collect_30"] = collection_count >= 30
    checks["collect_50"] = collection_count >= 50

    # Проверка всех редкостей
    collected_rarities = await db.get_collection_rarities(user_id)
    all_rarities = {RARITY_COMMON, RARITY_RARE, RARITY_EPIC, RARITY_LEGENDARY}
    checks["all_rarities"] = collected_rarities >= all_rarities

    # Безупречный — 10+ игр, 100% точность
    total = player["correct_answers"] + player["wrong_answers"]
    if total >= 10 and player["wrong_answers"] == 0:
        checks["perfect_10"] = True
    else:
        checks["perfect_10"] = False

    # Разблокируем
    for ach_id, condition in checks.items():
        if condition and ach_id in ACHIEVEMENTS:
            unlocked = await db.unlock_achievement(user_id, ach_id)
            if unlocked:
                reward_xp = ACHIEVEMENTS[ach_id]["reward_xp"]
                await db.add_xp(user_id, reward_xp)
                new_achievements.append(ach_id)

    return new_achievements


def format_new_achievements(achievement_ids: list) -> str:
    """Форматировать текст новых достижений"""
    if not achievement_ids:
        return ""
    lines = ["", "🏅 <b>Новые достижения!</b>"]
    for ach_id in achievement_ids:
        ach = ACHIEVEMENTS[ach_id]
        lines.append(f"  {ach['icon']} {ach['name']} — +{ach['reward_xp']} XP")
    return "\n".join(lines)


# ============ КОМАНДЫ ============

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    """Команда /start"""
    await ensure_player(message.from_user)

    # Ежедневный бонус
    daily_text = await check_daily_bonus(message.from_user.id)

    text = config.TEXTS["welcome"]
    if daily_text:
        text = daily_text + "\n" + text

    await message.answer(text, reply_markup=get_main_keyboard())


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    """Помощь"""
    await message.answer(config.TEXTS["help"], reply_markup=get_main_keyboard())


@dp.message(Command("play"))
async def cmd_play(message: types.Message):
    """Начать игру"""
    await ensure_player(message.from_user)
    await message.answer("🎮 <b>Выбери режим игры:</b>", reply_markup=get_play_keyboard())


@dp.message(Command("profile"))
async def cmd_profile(message: types.Message):
    """Профиль"""
    await ensure_player(message.from_user)
    await show_profile(message.from_user.id, message)


@dp.message(Command("top"))
async def cmd_top(message: types.Message):
    """Топ игроков"""
    await ensure_player(message.from_user)
    await show_leaderboard(message.from_user.id, message)


@dp.message(Command("achievements"))
async def cmd_achievements(message: types.Message):
    """Достижения"""
    await ensure_player(message.from_user)
    await show_achievements(message.from_user.id, message)


@dp.message(Command("collection"))
async def cmd_collection(message: types.Message):
    """Коллекция"""
    await ensure_player(message.from_user)
    await show_collection(message.from_user.id, message, page=1)


@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    """Статистика (админ)"""
    if message.from_user.id != config.ADMIN_ID:
        return
    stats = await db.get_bot_stats()
    await message.answer(
        f"📊 <b>Статистика бота</b>\n\n"
        f"👥 Игроков: {stats['total_players']}\n"
        f"📈 Активных сегодня: {stats['active_today']}\n"
        f"🎮 Всего игр: {stats['total_games']}\n"
        f"✅ Правильных ответов: {stats['total_correct']}"
    )


# ============ CALLBACK ОБРАБОТЧИКИ ============

@dp.callback_query(F.data == "menu")
async def cb_menu(callback: types.CallbackQuery):
    await callback.answer()
    # Ежедневный бонус
    await ensure_player(callback.from_user)
    daily_text = await check_daily_bonus(callback.from_user.id)
    text = config.TEXTS["welcome"]
    if daily_text:
        text = daily_text + "\n" + text
    try:
        await callback.message.edit_text(text, reply_markup=get_main_keyboard())
    except Exception:
        await callback.message.answer(text, reply_markup=get_main_keyboard())


@dp.callback_query(F.data == "help")
async def cb_help(callback: types.CallbackQuery):
    await callback.answer()
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]
    ])
    try:
        await callback.message.edit_text(config.TEXTS["help"], reply_markup=back_kb)
    except Exception:
        await callback.message.answer(config.TEXTS["help"], reply_markup=back_kb)


@dp.callback_query(F.data == "play")
async def cb_play(callback: types.CallbackQuery):
    await callback.answer()
    try:
        await callback.message.edit_text("🎮 <b>Выбери режим игры:</b>", reply_markup=get_play_keyboard())
    except Exception:
        await callback.message.answer("🎮 <b>Выбери режим игры:</b>", reply_markup=get_play_keyboard())


@dp.callback_query(F.data == "prof")
async def cb_profile(callback: types.CallbackQuery):
    await callback.answer()
    await ensure_player(callback.from_user)
    await show_profile(callback.from_user.id, callback.message, edit=True)


@dp.callback_query(F.data == "top")
async def cb_top(callback: types.CallbackQuery):
    await callback.answer()
    await ensure_player(callback.from_user)
    await show_leaderboard(callback.from_user.id, callback.message, edit=True)


@dp.callback_query(F.data == "ach")
async def cb_achievements(callback: types.CallbackQuery):
    await callback.answer()
    await ensure_player(callback.from_user)
    await show_achievements(callback.from_user.id, callback.message, edit=True)


@dp.callback_query(F.data == "col")
async def cb_collection(callback: types.CallbackQuery):
    await callback.answer()
    await ensure_player(callback.from_user)
    await show_collection(callback.from_user.id, callback.message, page=1, edit=True)


@dp.callback_query(F.data.startswith("colp_"))
async def cb_collection_page(callback: types.CallbackQuery):
    await callback.answer()
    page = int(callback.data.replace("colp_", ""))
    await show_collection(callback.from_user.id, callback.message, page=page, edit=True)


# ============ ИГРОВЫЕ РЕЖИМЫ ============

@dp.callback_query(F.data.in_({"gm_i", "gm_q", "gm_r"}))
async def cb_start_game(callback: types.CallbackQuery):
    """Начать игру в выбранном режиме"""
    await callback.answer()
    await ensure_player(callback.from_user)

    mode_map = {"gm_i": "image", "gm_q": "quote", "gm_r": random.choice(["image", "quote"])}
    mode = mode_map[callback.data]

    game_id, game_data = create_game(callback.from_user.id, mode)
    keyboard = get_options_keyboard(game_id, game_data["options"])

    if mode == "image":
        # Получаем картинку аниме
        anime = game_data["correct_anime"]
        image_url = await get_anime_image_url(anime["mal_id"])

        if image_url:
            try:
                # Удаляем предыдущее сообщение (если возможно)
                try:
                    await callback.message.delete()
                except Exception:
                    pass

                await bot.send_photo(
                    chat_id=callback.from_user.id,
                    photo=image_url,
                    caption="🖼 <b>Угадай аниме по картинке!</b>\n\n"
                            f"{RARITY_EMOJI[anime['rarity']]} Редкость: {RARITY_NAMES[anime['rarity']]}\n\n"
                            "Выбери правильный ответ:",
                    reply_markup=keyboard
                )
                return
            except Exception as e:
                logger.error(f"Failed to send image: {e}")

        # Фоллбэк — если не удалось загрузить картинку
        try:
            await callback.message.edit_text(
                "🖼 <b>Угадай аниме!</b>\n\n"
                f"⚠️ Не удалось загрузить картинку.\n"
                f"🎌 MAL ID: {anime['mal_id']}\n"
                f"{RARITY_EMOJI[anime['rarity']]} Редкость: {RARITY_NAMES[anime['rarity']]}\n\n"
                "Выбери правильный ответ:",
                reply_markup=keyboard
            )
        except Exception:
            await callback.message.answer(
                "🖼 <b>Угадай аниме!</b>\n\nВыбери правильный ответ:",
                reply_markup=keyboard
            )

    elif mode == "quote":
        quote = game_data["quote"]
        anime = game_data["correct_anime"]

        quote_text = f"<i>«{quote['text']}»</i>"
        if quote.get("character"):
            quote_text += f"\n\n— {quote['character']}"

        text = (
            f"💬 <b>Угадай аниме по цитате!</b>\n\n"
            f"{quote_text}\n\n"
            f"{RARITY_EMOJI[anime['rarity']]} Редкость: {RARITY_NAMES[anime['rarity']]}\n\n"
            f"Из какого это аниме?"
        )

        try:
            # Пытаемся удалить предыдущее фото-сообщение если было
            try:
                await callback.message.delete()
            except Exception:
                pass
            await bot.send_message(
                chat_id=callback.from_user.id,
                text=text,
                reply_markup=keyboard
            )
        except Exception:
            await callback.message.answer(text, reply_markup=keyboard)


# ============ ОБРАБОТКА ОТВЕТА ============

@dp.callback_query(F.data.startswith("ans_"))
async def cb_answer(callback: types.CallbackQuery):
    """Обработка ответа игрока"""
    parts = callback.data.split("_")
    if len(parts) != 3:
        await callback.answer("⚠️ Ошибка", show_alert=True)
        return

    game_id = parts[1]
    try:
        chosen_index = int(parts[2])
    except ValueError:
        await callback.answer("⚠️ Ошибка", show_alert=True)
        return

    # Получаем игру
    game = active_games.get(game_id)
    if not game:
        await callback.answer("⏰ Игра устарела! Начни новую.", show_alert=True)
        return

    # Проверяем, что отвечает тот же пользователь
    if game["user_id"] != callback.from_user.id:
        await callback.answer("🚫 Это не твоя игра!", show_alert=True)
        return

    await callback.answer()

    # Удаляем игру из активных
    del active_games[game_id]

    correct_anime = game["correct_anime"]
    mode = game["mode"]
    is_correct = chosen_index == game["correct_index"]
    answer_time = time.time() - game["created_at"]

    if is_correct:
        # Правильный ответ
        rarity = correct_anime["rarity"]
        base_xp = RARITY_POINTS[rarity]

        # Получаем текущий стрик ДО обновления
        old_streak = await db.get_player_streak(callback.from_user.id)
        new_streak = old_streak + 1

        # Бонус за серию
        streak_bonus = min(new_streak * config.STREAK_BONUS_XP, config.MAX_STREAK_BONUS)
        total_xp = base_xp + streak_bonus

        # Записываем
        await db.record_correct_answer(callback.from_user.id, mode, correct_anime["id"], total_xp)

        # Проверяем достижения
        extra = {
            "speed_answer": answer_time <= config.SPEED_BONUS_TIME,
            "guessed_legendary": rarity == RARITY_LEGENDARY,
        }
        new_achs = await check_achievements(callback.from_user.id, extra)

        streak_text = f"(🔥 серия ×{new_streak}: +{streak_bonus})" if streak_bonus > 0 else ""

        text = config.TEXTS["game_correct"].format(
            anime_name=f"{correct_anime['name_ru']} ({correct_anime['name']})",
            rarity_emoji=RARITY_EMOJI[rarity],
            rarity_name=RARITY_NAMES[rarity],
            xp_earned=total_xp,
            streak_text=streak_text,
            streak=new_streak,
            new_achievements=format_new_achievements(new_achs),
        )
    else:
        # Неправильный ответ
        old_streak = await db.get_player_streak(callback.from_user.id)
        await db.record_wrong_answer(callback.from_user.id, mode, correct_anime["id"])

        new_achs = await check_achievements(callback.from_user.id)

        text = config.TEXTS["game_wrong"].format(
            anime_name=f"{correct_anime['name_ru']} ({correct_anime['name']})",
            rarity_emoji=RARITY_EMOJI[correct_anime["rarity"]],
            rarity_name=RARITY_NAMES[correct_anime["rarity"]],
            old_streak=old_streak,
            new_achievements=format_new_achievements(new_achs),
        )

    keyboard = get_play_again_keyboard(mode)

    try:
        # Пытаемся удалить сообщение с вопросом
        try:
            await callback.message.delete()
        except Exception:
            pass
        await bot.send_message(
            chat_id=callback.from_user.id,
            text=text,
            reply_markup=keyboard
        )
    except Exception:
        try:
            await callback.message.edit_text(text, reply_markup=keyboard)
        except Exception:
            await callback.message.answer(text, reply_markup=keyboard)


# ============ ОТОБРАЖЕНИЕ ПРОФИЛЯ ============

async def show_profile(user_id: int, message: types.Message, edit: bool = False):
    """Показать профиль игрока"""
    player = await db.get_player(user_id)
    if not player:
        return

    rank = get_rank(player["xp"])
    total = player["correct_answers"] + player["wrong_answers"]
    accuracy = round(player["correct_answers"] / total * 100, 1) if total > 0 else 0

    collection_count = await db.get_collection_count(user_id)
    achievements = await db.get_player_achievements(user_id)

    text = config.TEXTS["profile"].format(
        user_id=user_id,
        joined_date=player["joined_at"][:10] if player["joined_at"] else "—",
        rank_icon=rank["name"].split()[0],
        rank_name=rank["name"],
        xp=player["xp"],
        xp_bar=get_xp_progress(player["xp"]),
        correct=player["correct_answers"],
        wrong=player["wrong_answers"],
        accuracy=accuracy,
        total_games=player["games_played"],
        streak=player["streak"],
        max_streak=player["max_streak"],
        daily_streak=player["daily_streak"],
        collection=collection_count,
        total_anime=len(ANIME_LIST),
        achievements_count=len(achievements),
        total_achievements=len(ACHIEVEMENTS),
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎮 Играть", callback_data="play")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")],
    ])

    if edit:
        try:
            await message.edit_text(text, reply_markup=keyboard)
        except Exception:
            await message.answer(text, reply_markup=keyboard)
    else:
        await message.answer(text, reply_markup=keyboard)


# ============ ЛИДЕРБОРД ============

async def show_leaderboard(user_id: int, message: types.Message, edit: bool = False):
    """Показать топ игроков"""
    leaders = await db.get_leaderboard(10)
    position = await db.get_player_position(user_id)

    medals = ["🥇", "🥈", "🥉"]
    entries = []
    for i, leader in enumerate(leaders):
        medal = medals[i] if i < 3 else f"#{i + 1}"
        name = leader["first_name"] or leader["username"] or f"ID:{leader['user_id']}"
        rank = get_rank(leader["xp"])
        entries.append(
            f"{medal} <b>{name}</b>\n"
            f"   {rank['name']} • ✨{leader['xp']} XP • ✅{leader['correct_answers']} • 🔥{leader['max_streak']}"
        )

    if not entries:
        entries_text = "🤷 Пока никто не играл!\nБудь первым — нажми «Играть»!"
    else:
        entries_text = "\n\n".join(entries)

    text = config.TEXTS["leaderboard"].format(
        entries=entries_text,
        your_position=position
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎮 Играть", callback_data="play")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")],
    ])

    if edit:
        try:
            await message.edit_text(text, reply_markup=keyboard)
        except Exception:
            await message.answer(text, reply_markup=keyboard)
    else:
        await message.answer(text, reply_markup=keyboard)


# ============ ДОСТИЖЕНИЯ ============

async def show_achievements(user_id: int, message: types.Message, edit: bool = False):
    """Показать достижения"""
    player_achs = await db.get_player_achievements(user_id)
    unlocked_ids = {a["id"] for a in player_achs}

    entries = []
    for ach_id, ach in ACHIEVEMENTS.items():
        if ach_id in unlocked_ids:
            entries.append(f"✅ {ach['icon']} <b>{ach['name']}</b> — {ach['description']}")
        else:
            entries.append(f"🔒 {ach['icon']} <b>{ach['name']}</b> — {ach['description']}")

    text = config.TEXTS["achievements_header"].format(
        unlocked=len(unlocked_ids),
        total=len(ACHIEVEMENTS),
        entries="\n".join(entries)
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎮 Играть", callback_data="play")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")],
    ])

    if edit:
        try:
            await message.edit_text(text, reply_markup=keyboard)
        except Exception:
            await message.answer(text, reply_markup=keyboard)
    else:
        await message.answer(text, reply_markup=keyboard)


# ============ КОЛЛЕКЦИЯ ============

async def show_collection(user_id: int, message: types.Message, page: int = 1, edit: bool = False):
    """Показать коллекцию аниме"""
    collection = await db.get_collection(user_id)
    collected_ids = {c["anime_id"] for c in collection}

    # Формируем список всех аниме с пометками
    all_items = []
    for anime in ANIME_LIST:
        if anime["id"] in collected_ids:
            times = next((c["times_guessed"] for c in collection if c["anime_id"] == anime["id"]), 0)
            all_items.append(
                f"✅ {RARITY_EMOJI[anime['rarity']]} <b>{anime['name_ru']}</b> ({anime['name']}) ×{times}"
            )
        else:
            all_items.append(
                f"❓ {RARITY_EMOJI[anime['rarity']]} ???"
            )

    # Пагинация
    per_page = 15
    total_pages = max(1, (len(all_items) + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    end = start + per_page
    page_items = all_items[start:end]

    text = config.TEXTS["collection_header"].format(
        collected=len(collected_ids),
        total=len(ANIME_LIST),
        entries="\n".join(page_items),
        page=page,
        total_pages=total_pages,
    )

    # Кнопки навигации
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data=f"colp_{page - 1}"))
    nav_buttons.append(InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"colp_{page + 1}"))

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        nav_buttons,
        [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")],
    ])

    if edit:
        try:
            await message.edit_text(text, reply_markup=keyboard)
        except Exception:
            await message.answer(text, reply_markup=keyboard)
    else:
        await message.answer(text, reply_markup=keyboard)


@dp.callback_query(F.data == "noop")
async def cb_noop(callback: types.CallbackQuery):
    """Пустая кнопка (номер страницы)"""
    await callback.answer()


# ============ ОБРАБОТКА ТЕКСТОВЫХ СООБЩЕНИЙ ============

@dp.message(F.text)
async def handle_text(message: types.Message):
    """Обработка любых текстовых сообщений"""
    await ensure_player(message.from_user)
    await message.answer(
        "🎌 <b>Угадай Аниме!</b>\n\n"
        "Используй кнопки ниже для навигации.\n"
        "Нажми /play чтобы начать игру!",
        reply_markup=get_main_keyboard()
    )


# ============ ЗАПУСК ============

async def main():
    """Запуск бота"""
    logger.info("🗄 Инициализация базы данных...")
    await db.init_db()

    logger.info("🎌 Запуск бота 'Угадай Аниме'...")

    # Удаляем вебхук
    await bot.delete_webhook(drop_pending_updates=True)

    # Запуск
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
