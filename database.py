"""
🗄 База данных для игры "Угадай Аниме"
Таблицы: игроки, достижения, коллекция, история игр
"""
import aiosqlite
from datetime import datetime, timedelta
from config import DATABASE_PATH


async def init_db():
    """Инициализация базы данных"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        # Таблица игроков
        await db.execute("""
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT DEFAULT '',
                first_name TEXT DEFAULT '',
                xp INTEGER DEFAULT 0,
                correct_answers INTEGER DEFAULT 0,
                wrong_answers INTEGER DEFAULT 0,
                streak INTEGER DEFAULT 0,
                max_streak INTEGER DEFAULT 0,
                games_played INTEGER DEFAULT 0,
                correct_by_image INTEGER DEFAULT 0,
                correct_by_quote INTEGER DEFAULT 0,
                daily_streak INTEGER DEFAULT 0,
                last_daily TEXT DEFAULT '',
                last_played TEXT DEFAULT '',
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Таблица достижений
        await db.execute("""
            CREATE TABLE IF NOT EXISTS achievements (
                user_id INTEGER,
                achievement_id TEXT,
                unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, achievement_id)
            )
        """)

        # Таблица коллекции (какие аниме угадал)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS collection (
                user_id INTEGER,
                anime_id INTEGER,
                first_guessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                times_guessed INTEGER DEFAULT 1,
                PRIMARY KEY (user_id, anime_id)
            )
        """)

        # Таблица истории игр
        await db.execute("""
            CREATE TABLE IF NOT EXISTS game_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                mode TEXT,
                anime_id INTEGER,
                was_correct INTEGER,
                xp_earned INTEGER DEFAULT 0,
                played_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.commit()


# ============ ИГРОКИ ============

async def get_player(user_id: int) -> dict | None:
    """Получить данные игрока"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM players WHERE user_id = ?", (user_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def create_player(user_id: int, username: str, first_name: str):
    """Создать нового игрока"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            INSERT OR IGNORE INTO players (user_id, username, first_name)
            VALUES (?, ?, ?)
        """, (user_id, username, first_name))
        await db.commit()


async def update_player_info(user_id: int, username: str, first_name: str):
    """Обновить информацию об игроке"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            UPDATE players SET username = ?, first_name = ? WHERE user_id = ?
        """, (username, first_name, user_id))
        await db.commit()


async def add_xp(user_id: int, xp: int):
    """Добавить XP игроку"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE players SET xp = xp + ? WHERE user_id = ?",
            (xp, user_id)
        )
        await db.commit()


async def record_correct_answer(user_id: int, mode: str, anime_id: int, xp_earned: int):
    """Записать правильный ответ"""
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DATABASE_PATH) as db:
        # Обновляем статистику игрока
        mode_field = "correct_by_image" if mode == "image" else "correct_by_quote"
        await db.execute(f"""
            UPDATE players SET
                correct_answers = correct_answers + 1,
                streak = streak + 1,
                max_streak = MAX(max_streak, streak + 1),
                games_played = games_played + 1,
                {mode_field} = {mode_field} + 1,
                xp = xp + ?,
                last_played = ?
            WHERE user_id = ?
        """, (xp_earned, now, user_id))

        # Записываем в историю
        await db.execute("""
            INSERT INTO game_history (user_id, mode, anime_id, was_correct, xp_earned)
            VALUES (?, ?, ?, 1, ?)
        """, (user_id, mode, anime_id, xp_earned))

        # Добавляем в коллекцию
        await db.execute("""
            INSERT INTO collection (user_id, anime_id)
            VALUES (?, ?)
            ON CONFLICT(user_id, anime_id) DO UPDATE SET times_guessed = times_guessed + 1
        """, (user_id, anime_id))

        await db.commit()


async def record_wrong_answer(user_id: int, mode: str, anime_id: int):
    """Записать неправильный ответ"""
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            UPDATE players SET
                wrong_answers = wrong_answers + 1,
                streak = 0,
                games_played = games_played + 1,
                last_played = ?
            WHERE user_id = ?
        """, (now, user_id))

        await db.execute("""
            INSERT INTO game_history (user_id, mode, anime_id, was_correct, xp_earned)
            VALUES (?, ?, ?, 0, 0)
        """, (user_id, mode, anime_id))

        await db.commit()


async def get_player_streak(user_id: int) -> int:
    """Получить текущую серию игрока"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            "SELECT streak FROM players WHERE user_id = ?", (user_id,)
        )
        row = await cursor.fetchone()
        return row[0] if row else 0


# ============ ЕЖЕДНЕВНЫЙ БОНУС ============

async def check_and_update_daily(user_id: int) -> dict | None:
    """Проверить и начислить ежедневный бонус. Возвращает None если уже получен сегодня."""
    today = datetime.now().strftime("%Y-%m-%d")
    player = await get_player(user_id)
    if not player:
        return None

    last_daily = player.get("last_daily", "")

    if last_daily == today:
        return None  # Уже получен

    # Проверяем стрик ежедневного входа
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if last_daily == yesterday:
        new_daily_streak = player["daily_streak"] + 1
    else:
        new_daily_streak = 1

    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            UPDATE players SET
                last_daily = ?,
                daily_streak = ?
            WHERE user_id = ?
        """, (today, new_daily_streak, user_id))
        await db.commit()

    return {"daily_streak": new_daily_streak}


# ============ ДОСТИЖЕНИЯ ============

async def get_player_achievements(user_id: int) -> list:
    """Получить список достижений игрока"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            "SELECT achievement_id, unlocked_at FROM achievements WHERE user_id = ?",
            (user_id,)
        )
        rows = await cursor.fetchall()
        return [{"id": row[0], "unlocked_at": row[1]} for row in rows]


async def has_achievement(user_id: int, achievement_id: str) -> bool:
    """Проверить, есть ли достижение у игрока"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM achievements WHERE user_id = ? AND achievement_id = ?",
            (user_id, achievement_id)
        )
        return await cursor.fetchone() is not None


async def unlock_achievement(user_id: int, achievement_id: str) -> bool:
    """Разблокировать достижение. Возвращает True если новое."""
    if await has_achievement(user_id, achievement_id):
        return False
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO achievements (user_id, achievement_id) VALUES (?, ?)",
            (user_id, achievement_id)
        )
        await db.commit()
    return True


# ============ КОЛЛЕКЦИЯ ============

async def get_collection(user_id: int) -> list:
    """Получить коллекцию игрока"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            "SELECT anime_id, first_guessed_at, times_guessed FROM collection WHERE user_id = ? ORDER BY first_guessed_at",
            (user_id,)
        )
        rows = await cursor.fetchall()
        return [{"anime_id": r[0], "first_guessed_at": r[1], "times_guessed": r[2]} for r in rows]


async def get_collection_count(user_id: int) -> int:
    """Получить количество аниме в коллекции"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM collection WHERE user_id = ?",
            (user_id,)
        )
        row = await cursor.fetchone()
        return row[0] if row else 0


async def get_collection_rarities(user_id: int) -> set:
    """Получить множество редкостей собранных аниме"""
    from anime_data import ANIME_LIST
    collection = await get_collection(user_id)
    collected_ids = {c["anime_id"] for c in collection}
    rarities = set()
    for anime in ANIME_LIST:
        if anime["id"] in collected_ids:
            rarities.add(anime["rarity"])
    return rarities


# ============ ЛИДЕРБОРД ============

async def get_leaderboard(limit: int = 10) -> list:
    """Получить топ игроков по XP"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("""
            SELECT user_id, username, first_name, xp, correct_answers, streak, max_streak
            FROM players
            ORDER BY xp DESC
            LIMIT ?
        """, (limit,))
        rows = await cursor.fetchall()
        return [
            {
                "user_id": r[0],
                "username": r[1],
                "first_name": r[2],
                "xp": r[3],
                "correct_answers": r[4],
                "streak": r[5],
                "max_streak": r[6],
            }
            for r in rows
        ]


async def get_player_position(user_id: int) -> int:
    """Получить позицию игрока в рейтинге"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("""
            SELECT COUNT(*) + 1 FROM players
            WHERE xp > (SELECT COALESCE(xp, 0) FROM players WHERE user_id = ?)
        """, (user_id,))
        row = await cursor.fetchone()
        return row[0] if row else 0


# ============ СТАТИСТИКА (АДМИН) ============

async def get_bot_stats() -> dict:
    """Получить статистику бота"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM players")
        total_players = (await cursor.fetchone())[0]

        today = datetime.now().strftime("%Y-%m-%d")
        cursor = await db.execute(
            "SELECT COUNT(*) FROM players WHERE last_played LIKE ?",
            (f"{today}%",)
        )
        active_today = (await cursor.fetchone())[0]

        cursor = await db.execute("SELECT SUM(games_played) FROM players")
        total_games = (await cursor.fetchone())[0] or 0

        cursor = await db.execute("SELECT SUM(correct_answers) FROM players")
        total_correct = (await cursor.fetchone())[0] or 0

        return {
            "total_players": total_players,
            "active_today": active_today,
            "total_games": total_games,
            "total_correct": total_correct,
        }
