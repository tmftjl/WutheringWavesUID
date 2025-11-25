import os
from datetime import datetime, timedelta
import logging

from .models import SlashSimpleRecord
logger = logging.getLogger(__name__)

# 活动周期基准时间
CYCLE_BASE = datetime(2025, 8, 4, 4, 0, 0)
CYCLE_WEEKS = 4
CLEAN_FLAG_FILE = os.path.join(os.path.dirname(__file__), "endless_rank_clean.flag")


async def check_and_clean():
    now = datetime.now()
    delta = now - CYCLE_BASE
    cycle_num = delta.days // (CYCLE_WEEKS * 7)
    last_cycle = None
    if os.path.exists(CLEAN_FLAG_FILE):
        with open(CLEAN_FLAG_FILE, "r") as f:
            try:
                last_cycle = int(f.read().strip())
            except Exception:
                last_cycle = None
    # 如果进入新周期，清理
    if last_cycle != cycle_num:
        try:
            await SlashSimpleRecord.clean_simple()
            logger.info(f"新周期，清理成功")
            with open(CLEAN_FLAG_FILE, "w") as f:
                f.write(str(cycle_num))
        except Exception as e:
            logger.error(f"[EndlessRankCleaner] 清理数据库异常: {e}")
