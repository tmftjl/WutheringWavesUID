"""
定时更新角色持有率缓存任务
"""
from gsuid_core.aps import scheduler
from gsuid_core.logger import logger

from ..database.models import WavesCharHoldRate
from ...wutheringwaves_config import WutheringWavesConfig


def get_update_hour() -> int:
    """获取持有率更新时间（小时，0-23）"""
    hour = WutheringWavesConfig.get_config("HoldRateUpdateHour").data
    if hour is None or not isinstance(hour, int) or hour < 0 or hour > 23:
        return 4  # 默认凌晨4点
    return hour


def get_update_minute() -> int:
    """获取持有率更新时间（分钟，0-59）"""
    minute = WutheringWavesConfig.get_config("HoldRateUpdateMinute").data
    if minute is None or not isinstance(minute, int) or minute < 0 or minute > 59:
        return 0  # 默认整点
    return minute


@scheduler.scheduled_job('cron', hour=get_update_hour(), minute=get_update_minute())
async def update_char_hold_rate_cache():
    """定时更新角色持有率缓存"""
    if not WutheringWavesConfig.get_config("EnableHoldRateCache").data:
        return

    update_time = f"{get_update_hour():02d}:{get_update_minute():02d}"
    logger.info(f"[鸣潮持有率] 定时任务: {update_time} 开始更新角色持有率缓存..")

    try:
        updated_count = await WavesCharHoldRate.update_all_hold_rates()
        logger.info(f"[鸣潮持有率] 角色持有率缓存更新成功，共更新 {updated_count} 个角色")
    except Exception as e:
        logger.exception(f"[鸣潮持有率] 角色持有率缓存更新失败: {e}")


async def manual_update_hold_rate() -> str:
    """手动触发更新持有率（供命令调用）"""
    try:
        logger.info("[鸣潮持有率] 手动触发: 开始更新角色持有率缓存...")
        updated_count = await WavesCharHoldRate.update_all_hold_rates()
        return f"角色持有率缓存更新成功，共更新 {updated_count} 个角色"
    except Exception as e:
        logger.exception(f"[鸣潮持有率] 手动更新失败: {e}")
        return f"角色持有率缓存更新失败: {str(e)}"
