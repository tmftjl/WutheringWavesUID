from typing import Any, List

from gsuid_core.bot import Bot
from gsuid_core.models import Event
from gsuid_core.sv import SV

from ..utils.at_help import ruser_id
from ..utils.button import WavesButton
from ..utils.database.models import WavesBind
from ..utils.error_reply import WAVES_CODE_103
from ..utils.hint import error_reply
from ..wutheringwaves_abyss.draw_abyss_card import draw_abyss_img
from .draw_challenge_card import draw_challenge_img
sv_waves_abyss = SV("waves查询深渊")
sv_waves_challenge = SV("waves查询全息")


@sv_waves_abyss.on_command(
    (
        "查询深渊",
        "sy",
        "st",
        "深渊",
        "逆境深塔",
        "深塔",
        "超载",
        "超载区",
        "稳定",
        "稳定区",
        "实验",
        "实验区",
    ),
    block=True,
)
async def send_waves_abyss_info(bot: Bot, ev: Event):
    await bot.logger.info("开始执行[鸣潮查询深渊信息]")

    user_id = ruser_id(ev)
    uid = await WavesBind.get_uid_by_game(user_id, ev.bot_id)
    if not uid:
        return await bot.send(error_reply(WAVES_CODE_103))
    await bot.logger.info(f"[鸣潮查询深渊信息]user_id:{user_id} uid: {uid}")

    im = await draw_abyss_img(ev, uid, user_id)
    if isinstance(im, str):
        at_sender = True if ev.group_id else False
        await bot.send(im, at_sender)
    else:
        buttons: List[Any] = [
            WavesButton("深塔", "深塔"),
            WavesButton("超载", "超载"),
            WavesButton("稳定", "稳定"),
            WavesButton("实验", "实验"),
        ]
        await bot.send_option(im, buttons)


@sv_waves_challenge.on_command(
    (
        "查询全息",
        "查询全息战略",
        "全息",
        "qx",
        "全息战略",
    ),
    block=True,
)
async def send_waves_challenge_info(bot: Bot, ev: Event):
    await bot.logger.info("开始执行[鸣潮查询全息战略信息]")

    user_id = ruser_id(ev)
    uid = await WavesBind.get_uid_by_game(user_id, ev.bot_id)
    if not uid:
        return await bot.send(error_reply(WAVES_CODE_103))
    await bot.logger.info(f"[鸣潮查询全息战略信息]user_id:{user_id} uid: {uid}")

    im = await draw_challenge_img(ev, uid, user_id)
    return await bot.send(im)
