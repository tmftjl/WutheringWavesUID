import json
import asyncio
from pathlib import Path
from typing import Dict, List, Tuple, Union, Optional

from gsuid_core.bot import Bot
from pydantic import BaseModel
from PIL import Image, ImageDraw
from gsuid_core.models import Event
from gsuid_core.logger import logger
from gsuid_core.utils.image.convert import convert_img
from gsuid_core.utils.image.image_tools import crop_center_img

from ..utils.util import hide_uid
from ..wutheringwaves_config import PREFIX
from ..utils.database.models import WavesBind
from ..utils.image import (
    GREY,
    SPECIAL_GOLD,
    add_footer,
    get_waves_bg,
    get_qq_avatar,
    get_square_avatar,
)
from ..utils.fonts.waves_fonts import (
    waves_font_14,
    waves_font_16,
    waves_font_18,
    waves_font_20,
    waves_font_30,
    waves_font_34,
    waves_font_40,
)

# --- 常量与资源加载 ---
RANK_LENGTH = 20  # 排行榜显示的长度
TEXT_PATH = Path(__file__).parent / "texture2d"
BAR_IMG = Image.open(TEXT_PATH / "bar.png")
LOGO_IMG = Image.open(TEXT_PATH / "logo_small_2.png")


# --- 数据模型 ---
class EndlessRankInfo(BaseModel):
    qid: str
    uid: str
    name: str
    endless_score: int
    rank_level: str
    half_list: List[dict] = []


# --- 数据处理函数 ---
async def get_all_endless_rank_info(
    group_users: List[WavesBind],
) -> List[EndlessRankInfo]:
    """一次性并发获取所有有效用户的最新排行数据"""
    from .models import SlashSimpleRecord

    user_uid_pairs = []
    for user in group_users:
        if user.uid:
            uids = user.uid.split("_")
            for uid in uids:
                user_uid_pairs.append((user.user_id, uid))

    if not user_uid_pairs:
        return []

    # 一次性从数据库获取所有需要的记录
    records = await SlashSimpleRecord.get_records_by_users_and_challenge(
        user_uid_pairs=user_uid_pairs, challenge_id=12
    )

    rank_info_list = []
    for record in records:
        try:
            half_list = json.loads(record.halfList) if record.halfList else []
        except (json.JSONDecodeError, AttributeError):
            half_list = []

        rank_info_list.append(
            EndlessRankInfo(
                qid=record.user_id,
                uid=record.wavesId,
                name=record.name or hide_uid(record.wavesId),
                endless_score=record.score,
                rank_level=record.rank.lower() if record.rank else "",
                half_list=half_list,
            )
        )

    return rank_info_list


# --- 图像资源获取 ---
async def get_avatar(qid: str) -> Image.Image:
    """获取圆形用户头像，包含备用方案"""
    try:
        pic = await get_qq_avatar(qid, size=100)
        size = min(pic.size)
        pic = crop_center_img(pic, size, size)
        mask = Image.new("L", (size, size), 0)
        draw = ImageDraw.Draw(mask)
        draw.ellipse((0, 0, size, size), fill=255)
        output = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        output.paste(pic, (0, 0), mask)
        return output
    except Exception as e:
        logger.warning(f"获取QQ头像失败 (qid: {qid}): {e}")
        return await get_square_avatar(1203)


# --- 图像绘制核心函数 ---
def _draw_team_info(
    bar_bg: Image.Image,
    roles: List[dict],
    score: int,
    start_x: int,
    center_y: int,
    team_name: str,
    char_avatars_map: Dict[str, Image.Image],
):
    """在排行榜条上绘制单个队伍的信息 (同步函数)"""
    bar_draw = ImageDraw.Draw(bar_bg)
    avatar_size, avatar_gap = 48, 4

    bar_draw.text(
        (start_x, center_y - 10), team_name, (255, 255, 255, 200), waves_font_16, "lm"
    )
    bar_draw.text((start_x, center_y + 12), str(score), "white", waves_font_18, "lm")

    text_block_width = 60
    avatar_start_x = start_x + text_block_width

    for i, role in enumerate(roles[:3]):
        char_avatar = char_avatars_map.get(role.get("roleId"))
        if not char_avatar:
            continue

        char_avatar = char_avatar.resize((avatar_size, avatar_size), Image.LANCZOS)
        cx = avatar_start_x + i * (avatar_size + avatar_gap)
        cy = center_y - avatar_size // 2
        bar_bg.paste(char_avatar, (cx, cy), char_avatar)

        chain_num_str = str(role.get("chain", 0))
        chain_block_size = (18, 18)
        chain_block = Image.new("RGBA", chain_block_size, (0, 0, 0, 180))
        ImageDraw.Draw(chain_block).text(
            (9, 10), chain_num_str, "white", waves_font_14, "mm"
        )
        bar_bg.alpha_composite(
            chain_block, (cx + avatar_size - 18, cy + avatar_size - 18)
        )


def _create_rank_bar(
    rank_info: EndlessRankInfo,
    rank_num: int,
    user_avatar: Image.Image,
    char_avatars_map: Dict[str, Image.Image],
    is_self_row: bool = False,
) -> Image.Image:
    """创建单个排行榜条目图像 (同步函数)"""
    bar_h = 90
    bar_bg = BAR_IMG.copy().resize((BAR_IMG.width, bar_h))
    bar_draw = ImageDraw.Draw(bar_bg)
    center_y = bar_h // 2

    rank_color = {
        0: (255, 215, 0, 220),
        1: (192, 192, 192, 220),
        2: (205, 127, 50, 220),
    }.get(rank_num - 1, (100, 100, 100, 180))
    rank_str = "999+" if rank_num > 999 else str(rank_num)
    bar_draw.text((45, center_y), rank_str, rank_color, waves_font_30, "mm")

    player_info_x, avatar_size = 90, 64
    user_avatar = user_avatar.resize((avatar_size, avatar_size), Image.LANCZOS)
    bar_bg.paste(user_avatar, (player_info_x, center_y - avatar_size // 2), user_avatar)
    bar_draw.text(
        (player_info_x + avatar_size + 10, center_y - 10),
        rank_info.name,
        "white",
        waves_font_20,
        "lm",
    )

    uid_color = (255, 80, 80, 220) if is_self_row else (255, 255, 255, 180)
    bar_draw.text(
        (player_info_x + avatar_size + 10, center_y + 15),
        f"UID:{hide_uid(rank_info.uid)}",
        uid_color,
        waves_font_16,
        "lm",
    )

    if rank_info.half_list:
        team_block_width = 60 + 3 * 48 + 2 * 4
        team_gap = 20
        total_teams_width = (
            len(rank_info.half_list) * team_block_width
            + max(0, len(rank_info.half_list) - 1) * team_gap
        )
        teams_start_x = 340 + (400 - total_teams_width) // 2
        team_names = ["队伍一", "队伍二"]
        for i, half in enumerate(rank_info.half_list[:2]):
            _draw_team_info(
                bar_bg,
                half.get("roleList", []),
                half.get("score", 0),
                teams_start_x + i * (team_block_width + team_gap),
                center_y,
                team_names[i],
                char_avatars_map,
            )

    bar_draw.text(
        (830, center_y), str(rank_info.endless_score), SPECIAL_GOLD, waves_font_34, "mm"
    )
    if rank_info.rank_level:
        try:
            score_img = Image.open(
                TEXT_PATH / f"score_{rank_info.rank_level}.png"
            ).resize((50, 50), Image.LANCZOS)
            bar_bg.alpha_composite(score_img, (930 - 25, center_y - 25))
        except FileNotFoundError:
            bar_draw.text(
                (930, center_y),
                rank_info.rank_level.upper(),
                SPECIAL_GOLD,
                waves_font_40,
                "mm",
            )
    else:
        bar_draw.text((930, center_y), "-", GREY, waves_font_40, "mm")

    return bar_bg


# --- 主函数 ---
async def draw_endless_rank_img(bot: Bot, ev: Event) -> Union[str, bytes]:
    users = await WavesBind.get_group_all_uid(ev.group_id)

    if not users:
        return f"[鸣潮] 群【{ev.group_id}】暂无登录用户。"

    rank_info_list = await get_all_endless_rank_info(list(users))

    if not rank_info_list:
        msg = [f"[鸣潮] 群【{ev.group_id}】暂无有效的无尽挑战数据。"]
        msg.append(f"请使用【{PREFIX}无尽】上传更新数据后再试。")
        return "\n".join(msg)

    rank_info_list.sort(key=lambda i: i.endless_score, reverse=True)

    self_uid = await WavesBind.get_uid_by_game(ev.user_id, ev.bot_id)
    self_rank_info = None
    self_rank_index = -1
    for i, rank in enumerate(rank_info_list):
        if rank.uid == self_uid:
            self_rank_info = rank
            self_rank_index = i
            break

    display_list = rank_info_list[:RANK_LENGTH]
    show_self_at_end = self_rank_info is not None

    users_to_draw = display_list[:]
    if show_self_at_end:
        # 确保自己的信息在待绘制列表中，以获取头像
        if not any(u.uid == self_rank_info.uid for u in users_to_draw):
            users_to_draw.append(self_rank_info)

    user_qids_needed = {rank.qid for rank in users_to_draw}
    char_ids_needed = {
        role.get("roleId")
        for rank in users_to_draw
        for team in rank.half_list
        for role in team.get("roleList", [])
        if role.get("roleId")
    }

    user_avatar_tasks = {qid: get_avatar(qid) for qid in user_qids_needed}
    char_avatar_tasks = {cid: get_square_avatar(cid) for cid in char_ids_needed}

    user_avatars_results, char_avatars_results = await asyncio.gather(
        asyncio.gather(*user_avatar_tasks.values()),
        asyncio.gather(*char_avatar_tasks.values()),
    )
    user_avatars_map = dict(zip(user_avatar_tasks.keys(), user_avatars_results))
    char_avatars_map = dict(zip(char_avatar_tasks.keys(), char_avatars_results))

    bar_h, header_h, title_h = 90, 40, 200
    total_rows = len(display_list) + (1 if show_self_at_end else 0)
    img_width, h = 1050, title_h + header_h + total_rows * bar_h + 80
    card_img = get_waves_bg(img_width, h, "bg3")
    draw = ImageDraw.Draw(card_img)

    logo_copy = LOGO_IMG.copy()
    logo_copy.thumbnail((150, 150), Image.LANCZOS)
    card_img.alpha_composite(logo_copy, dest=(50, 55))
    draw.text((img_width // 2, 80), "海蚀无尽排行", "white", waves_font_40, "mm")
    draw.text(
        (img_width // 2, 125),
        "数据来源: 千咲 · 群内排行",
        SPECIAL_GOLD,
        waves_font_20,
        "mm",
    )

    if rank_info_list:
        total_players = len(rank_info_list)
        avg_score = (
            sum(i.endless_score for i in rank_info_list) // total_players
            if total_players
            else 0
        )
        max_score_info = rank_info_list[0]
        stats_text = (
            f"最高分: {max_score_info.endless_score} (by {max_score_info.name})    "
            f"平均分: {avg_score}"
        )
        draw.text(
            (img_width // 2, 165),
            stats_text,
            (255, 255, 255, 200),
            waves_font_18,
            "mm",
        )

    header_y = title_h + header_h / 2
    centered_x = (img_width - BAR_IMG.width) // 2
    headers = {45: "排名", 190: "玩家信息", 520: "队伍阵容", 830: "总评分", 930: "评级"}
    for x, text in headers.items():
        draw.text(
            (centered_x + x, header_y), text, (255, 255, 255, 180), waves_font_16, "mm"
        )

    y_pos_start = title_h + header_h
    for i, rank in enumerate(display_list):
        user_avatar = user_avatars_map.get(rank.qid)
        if user_avatar:
            bar_image = _create_rank_bar(
                rank, i + 1, user_avatar, char_avatars_map, is_self_row=False
            )
            card_img.paste(bar_image, (centered_x, y_pos_start + i * bar_h), bar_image)

    if show_self_at_end:
        self_avatar = user_avatars_map.get(self_rank_info.qid)
        if self_avatar:
            bar_image = _create_rank_bar(
                self_rank_info,
                self_rank_index + 1,
                self_avatar,
                char_avatars_map,
                is_self_row=True,
            )
            card_img.paste(
                bar_image,
                (centered_x, y_pos_start + len(display_list) * bar_h),
                bar_image,
            )

    card_img = add_footer(card_img)
    return await convert_img(card_img)
