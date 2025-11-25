import asyncio
from pathlib import Path
from typing import List, Union

from PIL import Image, ImageDraw

from gsuid_core.bot import Bot
from gsuid_core.logger import logger
from gsuid_core.models import Event
from gsuid_core.utils.image.convert import convert_img

from ..utils.api.model import RoleDetailData
from ..utils.database.models import WavesBind, WavesRoleData
from ..utils.fonts.waves_fonts import (
    waves_font_16,
    waves_font_18,
    waves_font_20,
    waves_font_24,
    waves_font_30,
    waves_font_34,
    waves_font_40,
    waves_font_44,
)
from ..utils.image import (
    CHAIN_COLOR,
    GREY,
    RED,
    SPECIAL_GOLD,
    add_footer,
    get_qq_avatar,
    get_square_avatar,
    get_waves_bg,
)
from ..utils.name_convert import alias_to_char_name, char_name_to_char_id
from ..utils.resource.constant import SPECIAL_CHAR, SPECIAL_CHAR_NAME
from ..wutheringwaves_config import PREFIX

TEXT_PATH = Path(__file__).parent / "texture2d"
avatar_mask = Image.open(TEXT_PATH / "avatar_mask.png")
pic_cache = {}


def get_chain_name(chain: int) -> str:
    """获取命座名称"""
    chain_names = ["初", "一", "二", "三", "四", "五", "六"]
    return chain_names[min(chain, 6)]


async def get_avatar(user_id: str) -> Image.Image:
    """获取用户头像"""
    if user_id in pic_cache:
        return pic_cache[user_id]

    try:
        avatar = await get_qq_avatar(qid=user_id)
        avatar = avatar.resize((110, 110))
        pic_cache[user_id] = avatar
        return avatar
    except Exception as e:
        logger.exception(f"获取头像失败 user_id={user_id}:", e)
        # 返回默认头像
        default_avatar = Image.new("RGBA", (110, 110), (128, 128, 128, 255))
        pic_cache[user_id] = default_avatar
        return default_avatar


async def draw_all_rank_card_local(
    bot: Bot, ev: Event, char: str, rank_type: str, pages: int
) -> Union[str, bytes]:
    """使用本地数据库绘制全局角色排行"""

    # 获取角色ID
    char_id = char_name_to_char_id(char)
    if not char_id:
        return f"[鸣潮] 角色名【{char}】无法找到, 可能暂未适配, 请先检查输入是否正确！\n"

    char_name = alias_to_char_name(char)

    # 处理漂泊者特殊情况
    if char_id in SPECIAL_CHAR:
        find_char_id = SPECIAL_CHAR[char_id]
    else:
        find_char_id = char_id

    # 从数据库获取排行数据
    rank_type_db = "damage" if rank_type == "伤害" else "score"
    rank_data_list, total_count = await WavesRoleData.get_global_role_rank(
        role_id=str(find_char_id),
        rank_type=rank_type_db,
        page=pages,
        page_size=20
    )

    if not rank_data_list:
        return f"[鸣潮] 暂无【{char_name}】的排行数据\n请先使用【{PREFIX}刷新面板】后再使用此功能！"

    # 获取自己的排名（如果有）
    self_uid = None
    self_rank = None
    try:
        self_uid = await WavesBind.get_uid_by_game(ev.user_id, ev.bot_id)
        if self_uid:
            self_rank = await WavesRoleData.get_role_rank_position(
                uid=self_uid,
                role_id=str(find_char_id),
                rank_type=rank_type_db
            )
    except Exception:
        pass

    # 设置图像尺寸
    width = 1300
    item_spacing = 120
    header_height = 510
    footer_height = 50
    char_list_len = len(rank_data_list)

    # 计算所需的总高度
    total_height = header_height + item_spacing * char_list_len + footer_height

    # 创建带背景的画布
    card_img = get_waves_bg(width, total_height, "bg9")

    # title
    title_bg = Image.open(TEXT_PATH / "slash.jpg")
    title_bg = title_bg.crop((0, 0, width, 500))

    # icon - 使用角色头像
    icon = await get_square_avatar(find_char_id)
    icon = icon.resize((128, 128))
    title_bg.paste(icon, (60, 240), icon)

    # title
    title_text = f"#{char_name}{rank_type}总排行"
    title_bg_draw = ImageDraw.Draw(title_bg)
    title_bg_draw.text((220, 290), title_text, "white", waves_font_44, "lm")

    # 显示总数和页码
    page_info = f"第{pages}页  共{total_count}人"
    title_bg_draw.text((width - 100, 290), page_info, SPECIAL_GOLD, waves_font_24, "rm")

    # 遮罩
    char_mask = Image.open(TEXT_PATH / "char_mask.png").convert("RGBA")
    char_mask = char_mask.resize((width, char_mask.height * width // char_mask.width))
    char_mask = char_mask.crop((0, char_mask.height - 500, width, char_mask.height))
    char_mask_temp = Image.new("RGBA", char_mask.size, (0, 0, 0, 0))
    char_mask_temp.paste(title_bg, (0, 0), char_mask)

    card_img.paste(char_mask_temp, (0, 0), char_mask_temp)

    # 获取所有user_id对应的绑定信息
    uid_to_user_id = {}
    all_binds = await WavesBind.get_all_bind()
    for bind in all_binds:
        if bind.uid:
            for uid in bind.uid.split("_"):
                if uid:
                    uid_to_user_id[uid] = bind.user_id

    # 获取所有头像
    tasks = [get_avatar(uid_to_user_id.get(role_data.uid, role_data.uid)) for role_data in rank_data_list]
    results = await asyncio.gather(*tasks)

    # 绘制每个排行项
    for rank_index, (role_data, role_avatar) in enumerate(zip(rank_data_list, results)):
        try:
            role_bg = Image.open(TEXT_PATH / "bar1.png")
            role_bg.paste(role_avatar, (100, 0), role_avatar)
            role_bg_draw = ImageDraw.Draw(role_bg)

            # 解析角色数据
            role_detail = RoleDetailData(**role_data.data) if role_data.data else None

            # 添加排名显示
            rank_id = (pages - 1) * 20 + rank_index + 1
            rank_color = (54, 54, 54)
            if rank_id == 1:
                rank_color = (255, 0, 0)
            elif rank_id == 2:
                rank_color = (255, 180, 0)
            elif rank_id == 3:
                rank_color = (185, 106, 217)

            def draw_rank_id(rank_id, size=(50, 50), draw=(24, 24), dest=(40, 30)):
                rank_box = Image.new("RGBA", size, (0, 0, 0, 0))
                rank_box_draw = ImageDraw.Draw(rank_box)
                rank_box_draw.rounded_rectangle(
                    [0, 0, size[0], size[1]], radius=8, fill=rank_color + (230,)
                )
                rank_box_draw.text(draw, f"#{rank_id}", "white", waves_font_30, "mm")
                role_bg.alpha_composite(rank_box, dest)

            draw_rank_id(rank_id)

            # 高亮自己的排名
            if self_uid and role_data.uid == self_uid:
                highlight_box = Image.new("RGBA", (role_bg.width, role_bg.height), (255, 215, 0, 30))
                role_bg = Image.alpha_composite(role_bg, highlight_box)
                role_bg_draw = ImageDraw.Draw(role_bg)

            # UID (隐藏部分)
            uid_display = f"{role_data.uid[:3]}****{role_data.uid[-3:]}"
            role_bg_draw.text((240, 30), uid_display, GREY, waves_font_20, "lm")

            if role_detail:
                # 等级
                level_text = f"Lv.{role_detail.level}"
                role_bg_draw.text((240, 60), level_text, "white", waves_font_18, "lm")

                # 命座
                chain_num = role_detail.get_chain_num()
                chain_name = get_chain_name(chain_num)
                info_block = Image.new("RGBA", (46, 24), color=(255, 255, 255, 0))
                info_block_draw = ImageDraw.Draw(info_block)
                fill = CHAIN_COLOR[chain_num] + (int(0.9 * 255),)
                info_block_draw.rounded_rectangle([0, 0, 46, 24], radius=6, fill=fill)
                info_block_draw.text((23, 12), chain_name, "white", waves_font_18, "mm")
                role_bg.alpha_composite(info_block, (340, 57))

            # 评分
            if role_data.score > 0:
                score_text = f"{role_data.score:.2f}"
                role_bg_draw.text((500, 50), "声骸评分", SPECIAL_GOLD, waves_font_18, "mm")
                role_bg_draw.text((500, 80), score_text, "white", waves_font_34, "mm")

            # 伤害
            if role_data.damage > 0:
                damage_text = f"{int(role_data.damage):,}"
                role_bg_draw.text((700, 50), "期望伤害", SPECIAL_GOLD, waves_font_18, "mm")
                role_bg_draw.text((700, 80), damage_text, "white", waves_font_34, "mm")

            # 粘贴到主画布
            y_pos = header_height + rank_index * item_spacing
            card_img.alpha_composite(role_bg, (0, y_pos))

        except Exception as e:
            logger.exception(f"绘制排行项失败 uid={role_data.uid}:", e)
            continue

    # 添加页脚
    card_img = add_footer(card_img)

    return await convert_img(card_img)
