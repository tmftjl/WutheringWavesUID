import asyncio
import time
from pathlib import Path
from typing import Optional, Union

from PIL import Image, ImageDraw

from gsuid_core.bot import Bot
from gsuid_core.logger import logger
from gsuid_core.models import Event
from gsuid_core.utils.image.convert import convert_img
from gsuid_core.utils.image.image_tools import crop_center_img

from ..utils.cache import TimedCache
from ..utils.database.models import WavesBind, WavesRoleData
from ..utils.fonts.waves_fonts import (
    waves_font_12,
    waves_font_16,
    waves_font_18,
    waves_font_20,
    waves_font_28,
    waves_font_30,
    waves_font_34,
    waves_font_58,
)
from ..utils.image import (
    GREY,
    RED,
    SPECIAL_GOLD,
    add_footer,
    get_ICON,
    get_qq_avatar,
    get_square_avatar,
    get_waves_bg,
)
from ..wutheringwaves_config import WutheringWavesConfig

TEXT_PATH = Path(__file__).parent / "texture2d"
avatar_mask = Image.open(TEXT_PATH / "avatar_mask.png")
char_mask = Image.open(TEXT_PATH / "char_mask.png")
pic_cache = TimedCache(600, 200)


async def draw_group_rank(bot: Bot, ev: Event) -> Union[str, bytes]:
    """绘制群练度排行

    排序规则：以群内用户所有角色声骸分数总和（分数>=175）为排序
    """
    if not ev.group_id:
        return "请在群聊中使用本功能"

    start_time = time.time()
    logger.info(f"[draw_group_rank] 开始获取群练度排行数据")

    self_uid = await WavesBind.get_uid_by_game(ev.user_id, ev.bot_id)
    if not self_uid:
        self_uid = ""

    # 获取群内所有绑定的UID
    group_binds = await WavesBind.get_group_all_uid(ev.group_id)
    if not group_binds:
        return "群内暂无用户绑定UID"

    # 收集所有UID
    all_uids = []
    uid_to_user_id = {}
    for bind in group_binds:
        if bind.uid:
            for uid in bind.uid.split("_"):
                if uid:
                    all_uids.append(uid)
                    uid_to_user_id[uid] = bind.user_id

    if not all_uids:
        return "群内暂无用户绑定UID"

    # 获取所有角色数据
    all_roles = await WavesRoleData.get_all_roles_by_uid_list(all_uids)

    # 按UID分组并计算总分
    uid_stats = {}
    for role in all_roles:
        if role.score < 175.0:
            continue

        if role.uid not in uid_stats:
            uid_stats[role.uid] = {
                "total_score": 0.0,
                "char_count": 0,
                "char_scores": []
            }

        uid_stats[role.uid]["total_score"] += role.score
        uid_stats[role.uid]["char_count"] += 1
        uid_stats[role.uid]["char_scores"].append({
            "role_id": role.role_id,
            "role_name": role.role_name,
            "score": role.score
        })

    if not uid_stats:
        return "[鸣潮] 群内暂无练度排行数据\n请先使用刷新面板功能后再试！"

    # 转换为列表并排序
    rank_data_list = []
    for uid, stats in uid_stats.items():
        rank_data_list.append({
            "uid": uid,
            "user_id": uid_to_user_id.get(uid, uid),
            "total_score": stats["total_score"],
            "char_count": stats["char_count"],
            "char_scores": sorted(stats["char_scores"], key=lambda x: x["score"], reverse=True)[:10]
        })

    # 按总分排序
    rank_data_list.sort(key=lambda x: x["total_score"], reverse=True)

    # 添加排名
    for idx, data in enumerate(rank_data_list):
        data["rank"] = idx + 1

    # 设置图像尺寸
    width = 1300
    text_bar_height = 130
    item_spacing = 120
    header_height = 510
    footer_height = 50

    totalNum = len(rank_data_list)

    # 计算所需的总高度
    total_height = (
        header_height + text_bar_height + item_spacing * totalNum + footer_height
    )

    # 创建带背景的画布 - 使用bg9
    card_img = get_waves_bg(width, total_height, "bg9")

    text_bar_img = Image.new("RGBA", (width, 130), color=(0, 0, 0, 0))
    text_bar_draw = ImageDraw.Draw(text_bar_img)
    # 绘制深灰色背景
    bar_bg_color = (36, 36, 41, 230)
    text_bar_draw.rounded_rectangle(
        [20, 20, width - 40, 110], radius=8, fill=bar_bg_color
    )

    # 绘制顶部的金色高亮线
    accent_color = (203, 161, 95)
    text_bar_draw.rectangle([20, 20, width - 40, 26], fill=accent_color)

    # 左侧标题
    text_bar_draw.text((40, 60), "排行说明", GREY, waves_font_28, "lm")
    text_bar_draw.text(
        (185, 50),
        "1. 综合所有角色的声骸分数。具备声骸套装的角色，全量刷新面板后生效。",
        SPECIAL_GOLD,
        waves_font_20,
        "lm",
    )
    text_bar_draw.text(
        (185, 85), "2. 显示前10个最强角色", SPECIAL_GOLD, waves_font_20, "lm"
    )

    # 备注
    temp_notes = "排行标准：以所有角色声骸分数总和（角色分数>=175）为排序的综合排名"
    text_bar_draw.text((1260, 100), temp_notes, SPECIAL_GOLD, waves_font_16, "rm")

    card_img.alpha_composite(text_bar_img, (0, header_height))

    # 导入必要的图片资源
    bar = Image.open(TEXT_PATH / "bar1.png")

    # 获取头像
    tasks = [get_avatar(detail["user_id"]) for detail in rank_data_list]
    results = await asyncio.gather(*tasks)

    # 绘制排行条目
    for rank_temp_index, temp in enumerate(zip(rank_data_list, results)):
        detail, role_avatar = temp
        y_pos = header_height + 130 + rank_temp_index * item_spacing

        # 创建条目背景
        bar_bg = bar.copy()
        bar_bg.paste(role_avatar, (100, 0), role_avatar)
        bar_draw = ImageDraw.Draw(bar_bg)

        # 绘制排名
        rank_id = detail["rank"]
        rank_color = (54, 54, 54)
        if rank_id == 1:
            rank_color = (255, 0, 0)
        elif rank_id == 2:
            rank_color = (255, 180, 0)
        elif rank_id == 3:
            rank_color = (185, 106, 217)

        # 排名背景
        info_rank = Image.new("RGBA", (50, 50), color=(255, 255, 255, 0))
        rank_draw = ImageDraw.Draw(info_rank)
        rank_draw.rounded_rectangle(
            [0, 0, 50, 50], radius=8, fill=rank_color + (int(0.9 * 255),)
        )
        rank_draw.text((25, 25), f"{rank_id}", "white", waves_font_34, "mm")
        bar_bg.alpha_composite(info_rank, (40, 35))

        # 绘制角色数量
        char_count = detail["char_count"]
        bar_draw.text((210, 45), "角色数:", (255, 255, 255), waves_font_18, "lm")
        bar_draw.text((280, 45), f"{char_count}", RED, waves_font_20, "lm")

        # UID
        uid_color = "white"
        if detail["uid"] == self_uid:
            uid_color = RED
        bar_draw.text(
            (210, 75), f"UID: {detail['uid']}", uid_color, waves_font_18, "lm"
        )

        # 总分数
        bar_draw.text(
            (1180, 45),
            f"{detail['total_score']:.1f}",
            (255, 255, 255),
            waves_font_34,
            "mm",
        )
        bar_draw.text((1180, 75), "总分", "white", waves_font_16, "mm")

        # 绘制角色信息
        char_scores = detail["char_scores"]
        if char_scores:
            # 按分数排序，取前10名
            sorted_chars = sorted(
                char_scores, key=lambda x: x["score"], reverse=True
            )[:10]

            char_size = 55
            char_spacing = 61
            char_start_x = 400
            char_start_y = 30

            for i, char in enumerate(sorted_chars):
                char_x = char_start_x + i * char_spacing

                # 获取角色头像
                char_avatar = await get_square_avatar(char["role_id"])
                char_avatar = char_avatar.resize((char_size, char_size))

                # 应用圆形遮罩
                char_mask_img = Image.open(TEXT_PATH / "char_mask.png")
                char_mask_resized = char_mask_img.resize((char_size, char_size))
                char_avatar_masked = Image.new("RGBA", (char_size, char_size))
                char_avatar_masked.paste(char_avatar, (0, 0), char_mask_resized)

                # 粘贴头像
                bar_bg.paste(
                    char_avatar_masked, (char_x, char_start_y), char_avatar_masked
                )

                # 绘制分数
                bar_draw.text(
                    (char_x + char_size // 2, char_start_y + char_size + 2),
                    f"{int(char['score'])}",
                    SPECIAL_GOLD,
                    waves_font_12,
                    "mm",
                )

            # 显示最高分
            if sorted_chars:
                best_score = f"{int(sorted_chars[0]['score'])} "
                bar_draw.text((1080, 45), best_score, "lightgreen", waves_font_30, "mm")
                bar_draw.text((1080, 75), "最高分", "white", waves_font_16, "mm")

        # 贴到背景
        card_img.paste(bar_bg, (0, y_pos), bar_bg)

    # title
    title_bg = Image.open(TEXT_PATH / "totalrank.jpg")
    title_bg = title_bg.crop((0, 0, width, 500))

    # icon
    icon = get_ICON()
    icon = icon.resize((128, 128))
    title_bg.paste(icon, (60, 240), icon)

    # title
    title_text = "#群练度排行"
    title_bg_draw = ImageDraw.Draw(title_bg)
    title_bg_draw.text((220, 290), title_text, "white", waves_font_58, "lm")

    # 统计信息
    stat_info = f"共{totalNum}人参与排行"
    title_bg_draw.text((220, 350), stat_info, SPECIAL_GOLD, waves_font_20, "lm")

    # 遮罩
    char_mask_img = Image.open(TEXT_PATH / "char_mask.png").convert("RGBA")
    # 根据width扩图
    char_mask_img = char_mask_img.resize((width, char_mask_img.height * width // char_mask_img.width))
    char_mask_img = char_mask_img.crop((0, char_mask_img.height - 500, width, char_mask_img.height))
    char_mask_temp = Image.new("RGBA", char_mask_img.size, (0, 0, 0, 0))
    char_mask_temp.paste(title_bg, (0, 0), char_mask_img)

    card_img.paste(char_mask_temp, (0, 0), char_mask_temp)

    card_img = add_footer(card_img)

    logger.info(f"[draw_group_rank] 耗时: {time.time() - start_time:.2f}秒")
    return await convert_img(card_img)


async def get_avatar(
    qid: Optional[str],
) -> Image.Image:
    # 检查qid 为纯数字
    if qid and qid.isdigit():
        if WutheringWavesConfig.get_config("QQPicCache").data:
            pic = pic_cache.get(qid)
            if not pic:
                pic = await get_qq_avatar(qid, size=100)
                pic_cache.set(qid, pic)
        else:
            pic = await get_qq_avatar(qid, size=100)
            pic_cache.set(qid, pic)
        pic_temp = crop_center_img(pic, 120, 120)

        img = Image.new("RGBA", (180, 180))
        avatar_mask_temp = avatar_mask.copy()
        mask_pic_temp = avatar_mask_temp.resize((120, 120))
        img.paste(pic_temp, (0, -5), mask_pic_temp)
    else:
        default_avatar_char_id = "1505"
        pic = await get_square_avatar(default_avatar_char_id)

        pic_temp = Image.new("RGBA", pic.size)
        pic_temp.paste(pic.resize((160, 160)), (10, 10))
        pic_temp = pic_temp.resize((160, 160))

        avatar_mask_temp = avatar_mask.copy()
        mask_pic_temp = Image.new("RGBA", avatar_mask_temp.size)
        mask_pic_temp.paste(avatar_mask_temp, (-20, -45), avatar_mask_temp)
        mask_pic_temp = mask_pic_temp.resize((160, 160))

        img = Image.new("RGBA", (180, 180))
        img.paste(pic_temp, (0, 0), mask_pic_temp)

    return img
