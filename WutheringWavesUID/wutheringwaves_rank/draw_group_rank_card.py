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


from typing import Optional, Union, List
import time
import asyncio
from PIL import Image, ImageDraw

async def draw_group_role_rank(bot: Bot, ev: Event, role_id: str, role_name: str) -> Union[str, bytes]:
    """绘制群内特定角色练度排行 (Top 20 + 自身)"""
    if not ev.group_id: return "请在群聊中使用本功能"
    
    start_time = time.time()
    logger.info(f"[draw_group_role_rank] 开始获取 {role_name} 排行数据")

    # 1. 获取基础信息
    self_uid = await WavesBind.get_uid_by_game(ev.user_id, ev.bot_id) or ""
    group_binds = await WavesBind.get_group_all_uid(ev.group_id)
    
    if not group_binds: return "群内暂无用户绑定UID"

    # 2. 收集群内所有UID及对应User ID
    all_uids, uid_to_user_id = [], {}
    for bind in group_binds:
        if bind.uid:
            for uid in bind.uid.split("_"):
                if uid:
                    all_uids.append(uid)
                    uid_to_user_id[uid] = bind.user_id

    if not all_uids: return "群内暂无用户绑定UID"

    # 3. 数据库查询：获取指定角色排行
    rank_data = await WavesRoleData.get_role_rank_by_group(
        session=None, # 如果外层有session管理则传入，否则依赖装饰器
        uid_list=all_uids,
        role_id=role_id,
        rank_type="score"
    )

    if not rank_data:
        return f"[鸣潮] 群内暂无 {role_name} 数据\n请先使用刷新面板功能！"

    # 4. 数据处理：构建排名列表
    full_list = []
    for idx, data in enumerate(rank_data):
        full_list.append({
            "data": data,
            "rank": idx + 1,
            "user_id": uid_to_user_id.get(data.uid, ""),
            "is_self": str(data.uid) == str(self_uid)
        })

    # 截取前20名
    display_list = full_list[:20]
    
    # 检查自身是否在榜，若不在前20则追加
    self_entry = next((x for x in full_list if x["is_self"]), None)
    has_appended = False
    if self_entry and self_entry["rank"] > 20:
        display_list.append(self_entry)
        has_appended = True

    # 5. 绘图初始化
    total_num = len(display_list)
    # 若有追加，增加额外间隔高度
    extra_h = 40 if has_appended else 0
    
    # 尺寸定义
    width, header_h, footer_h = 1300, 510, 50
    text_bar_h, item_spacing = 130, 120
    total_height = header_h + text_bar_h + item_spacing * total_num + footer_h + extra_h

    card_img = get_waves_bg(width, total_height, "bg9")

    # 6. 绘制顶部说明栏
    text_bar = Image.new("RGBA", (width, 130), (0, 0, 0, 0))
    bar_draw = ImageDraw.Draw(text_bar)
    bar_draw.rounded_rectangle([20, 20, width - 40, 110], radius=8, fill=(36, 36, 41, 230))
    bar_draw.rectangle([20, 20, width - 40, 26], fill=(203, 161, 95))
    
    bar_draw.text((40, 60), f"{role_name}排行", GREY, waves_font_28, "lm")
    bar_draw.text((200, 50), "1. 仅展示群内前20名，以及自己的排名", SPECIAL_GOLD, waves_font_20, "lm")
    bar_draw.text((200, 85), "2. 数据来源：请在群内使用【刷新面板】更新数据", SPECIAL_GOLD, waves_font_20, "lm")
    card_img.alpha_composite(text_bar, (0, header_h))

    # 7. 批量获取头像
    avatars = await asyncio.gather(*[get_avatar(item["user_id"]) for item in display_list])
    bar_res = Image.open(TEXT_PATH / "bar1.png")

    # 8. 循环绘制列表
    for i, (item, avatar) in enumerate(zip(display_list, avatars)):
        d_data = item["data"]
        rank_idx = item["rank"]
        
        # 计算Y坐标，追加的条目增加间隔
        y_pos = header_h + 130 + i * item_spacing
        if has_appended and i == len(display_list) - 1:
            y_pos += 40
            # 绘制分割线
            line_draw = ImageDraw.Draw(card_img)
            line_draw.line([(100, y_pos - 20), (width - 100, y_pos - 20)], fill=(255, 255, 255, 80), width=2)

        # 条目背景
        item_bg = bar_res.copy()
        item_bg.paste(avatar, (100, 0), avatar)
        item_draw = ImageDraw.Draw(item_bg)

        # 排名图标颜色
        r_color = (54, 54, 54)
        if rank_idx == 1: r_color = (255, 0, 0)
        elif rank_idx == 2: r_color = (255, 180, 0)
        elif rank_idx == 3: r_color = (185, 106, 217)

        # 绘制排名
        rank_icon = Image.new("RGBA", (50, 50), (0, 0, 0, 0))
        r_draw = ImageDraw.Draw(rank_icon)
        r_draw.rounded_rectangle([0, 0, 50, 50], radius=8, fill=r_color + (230,))
        r_draw.text((25, 25), str(rank_idx), "white", waves_font_34, "mm")
        item_bg.alpha_composite(rank_icon, (40, 35))

        # 角色名与UID (如果是自己则UID变红)
        uid_color = RED if item["is_self"] else "white"
        item_draw.text((210, 45), role_name, (255, 255, 255), waves_font_28, "lm")
        item_draw.text((210, 85), f"UID: {d_data.uid}", uid_color, waves_font_18, "lm")

        # 数值展示 (分数 & 伤害期望)
        item_draw.text((1180, 45), f"{d_data.score:.1f}", (255, 255, 255), waves_font_34, "mm")
        item_draw.text((1180, 75), "评分", "white", waves_font_16, "mm")
        
        if hasattr(d_data, 'damage'):
            item_draw.text((1000, 45), f"{int(d_data.damage)}", SPECIAL_GOLD, waves_font_30, "mm")
            item_draw.text((1000, 75), "期望伤害", "white", waves_font_16, "mm")

        card_img.paste(item_bg, (0, y_pos), item_bg)

    # 9. 标题与页脚
    title_bg = Image.open(TEXT_PATH / "totalrank.jpg").crop((0, 0, width, 500))
    icon = get_ICON().resize((128, 128))
    title_bg.paste(icon, (60, 240), icon)
    
    t_draw = ImageDraw.Draw(title_bg)
    t_draw.text((220, 290), f"#{role_name}群排行", "white", waves_font_58, "lm")
    t_draw.text((220, 350), f"共{len(rank_data)}人参与排行", SPECIAL_GOLD, waves_font_20, "lm")

    # 顶部遮罩处理
    mask = Image.open(TEXT_PATH / "char_mask.png").convert("RGBA")
    mask = mask.resize((width, mask.height * width // mask.width))
    mask = mask.crop((0, mask.height - 500, width, mask.height))
    
    mask_temp = Image.new("RGBA", mask.size, (0, 0, 0, 0))
    mask_temp.paste(title_bg, (0, 0), mask)
    card_img.paste(mask_temp, (0, 0), mask_temp)

    card_img = add_footer(card_img)
    
    logger.info(f"[draw_group_role_rank] 完成，耗时: {time.time() - start_time:.2f}s")
    return await convert_img(card_img)