import asyncio
import time
from pathlib import Path
from typing import List, Optional, Union

from PIL import Image, ImageDraw
from pydantic import BaseModel

from gsuid_core.bot import Bot
from gsuid_core.logger import logger
from gsuid_core.models import Event
from gsuid_core.utils.image.convert import convert_img
from gsuid_core.utils.image.image_tools import crop_center_img

from ..utils.api.model import RoleDetailData, WeaponData
from ..utils.cache import TimedCache
from ..utils.calc import WuWaCalc
from ..utils.calculate import (
    get_calc_map,
    get_total_score_bg,
)
from ..utils.damage.abstract import DamageRankRegister
from ..utils.database.models import WavesBind, WavesRoleData
from ..utils.fonts.waves_fonts import (
    waves_font_14,
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
    WEAPON_RESONLEVEL_COLOR,
    add_footer,
    get_attribute,
    get_attribute_effect,
    get_qq_avatar,
    get_role_pile_old,
    get_square_avatar,
    get_square_weapon,
    get_waves_bg,
)
from ..utils.name_convert import alias_to_char_name, char_name_to_char_id
from ..utils.resource.constant import SPECIAL_CHAR, SPECIAL_CHAR_NAME
from ..utils.util import hide_uid
from ..wutheringwaves_config import PREFIX, WutheringWavesConfig

rank_length = 20  # 单页排行长度
TEXT_PATH = Path(__file__).parent / "texture2d"
TITLE_I = Image.open(TEXT_PATH / "title.png")
avatar_mask = Image.open(TEXT_PATH / "avatar_mask.png")
weapon_icon_bg_3 = Image.open(TEXT_PATH / "weapon_icon_bg_3.png")
weapon_icon_bg_4 = Image.open(TEXT_PATH / "weapon_icon_bg_4.png")
weapon_icon_bg_5 = Image.open(TEXT_PATH / "weapon_icon_bg_5.png")
char_mask = Image.open(TEXT_PATH / "char_mask.png")
logo_img = Image.open(TEXT_PATH / "logo_small_2.png")
bar_img = Image.open(TEXT_PATH / "bar.png")
pic_cache = TimedCache(86400, 200)


class RankInfo(BaseModel):
    roleDetail: RoleDetailData
    qid: str
    uid: str
    level: int
    chain: int
    chainName: str
    score: float
    score_bg: str
    expected_damage: str
    expected_damage_int: int
    sonata_name: str
    rank_id: int  # 显式存储排名


async def get_avatar(
    ev: Event,
    qid: Optional[Union[int, str]],
    char_id: Union[int, str],
) -> Image.Image:
    """获取圆形头像，修复遮罩边缘"""
    if ev.bot_id == "onebot":
        if WutheringWavesConfig.get_config("QQPicCache").data:
            pic = pic_cache.get(qid)
            if not pic:
                pic = await get_qq_avatar(qid, size=100)
                pic_cache.set(qid, pic)
        else:
            pic = await get_qq_avatar(qid, size=100)
            pic_cache.set(qid, pic)
        
        pic_temp = crop_center_img(pic, 120, 120)
        pic_temp = pic_temp.resize((120, 120))
        
        img = Image.new("RGBA", (180, 180), (0, 0, 0, 0))
        mask = avatar_mask.copy().resize((120, 120)).convert("L")
        img.paste(pic_temp, (30, 30), mask) 
        
    else:
        pic = await get_square_avatar(char_id)
        pic_temp = pic.resize((160, 160))
        
        img = Image.new("RGBA", (180, 180), (0, 0, 0, 0))
        mask = avatar_mask.copy().resize((160, 160)).convert("L")
        img.paste(pic_temp, (10, 10), mask)

    return img


def get_weapon_icon_bg(star: int = 3) -> Image.Image:
    if star < 3:
        star = 3
    if star == 3:
        return weapon_icon_bg_3.copy()
    elif star == 4:
        return weapon_icon_bg_4.copy()
    else:
        return weapon_icon_bg_5.copy()


async def process_rank_data(role_data, rank_id, uid_to_user_id) -> Optional[RankInfo]:
    """处理单条排行数据转换为RankInfo对象"""
    try:
        role_detail = RoleDetailData(**role_data.data) if role_data.data else None
        if not role_detail:
            return None

        user_id = uid_to_user_id.get(role_data.uid, role_data.uid)

        # 1. 重新计算属性以获取 calc_temp (用于评级 S/A/B/C)
        calc = WuWaCalc(role_detail)
        calc.phantom_pre = calc.prepare_phantom()
        calc.phantom_card = calc.enhance_summation_phantom_value(calc.phantom_pre)
        
        # 获取毕业标准 Map
        calc_temp = get_calc_map(
            calc.phantom_card,
            role_detail.role.roleName,
            role_detail.role.roleId,
        )

        # 计算合鸣(声骸)效果
        sonata_name = ""
        # 优先获取5件套效果
        ph_detail = calc.phantom_card.get("ph_detail", [])
        if isinstance(ph_detail, list):
            for ph in ph_detail:
                if ph.get("ph_num") == 5:
                    sonata_name = ph.get("ph_name", "")
                    break
                # 兼容逻辑
                if ph.get("isFull") and not sonata_name:
                    sonata_name = ph.get("ph_name", "")

        # 2. 计算评级背景 (传入 calc_temp)
        score_bg_val = "C"
        if role_data.score > 0:
            score_bg_val = get_total_score_bg(
                role_detail.role.roleName,
                role_data.score,
                calc_temp  # 这里如果不传，就会全是C
            )

        return RankInfo(
            roleDetail=role_detail,
            qid=str(user_id),
            uid=role_data.uid,
            level=role_detail.role.level,
            chain=role_detail.get_chain_num(),
            chainName=role_detail.get_chain_name(),
            score=role_data.score,
            score_bg=score_bg_val,
            expected_damage=f"{int(role_data.damage):,}" if role_data.damage > 0 else "0",
            expected_damage_int=int(role_data.damage),
            sonata_name=sonata_name,
            rank_id=rank_id
        )
    except Exception as e:
        logger.warning(f"[Rank] 解析UID {role_data.uid} 数据失败: {e}")
        return None


async def draw_all_rank_card(
    bot: Bot, ev: Event, char: str, rank_type: str, pages: int
) -> Union[str, bytes]:
    # 1. 基础信息处理
    char_id = char_name_to_char_id(char)
    if not char_id:
        return f"[鸣潮] 角色名【{char}】无法找到, 请检查输入！\n"

    char_name = alias_to_char_name(char)
    find_char_id = SPECIAL_CHAR.get(char_id, char_id)
    rank_type_db = "damage" if rank_type == "伤害" else "score"

    start_time = time.time()
    
    # 2. 数据库查询 (全局排行)
    rank_data_list, total_count = await WavesRoleData.get_global_role_rank(
        role_id=str(find_char_id),
        rank_type=rank_type_db,
        page=pages,
        page_size=rank_length
    )

    if not rank_data_list:
        return f"[鸣潮] 暂无【{char_name}】的排行数据\n请先使用【{PREFIX}刷新面板】！"

    # 3. 获取UID映射
    uid_to_user_id = {}
    all_binds = await WavesBind.get_all_bind()
    for bind in all_binds:
        if bind.uid:
            for uid in bind.uid.split("_"):
                if uid:
                    uid_to_user_id[uid] = bind.user_id

    # 4. 获取自身数据
    self_uid = await WavesBind.get_uid_by_game(ev.user_id, ev.bot_id)
    self_rank_pos = None
    
    if self_uid:
        self_rank_pos = await WavesRoleData.get_role_rank_position(
            uid=self_uid,
            role_id=str(find_char_id),
            rank_type=rank_type_db
        )

    # 5. 构建列表 RankInfoList
    rankInfoList: List[RankInfo] = []
    
    # 统计用
    total_score = 0
    total_damage = 0
    valid_count = 0

    # 处理每一条数据
    for idx, role_data in enumerate(rank_data_list):
        current_rank = (pages - 1) * rank_length + idx + 1
        info = await process_rank_data(role_data, current_rank, uid_to_user_id)
        if info:
            rankInfoList.append(info)
            total_score += info.score
            total_damage += info.expected_damage_int
            valid_count += 1

    # 6. 处理自身排名 (核心逻辑修改：追加而非覆盖)
    is_self_in_list = False
    if self_uid:
        for info in rankInfoList:
            if info.uid == self_uid:
                is_self_in_list = True
                break
    
    # 如果自己不在当前列表，但有有效排名，则查询并追加到最后
    if self_uid and self_rank_pos and not is_self_in_list:
        try:
            from ..utils.database.base import get_session
            from sqlalchemy import select
            
            async with get_session() as session:
                stmt = select(WavesRoleData).where(
                    WavesRoleData.uid == self_uid,
                    WavesRoleData.role_id == str(find_char_id)
                )
                res = await session.execute(stmt)
                self_db_data = res.scalar_one_or_none()
            
            if self_db_data:
                # 这里的排名传入真实的 self_rank_pos
                self_info = await process_rank_data(self_db_data, self_rank_pos, uid_to_user_id)
                if self_info:
                    rankInfoList.append(self_info)
        except Exception as e:
            logger.exception(f"获取自身数据失败: {e}")

    # 7. 绘图准备
    avg_score_val = total_score / valid_count if valid_count else 0
    avg_damage_val = total_damage / valid_count if valid_count else 0

    title_h = 500
    bar_star_h = 110
    # 高度动态计算：列表有多长就画多长，正好容纳下追加的自己
    h = title_h + len(rankInfoList) * bar_star_h + 80
    card_img = get_waves_bg(1050, h, "bg3")
    
    tasks = [get_avatar(ev, rank.qid, rank.roleDetail.role.roleId) for rank in rankInfoList]
    avatar_imgs = await asyncio.gather(*tasks)

    # 8. 循环绘制
    for index, (rank, role_avatar) in enumerate(zip(rankInfoList, avatar_imgs)):
        bar_bg = bar_img.copy()
        bar_draw = ImageDraw.Draw(bar_bg)
        
        # 头像
        bar_bg.paste(role_avatar, (85, -10), role_avatar)

        # 属性
        role_attribute = await get_attribute(
            rank.roleDetail.role.attributeName or "导电", is_simple=True
        )
        role_attribute = role_attribute.resize((40, 40)).convert("RGBA")
        bar_bg.alpha_composite(role_attribute, (300, 20))

        # 命座
        info_block = Image.new("RGBA", (46, 20), (255, 255, 255, 0))
        ib_draw = ImageDraw.Draw(info_block)
        fill = CHAIN_COLOR[rank.chain] + (230,)
        ib_draw.rounded_rectangle([0, 0, 46, 20], radius=6, fill=fill)
        ib_draw.text((23, 10), f"{rank.chainName}", "white", waves_font_18, "mm")
        bar_bg.alpha_composite(info_block, (190, 30))

        # 等级
        info_block = Image.new("RGBA", (60, 20), (255, 255, 255, 0))
        ib_draw = ImageDraw.Draw(info_block)
        ib_draw.rounded_rectangle([0, 0, 60, 20], radius=6, fill=(54, 54, 54, 230))
        ib_draw.text((30, 10), f"Lv.{rank.level}", "white", waves_font_18, "mm")
        bar_bg.alpha_composite(info_block, (240, 30))

        # 评分 (评级图)
        if rank.score > 0.0:
            score_file = TEXT_PATH / f"score_{rank.score_bg}.png"
            if score_file.exists():
                score_bg = Image.open(score_file)
                bar_bg.alpha_composite(score_bg, (320, 2))
            
            bar_draw.text((466, 42), f"{rank.score:.2f}", "white", waves_font_30, "mm")
            bar_draw.text((466, 75), "声骸分数", SPECIAL_GOLD, waves_font_16, "mm")

        # 合鸣
        if rank.sonata_name:
            effect_image = await get_attribute_effect(rank.sonata_name)
            if effect_image:
                effect_image = effect_image.resize((50, 50))
                bar_bg.alpha_composite(effect_image, (533, 15))
            sonata_text = rank.sonata_name
        else:
            sonata_text = "合鸣效果"

        s_font = waves_font_16 if len(sonata_text) <= 4 else waves_font_14
        bar_draw.text((558, 75), sonata_text, "white", s_font, "mm")

        # 武器
        w_data: WeaponData = rank.roleDetail.weaponData
        if w_data and w_data.weapon:
            w_container = Image.new("RGBA", (260, 130))
            w_draw = ImageDraw.Draw(w_container)
            
            w_icon = await get_square_weapon(w_data.weapon.weaponId)
            w_icon = crop_center_img(w_icon, 110, 110)
            w_bg = get_weapon_icon_bg(w_data.weapon.weaponStarLevel)
            w_bg.paste(w_icon, (10, 20), w_icon)
            w_container.alpha_composite(w_bg, (0, 0))

            w_draw.text((150, 30), w_data.weapon.weaponName, SPECIAL_GOLD, waves_font_34, "lm")
            w_draw.text((153, 75), f"Lv.{w_data.level}/90", "white", waves_font_30, "lm")

            wrc_fill = WEAPON_RESONLEVEL_COLOR[w_data.resonLevel or 1] + (200,)
            w_draw.rounded_rectangle([170, 105, 235, 130], radius=5, fill=wrc_fill)
            w_draw.text((202, 117), f"精{w_data.resonLevel}", "white", waves_font_20, "mm")
            
            bar_bg.alpha_composite(w_container, (580, 25))

        # 期望伤害
        rankDetail = DamageRankRegister.find_class(char_id)
        dmg_title = (rankDetail and rankDetail["title"]) or "等待更新"
        bar_draw.text((870, 45), rank.expected_damage, SPECIAL_GOLD, waves_font_34, "mm")
        bar_draw.text((870, 75), dmg_title, "white", waves_font_16, "mm")

        # 排名角标
        real_rank = rank.rank_id
        if real_rank == 1:
            rank_color = (255, 0, 0)
        elif real_rank == 2:
            rank_color = (255, 180, 0)
        elif real_rank == 3:
            rank_color = (185, 106, 217)
        else:
            rank_color = (54, 54, 54)

        if real_rank >= 1000:
            box_w, box_h = 90, 50
            font_pos = (box_w//2, box_h//2)
            dest_pos = (15, 30)
            rank_str = "999+"
        elif real_rank >= 100:
            box_w, box_h = 75, 50
            font_pos = (box_w//2, box_h//2)
            dest_pos = (25, 30)
            rank_str = str(real_rank)
        else:
            box_w, box_h = 50, 50
            font_pos = (25, 25)
            dest_pos = (40, 30)
            rank_str = str(real_rank)

        rank_badge = Image.new("RGBA", (box_w, box_h), (0,0,0,0))
        rb_draw = ImageDraw.Draw(rank_badge)
        rb_draw.rounded_rectangle([0, 0, box_w, box_h], radius=8, fill=rank_color + (230,))
        rb_draw.text(font_pos, rank_str, "white", waves_font_34, "mm")
        bar_bg.alpha_composite(rank_badge, dest_pos)

        # UID
        uid_color = RED if (self_uid and rank.uid == self_uid) else "white"
        bar_draw.text((210, 75), hide_uid(rank.uid), uid_color, waves_font_20, "lm")

        card_img.paste(bar_bg, (0, title_h + index * bar_star_h), bar_bg)

    # 9. 标题区域
    title = TITLE_I.copy()
    t_draw = ImageDraw.Draw(title)
    title.alpha_composite(logo_img, (50, 65))
    
    pile = await get_role_pile_old(char_id, custom=True)
    if pile:
        title.paste(pile, (450, -80), pile)

    t_draw.text((200, 335), f"{avg_score_val:.1f}", "white", waves_font_44, "mm")
    t_draw.text((200, 375), "平均声骸分数", SPECIAL_GOLD, waves_font_20, "mm")

    if total_damage > 0:
        t_draw.text((390, 335), f"{avg_damage_val:,.0f}", "white", waves_font_44, "mm")
        t_draw.text((390, 375), "平均伤害", SPECIAL_GOLD, waves_font_20, "mm")

    display_name = SPECIAL_CHAR_NAME.get(char_id, char_name)
    title_text = f"{display_name} {rank_type}总排行"
    t_draw.text((140, 265), title_text, "black", waves_font_30, "lm")
    t_draw.text((600, 265), f"第{pages}页 / 共{total_count}人", "black", waves_font_20, "lm")

    t_draw.text((20, 420), "入榜条件", SPECIAL_GOLD, waves_font_16, "lm")
    t_draw.text((90, 420), f"使用命令【{PREFIX}刷新面板】刷新过面板且拥有有效CK", GREY, waves_font_16, "lm")

    img_temp = Image.new("RGBA", char_mask.size)
    img_temp.paste(title, (0, 0), char_mask)
    card_img.alpha_composite(img_temp, (0, 0))

    card_draw = ImageDraw.Draw(card_img)
    note_text = "排行标准：以期望伤害" if rank_type == "伤害" else "排行标准：以声骸分数"
    note_text += "（仅供参考，不代表实际强度）"
    card_draw.text((450, 500), note_text, SPECIAL_GOLD, waves_font_16, "lm")

    card_img = add_footer(card_img)
    result = await convert_img(card_img)
    
    logger.info(f"[draw_all_rank_card] 耗时: {time.time() - start_time:.2f}秒")
    return result