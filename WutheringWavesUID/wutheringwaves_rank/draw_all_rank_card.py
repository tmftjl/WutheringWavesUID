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
from ..utils.database.models import WavesBind, WavesRoleData, WavesUser
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

rank_length = 20  # 排行长度
TEXT_PATH = Path(__file__).parent / "texture2d"
TITLE_I = Image.open(TEXT_PATH / "title.png")
avatar_mask = Image.open(TEXT_PATH / "avatar_mask.png")
weapon_icon_bg_3 = Image.open(TEXT_PATH / "weapon_icon_bg_3.png")
weapon_icon_bg_4 = Image.open(TEXT_PATH / "weapon_icon_bg_4.png")
weapon_icon_bg_5 = Image.open(TEXT_PATH / "weapon_icon_bg_5.png")
char_mask = Image.open(TEXT_PATH / "char_mask.png")
logo_img = Image.open(TEXT_PATH / "logo_small_2.png")
pic_cache = TimedCache(86400, 200)


class RankInfo(BaseModel):
    roleDetail: RoleDetailData  # 角色明细
    qid: str  # qq id
    uid: str  # uid
    level: int  # 角色等级
    chain: int  # 命座
    chainName: str  # 命座
    score: float  # 角色评分
    score_bg: str  # 评分背景
    expected_damage: str  # 期望伤害
    expected_damage_int: int  # 期望伤害
    sonata_name: str  # 合鸣效果
    rank_id: int # 实际排名


async def get_avatar(
    ev: Event,
    qid: Optional[Union[int, str]],
    char_id: Union[int, str],
) -> Image.Image:
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

        img = Image.new("RGBA", (180, 180))
        avatar_mask_temp = avatar_mask.copy()
        mask_pic_temp = avatar_mask_temp.resize((120, 120))
        img.paste(pic_temp, (0, -5), mask_pic_temp)
    else:
        pic = await get_square_avatar(char_id)

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
    """处理数据库数据转为RankInfo，包含评级计算"""
    try:
        role_detail = RoleDetailData(**role_data.data) if role_data.data else None
        if not role_detail:
            return None

        user_id = uid_to_user_id.get(role_data.uid, role_data.uid)

        # === 核心修复：重新计算标准以获取正确的 score_bg ===
        calc = WuWaCalc(role_detail)
        calc.phantom_pre = calc.prepare_phantom()
        calc.phantom_card = calc.enhance_summation_phantom_value(calc.phantom_pre)
        
        # 获取毕业标准 Map
        calc_temp = get_calc_map(
            calc.phantom_card,
            role_detail.role.roleName,
            role_detail.role.roleId,
        )

        # 计算评级背景 S/A/B/C
        score_bg_val = "C"
        if role_data.score > 0:
            score_bg_val = get_total_score_bg(
                role_detail.role.roleName,
                role_data.score,
                calc_temp 
            )
        # ===============================================

        # 计算合鸣效果
        sonata_name = ""
        ph_detail = calc.phantom_card.get("ph_detail", [])
        if isinstance(ph_detail, list):
            for ph in ph_detail:
                if ph.get("ph_num") == 5:
                    sonata_name = ph.get("ph_name", "")
                    break
                if ph.get("isFull") and not sonata_name:
                    sonata_name = ph.get("ph_name", "")

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
    bot: Bot, ev: Event, char: str, rank_type: str
) -> Union[str, bytes]:
    char_id = char_name_to_char_id(char)
    if not char_id:
        return f"[鸣潮] 角色名【{char}】无法找到, 请检查输入！\n"

    char_name = alias_to_char_name(char)
    find_char_id = SPECIAL_CHAR.get(char_id, char_id)
    rank_type_db = "damage" if rank_type == "伤害" else "score"
    
    # 默认只看前20
    limit_num = 20
    start_time = time.time()

    # 获取 UID
    self_uid = await WavesBind.get_uid_by_game(ev.user_id, ev.bot_id)
    target_uid = None

    # 只有 cookie 有效时才获取自己的排行
    if self_uid:
        user_data = await WavesUser.select_waves_user(self_uid, ev.user_id, ev.bot_id)
        if user_data and user_data.cookie and (not user_data.status or user_data.status == ""):
            target_uid = self_uid

    uid_to_user_id = {}
    all_binds = await WavesBind.get_all_bind()
    for bind in all_binds:
        if bind.uid:
            for uid in bind.uid.split("_"):
                if uid:
                    uid_to_user_id[uid] = bind.user_id

    # Ensure the current viewer sees their own avatar in the list for their UID
    if target_uid:
        uid_to_user_id[str(target_uid)] = str(ev.user_id)
    # 调用新接口获取排行数据
    rank_result = await WavesRoleData.get_role_rank_data(
        role_id=str(find_char_id),
        rank_type=rank_type_db,
        limit=limit_num,
        target_uid=target_uid
    )
    
    rank_rows = rank_result["list"]
    self_info_data = rank_result["self_info"]

    if not rank_rows and not self_info_data:
        return f"[鸣潮] 暂无【{char_name}】的排行数据\n请先使用【{PREFIX}刷新面板】！"

    rankInfoList: List[RankInfo] = []
    for idx, role_data in enumerate(rank_rows):
        current_rank_id = idx + 1
        info = await process_rank_data(role_data, current_rank_id, uid_to_user_id)
        if info:
            rankInfoList.append(info)

    # 处理自己的排名
    self_appended = False
    if self_info_data:
        # 检查是否已经在列表中
        is_in_list = False
        for info in rankInfoList:
            if str(info.uid) == str(self_info_data["data"].uid):
                is_in_list = True
                break
        
        # 如果不在列表中（说明在20名以外），则追加
        if not is_in_list:
            self_rank_obj = await process_rank_data(
                self_info_data["data"], 
                self_info_data["rank"], 
                uid_to_user_id
            )
            if self_rank_obj:
                rankInfoList.append(self_rank_obj)
                self_appended = True

    # 计算平均值
    calc_list = rankInfoList[:-1] if self_appended else rankInfoList
    
    total_score = 0
    total_damage = 0
    for info in calc_list:
        total_score += info.score
        total_damage += info.expected_damage_int
        
    calc_num = len(calc_list)
    avg_score = f"{total_score / calc_num:.1f}" if calc_num != 0 else "0"
    avg_damage = f"{total_damage / calc_num:,.0f}" if calc_num != 0 else "0"

    # 绘制图片
    totalNum = len(rankInfoList)
    title_h = 500
    bar_star_h = 110
    h = title_h + totalNum * bar_star_h + 80
    card_img = get_waves_bg(1050, h, "bg3")
    card_img_draw = ImageDraw.Draw(card_img)

    bar = Image.open(TEXT_PATH / "bar.png")

    # 获取头像
    tasks = [
        get_avatar(ev, rank.qid, rank.roleDetail.role.roleId) for rank in rankInfoList
    ]
    results = await asyncio.gather(*tasks)

    for index, temp in enumerate(zip(rankInfoList, results)):
        rank, role_avatar = temp
        rank: RankInfo
        rank_role_detail: RoleDetailData = rank.roleDetail
        bar_bg = bar.copy()
        bar_star_draw = ImageDraw.Draw(bar_bg)
        
        # 头像
        bar_bg.paste(role_avatar, (100, 0), role_avatar)

        # 属性
        role_attribute = await get_attribute(
            rank_role_detail.role.attributeName or "导电", is_simple=True
        )
        role_attribute = role_attribute.resize((40, 40)).convert("RGBA")
        bar_bg.alpha_composite(role_attribute, (300, 20))

        # 命座
        info_block = Image.new("RGBA", (46, 20), color=(255, 255, 255, 0))
        info_block_draw = ImageDraw.Draw(info_block)
        fill = CHAIN_COLOR[rank.chain] + (int(0.9 * 255),)
        info_block_draw.rounded_rectangle([0, 0, 46, 20], radius=6, fill=fill)
        info_block_draw.text((5, 10), f"{rank.chainName}", "white", waves_font_18, "lm")
        bar_bg.alpha_composite(info_block, (190, 30))

        # 等级
        info_block = Image.new("RGBA", (60, 20), color=(255, 255, 255, 0))
        info_block_draw = ImageDraw.Draw(info_block)
        info_block_draw.rounded_rectangle(
            [0, 0, 60, 20], radius=6, fill=(54, 54, 54, int(0.9 * 255))
        )
        info_block_draw.text((5, 10), f"Lv.{rank.level}", "white", waves_font_18, "lm")
        bar_bg.alpha_composite(info_block, (240, 30))

        # 评分
        if rank.score > 0.0:
            score_bg = Image.open(TEXT_PATH / f"score_{rank.score_bg}.png")
            bar_bg.alpha_composite(score_bg, (320, 2))
            bar_star_draw.text(
                (466, 42),
                f"{int(rank.score * 100) / 100:.2f}",
                "white",
                waves_font_30,
                "mm",
            )
            bar_star_draw.text((466, 75), "声骸分数", SPECIAL_GOLD, waves_font_16, "mm")

        # 合鸣效果
        if rank.sonata_name:
            effect_image = await get_attribute_effect(rank.sonata_name)
            effect_image = effect_image.resize((50, 50))
            bar_bg.alpha_composite(effect_image, (533, 15))
            sonata_name = rank.sonata_name
        else:
            sonata_name = "合鸣效果"

        sonata_font = waves_font_16
        if len(sonata_name) > 4:
            sonata_font = waves_font_14
        bar_star_draw.text((558, 75), f"{sonata_name}", "white", sonata_font, "mm")

        # 武器
        weapon_bg_temp = Image.new("RGBA", (600, 300))
        weaponData: WeaponData = rank_role_detail.weaponData
        weapon_icon = await get_square_weapon(weaponData.weapon.weaponId)
        weapon_icon = crop_center_img(weapon_icon, 110, 110)
        weapon_icon_bg = get_weapon_icon_bg(weaponData.weapon.weaponStarLevel)
        weapon_icon_bg.paste(weapon_icon, (10, 20), weapon_icon)

        weapon_bg_temp_draw = ImageDraw.Draw(weapon_bg_temp)
        weapon_bg_temp_draw.text(
            (200, 30),
            f"{weaponData.weapon.weaponName}",
            SPECIAL_GOLD,
            waves_font_40,
            "lm",
        )
        weapon_bg_temp_draw.text(
            (203, 75), f"Lv.{weaponData.level}/90", "white", waves_font_30, "lm"
        )

        _x = 220
        _y = 120
        wrc_fill = WEAPON_RESONLEVEL_COLOR[weaponData.resonLevel or 0] + (
            int(0.8 * 255),
        )
        weapon_bg_temp_draw.rounded_rectangle(
            [_x - 15, _y - 15, _x + 50, _y + 15], radius=7, fill=wrc_fill
        )
        weapon_bg_temp_draw.text(
            (_x, _y), f"精{weaponData.resonLevel}", "white", waves_font_24, "lm"
        )

        weapon_bg_temp.alpha_composite(weapon_icon_bg, dest=(45, 0))
        bar_bg.alpha_composite(weapon_bg_temp.resize((260, 130)), dest=(580, 25))

        # 伤害
        rankDetail = DamageRankRegister.find_class(char_id)
        damage_title = (rankDetail and rankDetail["title"]) or "无"

        if damage_title == "无":
            bar_star_draw.text((870, 55), "等待更新(:", GREY, waves_font_34, "mm")
        else:
            bar_star_draw.text(
                (870, 45), f"{rank.expected_damage}", SPECIAL_GOLD, waves_font_34, "mm"
            )
            bar_star_draw.text(
                (870, 75), f"{damage_title}", "white", waves_font_16, "mm"
            )

        # 排名角标
        rank_color = (54, 54, 54)
        if rank.rank_id == 1:
            rank_color = (255, 0, 0)
        elif rank.rank_id == 2:
            rank_color = (255, 180, 0)
        elif rank.rank_id == 3:
            rank_color = (185, 106, 217)

        def draw_rank_id(rank_id_text, size=(50, 50), draw=(24, 24), dest=(40, 30)):
            info_rank = Image.new("RGBA", size, color=(255, 255, 255, 0))
            rank_draw = ImageDraw.Draw(info_rank)
            rank_draw.rounded_rectangle(
                [0, 0, size[0], size[1]], radius=8, fill=rank_color + (int(0.9 * 255),)
            )
            rank_draw.text(draw, f"{rank_id_text}", "white", waves_font_34, "mm")
            bar_bg.alpha_composite(info_rank, dest)

        # 使用存储在 RankInfo 中的真实排名
        rank_id = rank.rank_id
        
        if rank_id > 999:
            draw_rank_id("999+", size=(100, 50), draw=(50, 24), dest=(10, 30))
        elif rank_id > 99:
            draw_rank_id(rank_id, size=(75, 50), draw=(37, 24), dest=(25, 30))
        else:
            draw_rank_id(rank_id, size=(50, 50), draw=(24, 24), dest=(40, 30))

        # uid
        uid_color = "white"
        if self_uid and rank.uid == self_uid:
            uid_color = RED
        bar_star_draw.text(
            (210, 75), f"{hide_uid(rank.uid)}", uid_color, waves_font_20, "lm"
        )

        # 贴到背景
        card_img.paste(bar_bg, (0, title_h + index * bar_star_h), bar_bg)

    # 9. 标题区域
    title = TITLE_I.copy()
    title_draw = ImageDraw.Draw(title)
    # logo
    title.alpha_composite(logo_img.copy(), dest=(50, 65))

    # 人物bg
    pile = await get_role_pile_old(char_id, custom=True)
    title.paste(pile, (450, -120), pile)
    title_draw.text((200, 335), f"{avg_score}", "white", waves_font_44, "mm")
    title_draw.text((200, 375), "平均声骸分数", SPECIAL_GOLD, waves_font_20, "mm")

    if damage_title != "无":
        title_draw.text((390, 335), f"{avg_damage}", "white", waves_font_44, "mm")
        title_draw.text((390, 375), "平均伤害", SPECIAL_GOLD, waves_font_20, "mm")

    if char_id in SPECIAL_CHAR_NAME:
        char_name = SPECIAL_CHAR_NAME[char_id]

    title_name = f"{char_name}{rank_type}bot排行"
    title_draw.text((140, 265), f"{title_name}", "black", waves_font_30, "lm")

    # 备注
    rank_row_title = "入榜条件"
    rank_row = f"使用命令【{PREFIX}刷新面板】刷新过面板且拥有有效token"
    title_draw.text((20, 420), f"{rank_row_title}", SPECIAL_GOLD, waves_font_16, "lm")
    title_draw.text((90, 420), f"{rank_row}", GREY, waves_font_16, "lm")

    if rank_type == "伤害":
        temp_notes = (
            "排行标准：以期望伤害（计算暴击率的伤害，不代表实际伤害) 为排序的排名"
        )
    else:
        temp_notes = "排行标准：以声骸分数（声骸评分高，不代表实际伤害高) 为排序的排名"
    card_img_draw.text((450, 500), f"{temp_notes}", SPECIAL_GOLD, waves_font_16, "lm")

    img_temp = Image.new("RGBA", char_mask.size)
    img_temp.paste(title, (0, 0), char_mask.copy())
    card_img.alpha_composite(img_temp, (0, 0))
    card_img = add_footer(card_img)
    card_img = await convert_img(card_img)

    logger.info(f"[draw_all_rank_card] 耗时: {time.time() - start_time:.2f}秒")
    return card_img
