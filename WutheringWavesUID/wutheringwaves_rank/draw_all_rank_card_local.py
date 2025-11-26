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
    calc_phantom_score,
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

rank_length = 20  # 排行长度
TEXT_PATH = Path(__file__).parent / "texture2d"
TITLE_I = Image.open(TEXT_PATH / "title.png")
TITLE_II = Image.open(TEXT_PATH / "title2.png")
avatar_mask = Image.open(TEXT_PATH / "avatar_mask.png")
weapon_icon_bg_3 = Image.open(TEXT_PATH / "weapon_icon_bg_3.png")
weapon_icon_bg_4 = Image.open(TEXT_PATH / "weapon_icon_bg_4.png")
weapon_icon_bg_5 = Image.open(TEXT_PATH / "weapon_icon_bg_5.png")
promote_icon = Image.open(TEXT_PATH / "promote_icon.png")
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


async def draw_all_rank_card_local(
    bot: Bot, ev: Event, char: str, rank_type: str, pages: int
) -> Union[str, bytes]:
    """使用本地数据库绘制全局角色排行（完全复制群排行样式）"""

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

    start_time = time.time()
    logger.info(f"[draw_all_rank_card_local] 开始获取排行数据")

    rank_data_list, total_count = await WavesRoleData.get_global_role_rank(
        role_id=str(find_char_id),
        rank_type=rank_type_db,
        page=pages,
        page_size=rank_length
    )

    if not rank_data_list:
        return f"[鸣潮] 暂无【{char_name}】的排行数据\n请先使用【{PREFIX}刷新面板】后再使用此功能！"

    # 获取所有user_id对应的绑定信息
    uid_to_user_id = {}
    all_binds = await WavesBind.get_all_bind()
    for bind in all_binds:
        if bind.uid:
            for uid in bind.uid.split("_"):
                if uid:
                    uid_to_user_id[uid] = bind.user_id

    # 获取自己的排名（如果有）
    self_uid = None
    rankId = None
    rankInfo = None
    try:
        self_uid = await WavesBind.get_uid_by_game(ev.user_id, ev.bot_id)
        if self_uid:
            self_rank = await WavesRoleData.get_role_rank_position(
                uid=self_uid,
                role_id=str(find_char_id),
                rank_type=rank_type_db
            )
            # 查找自己的数据
            for idx, role_data in enumerate(rank_data_list):
                if role_data.uid == self_uid:
                    rankId = (pages - 1) * rank_length + idx + 1
                    rankInfo = role_data
                    break
            else:
                # 如果不在当前页，但有排名
                if self_rank and self_rank > (pages - 1) * rank_length + len(rank_data_list):
                    rankId = self_rank
    except Exception:
        pass

    # 获取伤害计算器
    rankDetail = DamageRankRegister.find_class(char_id)
    damage_title = (rankDetail and rankDetail["title"]) or "无"

    # 计算图片高度
    totalNum = len(rank_data_list)
    if rankId and rankId > (pages - 1) * rank_length + len(rank_data_list):
        totalNum += 1

    title_h = 500
    bar_star_h = 110
    h = title_h + totalNum * bar_star_h + 80
    card_img = get_waves_bg(1050, h, "bg3")
    card_img_draw = ImageDraw.Draw(card_img)

    bar = Image.open(TEXT_PATH / "bar.png")
    total_score = 0
    total_damage = 0

    # 构建rankInfo列表（和群排行一样的结构）
    rankInfoList = []
    for role_data in rank_data_list:
        try:
            role_detail = RoleDetailData(**role_data.data) if role_data.data else None
            if not role_detail:
                continue

            # 获取用户ID
            user_id = uid_to_user_id.get(role_data.uid, role_data.uid)

            # 计算合鸣效果
            sonata_name = ""
            if role_detail.phantomData and role_detail.phantomData.equipPhantomList:
                calc = WuWaCalc(role_detail)
                calc.phantom_pre = calc.prepare_phantom()
                calc.phantom_card = calc.enhance_summation_phantom_value(calc.phantom_pre)

                ph_detail = calc.phantom_card.get("ph_detail", [])
                if isinstance(ph_detail, list):
                    for ph in ph_detail:
                        if ph.get("ph_num") == 5:
                            sonata_name = ph.get("ph_name", "")
                            break
                        if ph.get("isFull"):
                            sonata_name = ph.get("ph_name", "")
                            break

            # 构建rankInfo - 使用RankInfo模型
            rankInfo_item = RankInfo(
                roleDetail=role_detail,
                qid=user_id,
                uid=role_data.uid,
                level=role_detail.role.level,
                chain=role_detail.get_chain_num(),
                chainName=role_detail.get_chain_name(),  # 使用role_detail的get_chain_name方法
                score=role_data.score,
                score_bg=get_total_score_bg(
                    role_detail.role.roleName,
                    role_data.score,
                    {}  # 简化处理
                ) if role_data.score > 0 else "C",
                expected_damage=f"{int(role_data.damage):,}" if role_data.damage > 0 else "0",
                expected_damage_int=int(role_data.damage),
                sonata_name=sonata_name,
            )
            rankInfoList.append(rankInfo_item)
        except Exception as e:
            logger.exception(f"解析角色数据失败 uid={role_data.uid}:", e)
            continue

    # 如果自己不在当前页但有排名，添加到最后
    if rankId and rankInfo and rankId > (pages - 1) * rank_length + len(rank_data_list):
        try:
            role_detail = RoleDetailData(**rankInfo.data) if rankInfo.data else None
            if role_detail:
                user_id = uid_to_user_id.get(rankInfo.uid, rankInfo.uid)

                # 计算合鸣效果
                sonata_name = ""
                if role_detail.phantomData and role_detail.phantomData.equipPhantomList:
                    calc = WuWaCalc(role_detail)
                    calc.phantom_pre = calc.prepare_phantom()
                    calc.phantom_card = calc.enhance_summation_phantom_value(calc.phantom_pre)

                    ph_detail = calc.phantom_card.get("ph_detail", [])
                    if isinstance(ph_detail, list):
                        for ph in ph_detail:
                            if ph.get("ph_num") == 5:
                                sonata_name = ph.get("ph_name", "")
                                break
                            if ph.get("isFull"):
                                sonata_name = ph.get("ph_name", "")
                                break

                rankInfo_item = RankInfo(
                    roleDetail=role_detail,
                    qid=user_id,
                    uid=rankInfo.uid,
                    level=role_detail.role.level,
                    chain=role_detail.get_chain_num(),
                    chainName=role_detail.get_chain_name(),  # 使用role_detail的get_chain_name方法
                    score=rankInfo.score,
                    score_bg=get_total_score_bg(
                        role_detail.role.roleName,
                        rankInfo.score,
                        {}
                    ) if rankInfo.score > 0 else "C",
                    expected_damage=f"{int(rankInfo.damage):,}" if rankInfo.damage > 0 else "0",
                    expected_damage_int=int(rankInfo.damage),
                    sonata_name=sonata_name,
                )
                rankInfoList.append(rankInfo_item)
        except Exception as e:
            logger.exception(f"解析自己的角色数据失败:", e)

    # 获取所有头像
    tasks = [get_avatar(ev, rank.qid, rank.roleDetail.role.roleId) for rank in rankInfoList]
    results = await asyncio.gather(*tasks)

    # 绘制每个排行项（完全复制群排行的绘制逻辑）
    for index, temp in enumerate(zip(rankInfoList, results)):
        rank, role_avatar = temp
        rank: RankInfo
        rank_role_detail: RoleDetailData = rank.roleDetail
        bar_bg = bar.copy()
        bar_star_draw = ImageDraw.Draw(bar_bg)
        bar_bg.paste(role_avatar, (100, 0), role_avatar)

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
        if damage_title == "无":
            bar_star_draw.text((870, 55), "等待更新(:", GREY, waves_font_34, "mm")
        else:
            bar_star_draw.text(
                (870, 45), f"{rank.expected_damage}", SPECIAL_GOLD, waves_font_34, "mm"
            )
            bar_star_draw.text(
                (870, 75), f"{damage_title}", "white", waves_font_16, "mm"
            )

        # 排名
        rank_color = (54, 54, 54)
        if index == 0 and pages == 1:
            rank_color = (255, 0, 0)
        elif index == 1 and pages == 1:
            rank_color = (255, 180, 0)
        elif index == 2 and pages == 1:
            rank_color = (185, 106, 217)

        def draw_rank_id(rank_id, size=(50, 50), draw=(24, 24), dest=(40, 30)):
            info_rank = Image.new("RGBA", size, color=(255, 255, 255, 0))
            rank_draw = ImageDraw.Draw(info_rank)
            rank_draw.rounded_rectangle(
                [0, 0, size[0], size[1]], radius=8, fill=rank_color + (int(0.9 * 255),)
            )
            rank_draw.text(draw, f"{rank_id}", "white", waves_font_34, "mm")
            bar_bg.alpha_composite(info_rank, dest)

        rank_id = (pages - 1) * rank_length + index + 1
        if rankId is not None and index == len(rankInfoList) - 1 and rank_id != rankId:
            rank_id = rankId

        if rank_id is not None and rank_id > 999:
            draw_rank_id("999+", size=(100, 50), draw=(50, 24), dest=(10, 30))
        elif rank_id is not None and rank_id > 99:
            draw_rank_id(rank_id, size=(75, 50), draw=(37, 24), dest=(25, 30))
        else:
            draw_rank_id(rank_id or 0, size=(50, 50), draw=(24, 24), dest=(40, 30))

        # uid
        uid_color = "white"
        if rankId is not None and rankId == rank_id:
            uid_color = RED
        bar_star_draw.text(
            (210, 75), f"{hide_uid(rank.uid)}", uid_color, waves_font_20, "lm"
        )

        # 贴到背景
        card_img.paste(bar_bg, (0, title_h + index * bar_star_h), bar_bg)

        if rank_id is not None and rank_id <= (pages * rank_length):
            total_score += rank.score
            total_damage += rank.expected_damage_int

    # 计算平均值
    if rankId is not None and rankId > (pages * rank_length):
        totalNum -= 1

    avg_score = f"{total_score / totalNum:.1f}" if totalNum != 0 else "0"
    avg_damage = f"{total_damage / totalNum:,.0f}" if totalNum != 0 else "0"

    # 绘制标题
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

    title_name = f"{char_name}{rank_type}总排行"
    title_draw.text((140, 265), f"{title_name}", "black", waves_font_30, "lm")

    # 页码信息
    page_info = f"第{pages}页 / 共{total_count}人"
    title_draw.text((600, 265), f"{page_info}", "black", waves_font_20, "lm")

    # 备注
    rank_row_title = "入榜条件"
    rank_row = f"使用命令【{PREFIX}刷新面板】刷新过面板且拥有有效CK"
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

    logger.info(f"[draw_all_rank_card_local] 耗时: {time.time() - start_time:.2f}秒")
    return card_img
