import asyncio
import json
from typing import Dict, List, Optional, Union

import aiofiles

from gsuid_core.logger import logger
from gsuid_core.models import Event

from ..utils.api.model import AccountBaseInfo, RoleList, RoleDetailData
from ..utils.error_reply import WAVES_CODE_101, WAVES_CODE_102
from ..utils.expression_ctx import WavesCharRank, get_waves_char_rank
from ..utils.hint import error_reply
from ..utils.queues.const import QUEUE_SCORE_RANK
from ..utils.queues.queues import push_item
from ..utils.resource.RESOURCE_PATH import PLAYER_PATH
from ..utils.util import get_version
from ..utils.waves_api import waves_api
from ..wutheringwaves_config import WutheringWavesConfig
from .resource.constant import SPECIAL_CHAR_INT_ALL
from .calc import WuWaCalc
from .database.models import WavesRoleData


def is_use_global_semaphore() -> bool:
    return WutheringWavesConfig.get_config("UseGlobalSemaphore").data or False


def get_refresh_card_concurrency() -> int:
    return WutheringWavesConfig.get_config("RefreshCardConcurrency").data or 2


class SemaphoreManager:
    def __init__(self):
        self._last_config: int = get_refresh_card_concurrency()
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(value=self._last_config)
        self._semaphore_lock = asyncio.Lock()

    async def get_semaphore(self) -> asyncio.Semaphore:
        current_config = get_refresh_card_concurrency()

        if is_use_global_semaphore():
            return await self._get_semaphore(current_config)  # 全局模式
        else:
            return asyncio.Semaphore(value=current_config)  # 独立模式

    async def _get_semaphore(self, current_config: int) -> asyncio.Semaphore:
        if self._last_config != current_config:
            async with self._semaphore_lock:
                if self._last_config != current_config:
                    self._semaphore = asyncio.Semaphore(value=current_config)
                    self._last_config = current_config

        return self._semaphore


semaphore_manager = SemaphoreManager()


async def send_card(
    uid: str,
    user_id: str,
    save_data: List,
    is_self_ck: bool = False,
    token: Optional[str] = "",
    role_info: Optional[RoleList] = None,
    waves_data: Optional[List] = None,
):
    waves_char_rank: Optional[List[WavesCharRank]] = None

    WavesToken = WutheringWavesConfig.get_config("WavesToken").data

    if WavesToken:
        waves_char_rank = await get_waves_char_rank(uid, save_data, True)

    if (
        is_self_ck
        and token
        and waves_char_rank
        and WavesToken
        and role_info
        and waves_data
        and user_id
    ):
        # 单角色上传排行
        if len(waves_data) != 1 and len(role_info.roleList) != len(save_data):
            logger.warning(
                f"角色数量不一致，role_info.roleNum:{len(role_info.roleList)} != waves_char_rank:{len(save_data)}"
            )
            return
        account_info = await waves_api.get_base_info(uid, token=token)
        if not account_info.success:
            return account_info.throw_msg()
        account_info = AccountBaseInfo.model_validate(account_info.data)
        if len(waves_data) != 1 and account_info.roleNum != len(save_data):
            logger.warning(
                f"角色数量不一致，role_info.roleNum:{account_info.roleNum} != waves_char_rank:{len(save_data)}"
            )
            return
        metadata = {
            "user_id": user_id,
            "waves_id": f"{account_info.id}",
            "kuro_name": account_info.name,
            "version": get_version(),
            "char_info": [r.to_rank_dict() for r in waves_char_rank],
            "role_num": account_info.roleNum,
            "single_refresh": 1 if len(waves_data) == 1 else 0,
        }
        # push_item(QUEUE_SCORE_RANK, metadata)


async def save_card_info(
    uid: str,
    waves_data: List,
    waves_map: Optional[Dict] = None,
    user_id: str = "",
    is_self_ck: bool = False,
    token: str = "",
    role_info: Optional[RoleList] = None,
):
    """
    处理角色数据更新、生成卡片并同步数据库
    """
    if not waves_data:
        return
    old_data_map = await WavesRoleData.get_role_data_map_by_uid(uid)
    new_role_ids = set(str(item["role"]["roleId"]) for item in waves_data)
    special_char_set = set(str(x) for x in SPECIAL_CHAR_INT_ALL)
    
    if not new_role_ids.isdisjoint(special_char_set):
        keys_to_remove = [k for k in old_data_map.keys() if k in special_char_set]
        for k in keys_to_remove:
            del old_data_map[k]

    refresh_update = {}
    refresh_unchanged = {}

    for item in waves_data:
        role_id = str(item["role"]["roleId"])
        old = old_data_map.get(role_id)
        if old != item:
            refresh_update[role_id] = item
        else:
            refresh_unchanged[role_id] = item
        old_data_map[role_id] = item

    # 生成最终的全量列表 (这是数据库最终应有的状态)
    final_save_data = list(old_data_map.values())

    # 生成/发送图片
    await send_card(uid, user_id, final_save_data, is_self_ck, token, role_info, waves_data)

    scores_map, damage_map = await WuWaCalc.calc_role_scores_and_damages(final_save_data)

    try:
        await WavesRoleData.save_role_data(
            uid=uid,
            final_role_list=final_save_data,
            scores_map=scores_map,
            damage_map=damage_map
        )
        logger.info(f"角色数据同步完成: uid={uid}, 角色总数={len(final_save_data)}")
    except Exception as e:
        logger.exception(f"保存角色数据到数据库失败 uid={uid}: {e}")

    # 回填状态供上层调用者使用
    if waves_map is not None:
        waves_map["refresh_update"] = refresh_update
        waves_map["refresh_unchanged"] = refresh_unchanged

async def refresh_char(
    ev: Event,
    uid: str,
    user_id: str,
    ck: Optional[str] = None,  # type: ignore
    waves_map: Optional[Dict] = None,
    is_self_ck: bool = False,
    refresh_type: Union[str, List[str]] = "all",
) -> Union[str, List]:
    waves_datas = []
    if not ck:
        is_self_ck, ck = await waves_api.get_ck_result(uid, user_id, ev.bot_id)
    if not ck:
        return error_reply(WAVES_CODE_102)
    # 共鸣者信息
    role_info = await waves_api.get_role_info(uid, ck)
    if not role_info.success:
        return role_info.throw_msg()

    try:
        role_info = RoleList.model_validate(role_info.data)
    except Exception as e:
        logger.exception(f"{uid} 角色信息解析失败", e)
        msg = f"鸣潮特征码[{uid}]获取数据失败\n1.是否注册过库街区\n2.库街区能否查询当前鸣潮特征码数据"
        return msg

    semaphore = await semaphore_manager.get_semaphore()

    async def limited_get_role_detail_info(role_id, uid, ck):
        async with semaphore:
            return await waves_api.get_role_detail_info(role_id, uid, ck)

    if is_self_ck:
        tasks = [
            limited_get_role_detail_info(f"{r.roleId}", uid, ck)
            for r in role_info.roleList
            if refresh_type == "all"
            or (isinstance(refresh_type, list) and f"{r.roleId}" in refresh_type)
        ]
    else:
        if role_info.showRoleIdList:
            tasks = [
                limited_get_role_detail_info(f"{r}", uid, ck)
                for r in role_info.showRoleIdList
                if refresh_type == "all"
                or (isinstance(refresh_type, list) and f"{r}" in refresh_type)
            ]
        else:
            tasks = [
                limited_get_role_detail_info(f"{r.roleId}", uid, ck)
                for r in role_info.roleList
                if refresh_type == "all"
                or (isinstance(refresh_type, list) and f"{r.roleId}" in refresh_type)
            ]
    results = await asyncio.gather(*tasks)

    charId2chainNum: Dict[int, int] = {
        r.roleId: r.chainUnlockNum
        for r in role_info.roleList
        if isinstance(r.chainUnlockNum, int)
    }
    # 处理返回的数据
    for role_detail_info in results:
        if not role_detail_info.success:
            continue

        role_detail_info = role_detail_info.data
        if (
            not isinstance(role_detail_info, dict)
            or "role" not in role_detail_info
            or role_detail_info["role"] is None
            or "level" not in role_detail_info
            or role_detail_info["level"] is None
        ):
            continue
        if role_detail_info["phantomData"]["cost"] == 0:
            role_detail_info["phantomData"]["equipPhantomList"] = None
        try:
            # 扰我道心 难道谐振几阶还算不明白吗
            del role_detail_info["weaponData"]["weapon"]["effectDescription"]
        except Exception as _:
            pass

        # 修正共鸣链
        try:
            role_id = role_detail_info["role"]["roleId"]
            for i in role_detail_info["chainList"]:
                if i["order"] <= charId2chainNum[role_id]:
                    i["unlocked"] = True
                else:
                    i["unlocked"] = False
        except Exception as e:
            logger.exception(f"{uid} 共鸣链修正失败", e)

        # 修正合鸣效果
        try:
            if (
                role_detail_info["phantomData"]
                and role_detail_info["phantomData"]["equipPhantomList"]
            ):
                for i in role_detail_info["phantomData"]["equipPhantomList"]:
                    if not isinstance(i, dict):
                        continue
                    sonata_name = i.get("fetterDetail", {}).get("name", "")
                    if sonata_name == "雷曜日冕之冠":
                        i["fetterDetail"]["name"] = "荣斗铸锋之冠"  # type: ignore
        except Exception as e:
            logger.exception(f"{uid} 合鸣效果修正失败", e)

        waves_datas.append(role_detail_info)

    await save_card_info(
        uid,
        waves_datas,
        waves_map,
        user_id,
        is_self_ck=is_self_ck,
        token=ck,
        role_info=role_info,
    )

    if not waves_datas:
        if refresh_type == "all":
            return error_reply(WAVES_CODE_101)
        else:
            return error_reply(code=-110, msg="库街区暂未查询到角色数据")

    return waves_datas

