import json
from typing import Any, Dict, Generator, List, Union, Optional

import aiofiles

from gsuid_core.logger import logger

from ..utils.api.model import RoleDetailData
from .resource.RESOURCE_PATH import PLAYER_PATH
from .database.models import WavesRoleData


async def get_all_role_detail_info_list(
    uid: str,
) -> Optional[List[RoleDetailData]]: 
    """从数据库获取所有角色详细信息，如果是极限面板(uid='1')则从JSON文件获取"""
    result = []
    if uid == "1":
        path = PLAYER_PATH / uid / "rawData.json"
        if path.exists():
            try:
                async with aiofiles.open(path, mode="r", encoding="utf-8") as f:
                    content = await f.read()
                    json_data = json.loads(content)
                result = [RoleDetailData.model_validate(r) for r in json_data]
            except Exception as e:
                logger.error(f"读取极限面板数据失败: {e}")
                return None
    else:
        try:
            role_data_list = await WavesRoleData.get_role_data_by_uid(uid)
            for role_data in role_data_list:
                if role_data.data:
                    result.append(RoleDetailData.model_validate(role_data.data))
        except Exception as e:
            logger.error(f"查询数据库角色数据失败: {e}")
            return None

    return result if result else None


async def get_all_role_detail_info(uid: str) -> Union[Dict[str, RoleDetailData], None]:
    _all = await get_all_role_detail_info_list(uid)
    if not _all:
        return None
    return {r.role.roleName: r for r in _all}


async def get_all_roleid_detail_info(
    uid: str,
) -> Union[Dict[str, RoleDetailData], None]:
    _all = await get_all_role_detail_info_list(uid)
    if not _all:
        return None
    return {str(r.role.roleId): r for r in _all}


async def get_all_roleid_detail_info_int(
    uid: str,
) -> Union[Dict[int, RoleDetailData], None]:
    _all = await get_all_role_detail_info_list(uid)
    if not _all:
        return None
    return {r.role.roleId: r for r in _all}
