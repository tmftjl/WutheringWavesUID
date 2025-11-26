import json
from typing import Any, Dict, Generator, List, Union

import aiofiles

from gsuid_core.logger import logger

from ..utils.api.model import RoleDetailData
from .resource.RESOURCE_PATH import PLAYER_PATH
from .database.models import WavesRoleData


async def get_all_role_detail_info_list(
    uid: str,
) -> Union[List[RoleDetailData], None]:
    """从数据库获取所有角色详细信息，如果数据库为空则尝试从JSON文件迁移"""
    role_data_list = await WavesRoleData.get_role_data_by_uid(uid)
    # 将数据库中的数据转换为 RoleDetailData 对象
    result = []
    for role_data in role_data_list:
        if role_data.data:
            try:
                result.append(RoleDetailData(**role_data.data))
            except Exception as e:
                logger.exception(f"解析角色数据失败 uid={uid}, role_id={role_data.role_id}:", e)

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
