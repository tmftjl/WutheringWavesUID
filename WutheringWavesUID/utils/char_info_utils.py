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
    try:
        role_data_list = await WavesRoleData.get_role_data_by_uid(uid)

        # 如果数据库中没有数据，尝试从JSON文件迁移
        if not role_data_list:
            path = PLAYER_PATH / uid / "rawData.json"
            if path.exists():
                try:
                    async with aiofiles.open(path, mode="r", encoding="utf-8") as f:
                        json_data = json.loads(await f.read())

                    logger.info(f"正在从JSON文件迁移数据到数据库: uid={uid}, 角色数量={len(json_data)}")

                    # 迁移数据到数据库
                    success = await WavesRoleData.migrate_from_json(uid, json_data)

                    if success:
                        logger.info(f"数据迁移成功: uid={uid}")
                        # 重新从数据库读取
                        role_data_list = await WavesRoleData.get_role_data_by_uid(uid)
                    else:
                        logger.error(f"数据迁移失败: uid={uid}")
                        # 如果迁移失败，直接从JSON返回数据
                        return [RoleDetailData(**r) for r in json_data]

                except Exception as e:
                    logger.exception(f"从JSON文件迁移数据失败 uid={uid}:", e)
                    return None

        if not role_data_list:
            return None

        # 将数据库中的数据转换为 RoleDetailData 对象
        result = []
        for role_data in role_data_list:
            if role_data.data:
                try:
                    result.append(RoleDetailData(**role_data.data))
                except Exception as e:
                    logger.exception(f"解析角色数据失败 uid={uid}, role_id={role_data.role_id}:", e)

        return result if result else None
    except Exception as e:
        logger.exception(f"从数据库获取角色数据失败 uid={uid}:", e)
        return None


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
