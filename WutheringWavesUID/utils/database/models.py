from typing import Any, Dict, List, Optional, Type, TypeVar

from sqlalchemy import delete, null, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import and_, or_
from sqlmodel import Field, col, select

from gsuid_core.utils.database.base_models import (
    Bind,
    Push,
    User,
    with_session,
)
from gsuid_core.utils.database.startup import exec_list
from gsuid_core.webconsole.mount_app import GsAdminModel, PageSchema, site

exec_list.extend(
    [
        'ALTER TABLE WavesUser ADD COLUMN platform TEXT DEFAULT ""',
        'ALTER TABLE WavesUser ADD COLUMN stamina_bg_value TEXT DEFAULT ""',
        'ALTER TABLE WavesUser ADD COLUMN bbs_sign_switch TEXT DEFAULT "off"',
        'ALTER TABLE WavesUser ADD COLUMN bat TEXT DEFAULT ""',
        'ALTER TABLE WavesUser ADD COLUMN did TEXT DEFAULT ""',
    ]
)

T_WavesBind = TypeVar("T_WavesBind", bound="WavesBind")
T_WavesUser = TypeVar("T_WavesUser", bound="WavesUser")


class WavesBind(Bind, table=True):
    __table_args__: Dict[str, Any] = {"extend_existing": True}
    uid: Optional[str] = Field(default=None, title="鸣潮UID")

    @classmethod
    @with_session
    async def get_group_all_uid(
        cls: Type[T_WavesBind], session: AsyncSession, group_id: Optional[str] = None
    ):
        """根据传入`group_id`获取该群号下所有绑定`uid`列表"""
        result = await session.scalars(
            select(cls).where(col(cls.group_id).contains(group_id))
        )
        return result.all()

    @classmethod
    async def insert_waves_uid(
        cls: Type[T_WavesBind],
        user_id: str,
        bot_id: str,
        uid: str,
        group_id: Optional[str] = None,
        lenth_limit: Optional[int] = None,
        is_digit: Optional[bool] = True,
        game_name: Optional[str] = None,
    ) -> int:
        if lenth_limit:
            if len(uid) != lenth_limit:
                return -1

        if is_digit:
            if not uid.isdigit():
                return -3
        if not uid:
            return -1

        # 第一次绑定
        if not await cls.bind_exists(user_id, bot_id):
            code = await cls.insert_data(
                user_id=user_id,
                bot_id=bot_id,
                **{"uid": uid, "group_id": group_id},
            )
            return code

        result = await cls.select_data(user_id, bot_id)
        # await user_bind_cache.set(user_id, result)

        uid_list = result.uid.split("_") if result and result.uid else []
        uid_list = [i for i in uid_list if i] if uid_list else []

        # 已经绑定了该UID
        res = 0 if uid not in uid_list else -2

        # 强制更新库表
        force_update = False
        if uid not in uid_list:
            uid_list.append(uid)
            force_update = True
        new_uid = "_".join(uid_list)

        group_list = result.group_id.split("_") if result and result.group_id else []
        group_list = [i for i in group_list if i] if group_list else []

        if group_id and group_id not in group_list:
            group_list.append(group_id)
            force_update = True
        new_group_id = "_".join(group_list)

        if force_update:
            await cls.update_data(
                user_id=user_id,
                bot_id=bot_id,
                **{"uid": new_uid, "group_id": new_group_id},
            )
        return res


class WavesUser(User, table=True):
    __table_args__: Dict[str, Any] = {"extend_existing": True}
    cookie: str = Field(default="", title="Cookie")
    uid: str = Field(default=None, title="鸣潮UID")
    record_id: Optional[str] = Field(default=None, title="鸣潮记录ID")
    platform: str = Field(default="", title="ck平台")
    stamina_bg_value: str = Field(default="", title="体力背景")
    bbs_sign_switch: str = Field(default="off", title="自动社区签到")
    bat: str = Field(default="", title="bat")
    did: str = Field(default="", title="did")

    @classmethod
    @with_session
    async def mark_cookie_invalid(
        cls: Type[T_WavesUser], session: AsyncSession, uid: str, cookie: str, mark: str
    ):
        sql = (
            update(cls)
            .where(col(cls.uid) == uid)
            .where(col(cls.cookie) == cookie)
            .values(status=mark)
        )
        await session.execute(sql)
        return True

    @classmethod
    @with_session
    async def select_cookie(
        cls: Type[T_WavesUser],
        session: AsyncSession,
        uid: str,
        user_id: str,
        bot_id: str,
    ) -> Optional[str]:
        sql = select(cls).where(
            cls.user_id == user_id,
            cls.uid == uid,
            cls.bot_id == bot_id,
        )
        result = await session.execute(sql)
        data = result.scalars().all()
        return data[0].cookie if data else None

    @classmethod
    @with_session
    async def select_waves_user(
        cls: Type[T_WavesUser],
        session: AsyncSession,
        uid: str,
        user_id: str,
        bot_id: str,
    ) -> Optional[T_WavesUser]:
        sql = select(cls).where(
            cls.user_id == user_id,
            cls.uid == uid,
            cls.bot_id == bot_id,
        )
        result = await session.execute(sql)
        data = result.scalars().all()
        return data[0] if data else None

    @classmethod
    @with_session
    async def select_user_cookie_uids(
        cls: Type[T_WavesUser],
        session: AsyncSession,
        user_id: str,
    ) -> List[str]:
        sql = select(cls).where(
            and_(
                col(cls.user_id) == user_id,
                col(cls.cookie) != null(),
                col(cls.cookie) != "",
                or_(col(cls.status) == null(), col(cls.status) == ""),
            )
        )
        result = await session.execute(sql)
        data = result.scalars().all()
        return [i.uid for i in data] if data else []

    @classmethod
    @with_session
    async def select_data_by_cookie(
        cls: Type[T_WavesUser], session: AsyncSession, cookie: str
    ) -> Optional[T_WavesUser]:
        sql = select(cls).where(cls.cookie == cookie)
        result = await session.execute(sql)
        data = result.scalars().all()
        return data[0] if data else None

    @classmethod
    @with_session
    async def select_data_by_cookie_and_uid(
        cls: Type[T_WavesUser], session: AsyncSession, cookie: str, uid: str
    ) -> Optional[T_WavesUser]:
        sql = select(cls).where(cls.cookie == cookie, cls.uid == uid)
        result = await session.execute(sql)
        data = result.scalars().all()
        return data[0] if data else None

    @classmethod
    async def get_user_by_attr(
        cls: Type[T_WavesUser],
        user_id: str,
        bot_id: str,
        attr_key: str,
        attr_value: str,
    ) -> Optional[Any]:
        user_list = await cls.select_data_list(user_id=user_id, bot_id=bot_id)
        if not user_list:
            return None
        for user in user_list:
            if getattr(user, attr_key) != attr_value:
                continue
            return user

    @classmethod
    @with_session
    async def get_waves_all_user(
        cls: Type[T_WavesUser], session: AsyncSession
    ) -> List[T_WavesUser]:
        """获取所有有效用户"""
        sql = select(cls).where(
            and_(
                or_(col(cls.status) == null(), col(cls.status) == ""),
                col(cls.cookie) != null(),
                col(cls.cookie) != "",
            )
        )

        result = await session.execute(sql)
        data = result.scalars().all()
        return list(data)

    @classmethod
    @with_session
    async def delete_all_invalid_cookie(cls, session: AsyncSession):
        """删除所有无效缓存"""
        sql = delete(cls).where(
            or_(col(cls.status) == "无效", col(cls.cookie) == ""),
        )
        result = await session.execute(sql)
        return result.rowcount

    @classmethod
    @with_session
    async def delete_cookie(
        cls,
        session: AsyncSession,
        uid: str,
        user_id: str,
        bot_id: str,
    ):
        sql = delete(cls).where(
            and_(
                col(cls.user_id) == user_id,
                col(cls.uid) == uid,
                col(cls.bot_id) == bot_id,
            )
        )
        result = await session.execute(sql)
        return result.rowcount


class WavesPush(Push, table=True):
    __table_args__: Dict[str, Any] = {"extend_existing": True}
    bot_id: str = Field(title="平台")
    uid: str = Field(default=None, title="鸣潮UID")
    resin_push: Optional[str] = Field(
        title="体力推送",
        default="off",
        schema_extra={"json_schema_extra": {"hint": "ww开启体力推送"}},
    )
    resin_value: Optional[int] = Field(title="体力阈值", default=180)
    resin_is_push: Optional[str] = Field(title="体力是否已推送", default="off")

class WavesRoleData(Bind, table=True):
    __table_args__: Dict[str, Any] = {"extend_existing": True}
    
    uid: str = Field(index=True, title="鸣潮UID")
    role_id: str = Field(index=True, title="角色ID")
    role_name: str = Field(default="", title="角色名称")
    score: float = Field(default=0.0, index=True, title="评分")
    damage: float = Field(default=0.0, index=True, title="伤害")
    
    data: Dict = Field(default={}, sa_column=Column(JSON))

    @classmethod
    @with_session
    async def save_role_data(
        cls, 
        session: AsyncSession, 
        uid: str, 
        role_data_list: List[Dict],
        scores_map: Dict[str, float] = {}, 
        damage_map: Dict[str, float] = {} 
    ):
        for role_info in role_data_list:
            role_id = str(role_info.get("role", {}).get("roleId", ""))
            if not role_id:
                continue
            
            # 获取该角色的分数和伤害
            current_score = scores_map.get(role_id, 0.0)
            current_damage = damage_map.get(role_id, 0.0)
            
            # 查找是否存在
            stmt = select(cls).where(cls.uid == uid, cls.role_id == role_id)
            result = await session.execute(stmt)
            obj = result.scalars().first()

            if obj:
                # 更新
                obj.role_name = role_info.get("role", {}).get("roleName", "")
                obj.data = role_info
                obj.score = current_score
                obj.damage = current_damage  # 更新伤害
                session.add(obj)
            else:
                # 新增
                session.add(cls(
                    uid=uid,
                    role_id=role_id,
                    role_name=role_info.get("role", {}).get("roleName", ""),
                    score=current_score,
                    damage=current_damage,   # 存入伤害
                    data=role_info
                ))
        await session.commit()
    @classmethod
    @with_session
    async def get_role_data_by_uid(
        cls, session: AsyncSession, uid: str
    ) -> List["WavesRoleData"]:
        result = await session.execute(select(cls).where(col(cls.uid) == uid))
        rows = result.scalars().all()
        return list(rows)

    @classmethod
    @with_session
    async def get_role_data_map_by_uid(
        cls, session: AsyncSession, uid: str
    ) -> Dict[str, Dict]:
        result = await session.execute(select(cls).where(col(cls.uid) == uid))
        rows = result.scalars().all()
        return {str(r.role_id): (r.data or {}) for r in rows}

    @classmethod
    @with_session
    async def get_role_rank_by_group(
        cls,
        session: AsyncSession,
        uid_list: List[str],
        role_id: str,
        rank_type: str = "score",  # "score" 或 "damage"
        limit: int = 100
    ) -> List["WavesRoleData"]:
        """获取群内特定角色的排行数据

        Args:
            uid_list: 群内所有UID列表
            role_id: 角色ID
            rank_type: 排行类型，"score"按评分排序，"damage"按伤害排序
            limit: 返回数量限制
        """
        stmt = select(cls).where(
            col(cls.uid).in_(uid_list),
            col(cls.role_id) == role_id
        )

        if rank_type == "damage":
            stmt = stmt.order_by(col(cls.damage).desc(), col(cls.score).desc())
        else:  # 默认按评分排序
            stmt = stmt.order_by(col(cls.score).desc(), col(cls.damage).desc())

        stmt = stmt.limit(limit)

        result = await session.execute(stmt)
        rows = result.scalars().all()
        return list(rows)

    @classmethod
    @with_session
    async def get_all_roles_by_uid_list(
        cls,
        session: AsyncSession,
        uid_list: List[str]
    ) -> List["WavesRoleData"]:
        """获取多个UID的所有角色数据"""
        result = await session.execute(
            select(cls).where(col(cls.uid).in_(uid_list))
        )
        rows = result.scalars().all()
        return list(rows)

    @classmethod
    @with_session
    async def get_global_role_rank(
        cls,
        session: AsyncSession,
        role_id: str,
        rank_type: str = "score",  # "score" 或 "damage"
        page: int = 1,
        page_size: int = 20
    ) -> tuple[List["WavesRoleData"], int]:
        """获取全局特定角色的排行数据（所有用户）

        Args:
            role_id: 角色ID
            rank_type: 排行类型，"score"按评分排序，"damage"按伤害排序
            page: 页码（从1开始）
            page_size: 每页数量

        Returns:
            (数据列表, 总数)
        """
        # 构建查询条件
        stmt = select(cls).where(col(cls.role_id) == role_id)

        # 排序
        if rank_type == "damage":
            stmt = stmt.order_by(col(cls.damage).desc(), col(cls.score).desc())
        else:  # 默认按评分排序
            stmt = stmt.order_by(col(cls.score).desc(), col(cls.damage).desc())

        # 计算总数
        count_stmt = select(cls).where(col(cls.role_id) == role_id)
        count_result = await session.execute(count_stmt)
        total_count = len(count_result.scalars().all())

        # 分页
        offset = (page - 1) * page_size
        stmt = stmt.offset(offset).limit(page_size)

        result = await session.execute(stmt)
        rows = result.scalars().all()
        return list(rows), total_count

    @classmethod
    @with_session
    async def get_role_rank_position(
        cls,
        session: AsyncSession,
        uid: str,
        role_id: str,
        rank_type: str = "score"
    ) -> Optional[int]:
        """获取某个角色在排行榜中的位置

        Args:
            uid: 用户UID
            role_id: 角色ID
            rank_type: 排行类型，"score"按评分排序，"damage"按伤害排序

        Returns:
            排名（从1开始），如果不存在则返回None
        """
        # 先获取该角色的数据
        stmt = select(cls).where(col(cls.uid) == uid, col(cls.role_id) == role_id)
        result = await session.execute(stmt)
        role_data = result.scalars().first()

        if not role_data:
            return None

        # 根据排行类型获取分数
        target_value = role_data.score if rank_type == "score" else role_data.damage

        # 查询排名更高的数量
        if rank_type == "damage":
            count_stmt = select(cls).where(
                col(cls.role_id) == role_id,
                col(cls.damage) > target_value
            )
        else:
            count_stmt = select(cls).where(
                col(cls.role_id) == role_id,
                col(cls.score) > target_value
            )

        count_result = await session.execute(count_stmt)
        higher_count = len(count_result.scalars().all())

        return higher_count + 1

    @classmethod
    async def migrate_from_json(cls, uid: str, json_data: List[Dict]) -> bool:
        """从JSON文件迁移数据到数据库

        Args:
            uid: 用户UID
            json_data: JSON文件中的角色数据列表

        Returns:
            是否迁移成功
        """
        try:
            scores_map, damage_map = await cls.calc_role_scores_and_damages(json_data)

            # 保存到数据库
            await cls.save_role_data(
                uid=uid,
                role_data_list=json_data,
                scores_map=scores_map,
                damage_map=damage_map
            )

            return True
        except Exception:
            return False

    @classmethod
    async def calc_role_scores_and_damages(
        cls, waves_data: List[Dict]
    ) -> tuple[Dict[str, float], Dict[str, float]]:
        """计算所有角色的评分和伤害（数据库操作方法）

        Args:
            waves_data: 角色数据列表

        Returns:
            (scores_map, damage_map): 角色ID -> 评分/伤害 的映射字典
        """
        from ..api.model import RoleDetailData
        from ..calc import WuWaCalc
        from ..calculate import calc_phantom_score, get_calc_map
        from ..damage.abstract import DamageRankRegister

        scores_map = {}
        damage_map = {}

        for role_data in waves_data:
            try:
                role_detail = RoleDetailData(**role_data)
                role_id = str(role_detail.role.roleId)

                # 如果没有声骸数据，跳过
                if (
                    not role_detail.phantomData
                    or not role_detail.phantomData.equipPhantomList
                ):
                    scores_map[role_id] = 0.0
                    damage_map[role_id] = 0.0
                    continue

                # 计算评分
                calc: WuWaCalc = WuWaCalc(role_detail)
                calc.phantom_pre = calc.prepare_phantom()
                calc.phantom_card = calc.enhance_summation_phantom_value(
                    calc.phantom_pre
                )
                calc.calc_temp = get_calc_map(
                    calc.phantom_card,
                    role_detail.role.roleName,
                    role_detail.role.roleId,
                )

                phantom_score = 0.0
                for _phantom in role_detail.phantomData.equipPhantomList:
                    if _phantom and _phantom.phantomProp:
                        props = _phantom.get_props()
                        _score, _bg = calc_phantom_score(
                            role_detail.role.roleId,
                            props,
                            _phantom.cost,
                            calc.calc_temp,
                        )
                        phantom_score += _score

                scores_map[role_id] = round(phantom_score, 2)

                # 计算伤害
                rankDetail = DamageRankRegister.find_class(role_id)
                if rankDetail:
                    calc.role_card = calc.enhance_summation_card_value(calc.phantom_card)
                    calc.damageAttribute = calc.card_sort_map_to_attribute(calc.role_card)
                    _, expected_damage = rankDetail["func"](
                        calc.damageAttribute, role_detail
                    )
                    # 去掉逗号并转换为浮点数
                    damage_map[role_id] = float(expected_damage.replace(",", ""))
                else:
                    damage_map[role_id] = 0.0

            except Exception as e:
                from gsuid_core.logger import logger

                logger.exception(
                    f"计算角色 {role_data.get('role', {}).get('roleId')} 评分和伤害失败:", e
                )
                role_id = str(role_data.get("role", {}).get("roleId", ""))
                scores_map[role_id] = 0.0
                damage_map[role_id] = 0.0

        return scores_map, damage_map


@site.register_admin
class WavesBindAdmin(GsAdminModel):
    pk_name = "id"
    page_schema = PageSchema(
        label="鸣潮绑定管理",
        icon="fa fa-users",
    )  # type: ignore

    # 配置管理模型
    model = WavesBind


@site.register_admin
class WavesUserAdmin(GsAdminModel):
    pk_name = "id"
    page_schema = PageSchema(
        label="鸣潮用户管理",
        icon="fa fa-users",
    )  # type: ignore

    # 配置管理模型
    model = WavesUser


@site.register_admin
class WavesPushAdmin(GsAdminModel):
    pk_name = "id"
    page_schema = PageSchema(label="鸣潮推送管理", icon="fa fa-bullhorn")  # type: ignore

    # 配置管理模型
    model = WavesPush
