import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Type, TypeVar, Tuple

from sqlalchemy import delete, null, update, Column, JSON, UniqueConstraint, Index, func, case, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import and_, or_
from sqlmodel import Field, col

from gsuid_core.utils.database.base_models import (
    Bind,
    Push,
    User,
    with_session,
    BaseIDModel,
)
from gsuid_core.utils.database.startup import exec_list
from gsuid_core.webconsole.mount_app import GsAdminModel, PageSchema, site
from ..api.model import RoleDetailData

# --- 数据库迁移补充 ---
exec_list.extend(
    []
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
    @with_session
    async def get_all_bind(
        cls: Type[T_WavesBind], session: AsyncSession
    ) -> List[T_WavesBind]:
        """获取所有绑定数据"""
        result = await session.scalars(select(cls))
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


class WavesAccountInfo(BaseIDModel, table=True):
    __table_args__: Dict[str, Any] = {"extend_existing": True}

    uid: str = Field(index=True, unique=True, title="鸣潮UID")
    name: str = Field(default="", title="账号昵称")
    level: int = Field(default=0, title="账号等级")
    world_level: int = Field(default=0, title="世界等级")
    create_time: int = Field(default=0, title="创建时间")  # 在此用作最后更新时间

    @classmethod
    @with_session
    async def save_account_info(
        cls,
        session: AsyncSession,
        uid: str,
        name: str,
        level: int,
        world_level: int,
        create_time: int
    ):
        """保存或更新账号基础信息"""
        stmt = select(cls).where(cls.uid == uid)
        result = await session.execute(stmt)
        obj = result.scalars().first()

        if obj:
            # 更新
            obj.name = name
            obj.level = level
            obj.world_level = world_level
            # 更新时间，作为判断活跃用户的依据
            obj.create_time = create_time
            session.add(obj)
        else:
            # 新增
            session.add(cls(
                uid=uid,
                name=name,
                level=level,
                world_level=world_level,
                create_time=create_time
            ))
        await session.commit()

    @classmethod
    @with_session
    async def get_account_info(
        cls,
        session: AsyncSession,
        uid: str
    ) -> Optional["WavesAccountInfo"]:
        """获取账号基础信息"""
        stmt = select(cls).where(cls.uid == uid)
        result = await session.execute(stmt)
        return result.scalars().first()


class WavesCharHoldRate(BaseIDModel, table=True):
    """角色持有率缓存表"""
    __table_args__ = (
        Index('ix_char_hold_rate_char_id', 'char_id'),
        Index('ix_char_hold_rate_update_time', 'update_time'),
        {'extend_existing': True},
    )

    char_id: str = Field(index=True, title="角色ID")
    char_name: str = Field(default="", title="角色名称")

    total_players: int = Field(default=0, title="总玩家数")
    hold_count: int = Field(default=0, title="持有人数")
    hold_rate: float = Field(default=0.0, title="持有率%")
    chain_distribution: Dict = Field(default_factory=dict, sa_column=Column(JSON), title="共鸣链分布")
    update_time: int = Field(default=0, title="更新时间戳")

    @classmethod
    @with_session
    async def get_all_hold_rates(
        cls,
        session: AsyncSession
    ) -> List["WavesCharHoldRate"]:
        """获取所有角色持有率"""
        stmt = select(cls).order_by(cls.hold_rate.desc())
        result = await session.execute(stmt)
        return list(result.scalars().all())

    @classmethod
    @with_session
    async def get_hold_rate_by_char_id(
        cls,
        session: AsyncSession,
        char_id: str
    ) -> Optional["WavesCharHoldRate"]:
        """获取指定角色的持有率"""
        result = await session.execute(
            select(cls).where(cls.char_id == char_id)
        )
        return result.scalars().first()

    @classmethod
    @with_session
    async def update_all_hold_rates(
        cls,
        session: AsyncSession
    ) -> int:
        """
        [优化版] 更新所有角色持有率
        统计标准：Cookie有效 AND 最近30天内更新过AccountInfo(活跃用户)
        """
        current_time = int(time.time())
        # 30天前的时间戳
        active_threshold = current_time - (30 * 24 * 60 * 60)

        # 1. 筛选有效且活跃的UID
        # 条件: (CK状态正常) AND (有CK) AND (账号信息最近30天更新过)
        # 结果去重 (distinct)
        valid_active_uids_stmt = (
            select(WavesUser.uid)
            .join(WavesAccountInfo, WavesAccountInfo.uid == WavesUser.uid)
            .where(
                or_(WavesUser.status == null(), WavesUser.status == ""),
                WavesUser.cookie != null(),
                WavesUser.cookie != "",
                WavesAccountInfo.create_time >= active_threshold
            )
            .distinct()
        )

        # 2. 统计总活跃且有效玩家数
        total_users_stmt = select(func.count()).select_from(valid_active_uids_stmt.subquery())
        total_player_count = (await session.execute(total_users_stmt)).scalar() or 0

        if total_player_count == 0:
            return 0

        # 3. 聚合查询角色数据
        # 仅查询 UID 在 valid_active_uids_stmt 中的角色数据
        stmt = (
            select(
                WavesRoleData.role_id,
                func.max(WavesRoleData.role_name).label("role_name"),
                WavesRoleData.chain_num,
                func.count(WavesRoleData.uid).label("count")
            )
            .where(WavesRoleData.uid.in_(valid_active_uids_stmt))
            .group_by(WavesRoleData.role_id, WavesRoleData.chain_num)
        )

        result = await session.execute(stmt)
        rows = result.all()

        # 4. 在内存中组装数据
        char_stats = {}

        for r in rows:
            role_id = r.role_id
            chain = str(r.chain_num)

            if role_id not in char_stats:
                char_stats[role_id] = {
                    "char_name": r.role_name,
                    "player_count": 0,
                    "chains": {str(i): 0 for i in range(7)}
                }

            count = r.count
            char_stats[role_id]["player_count"] += count

            if chain in char_stats[role_id]["chains"]:
                char_stats[role_id]["chains"][chain] += count

        # 5. 获取现有的所有缓存记录
        existing_stmt = select(cls)
        existing_result = await session.execute(existing_stmt)
        existing_records = {r.char_id: r for r in existing_result.scalars().all()}

        # 6. 更新 or 插入
        updated_count = 0

        for char_id, stats in char_stats.items():
            player_count = stats["player_count"]
            if player_count == 0:
                continue

            hold_rate = round(player_count / total_player_count * 100, 2)

            chain_distribution = {}
            for chain, count in stats["chains"].items():
                if count > 0:
                    chain_distribution[chain] = round(count / player_count * 100, 2)

            if char_id in existing_records:
                record = existing_records[char_id]
                record.char_name = stats["char_name"]
                record.total_players = total_player_count
                record.hold_count = player_count
                record.hold_rate = hold_rate
                record.chain_distribution = chain_distribution
                record.update_time = current_time
                session.add(record)
            else:
                new_record = cls(
                    char_id=char_id,
                    char_name=stats["char_name"],
                    total_players=total_player_count,
                    hold_count=player_count,
                    hold_rate=hold_rate,
                    chain_distribution=chain_distribution,
                    update_time=current_time
                )
                session.add(new_record)

            updated_count += 1

        await session.commit()
        return updated_count

    @classmethod
    @with_session
    async def get_last_update_time(
        cls,
        session: AsyncSession
    ) -> Optional[int]:
        """获取最后更新时间"""
        result = await session.execute(
            select(func.max(cls.update_time))
        )
        return result.scalar()


class WavesRoleData(BaseIDModel, table=True):
    __table_args__ = (
        UniqueConstraint('uid', 'role_id', name='uq_waves_role_uid_role'),
        Index('ix_role_id_score', 'role_id', 'score'),
        Index('ix_role_id_damage', 'role_id', 'damage'),
        {'extend_existing': True},
    )

    uid: str = Field(index=True, title="鸣潮UID")
    role_id: str = Field(index=True, title="角色ID")
    role_name: str = Field(default="", title="角色名称")
    chain_num: int = Field(default=0, index=True, title="链数")
    score: float = Field(default=0.0, index=True, title="评分")
    damage: float = Field(default=0.0, index=True, title="伤害")
    data: Dict = Field(default={}, sa_column=Column(JSON))

    @staticmethod
    def _get_valid_uids_stmt():
        """
        获取去重后的有效用户UID子查询 (Status空 + Cookie非空)
        """
        return (
            select(WavesUser.uid)
            .where(
                or_(WavesUser.status == null(), WavesUser.status == ""),
                WavesUser.cookie != null(),
                WavesUser.cookie != ""
            )
            .distinct()
        )

    @classmethod
    @with_session
    async def save_role_data(
        cls, 
        session: AsyncSession, 
        uid: str, 
        role_data_list: List[Dict],
        scores_map: Optional[Dict[str, float]] = None, 
        damage_map: Optional[Dict[str, float]] = None 
    ):
        """
        批量保存角色数据，包含星级和命座的清洗存储
        """
        if not role_data_list:
            return

        if scores_map is None:
            scores_map = {}
        if damage_map is None:
            damage_map = {}

        # 1. 一次性查出该用户所有已存在的角色数据
        stmt = select(cls).where(cls.uid == uid)
        result = await session.execute(stmt)
        existing_roles = result.scalars().all()
        existing_map = {r.role_id: r for r in existing_roles}

        to_add = []
        
        for role_info in role_data_list:
            role_id = str(role_info.get("role", {}).get("roleId", ""))
            if not role_id:
                continue
            
            new_role_name = role_info.get("role", {}).get("roleName", "")
            current_score = scores_map.get(role_id, 0.0)
            current_damage = damage_map.get(role_id, 0.0)
            
            # 提取星级和命座
            role_detail = RoleDetailData(**role_info)
            chain_num = role_detail.get_chain_num()
            if role_id in existing_map:
                # 更新
                obj = existing_map[role_id]
                obj.role_name = new_role_name
                obj.data = role_info
                obj.score = current_score
                obj.damage = current_damage
                obj.chain_num = chain_num
                session.add(obj)
            else:
                # 新增
                new_obj = cls(
                    uid=uid,
                    role_id=role_id,
                    role_name=new_role_name,
                    score=current_score,
                    damage=current_damage,
                    data=role_info,
                    chain_num=chain_num,
                )
                to_add.append(new_obj)

        if to_add:
            session.add_all(to_add)
        
        await session.commit()

    @classmethod
    @with_session
    async def get_role_data_by_uid(
        cls, session: AsyncSession, uid: str
    ) -> List["WavesRoleData"]:
        result = await session.execute(select(cls).where(cls.uid == uid))
        rows = result.scalars().all()
        return list(rows)

    @classmethod
    @with_session
    async def get_role_data_map_by_uid(
        cls, session: AsyncSession, uid: str
    ) -> Dict[str, Dict]:
        result = await session.execute(select(cls).where(cls.uid == uid))
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
        """获取群内特定角色的排行数据"""
        stmt = select(cls).where(
            cls.uid.in_(uid_list),
            cls.role_id == role_id
        )

        if rank_type == "damage":
            stmt = stmt.order_by(cls.damage.desc(), cls.score.desc())
        else:  # 默认按评分排序
            stmt = stmt.order_by(cls.score.desc(), cls.damage.desc())

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
            select(cls).where(cls.uid.in_(uid_list))
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
    ) -> Tuple[List["WavesRoleData"], int]:
        """
        获取全局特定角色的排行数据（只包含有效CK的用户）
        修正：通过 IN 子查询过滤UID，去重防止多对一
        """
        
        valid_uids_subquery = cls._get_valid_uids_stmt()

        # 基础查询
        base_query = select(cls).where(
            cls.role_id == role_id,
            cls.uid.in_(valid_uids_subquery)
        )

        # 计算总数
        count_stmt = select(func.count()).select_from(cls).where(
            cls.role_id == role_id,
            cls.uid.in_(valid_uids_subquery)
        )
        total_count = (await session.execute(count_stmt)).scalar() or 0

        # 排序
        if rank_type == "damage":
            stmt = base_query.order_by(cls.damage.desc(), cls.score.desc())
        else:
            stmt = base_query.order_by(cls.score.desc(), cls.damage.desc())

        # 分页
        offset = max(0, (page - 1) * page_size)
        stmt = stmt.offset(offset).limit(page_size)

        result = await session.execute(stmt)
        rows = result.scalars().all()
        return list(rows), int(total_count)

    @classmethod
    @with_session
    async def get_role_rank_position(
        cls,
        session: AsyncSession,
        uid: str,
        role_id: str,
        rank_type: str = "score"
    ) -> Optional[int]:
        """获取某个角色在排行榜中的位置（只在有效CK用户中排名）"""
        
        # 1. 先获取该角色的数据
        stmt = select(cls).where(cls.uid == uid, cls.role_id == role_id)
        result = await session.execute(stmt)
        role_data = result.scalars().first()

        if not role_data:
            return None

        # 2. 确认此UID为有效CK用户 (去重)
        valid_check_stmt = select(func.count()).select_from(WavesUser).where(
            WavesUser.uid == uid,
            or_(WavesUser.status == null(), WavesUser.status == ""),
            WavesUser.cookie != null(),
            WavesUser.cookie != ""
        )
        is_valid = (await session.execute(valid_check_stmt)).scalar() or 0
        
        if not is_valid:
            return None

        # 3. 统计比该值更高的人数 (使用IN子查询，避免Join产生的重复)
        target_value = role_data.score if rank_type == "score" else role_data.damage
        compare_col = cls.damage if rank_type == "damage" else cls.score

        valid_uids_subquery = cls._get_valid_uids_stmt()

        count_stmt = select(func.count()).select_from(cls).where(
            cls.role_id == role_id,
            compare_col > target_value,
            cls.uid.in_(valid_uids_subquery)
        )

        higher_count = (await session.execute(count_stmt)).scalar() or 0
        return int(higher_count) + 1

    @classmethod
    @with_session
    async def get_total_rank(
        cls,
        session: AsyncSession,
        page: int = 1,
        page_size: int = 20,
        min_score: float = 175.0
    ) -> Tuple[List[Dict], int]:
        """获取练度总排行 (修正去重逻辑)"""
        
        valid_uids_subquery = cls._get_valid_uids_stmt()

        # 聚合子查询：计算每个有效用户的总分和角色数
        # 使用 IN 过滤 valid_uids
        subquery = (
            select(
                cls.uid.label("uid"),
                func.sum(
                    case(
                        (cls.score >= min_score, cls.score),
                        else_=0
                    )
                ).label("total_score"),
                func.count(
                    case(
                        (cls.score >= min_score, 1),
                        else_=None
                    )
                ).label("char_count")
            )
            .where(
                cls.score >= min_score,
                cls.uid.in_(valid_uids_subquery)
            )
            .group_by(cls.uid)
            .subquery()
        )

        # 计算总人数
        total_count = (
            await session.execute(select(func.count()).select_from(subquery))
        ).scalar_one()

        # 分页查询聚合结果
        offset = max(0, (page - 1) * page_size)
        page_stmt = (
            select(subquery)
            .order_by(subquery.c.total_score.desc())
            .offset(offset)
            .limit(page_size)
        )
        page_rows = (await session.execute(page_stmt)).all()

        if not page_rows:
            return [], int(total_count)

        # 收集本页的 UID
        uids = [row.uid for row in page_rows]

        # 批量拉取这些用户的角色详情
        char_stmt = (
            select(cls)
            .where(cls.uid.in_(uids), cls.score >= min_score)
            .order_by(cls.uid, cls.score.desc())
        )
        char_rows = (await session.execute(char_stmt)).scalars().all()

        # 内存分组
        chars_by_uid = defaultdict(list)
        for cr in char_rows:
            chars_by_uid[cr.uid].append(cr)

        # 组装最终结果
        result_list = []
        for idx, row in enumerate(page_rows):
            uid = row.uid
            total_score = float(row.total_score or 0)
            char_count = int(row.char_count or 0)

            top_chars = chars_by_uid.get(uid, [])[:10]
            char_scores = [
                {
                    "role_id": c.role_id,
                    "role_name": c.role_name,
                    "score": c.score,
                    "data": c.data,
                }
                for c in top_chars
            ]

            result_list.append(
                {
                    "rank": offset + idx + 1,
                    "uid": uid,
                    "total_score": total_score,
                    "char_count": char_count,
                    "char_scores": char_scores,
                }
            )

        return result_list, int(total_count)

    @classmethod
    @with_session
    async def get_total_rank_position(
        cls,
        session: AsyncSession,
        uid: str,
        min_score: float = 175.0
    ) -> Optional[int]:
        """获取某个用户在练度总排行中的位置"""

        valid_uids_subquery = cls._get_valid_uids_stmt()

        # 1. 检查用户是否有效
        valid_exist = (
            await session.execute(
                select(func.count()).select_from(WavesUser).where(
                    WavesUser.uid == uid,
                    or_(WavesUser.status == null(), WavesUser.status == ""),
                    WavesUser.cookie != null(),
                    WavesUser.cookie != ""
                )
            )
        ).scalar() or 0
        
        if valid_exist == 0:
            return None

        # 2. 计算当前用户的总分
        user_stmt = (
            select(
                func.sum(
                    case(
                        (cls.score >= min_score, cls.score),
                        else_=0,
                    )
                )
            )
            .where(
                cls.uid == uid,
                cls.score >= min_score
            )
        )
        user_total_score = (await session.execute(user_stmt)).scalar() or 0
        
        if user_total_score == 0:
            return None

        # 3. 统计总分高于该用户的人数 (使用IN子查询)
        higher_subq = (
            select(cls.uid)
            .where(
                cls.score >= min_score,
                cls.uid.in_(valid_uids_subquery)
            )
            .group_by(cls.uid)
            .having(
                func.sum(
                    case(
                        (cls.score >= min_score, cls.score),
                        else_=0,
                    )
                ) > user_total_score
            )
            .subquery()
        )

        higher_count = (
            await session.execute(select(func.count()).select_from(higher_subq))
        ).scalar_one()

        return int(higher_count) + 1

    @classmethod
    async def calc_role_scores_and_damages(
        cls, waves_data: List[Dict]
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        """计算所有角色的评分和伤害"""
        from ..api.model import RoleDetailData
        from ..calc import WuWaCalc
        from ..calculate import calc_phantom_score, get_calc_map
        from ..damage.abstract import DamageRankRegister
        from gsuid_core.logger import logger

        scores_map = {}
        damage_map = {}

        for role_data in waves_data:
            role_id = str(role_data.get("role", {}).get("roleId", ""))
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
                calc = WuWaCalc(role_detail)
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
                    # 去掉千位分隔符转为浮点数
                    damage_map[role_id] = float(str(expected_damage).replace(",", ""))
                else:
                    damage_map[role_id] = 0.0

            except Exception as e:
                logger.exception(
                    f"计算角色 {role_id} 评分和伤害失败:", e
                )
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
    model = WavesBind


@site.register_admin
class WavesUserAdmin(GsAdminModel):
    pk_name = "id"
    page_schema = PageSchema(
        label="鸣潮用户管理",
        icon="fa fa-users",
    )  # type: ignore
    model = WavesUser


@site.register_admin
class WavesPushAdmin(GsAdminModel):
    pk_name = "id"
    page_schema = PageSchema(label="鸣潮推送管理", icon="fa fa-bullhorn")  # type: ignore
    model = WavesPush


@site.register_admin
class WavesCharHoldRateAdmin(GsAdminModel):
    pk_name = "id"
    page_schema = PageSchema(
        label="鸣潮持有率管理",
        icon="fa fa-users",
    )  # type: ignore
    model = WavesCharHoldRate


@site.register_admin
class WavesAccountInfoAdmin(GsAdminModel):
    pk_name = "id"
    page_schema = PageSchema(
        label="鸣潮用户账户信息管理",
        icon="fa fa-users",
    )  # type: ignore
    model = WavesAccountInfo


@site.register_admin
class WavesRoleDataAdmin(GsAdminModel):
    pk_name = "id"
    page_schema = PageSchema(
        label="鸣潮用户角色管理",
        icon="fa fa-users",
    )  # type: ignore
    model = WavesRoleData