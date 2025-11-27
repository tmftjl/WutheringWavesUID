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
        更新所有角色持有率
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
    async def sync_role_data(
        cls, 
        session: AsyncSession, 
        uid: str, 
        final_role_list: List[Dict], 
        scores_map: Dict[str, float], 
        damage_map: Dict[str, float]
    ):
        """
        数据层：全量同步角色数据。
        """
        if not final_role_list:
            return

        stmt = select(cls).where(cls.uid == uid)
        result = await session.execute(stmt)
        # 建立 角色ID -> 数据库对象 的映射
        db_map = {r.role_id: r for r in result.scalars().all()}
        # 记录本次要保留的ID集合
        incoming_ids = set()
        
        to_add = []

        for item in final_role_list:
            role_id = str(item["role"]["roleId"])
            incoming_ids.add(role_id)
            
            role_detail = RoleDetailData(**item) 
            chain_num = role_detail.get_chain_num()
            score = scores_map.get(role_id, 0.0)
            damage = damage_map.get(role_id, 0.0)
            name = item["role"]["roleName"]

            if role_id in db_map:
                # --- Update: 存在则更新 ---
                obj = db_map[role_id]
                obj.role_name = name
                obj.data = item
                obj.score = score
                obj.damage = damage
                obj.chain_num = chain_num
                session.add(obj)
            else:
                # --- Insert: 不存在则新增 ---
                new_obj = cls(
                    uid=uid,
                    role_id=role_id,
                    role_name=name,
                    score=score,
                    damage=damage,
                    data=item,
                    chain_num=chain_num,
                )
                to_add.append(new_obj)

        # 数据库里有，但传入列表里没有的，说明是需要剔除的脏数据（例如旧的漂泊者）
        for role_id, obj in db_map.items():
            if role_id not in incoming_ids:
                await session.delete(obj)
        # 4. 批量新增
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
    async def get_group_all_data(
        cls,
        uid_list: List[str],
        role_id: str,
        rank_type: str = "score",  # "score" 或 "damage"
    ) -> List["WavesRoleData"]:
        """
        获取群内该角色所有数据，直接在数据库层完成排序
        """
        async with get_session() as session:
            stmt = select(cls).where(
                cls.uid.in_(uid_list),
                cls.role_id == role_id
            )
            if rank_type == "damage":
                stmt = stmt.order_by(cls.damage.desc(), cls.score.desc())
            else:
                stmt = stmt.order_by(cls.score.desc(), cls.damage.desc())

            result = await session.execute(stmt)
            return list(result.scalars().all())

    @classmethod
    @with_session
    async def get_role_rank_data(
        cls,
        session: AsyncSession,
        role_id: str,
        rank_type: str = "score",  # "score" 或 "damage"
        limit: int = 20,           # 默认前 20
        target_uid: Optional[str] = None
    ) -> Dict:
        """
        获取单角色排行数据 (Top N + 指定用户的排名信息)
        """
        valid_uids_subquery = cls._get_valid_uids_stmt()
        base_where = [
            cls.role_id == role_id,
            cls.uid.in_(valid_uids_subquery)
        ]
        if rank_type == "damage":
            order_criteria = [cls.damage.desc(), cls.score.desc()]
        else:
            order_criteria = [cls.score.desc(), cls.damage.desc()]
        list_stmt = (
            select(cls)
            .where(*base_where)
            .order_by(*order_criteria)
            .limit(limit)
        )
        rank_rows = (await session.execute(list_stmt)).scalars().all()
        self_info = None
        
        if target_uid:
            for idx, row in enumerate(rank_rows):
                if str(row.uid) == str(target_uid):
                    self_info = {
                        "rank": idx + 1,
                        "data": row
                    }
                    break
            if not self_info:
                self_data_stmt = select(cls).where(
                    cls.uid == target_uid,
                    cls.role_id == role_id
                )
                self_row = (await session.execute(self_data_stmt)).scalar_one_or_none()

                if self_row:
                    if rank_type == "damage":
                        better_condition = or_(
                            cls.damage > self_row.damage,
                            and_(cls.damage == self_row.damage, cls.score > self_row.score)
                        )
                    else:
                        better_condition = or_(
                            cls.score > self_row.score,
                            and_(cls.score == self_row.score, cls.damage > self_row.damage)
                        )

                    rank_count_stmt = (
                        select(func.count())
                        .select_from(cls)
                        .where(*base_where, better_condition)
                    )
                    rank_pos = (await session.execute(rank_count_stmt)).scalar() or 0
                    
                    self_info = {
                        "rank": rank_pos + 1,
                        "data": self_row
                    }

        return {
            "list": list(rank_rows),
            "self_info": self_info
        }

    @classmethod
    @with_session
    async def get_role_rank_position(
        cls,
        session: AsyncSession,
        uid: str,
        role_id: str,
        rank_type: str = "score"
    ) -> Optional[int]:
        """获取某个角色在总排行榜中的位置（只在有效CK用户中排名）"""
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
    async def get_rank_data(
        cls,
        session: AsyncSession,
        limit: int = 20,              # 默认只看前20名
        min_score: float = 175.0,
        target_uid: Optional[str] = None
    ) -> Dict:
        """
        获取练度排行 (Top N + 个人信息)，不统计总人数
        """
        valid_uids_subquery = cls._get_valid_uids_stmt()
        # 计算每个用户的总分 (subquery)
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

        # 获取 Top N 列表
        rank_stmt = (
            select(subquery)
            .order_by(subquery.c.total_score.desc())
            .limit(limit)
        )
        rank_rows = (await session.execute(rank_stmt)).all()

        # 处理 "当前用户" 的排名
        self_rank_info = None
        target_in_list = False
        
        # 收集需要查详情的 UID (包括榜单上的 和 自己的)
        uids_to_fetch = [row.uid for row in rank_rows]

        if target_uid:
            # 检查自己是否在榜单内 (节省一次查询)
            for idx, row in enumerate(rank_rows):
                if str(row.uid) == str(target_uid):
                    target_in_list = True
                    self_rank_info = {
                        "rank": idx + 1,
                        "uid": row.uid,
                        "total_score": float(row.total_score or 0),
                        "char_count": int(row.char_count or 0),
                    }
                    break
            
            # 如果不在榜单内，单独查询自己的分数和排名
            if not target_in_list:
                # 查自己的聚合数据
                user_agg_stmt = select(subquery).where(subquery.c.uid == target_uid)
                user_row = (await session.execute(user_agg_stmt)).first()

                if user_row:
                    user_total_score = float(user_row.total_score or 0)
                    if user_total_score > 0:
                        # 统计分数比自己高的人数 (确定排名)
                        higher_count_stmt = (
                            select(func.count())
                            .select_from(subquery)
                            .where(subquery.c.total_score > user_total_score)
                        )
                        higher_count = (await session.execute(higher_count_stmt)).scalar_one()
                        
                        self_rank_info = {
                            "rank": higher_count + 1,
                            "uid": user_row.uid,
                            "total_score": user_total_score,
                            "char_count": int(user_row.char_count or 0)
                        }
                        # 把自己加入待查询详情列表
                        uids_to_fetch.append(target_uid)

        # 批量获取角色详情
        if not uids_to_fetch:
            return {"list": [], "self_rank": None}

        # 一次性查出所有涉及用户的角色
        char_stmt = (
            select(cls)
            .where(cls.uid.in_(uids_to_fetch), cls.score >= min_score)
            .order_by(cls.uid, cls.score.desc())
        )
        char_rows = (await session.execute(char_stmt)).scalars().all()

        # 内存分组: uid -> [Role1, Role2...]
        chars_by_uid = defaultdict(list)
        for cr in char_rows:
            chars_by_uid[cr.uid].append(cr)

        def format_chars(uid_):
            # 取前10个最高分角色
            top_chars = chars_by_uid.get(uid_, [])[:10]
            return [
                {
                    "role_id": c.role_id,
                    "role_name": c.role_name,
                    "score": c.score,
                    "data": c.data,
                }
                for c in top_chars
            ]

        # 组装最终结果
        result_list = []
        for idx, row in enumerate(rank_rows):
            result_list.append({
                "rank": idx + 1,
                "uid": row.uid,
                "total_score": float(row.total_score or 0),
                "char_count": int(row.char_count or 0),
                "char_scores": format_chars(row.uid),
            })

        # 补全自己的角色详情
        if self_rank_info:
            self_rank_info["char_scores"] = format_chars(self_rank_info["uid"])

        return {
            "list": result_list,
            "self_rank": self_rank_info
        }
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