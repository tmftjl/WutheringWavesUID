import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

from sqlalchemy import delete
from sqlalchemy.orm import aliased
from sqlmodel import Field, SQLModel, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func, tuple_, operators
from gsuid_core.utils.database.startup import exec_list
from gsuid_core.utils.database.base_models import with_session

logger = logging.getLogger(__name__)

class SlashSimpleRecord(SQLModel, table=True):
    __tablename__ = "slash_simple_records"
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: str = Field(index=True)
    wavesId: str = Field(index=True)
    name: str
    challengeId: int
    challengeName: str
    halfList: str
    rank: str
    score: int

    @classmethod
    @with_session
    async def save_simple_record(
        cls, session: AsyncSession, payload: Dict[str, Any], user_id: str
    ) -> "SlashSimpleRecord":
        try:
            waves_id = payload.get("wavesId", "")
            name = payload.get("name", "")
            challenge_id = int(payload.get("challengeId", 0))

            result = await session.execute(
                select(cls).where(
                    (cls.wavesId == waves_id)
                    & (cls.challengeId == challenge_id)
                    & (cls.user_id == user_id)
                )
            )
            existing_record = result.scalar_one_or_none()

            update_payload = {
                "name": name,
                "challengeName": payload.get("challengeName", ""),
                "halfList": json.dumps(payload.get("halfList", []), ensure_ascii=False),
                "rank": payload.get("rank", ""),
                "score": int(payload.get("score", 0)),
            }

            if existing_record:
                for key, value in update_payload.items():
                    setattr(existing_record, key, value)
                session.add(existing_record)
                await session.commit()
                await session.refresh(existing_record)
                return existing_record
            else:
                record = cls(
                    user_id=user_id,
                    wavesId=waves_id,
                    challengeId=challenge_id,
                    **update_payload,
                )
                session.add(record)
                await session.commit()
                await session.refresh(record)
                return record
        except Exception:
            await session.rollback()
            raise

    @classmethod
    @with_session
    async def get_records_by_users_and_challenge(
        cls,
        session: AsyncSession,
        user_uid_pairs: List[Tuple[str, str]],
        challenge_id: int,
    ) -> List["SlashSimpleRecord"]:
        """一次性获取多个用户在指定挑战中的记录。"""
        if not user_uid_pairs:
            return []
        try:
            # 使用 operators.in_op 来提高版本兼容性
            statement = select(cls).where(
                cls.challengeId == challenge_id,
                operators.in_op(tuple_(cls.user_id, cls.wavesId), user_uid_pairs),
            )
            result = await session.execute(statement)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"批量获取挑战记录失败: {e}")
            return []

    @classmethod
    @with_session
    async def get_all_records(cls, session: AsyncSession) -> List["SlashSimpleRecord"]:
        """获取所有用户的记录。"""
        try:
            subquery = (
                select(
                    cls,
                    func.row_number()
                    .over(
                        partition_by=cls.wavesId,
                        order_by=cls.score.desc(),
                    )
                    .label("row_num"),
                )
                .subquery()
            )
            ranked_records_alias = aliased(cls, subquery)
            statement = select(ranked_records_alias).where(subquery.c.row_num == 1)

            result = await session.execute(statement)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"获取所有记录失败: {e}")
            return []

    @classmethod 
    @with_session
    async def clean_simple(cls, session: AsyncSession):
        await session.execute(delete(SlashSimpleRecord))
        await session.commit()
