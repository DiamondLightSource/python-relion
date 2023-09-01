from __future__ import annotations

# import datetime
import logging
from typing import NamedTuple, Optional

import ispyb.sqlalchemy
import sqlalchemy.exc

# from sqlalchemy import delete

logger = logging.getLogger("relion.zocalo.ispybsvc_buffer")


class BufferResult(NamedTuple):
    success: bool
    value: Optional[int]


def evict(*, session):
    """Throw away buffered information after a certain time.

    This needs to be run periodically to ensure the buffer tables don't
    become unnecessarily large. Information can be deleted when it is very
    unlikely that it needs to be referred to again. As buffer entries are tied
    to a single AutoProcProgram run it should be a safe bet that the buffer
    information is only of limited use after that program has completed. We
    give 30 days to deal with transient database and service problems and any
    messages stuck in the DLQ.
    """

    # Not quite clear yet how we should do this. SQLAlchemy vs stored procedure.
    # Suggested SQL something along the lines of

    # DELETE zb
    # FROM zc_ZocaloBuffer zb
    # JOIN AutoProcProgram app
    # WHERE app.autoProcProgramId = zb.AutoProcProgramId
    # AND (DATE(app.processingEndTime) < DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    #      OR DATE(app.recordTimeStamp) < DATE_SUB(CURDATE(), INTERVAL 60 DAY))

    # (
    #     delete(ispyb.sqlalchemy.ZcZocaloBuffer)
    #     .where(
    #         ispyb.sqlalchemy.AutoProcProgram.autoProcProgramId
    #         == ispyb.sqlalchemy.ZcZocaloBuffer.AutoProcProgramID
    #     )
    #     .where(
    #         (
    #             ispyb.sqlalchemy.AutoProcProgram.processingEndTime
    #             < datetime.datetime.now() - datetime.timedelta(days=30)
    #         )
    #         | (
    #             ispyb.sqlalchemy.AutoProcProgram.recordTimeStamp
    #             < datetime.datetime.now() - datetime.timedelta(days=60)
    #         )
    #     )
    # )
    # session.commit()


def load(*, session, program: int, uuid: int) -> BufferResult:
    """Load an entry from the zc_ZocaloBuffer table.

    Given an AutoProcProgramID and a client-defined unique reference (uuid)
    retrieve a reference value from the database if possible.
    """
    query = (
        session.query(ispyb.sqlalchemy.ZcZocaloBuffer)
        .filter(ispyb.sqlalchemy.ZcZocaloBuffer.AutoProcProgramID == program)
        .filter(ispyb.sqlalchemy.ZcZocaloBuffer.UUID == uuid)
    )
    try:
        result = query.one()
        logger.info(
            f"buffer lookup for {program}.{uuid} succeeded (={result.Reference})"
        )
        return BufferResult(success=True, value=result.Reference)
    except sqlalchemy.exc.NoResultFound:
        logger.info(f"buffer lookup for {program}.{uuid} failed")
        return BufferResult(success=False, value=None)


def store(*, session, program: int, uuid: int, reference: int):
    """Write an entry into the zc_ZocaloBuffer table.

    The buffer table allows decoupling of the message-sending client
    and the database server-side assigned primary keys. The client defines
    a unique reference (uuid) that it will use to refer to a real primary
    key value (reference). All uuids are relative to an AutoProcProgramID
    and will be stored for a limited time based on the underlying
    AutoProcProgram record.
    """
    entry = ispyb.sqlalchemy.ZcZocaloBuffer(
        AutoProcProgramID=program,
        UUID=uuid,
        Reference=reference,
    )
    session.merge(entry)
    logger.info(f"buffering value {reference} for {program}.{uuid}")
    session.commit()
