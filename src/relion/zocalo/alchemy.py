from sqlalchemy import Column
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class ZocaloBuffer(Base):
    __tablename__ = "ZocaloBuffer"

    AutoProcProgramID = Column(
        INTEGER(10),
        primary_key=True,
        comment="Reference to an existing AutoProcProgram",
        autoincrement=False,
        nullable=False,
    )
    UUID = Column(
        INTEGER(10),
        primary_key=True,
        comment="AutoProcProgram-specific unique identifier",
        autoincrement=False,
        nullable=False,
    )
    Reference = Column(
        INTEGER(10),
        comment="Context-dependent reference to primary key IDs in other ISPyB tables",
    )


def buffer_url() -> str:
    import ispyb.sqlalchemy

    sqlalchemy_url = ispyb.sqlalchemy.url()
    local_url = "/".join(sqlalchemy_url.split("/")[:-1]) + "/zocalo"
    return local_url
