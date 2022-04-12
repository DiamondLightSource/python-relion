from __future__ import annotations

from sqlalchemy import TIMESTAMP, Column, ForeignKey, String
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

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


class RelionPipelineInfo(Base):
    __tablename__ = "RelionPipelineInfo"

    pipeline_id = Column(INTEGER(10), primary_key=True)
    image_x = Column(
        INTEGER(5),
        comment="Number of pixels in the x direction",
        autoincrement=False,
    )
    image_y = Column(
        INTEGER(5),
        comment="Number of pixels in the y direction",
        autoincrement=False,
    )
    microscope = Column(
        String(10),
        nullable=False,
        comment="Microscope name",
    )
    project_path = Column(
        String(250),
        nullable=False,
        comment="Path to the project directory",
    )


class ClusterJobInfo(Base):
    __tablename__ = "ClusterJobInfo"

    cluster = Column(
        String(250),
        nullable=False,
        comment="Name of cluster",
        primary_key=True,
    )
    cluster_id = Column(
        INTEGER(10),
        primary_key=True,
        comment="ID of the cluster job",
        autoincrement=False,
        nullable=False,
    )
    auto_proc_program_id = Column(
        INTEGER(10),
        comment="Reference to the AutoProcProgram the cluster job is attached to",
        autoincrement=False,
    )
    start_time = Column(
        TIMESTAMP,
        comment="Start time of cluster job",
    )
    end_time = Column(
        TIMESTAMP,
        comment="End time of cluster job",
    )


class RelionJobInfo(Base):
    __tablename__ = "RelionJobInfo"

    job_id = Column(INTEGER(10), primary_key=True)
    cluster_id = Column(
        INTEGER(10),
        comment="ID of the cluster job",
        autoincrement=False,
    )
    relion_start_time = Column(
        TIMESTAMP,
        comment="Start time of Relion job",
    )
    num_micrographs = Column(
        INTEGER(10),
        comment="Number of micrographs processed by the job if applicable",
        autoincrement=False,
    )
    job_name = Column(
        String(250),
        nullable=False,
        comment="Name of Relion job",
    )
    pipeline_id = Column(ForeignKey("RelionPipelineInfo.pipeline_id"), index=True)
    RelionPipelineInfo = relationship("RelionPipelineInfo")


def buffer_url() -> str:
    import ispyb.sqlalchemy

    sqlalchemy_url = ispyb.sqlalchemy.url()
    local_url = "/".join(sqlalchemy_url.split("/")[:-1]) + "/zocalo"
    return local_url
