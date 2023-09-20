from __future__ import annotations

import decimal
import logging
import os
import re
import uuid

import ispyb.sqlalchemy as models
import marshmallow.fields
import sqlalchemy
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from sqlalchemy.orm import selectinload, sessionmaker

logger = logging.getLogger("relion.zocalo.ispyb_recipe_tools")

Session = sessionmaker(
    bind=sqlalchemy.create_engine(models.url(), connect_args={"use_pure": True})
)

re_visit_base = re.compile(r"^(.*/([a-z][a-z][0-9]+-[0-9]+))/")


def setup_marshmallow_schema(session):
    # https://marshmallow-sqlalchemy.readthedocs.io/en/latest/recipes.html#automatically-generating-schemas-for-sqlalchemy-models
    for class_ in models.Base.registry._class_registry.values():
        if hasattr(class_, "__tablename__"):

            class Meta:
                model = class_
                sqla_session = session
                load_instance = True
                include_fk = True

            TYPE_MAPPING = SQLAlchemyAutoSchema.TYPE_MAPPING.copy()
            TYPE_MAPPING.update({decimal.Decimal: marshmallow.fields.Float})
            schema_class_name = "%sSchema" % class_.__name__
            schema_class = type(
                schema_class_name,
                (SQLAlchemyAutoSchema,),
                {
                    "Meta": Meta,
                    "TYPE_MAPPING": TYPE_MAPPING,
                },
            )
            setattr(class_, "__marshmallow__", schema_class)


def get_reprocessing_info(message, parameters, session: sqlalchemy.orm.session.Session):
    reprocessing_id = parameters.get(
        "ispyb_reprocessing_id", parameters.get("ispyb_process")
    )
    if reprocessing_id:
        parameters["ispyb_process"] = reprocessing_id
        query = (
            session.query(models.ProcessingJob)
            .options(
                selectinload(models.ProcessingJob.ProcessingJobParameters),
                selectinload(models.ProcessingJob.ProcessingJobImageSweeps)
                .selectinload(models.ProcessingJobImageSweep.DataCollection)
                .load_only(
                    models.DataCollection.imageDirectory,
                    models.DataCollection.fileTemplate,
                ),
            )
            .filter(models.ProcessingJob.processingJobId == reprocessing_id)
        )
        rp = query.first()
        if not rp:
            logger.warning(f"Reprocessing ID {reprocessing_id} not found")
        # ispyb_reprocessing_parameters is the deprecated method of
        # accessing the processing parameters
        parameters["ispyb_reprocessing_parameters"] = {
            p.parameterKey: p.parameterValue for p in rp.ProcessingJobParameters
        }
        # ispyb_processing_parameters is the preferred method of
        # accessing the processing parameters
        processing_parameters: dict[str, list[str]] = {}
        for p in rp.ProcessingJobParameters:
            processing_parameters.setdefault(p.parameterKey, [])
            processing_parameters[p.parameterKey].append(p.parameterValue)
        parameters["ispyb_processing_parameters"] = processing_parameters
        schema = models.ProcessingJob.__marshmallow__()
        parameters["ispyb_processing_job"] = schema.dump(rp)
        if "ispyb_dcid" not in parameters:
            parameters["ispyb_dcid"] = rp.dataCollectionId

    return message, parameters


def get_dc_info(dcid: int, session: sqlalchemy.orm.session.Session):

    query = session.query(models.DataCollection).filter(
        models.DataCollection.dataCollectionId == dcid
    )
    dc = query.first()
    if dc is None:
        return {}
    schema = models.DataCollection.__marshmallow__()
    return schema.dump(dc)


def get_beamline_from_dcid(dcid: int, session: sqlalchemy.orm.session.Session):
    query = (
        session.query(models.BLSession)
        .join(models.DataCollectionGroup)
        .join(models.DataCollection)
        .filter(models.DataCollection.dataCollectionId == dcid)
    )
    bl_session = query.first()
    if bl_session:
        return bl_session.beamLineName


def get_visit_directory_from_image_directory(directory):
    """/dls/${beamline}/data/${year}/${visit}/...
    -> /dls/${beamline}/data/${year}/${visit}"""
    if not directory:
        return None
    visit_base = re_visit_base.search(directory)
    if not visit_base:
        return None
    return visit_base.group(1)


def get_visit_from_image_directory(directory):
    """/dls/${beamline}/data/${year}/${visit}/...
    -> ${visit}"""
    if not directory:
        return None
    visit_base = re_visit_base.search(directory)
    if not visit_base:
        return None
    return visit_base.group(2)


def dc_info_to_working_directory(dc_info):
    directory = dc_info.get("imageDirectory")
    if not directory:
        return None
    visit = get_visit_directory_from_image_directory(directory)
    rest = directory[len(visit) + 1 :]

    collection_path = dc_info["imagePrefix"] or ""
    dc_number = dc_info["dataCollectionNumber"] or ""
    if collection_path or dc_number:
        collection_path = f"{collection_path}_{dc_number}"
    return os.path.join(visit, "tmp", "zocalo", rest, collection_path, dc_info["uuid"])


def dc_info_to_results_directory(dc_info):
    directory = dc_info.get("imageDirectory")
    if not directory:
        return None
    visit = get_visit_directory_from_image_directory(directory)
    rest = directory[len(visit) + 1 :]

    collection_path = dc_info["imagePrefix"] or ""
    dc_number = dc_info["dataCollectionNumber"] or ""
    if collection_path or dc_number:
        collection_path = f"{collection_path}_{dc_number}"
    return os.path.join(visit, "processed", rest, collection_path, dc_info["uuid"])


def ready_for_processing(
    message, parameters, session: sqlalchemy.orm.session.Session | None = None
):
    """Check whether this message is ready for templatization."""

    if session is None:
        session = Session()

    if not parameters.get("ispyb_wait_for_runstatus"):
        return True

    dcid = parameters.get("ispyb_dcid")
    if not dcid:
        return True

    query = session.query(models.DataCollection.runStatus).filter(
        models.DataCollection.dataCollectionId == dcid
    )
    return query.scalar() is not None


def ispyb_filter(
    message,
    parameters,
    session: sqlalchemy.orm.session.Session | None = None,
):
    """Do something to work out what to do with this data..."""

    if session is None:
        session = Session()

    setup_marshmallow_schema(session)

    message, parameters = get_reprocessing_info(message, parameters, session)

    if "ispyb_dcid" not in parameters:
        return message, parameters

    dc_id = parameters["ispyb_dcid"]

    dc_info = get_dc_info(dc_id, session)
    if not dc_info:
        raise ValueError(f"No database entry found for dcid={dc_id}: {dc_id}")
    dc_info["uuid"] = parameters.get("guid") or str(uuid.uuid4())
    parameters["ispyb_beamline"] = get_beamline_from_dcid(dc_id, session)

    parameters["ispyb_preferred_datacentre"] = "cs05r"
    parameters["ispyb_preferred_scheduler"] = "slurm"

    parameters["ispyb_dc_info"] = dc_info

    parameters["ispyb_visit"] = get_visit_from_image_directory(
        dc_info.get("imageDirectory")
    )
    parameters["ispyb_visit_directory"] = get_visit_directory_from_image_directory(
        dc_info.get("imageDirectory")
    )
    parameters["ispyb_working_directory"] = dc_info_to_working_directory(dc_info)
    parameters["ispyb_results_directory"] = dc_info_to_results_directory(dc_info)

    if (
        "ispyb_processing_job" in parameters
        and parameters["ispyb_processing_job"]["recipe"]
        and not message.get("recipes")
        and not message.get("custom_recipe")
    ):
        # Prefix recipe name coming from ispyb/synchweb with 'ispyb-'
        message["recipes"] = ["ispyb-" + parameters["ispyb_processing_job"]["recipe"]]

    return message, parameters
