from functools import partial
from typing import Any, Callable, Dict

from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions
from relion.pipeline.job_tracker import JobTracker


def _from_import(
    tracker: JobTracker, *args, in_key: str = "input_star_mics"
) -> Dict[str, Any]:
    return {in_key: str(tracker.path_to("relion.import.movies", "movies.star"))}


def _from_motioncorr(
    tracker: JobTracker, options: RelionItOptions, in_key: str = "input_star_mics"
) -> Dict[str, Any]:
    if options.images_are_movies:
        if options.motioncor_do_own:
            return {
                in_key: str(
                    tracker.path_to(
                        "relion.motioncorr.own", "corrected_micrographs.star"
                    )
                )
            }
        return {
            in_key: str(
                tracker.path_to(
                    "relion.motioncorr.motioncorr2", "corrected_micrographs.star"
                )
            )
        }
    return _from_import(tracker, in_key=in_key)


def _from_ctf(
    tracker: JobTracker, options: RelionItOptions, in_key: str = "fn_input_autopick"
) -> Dict[str, Any]:
    if options.use_ctffind_instead:
        return {
            in_key: str(
                tracker.path_to("relion.ctffind.ctffind4", "micrographs_ctf.star")
            )
        }
    return {in_key: str(tracker.path_to("relion.ctffind.gctf", "micrographs_ctf.star"))}


def _from_ib(
    tracker: JobTracker, options: RelionItOptions, in_key: str = "in_mics"
) -> Dict[str, Any]:
    return {
        in_key: str(
            tracker.path_to(
                "icebreaker.micrograph_analysis.micrographs", "grouped_micrographs.star"
            )
        )
    }


def _extract(
    tracker: JobTracker, options: RelionItOptions, ref: bool = False
) -> Dict[str, Any]:
    res = {}
    if options.autopick_do_cryolo:
        res["coords_suffix"] = str(
            tracker.path_to("cryolo.autopick", "coords_suffix_autopick.star")
        )
    elif ref:
        res["coords_suffix"] = str(
            tracker.path_to("relion.autopick.ref3d", "autopick.star")
        )
    else:
        res["coords_suffix"] = str(
            tracker.path_to("relion.autopick.log", "autopick.star")
        )
    res.update(_from_ctf(tracker, options, in_key="star_mics"))
    return res


def _select(
    tracker: JobTracker, options: RelionItOptions, ref: str = ""
) -> Dict[str, Any]:
    return {"fn_data": str(tracker.path_to("relion.extract" + ref, "particles.star"))}


_extra_options: Dict[str, Callable] = {
    "relion.motioncorr.motioncorr2": _from_import,
    "relion.motioncorr.own": _from_import,
    "icebreaker.micrograph_analysis.micrographs": partial(
        _from_motioncorr, in_key="in_mics"
    ),
    "icebreaker.micrograph_analysis.enhancecontrast": partial(
        _from_motioncorr, in_key="in_mics"
    ),
    "icebreaker.micrograph_analysis.summary": partial(
        _from_ib,
    ),
    "relion.ctffind.ctffind4": _from_motioncorr,
    "relion.ctffind.gctf": _from_motioncorr,
    "relion.autopick.log": _from_ctf,
    "relion.autopick.ref3d": _from_ctf,
    "cryolo.autopick": partial(_from_ctf, in_key="input_file"),
    "relion.extract": _extract,
    "relion.select.split": _select,
}


def generate_extra_options(
    job: str, tracker: JobTracker, options: RelionItOptions
) -> Dict[str, Any]:
    if job.startswith("relion.extract"):
        refb = bool(job.replace("relion.extract", ""))
        return _extra_options["relion.extract"](tracker, options, ref=refb)
    elif job.startswith("relion.select.split"):
        ref = job.replace("relion.select.split", "")
        return _extra_options["relion.select.split"](tracker, options, ref=ref)
    else:
        try:
            return _extra_options[job](tracker, options)
        except KeyError:
            return {}
