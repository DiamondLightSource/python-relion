from __future__ import annotations

import os
from pathlib import Path

import yaml
from pipeliner.api.api_utils import (
    edit_jobstar,
    job_default_parameters_dict,
    write_default_jobstar,
)
from pipeliner.api.manage_project import PipelinerProject

cluster_config = {}
if os.getenv("RELION_CLUSTER_CONFIG"):
    with open(os.getenv("RELION_CLUSTER_CONFIG"), "r") as config:
        cluster_config = yaml.safe_load(config)

cluster_options = {
    "do_queue": "Yes",
    "qsubscript": cluster_config.get("queue_submission_template")
    or "/dls_sw/apps/EM/relion/qsub_templates/qsub_template_hamilton_pipeliner",
    "use_gpu": "Yes",
    "gpu_ids": "0:1:2:3",
    "nr_mpi": 5,
    "nr_threads": 8,
}


class RefinePipelineRunner:
    def __init__(
        self,
        project_path: str,
        particles_star_file: str,
        ref_model: str,
        mask: str = "",
        particle_diameter: float = 170,
        autob_highres: float = 4.75,
        extract_size: int = 0,
        symmetry: str = "C1",
        ini_high: float = 60,
    ):
        self._proj_path = Path(project_path)
        self._particles_star = Path(particles_star_file)
        self._proj = PipelinerProject()
        self._ref_model = ref_model
        self._default_params = {
            "relion.refine3d": {
                "particle_diameter": particle_diameter,
                "sym_name": symmetry,
                "ini_high": ini_high,
            },
            "relion.postprocess": {"other_args": f"--autob_highres {autob_highres}"},
        }
        self._mask = mask
        self._extract_size = extract_size

    def _run_job(self, job: str, params: dict, cluster=True, gpu: bool = True) -> str:
        write_default_jobstar(job)
        _params = job_default_parameters_dict(job)
        _params.update(params)
        _params.update(self._default_params.get(job, {}))
        if cluster:
            _params.update(cluster_options)
        if not gpu:
            _params.pop("use_gpu")
            _params.pop("gpu_ids")
            _params.pop("nr_threads")
        edit_jobstar(
            f"{job.replace('.', '_')}_job.star",
            _params,
            f"{job.replace('.', '_')}_job.star",
        )
        job_obj = self._proj.run_job(
            f"{job.replace('.', '_')}_job.star", wait_for_queued=True
        )
        return job_obj.output_dir

    def _run_import(
        self, fn_in: str = "", node_type: str = "Particles STAR file (.star)"
    ) -> str:
        job = "relion.import.other"
        params = {
            "fn_in_other": fn_in or str(self._particles_star),
            "node_type": node_type,
        }
        return self._run_job(job, params, cluster=False)

    def _run_refine3d(self, imported_star_file: str):
        job = "relion.refine3d"
        if "job" in self._ref_model:
            model = self._ref_model
        else:
            model_import = self._run_import(
                fn_in=self._ref_model, node_type="3D reference (.mrc)"
            )
            model = f"{model_import}/{Path(self._ref_model).name}"
        params = {"fn_img": imported_star_file, "fn_ref": model}
        return self._run_job(job, params)

    def _run_postprocess(self, input_model: str):
        job = "relion.postprocess"
        if "job" in self._mask:
            mask = self._mask
        else:
            mask_import = self._run_import(
                fn_in=self._mask, node_type="3D reference (.mrc)"
            )
            mask = f"{mask_import}/{Path(self._mask).name}"
        params = {"fn_in": input_model, "fn_mask": mask, "angpix": -1}
        return self._run_job(job, params, cluster=False)

    def _run_extract(
        self,
        particles_star: str,
        micrographs: str = "CtfFind/job003/micrographs_ctf.star",
    ):
        job = "relion.extract.reextract"
        params = {
            "star_mics": micrographs,
            "fndata_reextract": particles_star,
            "extract_size": self._extract_size,
        }
        return self._run_job(job, params, gpu=False)

    def __call__(self, micrographs_star: str = "CtfFind/job003/micrographs_ctf.star"):
        if "/job" in str(self._particles_star):
            import_path = str(self._particles_star)
        else:
            import_path = self._run_import() + f"/{self._particles_star.name}"
        if self._extract_size:
            import_path = (
                self._run_extract(import_path, micrographs=micrographs_star)
                + "/particles.star"
            )
        refine_path = self._run_refine3d(import_path)
        half_map = list(Path(refine_path).glob("run_half1*unfil.mrc"))[0]
        self._run_postprocess(str(half_map.relative_to(self._proj_path)))
