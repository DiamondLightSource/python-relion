from __future__ import annotations

from pathlib import Path

from pipeliner.api.api_utils import (
    edit_jobstar,
    job_parameters_dict,
    write_default_jobstar,
)
from pipeliner.api.manage_project import PipelinerProject

cluster_options = {
    "do_queue": "Yes",
    "qsubscript": "/dls_sw/apps/EM/relion/qsub_templates/qsub_template_hamilton_pipeliner",
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
    ):
        self._proj_path = Path(project_path)
        self._particles_star = Path(particles_star_file)
        self._proj = PipelinerProject()
        self._ref_model = ref_model
        self._default_params = {
            "relion.refine3d": {"particle_diameter": particle_diameter},
            "relion.postprocess": {"other_args": f"--autob_highres {autob_highres}"},
        }
        self._mask = mask
        self._extract_size = extract_size

    def _run_job(self, job: str, params: dict, cluster=True) -> str:
        write_default_jobstar(job)
        _params = job_parameters_dict(job)
        _params.update(params)
        _params.update(self._default_params.get(job, {}))
        if cluster:
            _params.update(cluster_options)
        edit_jobstar(
            f"{job.replace('.', '_')}_job.star",
            _params,
            f"{job.replace('.', '_')}_job.star",
        )
        job_path = self._proj.run_job(
            f"{job.replace('.', '_')}_job.star", wait_for_queued=True
        )
        return job_path

    def _run_import(self) -> str:
        job = "relion.import.other"
        params = {
            "fn_in_other": str(self._particles_star),
            "node_type": "Particles STAR file (.star)",
        }
        return self._run_job(job, params, cluster=False)

    def _run_refine3d(self, imported_star_file: str):
        job = "relion.refine3d"
        params = {"fn_img": imported_star_file, "fn_ref": self._ref_model}
        return self._run_job(job, params)

    def _run_postprocess(self, input_model: str):
        job = "relion.postprocess"
        params = {"fn_in": input_model, "fn_mask": self._mask, "angpix": -1}
        if self._mask:
            params["fn_mask"] = self._mask
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
        return self._run_job(job, params)

    def __call__(self, micrographs_star: str = "CtfFind/job003/micrographs_ctf.star"):
        import_path = self._run_import() + f"/{self._particles_star.name}"
        if self._extract_size:
            import_path = (
                self._run_extract(import_path, micrographs=micrographs_star)
                + "/particles.star"
            )
        refine_path = self._run_refine3d(import_path)
        self._run_postprocess(refine_path)
