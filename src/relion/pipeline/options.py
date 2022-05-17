from __future__ import annotations

from typing import Any, Dict

from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions


def generate_pipeline_options(
    relion_it_options: RelionItOptions, submission_types: Dict[str, str]
) -> dict:
    for queue in submission_types.values():
        if queue not in ("gpu", "cpu", "gpu-smp", "cpu-smp", ""):
            raise ValueError(
                f'The queue for a job must be either gpu, cpu, gpu-smp, cpu-smp or "", not {queue}'
            )

    queue_options_gpu = {
        "do_queue": "Yes",
        "qsubscript": relion_it_options.queue_submission_template,
    }
    queue_options_gpu_smp = {
        "do_queue": "Yes",
        "qsubscript": relion_it_options.queue_submission_template_smp,
    }
    queue_options_cpu = {
        "do_queue": "Yes",
        "qsubscript": relion_it_options.queue_submission_template_cpu,
    }
    queue_options_cpu_smp = {
        "do_queue": "Yes",
        "qsubscript": relion_it_options.queue_submission_template_cpu_smp,
    }
    queue_options: Dict[str, dict] = {
        "gpu": queue_options_gpu,
        "cpu": queue_options_cpu,
        "gpu-smp": queue_options_gpu_smp,
        "cpu-smp": queue_options_cpu_smp,
        "": {},
    }

    job_options: Dict[str, Any] = {}

    job_options["relion.import.movies"] = {
        "fn_in_raw": relion_it_options.import_images,
        "angpix": relion_it_options.angpix,
    }

    job_options["relion.motioncorr.motioncorr2"] = {
        "fn_motioncor2_exe": relion_it_options.motioncor_exe,
        "fn_defect": relion_it_options.motioncor_defectfile,
        "dose_per_frame": relion_it_options.motioncor_doseperframe,
        "fn_gain_ref": relion_it_options.motioncor_gainreference,
        "eer_grouping": relion_it_options.eer_grouping,
        "patch_x": relion_it_options.motioncor_patches_x,
        "patch_y": relion_it_options.motioncor_patches_y,
        "bfactor": relion_it_options.motioncor_bfactor,
        "bin_factor": relion_it_options.motioncor_binning,
        "gain_flip": relion_it_options.motioncor_gainflip,
        "gain_rot": relion_it_options.motioncor_gainrot,
        "other_motioncor2_args": relion_it_options.motioncor2_other_args,
        "gpu_ids": "0:1:2:3",
        "nr_mpi": relion_it_options.motioncor_mpi,
        "nr_threads": relion_it_options.motioncor_threads,
    }

    job_options["relion.motioncorr.own"] = job_options["relion.motioncorr.motioncorr2"]

    job_options["icebreaker.micrograph_analysis.micrographs"] = {
        "nr_threads": relion_it_options.icebreaker_threads_number,
        "nr_mpi": 1,
    }

    job_options["icebreaker.micrograph_analysis.enhancecontrast"] = {
        "nr_threads": relion_it_options.icebreaker_threads_number,
        "nr_mpi": 1,
    }

    job_options["icebreaker.micrograph_analysis.summary"] = {
        "nr_threads": relion_it_options.icebreaker_threads_number,
        "nr_mpi": 1,
    }

    job_options["relion.ctffind.ctffind4"] = {
        "dast": relion_it_options.ctffind_astigmatism,
        "box": relion_it_options.ctffind_boxsize,
        "dfmax": relion_it_options.ctffind_defocus_max,
        "dfmin": relion_it_options.ctffind_defocus_min,
        "dfstep": relion_it_options.ctffind_defocus_step,
        "resmax": relion_it_options.ctffind_maxres,
        "resmin": relion_it_options.ctffind_minres,
        "gpu_ids": "0:1:2:3",
        "nr_mpi": relion_it_options.ctffind_mpi,
    }

    job_options["relion.autopick.log"] = {
        "log_diam_min": relion_it_options.autopick_LoG_diam_min,
        "log_diam_max": relion_it_options.autopick_LoG_diam_max,
        "log_maxres": relion_it_options.autopick_lowpass,
        "log_adjust_thr": relion_it_options.autopick_LoG_adjust_threshold,
        "log_upper_thr": relion_it_options.autopick_LoG_upper_threshold,
        "gpu_ids": "0:1:2:3",
        "nr_mpi": relion_it_options.autopick_mpi,
    }

    job_options["relion.autopick.ref3d"] = {
        "ref3d_symmetry": relion_it_options.autopick_3dref_symmetry,
        "ref3d_sampling": relion_it_options.autopick_3dref_sampling,
        "lowpass": relion_it_options.autopick_lowpass,
        "angpix_ref": relion_it_options.autopick_ref_angpix,
        "threshold_autopick": relion_it_options.autopick_refs_threshold,
        "mindist_autopick": relion_it_options.autopick_refs_min_distance,
        "maxstddevnoise_autopick": relion_it_options.autopick_stddev_noise,
        "minavgnoise_autopick": relion_it_options.autopick_avg_noise,
        "gpu_ids": "0:1:2:3",
        "nr_mpi": relion_it_options.autopick_mpi,
    }

    job_options["cryolo.autopick"] = {
        "model_path": relion_it_options.cryolo_gmodel,
        "config_file": relion_it_options.cryolo_config,
        "box_size": int(
            relion_it_options.extract_boxsize / relion_it_options.motioncor_binning
        ),
        "confidence_threshold": relion_it_options.cryolo_threshold,
        "gpus": relion_it_options.cryolo_pick_gpus,
    }

    bin_corrected_box_size = int(
        relion_it_options.extract_boxsize / relion_it_options.motioncor_binning
    )
    job_options["relion.extract"] = {
        "bg_diameter": relion_it_options.extract_bg_diameter,
        "extract_size": bin_corrected_box_size + bin_corrected_box_size % 2,
        "do_rescale": bool(relion_it_options.extract_downscale),
        "rescale": relion_it_options.extract_small_boxsize,
        "nr_mpi": relion_it_options.extract_mpi,
    }

    job_options["relion.select.split"] = {
        "split_size": relion_it_options.batch_size,
    }

    job_options["icebreaker.micrograph_analysis.particles"] = {
        "nr_threads": relion_it_options.icebreaker_threads_number,
        "nr_mpi": 1,
    }

    job_options["relion.class2d.em"] = {
        "nr_classes": relion_it_options.class2d_nr_classes,
        "nr_iter_em": relion_it_options.class2d_nr_iter,
        "psi_sampling": relion_it_options.class2d_angle_step,
        "offset_range": relion_it_options.class2d_offset_range,
        "offset_step": relion_it_options.class2d_offset_step,
        "ctf_intact_first_peak": relion_it_options.class2d_ctf_ign1stpeak,
        "do_preread_images": relion_it_options.refine_preread_images,
        "scratch_dir": relion_it_options.refine_scratch_disk,
        "nr_pool": relion_it_options.refine_nr_pool,
        "use_gpu": relion_it_options.refine_do_gpu,
        "gpu_ids": "0:1:2:3",
        "nr_mpi": relion_it_options.refine_mpi,
        "nr_threads": relion_it_options.refine_threads,
    }

    job_options["relion.class2d.vdam"] = {
        "nr_classes": relion_it_options.class2d_nr_classes,
        "nr_iter_grad": relion_it_options.class2d_nr_iter,
        "psi_sampling": relion_it_options.class2d_angle_step,
        "offset_range": relion_it_options.class2d_offset_range,
        "offset_step": relion_it_options.class2d_offset_step,
        "ctf_intact_first_peak": relion_it_options.class2d_ctf_ign1stpeak,
        "do_preread_images": relion_it_options.refine_preread_images,
        "scratch_dir": relion_it_options.refine_scratch_disk,
        "nr_pool": relion_it_options.refine_nr_pool,
        "use_gpu": relion_it_options.refine_do_gpu,
        "gpu_ids": "0,1,2,3",
        "nr_mpi": 1,
        "nr_threads": relion_it_options.inimodel_threads,
    }

    job_options["relion.initialmodel"] = {
        "nr_classes": relion_it_options.inimodel_nr_classes,
        "sampling": relion_it_options.inimodel_angle_step,
        "offset_step": relion_it_options.inimodel_offset_step,
        "offset_range": relion_it_options.inimodel_offset_range,
        "nr_iter": relion_it_options.inimodel_nr_iter_inbetween,
        "do_preread_images": relion_it_options.refine_preread_images,
        "scratch_dir": relion_it_options.refine_scratch_disk,
        "nr_pool": relion_it_options.refine_nr_pool,
        "use_gpu": relion_it_options.refine_do_gpu,
        "gpu_ids": "0,1,2,3",
        "nr_mpi": 1,
        "nr_threads": relion_it_options.inimodel_threads,
    }

    job_options["relion.class3d"] = {
        "fn_mask": relion_it_options.class3d_reference,
        "nr_classes": relion_it_options.class3d_nr_classes,
        "sym_name": relion_it_options.symmetry,
        "ini_high": relion_it_options.class3d_ini_lowpass,
        "tau_fudge": relion_it_options.class3d_T_value,
        "particle_diameter": relion_it_options.mask_diameter,
        "nr_iter": relion_it_options.class3d_nr_iter,
        "sampling": relion_it_options.class3d_angle_step,
        "offset_range": relion_it_options.class3d_offset_range,
        "offset_step": relion_it_options.class3d_offset_step,
        "ref_correct_greyscale": relion_it_options.class3d_ref_is_correct_greyscale,
        "do_ctf_correction": relion_it_options.class3d_ref_is_ctf_corrected,
        "ctf_intact_first_peak": relion_it_options.class3d_ctf_ign1stpeak,
        "do_preread_images": relion_it_options.refine_preread_images,
        "use_gpu": relion_it_options.refine_do_gpu,
        "gpu_ids": "0:1:2:3",
        "nr_threads": relion_it_options.refine_threads,
    }

    pipeline_options = {
        key: {**job_options[key], **queue_options[q]}
        for key, q in submission_types.items()
    }
    return pipeline_options
