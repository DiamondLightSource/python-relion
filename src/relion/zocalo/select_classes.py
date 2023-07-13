from __future__ import annotations

import re
import subprocess
from pathlib import Path

import numpy as np
import workflows.recipe
from gemmi import cif
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService

from relion.zocalo.spa_relion_service_options import RelionServiceOptions


class SelectClassesParameters(BaseModel):
    input_file: str = Field(..., min_length=1)
    combine_star_job_number: int
    particles_file: str = "particles.star"
    classes_file: str = "class_averages.star"
    python_exe: str = "/dls_sw/apps/EM/relion/4.0/conda/bin/python"
    min_score: float = 0
    min_particles: int = 500
    class3d_batch_size: int = 50000
    class3d_max_size: int = 200000
    relion_options: RelionServiceOptions


class SelectClasses(CommonService):
    """
    A service for running Relion autoselection on 2D classes
    """

    # Human readable service name
    _service_name = "SelectClasses"

    # Logger name
    _logger_name = "relion.zocalo.select.classes"

    # Job name
    job_type = "relion.select.class2dauto"

    # Values to extract for ISPyB
    previous_total_count = 0
    total_count = 0

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("Select particles service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "select.classes",
            self.select_classes,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def parse_combiner_output(self, combiner_stdout: str):
        """
        Read the output logs of the star file combination
        """
        for line in combiner_stdout.split("\n"):
            if line.startswith("Adding") and "particles_all.star" in line:
                line_split = line.split()
                self.previous_total_count = int(line_split[3])

            if line.startswith("Combined"):
                line_split = line.split()
                self.total_count = int(line_split[6])

    def select_classes(self, rw, header: dict, message: dict):
        class MockRW:
            def dummy(self, *args, **kwargs):
                pass

        if not rw:
            print(
                "Incoming message is not a recipe message. Simple messages can be valid"
            )
            if (
                not isinstance(message, dict)
                or not message.get("parameters")
                or not message.get("content")
            ):
                self.log.error("Rejected invalid simple message")
                self._transport.nack(header)
                return
            self.log.debug("Received a simple message")

            # Create a wrapper-like object that can be passed to functions
            # as if a recipe wrapper was present.
            rw = MockRW()
            rw.transport = self._transport
            rw.recipe_step = {"parameters": message["parameters"]}
            rw.environment = {"has_recipe_wrapper": False}
            rw.set_default_channel = rw.dummy
            rw.send = rw.dummy
            message = message["content"]

        try:
            if isinstance(message, dict):
                autoselect_params = SelectClassesParameters(
                    **{**rw.recipe_step.get("parameters", {}), **message}
                )
            else:
                autoselect_params = SelectClassesParameters(
                    **{**rw.recipe_step.get("parameters", {})}
                )
        except (ValidationError, TypeError):
            self.log.warning(
                f"Selection parameter validation failed for message: {message} "
                f"and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        self.log.info(f"Inputs: {autoselect_params.input_file}")

        class2d_job_dir = Path(
            re.search(".+/job[0-9]{3}/", autoselect_params.input_file)[0]
        )
        project_dir = class2d_job_dir.parent.parent

        select_job_num = (
            int(re.search("/job[0-9]{3}", str(class2d_job_dir))[0][4:7]) + 2
        )
        select_dir = project_dir / f"Select/job{select_job_num:03}"
        select_dir.mkdir(parents=True, exist_ok=True)

        autoselect_flags = {
            "particles_file": "--fn_sel_parts",
            "classes_file": "--fn_sel_classavgs",
            "python_exe": "--python",
            "min_particles": "--select_min_nr_particles",
        }
        # Create the class selection command
        autoselect_command = [
            "relion_class_ranker",
            "--opt",
            autoselect_params.input_file,
            "--o",
            f"{select_dir.relative_to(project_dir)}/",
            "--auto_select",
            "--fn_root",
            "rank",
            "--do_granularity_features",
        ]
        for k, v in autoselect_params.dict().items():
            if v and (k in autoselect_flags):
                if type(v) is tuple:
                    autoselect_command.extend(
                        (autoselect_flags[k], " ".join(str(_) for _ in v))
                    )
                else:
                    autoselect_command.extend((autoselect_flags[k], str(v)))
        autoselect_command.extend(
            ("--pipeline_control", f"{select_dir.relative_to(project_dir)}/")
        )

        if not autoselect_params.min_score:
            autoselect_command.extend(("--min_score", "0.0"))
        else:
            autoselect_command.extend(("--min_score", str(autoselect_params.min_score)))

        # Run the class selection
        result = subprocess.run(
            autoselect_command, cwd=str(project_dir), capture_output=True
        )
        if result.returncode:
            self.log.error(
                f"2D autoselection failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        if not autoselect_params.min_score:
            # If a minimum score isn't given, then work it out and rerun the job
            star_doc = cif.read_file(str(select_dir / "rank_model.star"))
            star_block = star_doc["model_classes"]
            class_scores = np.array(star_block.find_loop("_rlnClassScore"), dtype=float)
            quantile_threshold = np.quantile(
                class_scores,
                float(
                    autoselect_params.relion_options.class2d_fraction_of_classes_to_remove
                ),
            )

            self.log.info(f"Sending new threshold {quantile_threshold} to Murfey")
            murfey_params = {
                "register": "save_class_selection_score",
                "class_selection_score": quantile_threshold,
            }
            if isinstance(rw, MockRW):
                rw.transport.send("murfey_feedback", murfey_params)
            else:
                rw.send_to("murfey_feedback", murfey_params)

            self.log.info(
                f"Re-running class selection with new threshold {quantile_threshold}"
            )
            autoselect_command[-1] = str(quantile_threshold)

            # Re-run the class selection
            result = subprocess.run(
                autoselect_command, cwd=str(project_dir), capture_output=True
            )
            if result.returncode:
                self.log.error(
                    f"2D autoselection failed with exitcode {result.returncode}:\n"
                    + result.stderr.decode("utf8", "replace")
                )
                rw.transport.nack(header)
                return

        # Send to node creator
        self.log.info(f"Sending {self.job_type} to node creator")
        autoselect_node_creator_params = {
            "job_type": self.job_type,
            "input_file": autoselect_params.input_file,
            "output_file": str(select_dir / autoselect_params.particles_file),
            "relion_options": dict(autoselect_params.relion_options),
            "command": " ".join(autoselect_command),
            "stdout": result.stdout.decode("utf8", "replace"),
            "stderr": result.stderr.decode("utf8", "replace"),
        }
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="spa.node_creator",
                message={
                    "parameters": autoselect_node_creator_params,
                    "content": "dummy",
                },
            )
        else:
            rw.send_to("spa.node_creator", autoselect_node_creator_params)

        # Run the combine star files job to combine the files into particles_all.star
        self.log.info("Running star file combination and splitting")
        combine_star_command = [
            "combine_star_files.py",
            str(select_dir / autoselect_params.particles_file),
        ]

        combine_star_dir = Path(
            project_dir / f"Select/job{autoselect_params.combine_star_job_number:03}"
        )
        if (combine_star_dir / "particles_all.star").exists():
            combine_star_command.append(str(combine_star_dir / "particles_all.star"))
        else:
            combine_star_dir.mkdir(parents=True, exist_ok=True)
            self.previous_total_count = 0
        combine_star_command.extend(("--output_dir", str(combine_star_dir)))

        result = subprocess.run(
            combine_star_command, cwd=str(project_dir), capture_output=True
        )
        self.parse_combiner_output(result.stdout.decode("utf8", "replace"))
        if result.returncode:
            self.log.error(
                f"Star file combination failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        # Determine the next split size to use and whether to run 3D classification
        send_to_3d_classification = False
        if self.previous_total_count == 0:
            # First run of this job, use class3d_max_size
            next_batch_size = autoselect_params.class3d_batch_size
            if self.total_count > autoselect_params.class3d_batch_size:
                # Do 3D classification if there are more particles than the batch size
                send_to_3d_classification = True
        elif self.previous_total_count >= autoselect_params.class3d_max_size:
            # Iterations beyond those where 3D classification is run
            next_batch_size = autoselect_params.class3d_max_size
        else:
            # Re-runs with fewer particles than the maximum
            previous_batch_multiple = (
                self.previous_total_count // autoselect_params.class3d_batch_size
            )
            new_batch_multiple = (
                self.total_count // autoselect_params.class3d_batch_size
            )
            if new_batch_multiple > previous_batch_multiple:
                # Do 3D classification if a batch threshold has been crossed
                send_to_3d_classification = True
                # Set the batch size from the total count, but do not exceed the maximum
                next_batch_size = (
                    new_batch_multiple * autoselect_params.class3d_batch_size
                )
                if next_batch_size > autoselect_params.class3d_max_size:
                    next_batch_size = autoselect_params.class3d_max_size
            else:
                # Otherwise just get the next threshold
                next_batch_size = (
                    previous_batch_multiple + 1
                ) * autoselect_params.class3d_batch_size

        # Run the combine star files job to split particles_all.star into batches
        split_star_command = [
            "combine_star_files.py",
            str(combine_star_dir / "particles_all.star"),
            "--output_dir",
            str(combine_star_dir),
            "--split",
            "--split_size",
            str(next_batch_size),
        ]

        result = subprocess.run(
            split_star_command, cwd=str(project_dir), capture_output=True
        )
        self.parse_combiner_output(result.stdout.decode("utf8", "replace"))
        if result.returncode:
            self.log.error(
                f"Star file splitting failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        # Send to node creator
        self.log.info("Sending combine_star_files_job to node creator")
        combine_node_creator_params = {
            "job_type": "combine_star_files_job",
            "input_file": f"{select_dir}/{autoselect_params.particles_file}",
            "output_file": f"{combine_star_dir}/particles_all.star",
            "relion_options": dict(autoselect_params.relion_options),
            "command": (
                " ".join(combine_star_command) + "\n" + " ".join(split_star_command)
            ),
            "stdout": result.stdout.decode("utf8", "replace"),
            "stderr": result.stderr.decode("utf8", "replace"),
        }
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="spa.node_creator",
                message={"parameters": combine_node_creator_params, "content": "dummy"},
            )
        else:
            rw.send_to("spa.node_creator", combine_node_creator_params)

        # Create 3D classification jobs
        if send_to_3d_classification:
            # Only send to 3D if a new multiple of the batch threshold is crossed
            # and the count has not passed the maximum
            self.log.info("Sending to Murfey for Class3D")
            class3d_params = {
                "particles_file": f"{combine_star_dir}/particles_split1.star",
                "class3d_dir": f"{project_dir}/Class3D/job",
                "batch_size": next_batch_size,
            }
            murfey_params = {
                "register": "run_class3d",
                "class3d_message": class3d_params,
            }
            if isinstance(rw, MockRW):
                rw.transport.send("murfey_feedback", murfey_params)
            else:
                rw.send_to("murfey_feedback", murfey_params)

        self.log.info(f"Done {self.job_type} for {autoselect_params.input_file}.")
        rw.transport.ack(header)