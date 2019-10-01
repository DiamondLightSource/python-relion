#!/dls_sw/apps/python/anaconda/4.6.14/64/envs/cryolo/bin/python
"""
External job for calling cryolo fine tune within Relion 3.0
in_parts is from a subset selection job.
external_cryolo_fine_3.py --o 'ExternalFine' --in_parts 'Select/job005/particles.star' --box_size 300
"""

import argparse
import json
import os
import os.path
import random
import sys
import shutil
import time

import gemmi

qsub_file = "/dls_sw/apps/EM/relion_cryolo/CryoloRelion-master/qsub.sh"


def run_job(project_dir, job_dir, args_list):
    # print("Project directory is {}".format(project_dir))
    # print("Job directory is {}".format(job_dir))
    # print("arguments are {}".format(args_list))
    parser = argparse.ArgumentParser()
    parser.add_argument("--in_parts", help="Input micrographs STAR file")
    parser.add_argument(
        "--j", dest="threads", help="Number of threads to run (ignored)"
    )
    parser.add_argument("--box_size", help="Size of box (~ particle size)")
    args = parser.parse_args(args_list)
    box_size = args.box_size

    # Making a cryolo config file with the correct box size and model location
    with open("/dls_sw/apps/EM/crYOLO/cryo_phosaurus/config.json", "r") as json_file:
        data = json.load(json_file)
        data["model"]["anchors"] = [int(box_size), int(box_size)]
        data["train"][
            "pretrained_weights"
        ] = "/dls_sw/apps/EM/crYOLO/cryo_phosaurus/gmodel_phosnet_20190516.h5"
    with open("config.json", "w") as outfile:
        json.dump(data, outfile)

    # Reading particle star file from relion
    while not os.path.exists(os.path.join(project_dir, args.in_parts)):
        time.sleep(1)
    in_doc = gemmi.cif.read_file(os.path.join(project_dir, args.in_parts))
    data_as_dict = json.loads(in_doc.as_json())["#"]

    try:
        os.mkdir("train_annotation")
    except:
        shutil.rmtree("train_annotation")
        os.mkdir("train_annotation")
    try:
        os.mkdir("train_image")
    except:
        shutil.rmtree("train_image")
        os.mkdir("train_image")

    # Arranging files for cryolo to train from
    for micro in range(len(data_as_dict["_rlnmicrographname"])):
        try:
            # print(os.path.join(project_dir, data_as_dict['_rlnmicrographname'][micro]))
            # print(os.path.join(project_dir, job_dir, 'train_image', os.path.split(data_as_dict['_rlnmicrographname'][micro])[-1]))
            os.link(
                os.path.join(project_dir, data_as_dict["_rlnmicrographname"][micro]),
                os.path.join(
                    project_dir,
                    job_dir,
                    "train_image",
                    os.path.split(data_as_dict["_rlnmicrographname"][micro])[-1],
                ),
            )
        except:
            pass
        # print('{} already exists'.format(micro))

        box_name = (
            os.path.splitext(
                os.path.split(data_as_dict["_rlnmicrographname"][micro])[-1]
            )[0]
            + ".box"
        )

        individual_files = open(os.path.join("train_annotation", box_name), "a+")
        individual_files.write(
            f"{data_as_dict['_rlncoordinatex'][micro] - int(box_size)/2}\t"
        )
        individual_files.write(
            f"{data_as_dict['_rlncoordinatey'][micro] - int(box_size)/2}\t"
        )
        individual_files.write(f"{box_size}\t")
        individual_files.write(f"{box_size}\n")
        individual_files.close()

    # Running cryolo
    # os.system(f"/dls_sw/apps/EM/crYOLO/qsub_cryolo_dls.sh cryolo_train.py -c config.json -w 0 -g 0 --fine_tune")
    os.system(f"{qsub_file} cryolo_train.py -c config.json -w 0 -g 0 --fine_tune")
    while not os.path.exists(".cry_predict_done"):
        time.sleep(1)
    os.remove(".cry_predict_done")

    # Writing a star file (This one is meaningless for now)
    part_doc = open("_manualpick.star", "w")
    part_doc.write(os.path.join(project_dir, args.in_parts))
    part_doc.close()

    # Required star file
    out_doc = gemmi.cif.Document()
    output_nodes_block = out_doc.add_new_block("output_nodes")
    loop = output_nodes_block.init_loop(
        "", ["_rlnPipeLineNodeName", "_rlnPipeLineNodeType"]
    )
    loop.add_row([os.path.join(job_dir, "_manualpick.star"), "2"])
    out_doc.write_file("RELION_OUTPUT_NODES.star")
    # with open('RELION_OUTPUT_NODES.star') as f:
    #     print(f.read())
    with open("DONE", "w"):
        pass
    print(" crYOLO Finished Fine-Tuning")


def main():
    """Change to the job working directory, then call run_job()"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--o", dest="out_dir", help="Output directory name")
    known_args, other_args = parser.parse_known_args()
    project_dir = os.getcwd()
    try:
        os.mkdir(known_args.out_dir)
    except:
        pass
    # print('External exists')
    os.chdir(known_args.out_dir)
    try:
        run_job(project_dir, known_args.out_dir, other_args)
    except:
        open("RELION_JOB_EXIT_FAILURE", "w").close()
        raise
    else:
        open("RELION_JOB_EXIT_SUCCESS", "w").close()


if __name__ == "__main__":
    main()
