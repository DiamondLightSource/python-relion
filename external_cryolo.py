#!/dls/ebic/data/staff-scratch/Donovan/anaconda/envs/cry_rel/bin/python
"""
External job for calling cryolo within Relion 3.1
"""

import argparse
import json
import os
import os.path
import random
import sys

import gemmi
import shutil


def run_job(project_dir, job_dir, args_list):
    print("Project directory is {}".format(project_dir))
    print("Job directory is {}".format(job_dir))
    print("arguments are {}".format(args_list))
    parser = argparse.ArgumentParser()
    parser.add_argument("--in_mics", help="Input micrographs STAR file")
    parser.add_argument(
        "--j", dest="threads", help="Number of threads to run (ignored)"
    )
    parser.add_argument("--box_size", help="Size of box (~ particle size)")
    parser.add_argument("--threshold", help="Threshold for picking (default = 0.3)")
    args = parser.parse_args(args_list)
    # print(args)
    thresh = args.threshold
    box_size = args.box_size
    print(box_size)
    with open("/dls_sw/apps/EM/crYOLO/cryo_phosaurus/config.json", "r") as json_file:
        data = json.load(json_file)
        print(data["model"]["anchors"])
        data["model"]["anchors"] = [int(box_size), int(box_size)]
        print(data["model"]["anchors"])

    with open("config.json", "w") as outfile:
        json.dump(data, outfile)

    in_doc = gemmi.cif.read_file(os.path.join(project_dir, args.in_mics))
    data_as_dict = json.loads(in_doc.as_json())["micrographs"]
    print(data_as_dict.keys())
    print(data_as_dict["_rlnmicrographname"])
    try:
        os.mkdir("cryolo_input")
    except:
        print("cryolo_input exists")
    for micrograph in data_as_dict["_rlnmicrographname"]:
        print(micrograph)
        try:
            os.link(
                os.path.join(project_dir, micrograph),
                os.path.join(
                    project_dir, job_dir, "cryolo_input", os.path.split(micrograph)[-1]
                ),
            )
        except:
            print("{} already exists".format(micrograph))
        print(
            os.path.join(
                project_dir, job_dir, "cryolo_input", os.path.split(micrograph)[-1]
            )
        )
        print(os.path.join(project_dir, micrograph))

    os.system(
        f"cryolo_predict.py -c config.json -i {os.path.join(project_dir, job_dir, 'cryolo_input')} -o {os.path.join(project_dir, job_dir, 'gen_pick')} -w /dls_sw/apps/EM/crYOLO/cryo_phosaurus/gmodel_phosnet_20190516.h5 -g 0 -t {thresh}"
    )
    try:
        os.mkdir("data")
    except:
        print("data file exists")
    print("done")
    for picked in os.listdir(os.path.join(project_dir, job_dir, "gen_pick", "STAR")):
        new_name = os.path.splitext(picked)[0] + "_crypick" + ".star"
        os.link(
            os.path.join(project_dir, job_dir, "gen_pick", "STAR", picked),
            os.path.join(project_dir, job_dir, "data", new_name),
        )

    part_doc = open("_crypick.star", "w")
    part_doc.write(os.path.join(project_dir, args.in_mics))
    part_doc.close()

    out_doc = gemmi.cif.Document()
    output_nodes_block = out_doc.add_new_block("output_nodes")
    loop = output_nodes_block.init_loop(
        "", ["_rlnPipeLineNodeName", "_rlnPipeLineNodeType"]
    )
    loop.add_row([os.path.join(job_dir, "_crypick.star"), "2"])
    out_doc.write_file("RELION_OUTPUT_NODES.star")
    with open("RELION_OUTPUT_NODES.star") as f:
        print(f.read())


def main():
    """Change to the job working directory, then call run_job()"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--o", dest="out_dir", help="Output directory name")
    known_args, other_args = parser.parse_known_args()
    project_dir = os.getcwd()
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
