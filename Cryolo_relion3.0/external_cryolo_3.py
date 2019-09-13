#!/dls/ebic/data/staff-scratch/Donovan/anaconda/envs/cry_rel/bin/python
"""
External job for calling cryolo within Relion 3.1
in_mics are the micrographs to be picked on
in_coords is the model to be used, empty will use general model!

Run in main Relion project directory
external_cryolo.py --o $PATH_WHERE_TO_STORE --in_mics $PATH_TO_MCORR/CTF_STARFILE --box_size $BOX_SIZE --threshold 0.3 (optional: --in_coords $PATH_TO_MODEL)
eg:
external_cryolo_3.py --o "External" --in_mics "CtfFind/job004/micrographs_ctf.star" --box_size 300 --threshold 0.3
"""

import argparse
import json
import os
import os.path
import random
import sys
import shutil
import correct_path_relion
import pathlib

import gemmi


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
    parser.add_argument("--in_coords", help="model from previous job")
    args = parser.parse_args(args_list)
    thresh = args.threshold
    box_size = args.box_size

    try:
        model = os.path.join(project_dir, args.in_coords)
        print(model)
    except:
        print("no model specified")

    # Making a cryolo config file with the correct box size
    with open("/dls_sw/apps/EM/crYOLO/cryo_phosaurus/config.json", "r") as json_file:
        data = json.load(json_file)
        print(data["model"]["anchors"])
        data["model"]["anchors"] = [int(box_size), int(box_size)]
        print(data["model"]["anchors"])
    with open("config.json", "w") as outfile:
        json.dump(data, outfile)

    # Reading the micrographs star file from relion
    in_doc = gemmi.cif.read_file(os.path.join(project_dir, args.in_mics))
    data_as_dict = json.loads(in_doc.as_json())["#"]

    try:
        os.mkdir("cryolo_input")
    except:
        shutil.rmtree("cryolo_input")
        os.mkdir("cryolo_input")

    # Arranging files for cryolo to predict particles from
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

    # Checking to see if a paticular model has been specified
    if args.in_coords is None:
        os.system(
            f"cryolo_predict.py -c config.json -i {os.path.join(project_dir, job_dir, 'cryolo_input')} -o {os.path.join(project_dir, job_dir, 'gen_pick')} -w /dls_sw/apps/EM/crYOLO/cryo_phosaurus/gmodel_phosnet_20190516.h5 -g 0 -t {thresh}"
        )
    else:
        os.system(
            f"cryolo_predict.py -c config.json -i {os.path.join(project_dir, job_dir, 'cryolo_input')} -o {os.path.join(project_dir, job_dir, 'gen_pick')} -w {model} -g 0 -t {thresh}"
        )
    try:
        os.mkdir("picked_stars")
    except:
        print("data file exists")
    print("done")

    # Arranging files for Relion to use
    for picked in os.listdir(os.path.join(project_dir, job_dir, "gen_pick", "STAR")):
        new_name = os.path.splitext(picked)[0] + "_crypick" + ".star"
        os.link(
            os.path.join(project_dir, job_dir, "gen_pick", "STAR", picked),
            os.path.join(project_dir, job_dir, "picked_stars", new_name),
        )

    # Writing a star file for Relion
    part_doc = open("_crypick.star", "w")
    part_doc.write(os.path.join(project_dir, args.in_mics))
    part_doc.close()

    # Required star file
    out_doc = gemmi.cif.Document()
    output_nodes_block = out_doc.add_new_block("output_nodes")
    loop = output_nodes_block.init_loop(
        "", ["_rlnPipeLineNodeName", "_rlnPipeLineNodeType"]
    )
    loop.add_row([os.path.join(job_dir, "_crypick.star"), "2"])
    out_doc.write_file("RELION_OUTPUT_NODES.star")
    with open("RELION_OUTPUT_NODES.star") as f:
        print(f.read())
    ctf_star = os.path.join(project_dir, args.in_mics)
    correct_path_relion.correct(ctf_star)


def main():
    """Change to the job working directory, then call run_job()"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--o", dest="out_dir", help="Output directory name")
    known_args, other_args = parser.parse_known_args()
    project_dir = os.getcwd()
    try:
        os.mkdir("External")
    except:
        print("External exists")
    # try: os.mkdir('External/ctfpick')
    # except: print('External/ctfpick exists')
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
