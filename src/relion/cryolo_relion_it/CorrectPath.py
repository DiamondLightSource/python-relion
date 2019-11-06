"""
Creates a folder tree that is the relion standard and then populates folder tree with the picked coordinate star files produced by Cryolo
"""
import gemmi
import os
import json
import pathlib
import shutil


def correct(ctf_star):
    in_doc = gemmi.cif.read_file(ctf_star)
    data_as_dict = json.loads(in_doc.as_json())["#"]

    for i in range(len(data_as_dict["_rlnctfimage"])):
        name = data_as_dict["_rlnctfimage"][i]
        dirs, ctf_file = os.path.split(name)
        full_dir = ""
        for d in dirs.split("/")[2:]:
            full_dir = os.path.join(full_dir, d)
        pathlib.Path(full_dir).mkdir(parents=True, exist_ok=True)
        picked_star = os.path.splitext(ctf_file)[0] + "_manualpick.star"
        try:
            shutil.move(
                os.path.join("picked_stars", picked_star),
                os.path.join(full_dir, picked_star),
            )
        except:
            print(f"cryolo found no particles in {picked_star} or already moved")


if __name__ == "__main__":
    correct()
