import mrcfile as mrc
import sys
import os.path
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("-i", "--input", help="Input file name")
args = parser.parse_args()


"""A range of testing files."""
# infile = '/dls/ebic/data/staff-scratch/Yuriy/DataFileTypes/m02K2EPU_Mrc/em16619-23/GridSquare_9897066/FoilHole_9907179_Data_9923659_9923661_20190711_122055.mrc' # K2 Non-linear
# infile = '/dls/ebic/data/staff-scratch/Yuriy/DataFileTypes/m06K3EPU_Mrc/em20287-23/FoilHole_7821234_Data_7825668_7825670_20190708_1608.mrc' # k3 Non-linear
# infile = '/dls/ebic/data/staff-scratch/Yuriy/DataFileTypes/m02K2EPU_Mrc/em16619-23/GridSquare_9897066/FoilHole_9907179_Data_9923659_9923661_20190711_122055-376198_frames.mrc' # K2 Non-linear frames
# infile = '/dls/ebic/data/staff-scratch/Yuriy/DataFileTypes/m06K3EPU_Mrc/em20287-23/FoilHole_7821234_Data_7825668_7825670_20190708_1608_fractions.mrc' # K3 Super resolution
infile = args.input
if infile is None:
    print "No input file given. Please run as 'ex_header.py -i FILE_NAME."
    sys.exit()
if os.path.exists(infile) == False:
    print "File does not exist"
    sys.exit()


"""Data to be extracted from extended header of MRC file."""
labels = {
    "Microscope Type": 4,
    "HT": 8,
    "Dose": 9,
    "Alpha Tilt": 10,
    "X stage": 12,
    "Y stage": 13,
    "Z stage": 14,
    "X Pixel Size": 17,
    "Y Pixel Size": 18,
    "Applied Defocus": 22,
    "Camera Name": 50,
    "nx": 53,
    "ny": 54,
    "Direct Detector Electron Counting": 57,
    "Phase Plate": 70,
    "Fraction Number": 92,
}
metadata = {}

"""Camera dimensions (x*y) for normal and super resolution."""
K2_dim = 14238980
K2_dim_super = K2_dim * 4
K3_dim = 23569920
K3_dim_super = K3_dim * 4


def metadata_mrc(doc, labels):
    for category, x in labels.items():
        print category, ":", doc[0][x]
        metadata[category] = doc[0][x]

    # Unknown Value
    print doc[0][19], "<----- What is this?"

    if metadata["Direct Detector Electron Counting"]:
        linMod = "In counting mode"
    else:
        linMod = "In linear mode"

    if metadata["nx"] * metadata["ny"] == K2_dim:
        detect = "K2"
        res = ""
    elif metadata["nx"] * metadata["ny"] == K2_dim_super:
        detect = "K2"
        res = "Super Resolution"
    elif metadata["nx"] * metadata["ny"] == K3_dim:
        detect = "K3"
        res = ""
    elif metadata["nx"] * metadata["ny"] == K3_dim_super:
        detect = "K3"
        res = "Super Resolution"
    else:
        detect = "Unsure of detector"
        res = ""

    print detect, linMod, res


def metadata_frames(doc_header, labels):
    nx = doc_header.nx
    ny = doc_header.ny

    if nx * ny == K2_dim:
        detect = "K2"
        res = ""
    elif nx * ny == K2_dim_super:
        detect = "K2"
        res = "Super Resolution"
    elif nx * ny == K3_dim:
        detect = "K3"
        res = ""
    elif nx * ny == K3_dim_super:
        detect = "K3"
        res = "Super Resolution"
    else:
        detect = "Unsure of detector"
        res = ""

    print detect, res


# Main
with mrc.open(infile, header_only=True) as f:
    doc_header = f.header
    doc = f.extended_header

if doc.shape == (0,):
    metadata_frames(doc_header, labels)
else:
    metadata_mrc(doc, labels)
