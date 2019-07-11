import mrcfile as mrc
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("-i", "--input", help="Input file name")
args = parser.parse_args()

# infile = '/dls/ebic/data/staff-scratch/Yuriy/DataFileTypes/m02K2EPU_Mrc/em16619-23/GridSquare_9897066/FoilHole_9907179_Data_9923659_9923661_20190711_122055.mrc' # K2 Non-linear
# infile = '/dls/ebic/data/staff-scratch/Yuriy/DataFileTypes/m06K3EPU_Mrc/em20287-23/FoilHole_7821234_Data_7825668_7825670_20190708_1608.mrc' # k3 Non-linear
# infile = '/dls/ebic/data/staff-scratch/Yuriy/DataFileTypes/m02K2EPU_Mrc/em16619-23/GridSquare_9897066/FoilHole_9907179_Data_9923659_9923661_20190711_122055-376198_frames.mrc' # K2 Non-linear frames
# infile = '/dls/ebic/data/staff-scratch/Yuriy/DataFileTypes/m06K3EPU_Mrc/em20287-23/FoilHole_7821234_Data_7825668_7825670_20190708_1608_fractions.mrc' # Does not work

infile = args.input


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


def metadata_mrc(doc, labels):
    for category, x in labels.items():
        print category, ":", doc[0][x]
        metadata[category] = doc[0][x]

    # Unknown Value
    print doc[0][19], "<----- What is this?"

    # for x in enumerate(doc):
    # 	print x

    if metadata["Direct Detector Electron Counting"]:
        linMod = "Not in linear mode"
    else:
        linMod = "\nIn linear mode"

    if (
        metadata["nx"] * metadata["ny"] == 14238980
        or metadata["nx"] * metadata["ny"] == 28477960
    ):  # Two values for super resolution
        detect = "K2"
        print metadata["nx"] * metadata["ny"]
    elif (
        metadata["nx"] * metadata["ny"] == 23569920
        or metadata["nx"] * metadata["ny"] == 47139840
    ):
        detect = "K3"
    else:
        detect = "Unsure of detector"

    print detect, linMod


def metadata_frames(doc_all, labels):
    nx = doc_all.shape[0]
    ny = doc_all.shape[1]

    if nx * ny == 14238980 or 28477960:  # Two values for super resolution
        detect = "K2"
    elif nx * ny == 23569920 or 47139840:
        detect = "K3"
    else:
        detect = "Unsure of detector"

    print detect


with mrc.open(infile) as data:
    doc_all = data.data
    doc = data.extended_header

if doc.shape == (0,):
    metadata_frames(doc_all, labels)
else:
    metadata_mrc(doc, labels)
