import mrcfile as mrc

# Non Linear
infile = "/dls/ebic/data/staff-scratch/Yuriy/V1Vo/nr18477-51/supervisor_20180330_005922_epu_d30carb_gr2/Images-Disc1/GridSquare_29289631/Data/FoilHole_30242671_Data_30241736_30241737_20180330_0440.mrc"
# infile = '/dls/ebic/data/staff-scratch/Yuriy/V1Vo/nr18477-51/raw/GridSquare_25123228/Data/FoilHole_25130916_Data_25128460_25128461_20180325_2342_Fractions.mrc' # Does not work, stack of images
# Linear
# infile = '/dls/ebic/data/staff-scratch/Yuriy/V1Vo/m03cm22937-2/supervisor_20190502_212658_V1Vo_EPUtest/Images-Disc1/GridSquare_1443044/FoilHoles/FoilHole_1468945_20190503_082746.mrc'
# infile = '/dls/ebic/data/staff-scratch/Yuriy/V1Vo/m03cm22937-2/supervisor_20190502_212658_V1Vo_EPUtest/Images-Disc1/GridSquare_1443044/FoilHoles/FoilHole_1469210_20190503_100156.mrc'
# infile = '/dls/ebic/data/staff-scratch/Yuriy/V1Vo/m03cm22937-2/supervisor_20190502_212658_V1Vo_EPUtest/Images-Disc1/GridSquare_1443044/GridSquare_20190502_225804.mrc'

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


with mrc.open(infile) as mrc:
    doc_small = mrc.header
    doc = mrc.extended_header

for category, x in labels.items():
    print category, ":", doc[0][x]
    metadata[category] = doc[0][x]

# Unknown Value
print doc[0][19], "<----- What is this?"

# for x in enumerate(doc):
# 	print x

if metadata["Direct Detector Electron Counting"]:
    print "\nNot in linear mode"
else:
    print "\nIn linear mode"
