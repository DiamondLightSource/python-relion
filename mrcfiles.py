import mrcfile as mrc
import matplotlib.pyplot as plt
from PIL import Image

with mrc.open(
    "/dls/ebic/data/staff-scratch/Donovan/relionTutor2/relion30_tutorial/Refine3D/polished_885_small_box/run_class001.mrc"
) as doc:
    print("mrc file for 3D model")
    im = doc.data
    header = doc.header
    # print(header)
    print("Dimensions = {}".format(im.shape))
    print("Pixel size = {}".format(doc.voxel_size))

with Image.open(
    "/dls/ebic/data/staff-scratch/Donovan/relionTutor2/relion30_tutorial/Movies/20170629_00021_frameImage.tiff"
) as tiff:
    print("tiff file Movie frame")
    print("Dimensions = {}".format(tiff.size))

with mrc.open(
    "/dls/ebic/data/staff-scratch/Donovan/relionTutor2/relion30_tutorial/Movies/gain.mrc"
) as doc:
    print("mrc file for gain")
    im = doc.data
    header = doc.header
    # print(header)
    print("Dimensions = {}".format(im.shape))
    print("Pixel size = {}".format(doc.voxel_size))

# Changed to see how git works!
