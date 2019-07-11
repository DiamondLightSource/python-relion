from PIL import Image

with Image.open(
    "/dls/ebic/data/staff-scratch/Donovan/relionTutor2/relion30_tutorial/Movies/20170629_00021_frameImage.tiff"
) as tiff:
    print ("Dimensions = {}".format(tiff.size))
    header = tiff.tag

for x in header:
    print x, header[x]
