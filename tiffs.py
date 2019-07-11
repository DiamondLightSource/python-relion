from PIL import Image

# infile = '/dls/ebic/data/staff-scratch/Donovan/relionTutor2/relion30_tutorial/Movies/20170629_00021_frameImage.tiff'
# infile = '/dls/ebic/data/staff-scratch/Yuriy/DataFileTypes/m06K3EPUcompTiff/em20287-23/FoilHole_7832439_Data_7825668_7825670_20190709_2345_fractions.tiff' #Compressed and super resolution
infile = "/dls/ebic/data/staff-scratch/Yuriy/DataFileTypes/m06K3EPUcompTiff/em20287-23/FoilHole_7832439_Data_7825668_7825670_20190709_2345.tiff"
with Image.open(infile) as tiff:
    print ("Dimensions = {}".format(tiff.size))
    header = tiff.tag

for x in header:
    print x, header[x]
