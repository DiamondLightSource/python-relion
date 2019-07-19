"""
Input an mrc or tiff file using 'ccpem-python ex_header.py -i FILE_NAME'. Output will show camera type and, 
depending on available metadata, whether the camera is running in linear or counting mode. If an extended 
header is available it will also output a range of useful parameters.
"""
import mrcfile as mrc
import sys
import os.path
from PIL import Image
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", help="Input file name")
args = parser.parse_args()
infile = args.input
ext = os.path.splitext(infile)[1]

"""A range of testing files."""
#infile = '/dls/ebic/data/staff-scratch/Yuriy/DataFileTypes/m02K2EPU_Mrc/em16619-23/GridSquare_9897066/FoilHole_9907179_Data_9923659_9923661_20190711_122055.mrc' # K2 Non-linear
#infile = '/dls/ebic/data/staff-scratch/Yuriy/DataFileTypes/m06K3EPU_Mrc/em20287-23/FoilHole_7821234_Data_7825668_7825670_20190708_1608.mrc' # k3 Non-linear
#infile = '/dls/ebic/data/staff-scratch/Yuriy/DataFileTypes/m02K2EPU_Mrc/em16619-23/GridSquare_9897066/FoilHole_9907179_Data_9923659_9923661_20190711_122055-376198_frames.mrc' # K2 Non-linear frames
#infile = '/dls/ebic/data/staff-scratch/Yuriy/DataFileTypes/m06K3EPU_Mrc/em20287-23/FoilHole_7821234_Data_7825668_7825670_20190708_1608_fractions.mrc' # K3 Super resolution

"""Data to be extracted from extended header of MRC file."""
labels = {
	 'Microscope Type': 4,
	 'HT': 8,
	 'Dose': 9,
	 'Alpha Tilt': 10,
	 'X stage': 12,
	 'Y stage': 13,
	 'Z stage': 14,
	 'X Pixel Size': 17,
	 'Y Pixel Size': 18,
	 'Applied Defocus': 22,
	 'Camera Name': 50,
	 'nx' : 53,
	 'ny' : 54,
	 'Direct Detector Electron Counting': 57,
	 'Phase Plate': 70,
	 'Fraction Number': 92
	 }
metadata = {}

"""Camera dimensions (x*y) for normal and super resolution."""
K2_dim = 14238980 
K2_dim_super = K2_dim * 4
K3_dim = 23569920 
K3_dim_super = K3_dim * 4
Fal_dim = 16777216
Fal_dim_super = Fal_dim * 4


def metadata_mrc(doc, labels):
"""Data output when mrc extended header is available"""
	for category, x in labels.items():
		print category,":", doc[0][x]
		metadata[category] = doc[0][x]

	# Unknown Value
	print doc[0][19], "<----- What is this?"

	if metadata['Direct Detector Electron Counting']: linMod = 'In counting mode'
	else: linMod = 'In linear mode'
	
	nx = metadata['nx']
	ny = metadata['ny']
	size = nx * ny 
	camera_type(size)
	print linMod

def metadata_frames(doc_header, labels):
"""Data output when mrc extended header is not available"""
	nx = doc_header.nx
	ny = doc_header.ny
	size = nx * ny
	camera_type(size)
	print 'No extended header data.'
	

def camera_type(size):

	if   size  == K2_dim: 	detect = 'K2'; res = ''
	elif size  == K2_dim_super: 	detect = 'K2'; res = 'Super Resolution'
	elif size  == K3_dim: 	detect = 'K3'; res = ''
	elif size  == K3_dim_super: 	detect = 'K3'; res = 'Super Resolution'
	elif size  == Fal_dim: 	detect = 'Falcon III'; res = ''
	elif size  == Fal_dim_super: 	detect = 'Falcon III'; res = 'Super Resolution'
	else: detect = 'Unsure of detector'; res = ''

	print detect, res


# Main
try:
	if ext == '.tiff':
		with Image.open(infile) as f:
			nx = f.size[0]
			ny = f.size[1]
			size = nx * ny
			camera_type(size)
			print 'No extended header data.'
	else:
		with mrc.open(infile, permissive = True, header_only = True) as f:
			doc_header = f.header
			doc = f.extended_header
		if doc.shape == (0,):
			metadata_frames(doc_header, labels)
		else: 
			metadata_mrc(doc, labels)
except IOError:
	print "Error: {}: No such file".format(infile)
	sys.exit()
except TypeError:
	print "No input file given. Please run as 'ccpem-python ex_header.py -i FILE_NAME'."
	sys.exit()
except AttributeError:
	print "Error: {}: Does not appear to be an MRC or tiff file".format(infile)
	sys.exit()
except:
   	print "Unexpected error:", sys.exc_info()[0]
    	raise
	sys.exit()
