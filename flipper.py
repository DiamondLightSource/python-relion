import mrcfile as mrc
import os.path
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import re

#infile = 'FoilHole_22247924_Data_22246559_22246561_20180623_1736-193306.mrc'
dir_main = '/dls/ebic/data/staff-scratch/Donovan/testdata/Ap_K2_PP_GO/cryolo_impurities/'

nx = 3830 - 200
ny= 3710 - 200




def files(no_ext, dir_main):
    im_file = dir_main+'train_image/'+no_ext
    box_file = dir_main+'train_annotation/'+no_ext
    with mrc.open(im_file+'.mrc', permissive=True) as f:
	image = f.data

    boxes = pd.read_csv(box_file+'.box', sep='\t', header = None)
    box_val = boxes.values
    
    flip_x_def(box_val, image, box_file, im_file)
    flip_y_def(box_val, image, box_file, im_file)
    flip_xy_def(box_val, image, box_file, im_file)


def flip_x_def(box_val, image, box_file, im_file, nx=nx):
    flip_x = np.flip(image, 1)
    flip_x_box = np.array(box_val)
    for i in range(box_val.shape[0]):
        flip_x_box[i][0] = nx - box_val[i][0]
        flip_x_box[i][1] = box_val[i][1]

    np.savetxt(box_file+'_flipx.box',flip_x_box, delimiter= '\t', fmt='%.2f')

    with mrc.new(im_file+'_flipx.mrc', overwrite=True) as newx:
        newx.set_data(flip_x)
    print('done x')


def flip_y_def(box_val, image, box_file, im_file, ny=ny):
    flip_y = np.flip(image, 0)
    flip_y_box = np.array(box_val)
    for i in range(box_val.shape[0]):
        flip_y_box[i][1] = ny - box_val[i][1]
        flip_y_box[i][0] = box_val[i][0]
    np.savetxt(box_file+'_flipy.box', flip_y_box, delimiter= '\t', fmt='%.2f')

    with mrc.new(im_file+'_flipy.mrc', overwrite=True) as newy:
        newy.set_data(flip_y)
    print('done y')


def flip_xy_def(box_val, image, box_file, im_file, ny=ny):
    flip_y = np.flip(image, 0)
    flip_xy = np.flip(flip_y, 1)
    flip_xy_box = np.array(box_val)
    for i in range(box_val.shape[0]):
        flip_xy_box[i][1] = ny - box_val[i][1]
        flip_xy_box[i][0] = nx - box_val[i][0]
    np.savetxt(box_file+'_flipxy.box', flip_xy_box, delimiter= '\t', fmt='%.2f')

    with mrc.new(im_file+'_flipxy.mrc', overwrite=True) as newy:
        newy.set_data(flip_xy)
    print('done xy')

# MAIN
ims = os.listdir(dir_main+'train_annotation')
for x in ims:
    if re.findall("flip", x) == []:
        print(x)
        infile = x
        no_ext = os.path.splitext(infile)[0]
        files(no_ext, dir_main)

