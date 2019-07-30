import mrcfile as mrc
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

infile = "/dls/ebic/data/staff-scratch/Donovan/testdata/Ap_K2_PP_GO/cryolo_impurities/train_image/FoilHole_22247924_Data_22246559_22246561_20180623_1736-193306.mrc"
boxfile = "/dls/ebic/data/staff-scratch/Donovan/testdata/Ap_K2_PP_GO/cryolo_impurities/train_annotation/FoilHole_22247924_Data_22246559_22246561_20180623_1736-193306.box"

nx = 3830 - 200
ny = 3710 - 200


with mrc.open(infile, permissive=True) as f:
    all_data = f.data
flip_y = np.flip(all_data, 0)
flip_x = np.flip(all_data, 1)
flip_xy = np.flip(flip_y, 1)

boxes = pd.read_csv(boxfile, sep="\t", header=None)
box_val = boxes.values
box_shape = boxes.values.shape


def flip_x_def(box_val, nx=nx):
    flip_x_box = np.array(box_val)
    for i in range(box_val.shape[0]):
        flip_x_box[i][0] = nx - box_val[i][0]
        flip_x_box[i][1] = box_val[i][1]

    np.savetxt(
        "/dls/ebic/data/staff-scratch/Donovan/testdata/Ap_K2_PP_GO/cryolo_impurities/train_annotation/FoilHole_22247924_Data_22246559_22246561_20180623_1736-193306_flipx.box",
        flip_x_box,
        delimiter="\t",
        fmt="%.2f",
    )

    with mrc.new(
        "/dls/ebic/data/staff-scratch/Donovan/testdata/Ap_K2_PP_GO/cryolo_impurities/train_image/FoilHole_22247924_Data_22246559_22246561_20180623_1736-193306_flipx.mrc",
        overwrite=True,
    ) as newx:
        newx.set_data(flip_x)
    print("done x")


def flip_y_def(box_val, ny=ny):
    flip_y_box = np.array(box_val)
    for i in range(box_val.shape[0]):
        flip_y_box[i][1] = ny - box_val[i][1]
        flip_y_box[i][0] = box_val[i][0]
    np.savetxt(
        "/dls/ebic/data/staff-scratch/Donovan/testdata/Ap_K2_PP_GO/cryolo_impurities/train_annotation/FoilHole_22247924_Data_22246559_22246561_20180623_1736-193306_flipy.box",
        flip_y_box,
        delimiter="\t",
        fmt="%.2f",
    )

    with mrc.new(
        "/dls/ebic/data/staff-scratch/Donovan/testdata/Ap_K2_PP_GO/cryolo_impurities/train_image/FoilHole_22247924_Data_22246559_22246561_20180623_1736-193306_flipy.mrc",
        overwrite=True,
    ) as newy:
        newy.set_data(flip_y)
    print("done y")


# MAIN

flip_x_def(box_val)
flip_y_def(box_val)
