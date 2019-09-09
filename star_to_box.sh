#!/bin/bash

rm temp_selected_particles_stars -r
mkdir temp_selected_particles_stars

for i in $(awk -F ' ' '{print $7}' particles.star | uniq | awk -F '/' '{print $2}')
do
#    echo "
#loop_
#rlnCoordinateX #1 
#rlnCoordinateY #2" > temp_selected_particles_stars/$i
    grep $i particles.star | awk -F ' ' '{printf("%.6s	%.6s	0	0\n", $1, $2)}' >> temp_selected_particles_stars/$i
    mv -- "temp_selected_particles_stars/$i" "temp_selected_particles_stars/${i%.mrc}.box"
done

python3.7 ~/Documents/pythonEM/box_correct.py
