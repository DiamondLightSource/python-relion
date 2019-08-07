#!/bin/bash

rm temp_selected_particles_stars -r
mkdir temp_selected_particles_stars

for i in $(awk -F ' ' '{print $14}' particles.star | uniq | awk -F '/' '{print $2}')
do
    echo "
loop_
_rlnCoordinateX #1 
_rlnCoordinateY #2" > temp_selected_particles_stars/$i
    grep $i particles.star | awk -F ' ' '{printf("%.6s	%.6s\n", $11, $12)}' >> temp_selected_particles_stars/$i
    mv -- "temp_selected_particles_stars/$i" "temp_selected_particles_stars/${i%.mrc}_DW.star"
done

