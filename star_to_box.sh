#!/bin/bash
mkdir temp

for i in $(awk -F ' ' '{print $7}' particles.star | uniq | awk -F '/' '{print $2}')
do
    head -10 particles.star > temp/$i
    grep $i particles.star | awk -F ' ' '{print $1, $2, 150, 150}' >> temp/$i
done

for f in temp/*.mrc
do
   mv -- "$f" "${f%.mrc}.star"
done
