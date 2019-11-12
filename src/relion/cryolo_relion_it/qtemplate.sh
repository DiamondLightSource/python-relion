# cryolo job submission script
#
# Submit single jobs with 'qsub cryolo_command'
# module depends on hamilton 
#
#$ -V
#$ -S /bin/bash
#$ -cwd
#$ -P em
#$ -l gpu=4
#$ -N cryolo_ham
#$ -l m_mem_free=12G
#$ -l exclusive
#$ -o cryolo_job.out -e cryolo_job.err
#$ -q all.q

"$@"
sleep 0.1
touch .cry_predict_done

