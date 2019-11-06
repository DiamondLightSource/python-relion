#####################################################################################################
##                                                                                                  #
##                               relion_it with crYOLO support                                      #
##                                                                                                  #
#####################################################################################################

relion_it is now running with python 3.6.8!  In options you can choose to pick via the crYOLO
general model or with the relion auto picker.  CrYOLO runs as an external job after ctfFind. Relion
then takes the particle coordinates found by crYOLO and then further processes them.  Particles
appear as a manual pick job in the relion gui and can be viewed there.


Normal usage for Diamond:

1. module load EM/cryolo/yolo_it       # Prepares python environment for relion_it and crYOLO

2. dls_yolo_relion_it                  # This opens a gui with options


Requirements for external use:

1. CrYOLO and Relion 3.0 installed.

2. Conda Environment for crYOLO and Relion

3. Edit paths in options.py for your MotionCor2 and Cryolo general model

4. Run by: cryolo_relion_it.py --gui


Scripts being use:

 - cryolo_relion_it.py: The main script that dls_yolo_relion calls. This houses the main pipeline
        and calls to all the other scripts.

 - CryoloPipeline.py: The crYOLO pipeline. This runs as a subprocess and exectutes many repeated
        times to Import, MotionCorr, CtfFind, crYOLO pick, Extract... as new movies are
        collected. As Relion 3.0 does not support external job types the YOLO pipeline is in fact 3
        seperate pipelines chained together.

 - CryoloExternalJob.py: Reads Relion star files and makes a directory that crYOLO can execute
        particle picking from.

 - CorrectPath.py: After crYOLO has picked particles, the coordinate star files must be placed in a
        directory tree that Relion is expecting. This does that!

 - CryoloFineTuneJob.py: After 2D classification, good classes can be selected to fine tune the
        cryolo general model. After the finetuning, crYOLO uses this new model to pick future
        particles in the current run.

 - options.py: Basic options for relion_it to run with.

 - qsub.sh: Cluster submit script for crYOLO.

 - qtemplate.sh: Cluster template for crYOLO. If using cluster must have template create a
                 '.cry_done' file so that the pipeline knows that cryolo has finished.


Note: Fine-tuning requires good 2D classes to be picked by hand after first 2D iteration
