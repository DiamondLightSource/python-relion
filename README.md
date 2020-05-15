# relion_it with crYOLO support

relion_it is now running with python 3.6.8!  In options you can choose to pick via the crYOLO
general model or with the relion auto picker.  CrYOLO runs as an external job after ctfFind. Relion
then takes the particle coordinates found by crYOLO and then further processes them.  Particles
appear as a manual pick job in the relion gui and can be viewed there.


## Requirements for external use:

1. CrYOLO and Relion 3.0 installed.

2. Conda Environment for crYOLO and Relion

3. Edit paths in options.py for MotionCor2 and Cryolo general model locations

4. Run by: `cryolo_relion_it.py /Path/To/options.py --gui`


## Scripts being used:

 - cryolo\_relion\_it.py: The main script that dls_yolo_relion calls. This houses the main pipeline
        and calls to all the other scripts.

 - cryolo\_pipeline.py: The crYOLO pipeline. This runs as a subprocess and exectutes many repeated
        times to Import, MotionCorr, CtfFind, crYOLO pick, Extract... as new movies are
        collected. As Relion 3.0 does not support external job types the YOLO pipeline is in fact 3
        seperate pipelines chained together.

 - cryolo\_external\_job.py: Reads Relion star files and makes a directory that crYOLO can execute
        particle picking from.

 - cryolo\_fine\_tune\_job.py: After 2D classification, good classes can be selected to fine tune the
        cryolo general model. After the finetuning, crYOLO uses this new model to pick future
        particles in the current run.

 - dls\_options.py: Example template options for the installation at DLS / eBIC.


Note: Fine-tuning requires good 2D classes to be picked by hand after first 2D iteration

