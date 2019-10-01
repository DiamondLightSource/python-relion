#########################################################################
##									#
##                relion_it with crYOLO support				#
##									#
#########################################################################

relion_it is now running with python 3.6.8!  In options you can choose
to pick via the crYOLO general model or with the relion auto picker.
CrYOLO runs as an external job after ctfFind. Relion then takes the
particle coordinates found by crYOLO and then further processes them.
cryolo_boxmanager.py can be used to visualise the chosen particles on
the micrographs if desired.


Normal usage:

1. module load EM/cryolo/yolo_it   # Prepares python environment 
			    	     for relion_it and crYOLO
2. dls_yolo_relion_it	# This opens a gui with options


Scripts being use:

 - relion_it_editted.py: 

   	The main script that dls_yolo_relion calls. This houses the
   	main pipeline and calls to all the other scripts.


 - RunJobsCryolo.py: 

   	The crYOLO pipeline. This runs as a subprocess and exectutes
   	many repeated times to Import, MotionCorr, CtfFind, crYOLO
   	pick, Extract... as new movies are collected. As Relion 3.0
   	does not support external job types the YOLO pipeline is in
   	fact 3 seperate pipelines chained together.

 - external_cryolo_3.py: 

   	Reads Relion star files and makes a directory that crYOLO can
   	execute particle picking from.


 - correct_path_relion.py: 

   	After crYOLO has picked particles, the coordinate star files
   	must be placed in a directory tree that Relion is
   	expecting. This does that!


 - external_cryolo_fine_3.py: 

   	After 2D classification, good classes can be selected to fine
   	tune the cryolo general model. After the finetuning, crYOLO
   	uses this new model to pick future particles in the current
   	run.


 - options.py: 

        Basic options for relion_it to run with.


 - qsub.sh: 

        Cluster submit script for crYOLO.


 - qtemplate.sh: 
 
        Cluster template for crYOLO.


*Still in Development* 

Finetuning can be done after Class2D by selecting good classes. These
'good' particles are then used to finetune the crYOLO general model
for future picking.

Visualising picked particles.
