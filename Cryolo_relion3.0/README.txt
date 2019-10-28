#########################################################################
##									#
##                relion_it with crYOLO support				#
##									#
#########################################################################

relion_it is now running with python 3.6.8!  In options you can choose
to pick via the crYOLO general model or with the relion auto picker.
CrYOLO runs as an external job after ctfFind. Relion then takes the
particle coordinates found by crYOLO and then further processes them.
Particles appear as a manual pick job in the relion gui and can be
viewed there.


Normal usage for Diamond:

1. module load EM/cryolo/yolo_it   # Prepares python environment 
			    	     for relion_it and crYOLO
2. dls_yolo_relion_it	# This opens a gui with options


Requirements for external use:

1. CrYOLO and Relion 3.0 installed.

2. Conda Environment for crYOLO and Relion_it (see conda.txt)

3. Edit paths in * scripts.

4. If using a cluster edit submit scripts accordingly. Otherwise edit
calls ** to run on local machine.

5. Edit all first lines (#!) of python files to your conda python environment.

6. Run by: relion_it_editted.py --gui

Scripts being use:

 - relion_it_editted.py: 

   	The main script that dls_yolo_relion calls. This houses the
   	main pipeline and calls to all the other scripts.

	* Line ~273: cryolo_relion_directory = '/PATH/TO/CRYOLO_RELION/EXECUTABLES/'


 - RunJobsCryolo.py: 

   	The crYOLO pipeline. This runs as a subprocess and exectutes
   	many repeated times to Import, MotionCorr, CtfFind, crYOLO
   	pick, Extract... as new movies are collected. As Relion 3.0
   	does not support external job types the YOLO pipeline is in
   	fact 3 seperate pipelines chained together.

	* Line ~18: cryolo_relion_directory = '/PATH/TO/CRYOLO_RELION/EXECUTABLES/'


 - external_cryolo_3.py: 

   	Reads Relion star files and makes a directory that crYOLO can
   	execute particle picking from.

	* Line ~28: qsub_file = '/PATH/TO/CRYOLO/CLUSTER/SUBMIT/SCRIPT.sh'
	* Line ~29: gen_model = '/PATH/TO/CRYOLO/GENERALMODEL.h5'

	** Line ~87: 
	1) os.system(f"{qsub_file} cryolo_predict.py -c config.json -i ...
	2) while not os.path.exists('.cry_predict_done'):
	3)    time.sleep(1)
	4) os.remove('.cry_predict_done')

	This is a slightly harder one. If using a cluster then the
	submit script and template will be used and things should
	work. The template makes a file called '.cry_predict_done'
	when cryolo has finished. This is done as the main pipeline
	needs to know when crYOLO finishes but if crYOLO is running on
	an external machine (eg. a cluster) whilst the main pipeline
	is on a local machine it is difficult to transfer messages
	between them. If a cluster is not being used then the submit
	scripts wont be used so '.cry_predict_done' will never be
	made. So remove {qsub_file} in line 1 and all of lines 2-4.


 - correct_path_relion.py: 

   	After crYOLO has picked particles, the coordinate star files
   	must be placed in a directory tree that Relion is
   	expecting. This does that!


 - external_cryolo_fine_3.py: 

   	After 2D classification, good classes can be selected to fine
   	tune the cryolo general model. After the finetuning, crYOLO
   	uses this new model to pick future particles in the current
   	run.

	* Line ~21: qsub_file = '/PATH/TO/CRYOLO/CLUSTER/SUBMIT/SCRIPT.sh'

	** Line ~73:
	1) os.system(f"{qsub_file} cryolo_train.py -c config.json ...
	2) while not os.path.exists('.cry_predict_done'):
	3)    time.sleep(1)
	4) os.remove('.cry_predict_done')

	Do the same as external_cryolo_3.py.


 - options.py: 

        Basic options for relion_it to run with.
	
	* Line ~142: motioncor_exe = '/dls_sw/apps/EM/MotionCor2/1.1.0/MotionCor2'
	* Line ~183: gctf_exe = '/dls_sw/apps/EM/Gctf/1.18/Gctf'
	* Line ~189: ctffind4_exe = '/dls_sw/apps/EM/ctffind/4.1.5-compat/ctffind'
	** Line ~339: Cluster details.
	** If not using cluster also set all 'XXX_submit_to_queue' options to False


 - qsub.sh: 

        Cluster submit script for crYOLO.
	
	* Change to location of qtemplate.


 - qtemplate.sh: 
 
        Cluster template for crYOLO.


*Still in Development* 

Finetuning can be done after Class2D by selecting good classes. These
'good' particles are then used to finetune the crYOLO general model
for future picking.

