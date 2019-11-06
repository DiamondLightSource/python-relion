# Options file for relion_it.py

### General parameters
# Pixel size in Angstroms in the input movies
angpix = 1.0
# Acceleration voltage (in kV)
voltage = 300.0
# Polara = 2.0; Talos/Krios = 2.7; some Cryo-ARM = 1.4
Cs = 2.7


### Import images (Linux wild card; movies as *.mrc, *.mrcs, *.tiff or *.tif; single-frame micrographs as *.mrc)
import_images = "Movies/*.tif"
# Are these multi-frame movies? Set to False for single-frame micrographs (and motion-correction will be skipped)
images_are_movies = True


### MotionCorrection parameters
# Dose in electrons per squared Angstrom per frame
motioncor_doseperframe = 1.0
# Gain-reference image in MRC format (only necessary if input movies are not yet gain-corrected, e.g. compressed TIFFs from K2)
motioncor_gainreference = "Movies/gain.mrc"


### CTF estimation parameters
# Most cases won't need changes here...


### Autopick parameters
# Use reference-free Laplacian-of-Gaussian picking (otherwise use reference-based template matching instead)
autopick_do_LoG = True
# Minimum and maximum diameter in Angstrom for the LoG filter
autopick_LoG_diam_min = 150
autopick_LoG_diam_max = 180
# Use positive values (0-1) to pick fewer particles; use negative values (-1-0) to pick more particles
autopick_LoG_adjust_threshold = 0.0
#
# OR:
#
# References for reference-based picking (when autopick_do_LoG = False)
autopick_2dreferences = ""
# OR: provide a 3D references for reference-based picking (when autopick_do_LoG = False)
autopick_3dreference = ""

# Threshold for reference-based autopicking (threshold 0 will pick too many particles. Default of 0.4 is hopefully better. Ultimately, just hope classification will sort it all out...)
autopick_refs_threshold = 0.4
# Minimum inter-particle distance for reference-based picking (~70% of particle diameter often works well)
autopick_refs_min_distance = 120
#
# For both LoG and refs:
#
# Use this to remove false positives from carbon edges (useful range: 1.0-1.2, -1 to switch off)
autopick_stddev_noise = -1
# Use this to remove false positives from carbon edges (useful range: -0.5-0.0; -999 to switch off)
autopick_avg_noise = -999
#
# OR:
#
### Cryolo
# True will run cryolo instead of gaussian and ref based autopickers
autopick_do_cryolo = True
# Threshold for cryolo autopicking (higher the threshold the more *discriminative the cryolo picker) ((* But beware it may still not be picking what you want! ))
cryolo_threshold = 0.3
# Finetune the cryolo general model by selecting good classes from 2D classification
cryolo_finetune = False


### Extract parameters
# Box size of particles in the averaged micrographs (in pixels) (Also used for cryolo boxsize)
extract_boxsize = 400
# Down-scale the particles upon extraction?
extract_downscale = True
# Box size of the down-scaled particles (in pixels)
extract_small_boxsize = 100
# In second pass, down-scale the particles upon extraction?
extract2_downscale = True
# In second pass, box size of the down-scaled particles (in pixels)
extract2_small_boxsize = 64


### Now perform 2D and/or 3D classification with the extracted particles?
do_class2d = True
# And/or perform 3D classification?
do_class3d = True
# Repeat 2D and/or 3D-classification for batches of this many particles
batch_size = 10000
# Number of 2D classes to use
class2d_nr_classes = 50
# Diameter of the mask used for 2D/3D classification (in Angstrom)
mask_diameter = 300.0
# Symmetry group (when using SGD for initial model generation, C1 may work best)
symmetry = "C1"
#
### 3D-classification parameters
# Number of 3D classes to use
class3d_nr_classes = 4
# Have initial 3D model? If not, calculate one using SGD initial model generation
have_3d_reference = False
# Initial reference model
class3d_reference = ""
# Is reference on correct greyscale?
class3d_ref_is_correct_greyscale = False
# Has the initial reference been CTF-corrected?
class3d_ref_is_ctf_corrected = True
# Initial lowpass filter on reference
class3d_ini_lowpass = 40


### Use the largest 3D class from the first batch as a 3D reference for a second pass of autopicking? (only when do_class3d is True)
do_second_pass = True
# Only move on to template-based autopicking if the 3D references achieves this resolution (in A)
minimum_resolution_3dref_2ndpass = 20
# In the second pass, perform 2D classification?
do_class2d_pass2 = True
# In the second pass, perform 3D classification?
do_class3d_pass2 = False
# Batch size in the second pass
batch_size_pass2 = 20000


###################################################################################
############ Often the parameters below can be kept the same for a given set-up
###################################################################################

### Repeat settings for entire pipeline
# Repeat the pre-processing runs this many times (or until RUNNING_PIPELINER_default_PREPROCESS file is deleted)
preprocess_repeat_times = 999
# Wait at least this many minutes between each repeat cycle
preprocess_repeat_wait = 1
### Stop after CTF estimation? I.e., skip autopicking, extraction, 2D/3D classification, etc?
stop_after_ctf_estimation = False
# Check every this many minutes if enough particles have been extracted for a new batch of 2D-classification
batch_repeat_time = 1


### MotionCorrection parameters
# Use RELION's own implementation of motion-correction (CPU-only) instead of the UCSF implementation?
motioncor_do_own = False
# The number of threads (only for RELION's own implementation) is optimal when nr_movie_frames/nr_threads = integer
motioncor_threads = 6
# Exectutable of UCSF MotionCor2
motioncor_exe = "/dls_sw/apps/EM/MotionCor2/1.1.0/MotionCor2"
# On which GPU(s) to execute UCSF MotionCor2
motioncor_gpu = ""
# How many MPI processes to use for running motion correction?
motioncor_mpi = 5
# Local motion-estimation patches for MotionCor2
motioncor_patches_x = 5
motioncor_patches_y = 5
# B-factor in A^2 for downweighting of high-spatial frequencies
motioncor_bfactor = 150
# Use binning=2 for super-resolution K2 movies
motioncor_binning = 1
# Provide a defect file for your camera if you have one
motioncor_defectfile = ""
# orientation of the gain-reference w.r.t your movies (if input movies are not yet gain-corrected, e.g. TIFFs)
motioncor_gainflip = "No flipping (0)"
motioncor_gainrot = "No rotation (0)"
# Other arguments for MotionCor2
motioncor2_other_args = ""
# Other arguments for Motion Correction
motioncor_other_args = "--do_at_most 15"
# Submit motion correction job to the cluster?
motioncor_submit_to_queue = True


### CTF estimation parameters
# Amplitude contrast (Q0)
ampl_contrast = 0.1
# CTFFIND-defined parameters
ctffind_boxsize = 512
ctffind_astigmatism = 100
ctffind_maxres = 5
ctffind_minres = 30
ctffind_defocus_max = 50000
ctffind_defocus_min = 5000
ctffind_defocus_step = 500
# For Gctf: ignore parameters on the 'Searches' tab?
ctffind_do_ignore_search_params = True
# For Gctf: perform equi-phase averaging?
ctffind_do_EPA = True
# Also estimate phase shifts (for VPP data)
ctffind_do_phaseshift = False
# Executable to Kai Zhang's Gctf
gctf_exe = "/dls_sw/apps/EM/Gctf/1.18/Gctf"
# On which GPU(s) to execute Gctf
gctf_gpu = ""
# Use Alexis Rohou's CTFFIND4 (CPU-only) instead?
use_ctffind_instead = False
# Executable for Alexis Rohou's CTFFIND4
ctffind4_exe = "/dls_sw/apps/EM/ctffind/4.1.5-compat/ctffind"
# How many MPI processes to use for running CTF estimation?
ctffind_mpi = 4
# Submit CTF estimation job to the cluster?
ctffind_submit_to_queue = True


### Autopick parameters
# Use GPU-acceleration for autopicking?
autopick_do_gpu = True
# Which GPU(s) to use for autopicking
autopick_gpu = ""
# Low-pass filter for auto-picking the micrographs
autopick_lowpass = 20
# Shrink factor for faster picking (0 = fastest; 1 = slowest)
autopick_shrink_factor = 0
# How many MPI processes to use for running auto-picking?
autopick_mpi = 4
# Additional arguments for autopicking
autopick_other_args = ""
# Submit Autopick job to the cluster?
autopick_submit_to_queue = True
# Are the references CTF-corrected?
autopick_refs_are_ctf_corrected = True
# Do the references have inverted contrast wrt the micrographs?
autopick_refs_have_inverted_contrast = True
# Ignore CTFs until the first peak
autopick_refs_ignore_ctf1stpeak = False
# Diameter of mask for the references (in A; negative value for automated detection of mask diameter)
autopick_refs_mask_diam = -1
# In-plane angular sampling interval
autopick_inplane_sampling = 10
# Symmetry of the 3D reference for autopicking
autopick_3dref_symmetry = "C1"
# 3D angular sampling for generating projections of the 3D reference for autopicking (30 degrees is usually enough)
autopick_3dref_sampling = "30 degrees"
# Pixel size in the provided 2D/3D references (negative for same as in motion-corrected movies)
autopick_ref_angpix = -1

### Extract parameters
# Diameter for background normalisation (in pixels; negative value: default is 75% box size)
extract_bg_diameter = -1
# How many MPI processes to use for running particle extraction?
extract_mpi = 30
# Submit Extract job to the cluster?
extract_submit_to_queue = True


## Discard particles based on average/stddev values? (this may be important for SGD initial model generation)
do_discard_on_image_statistics = False
# Discard images that have average/stddev values that are more than this many sigma away from the ensemble average
discard_sigma = 4
# Submit discard job to the cluster?
discard_submit_to_queue = False


#### Common relion_refine paremeters used for 2D/3D classification and initial model generation
# Read all particles in one batch into memory?
refine_preread_images = False
# Or copy particles to scratch disk?
refine_scratch_disk = ""
# Number of pooled particles?
refine_nr_pool = 10
# Use GPU-acceleration?
refine_do_gpu = True
# Which GPU to use (different from GPU used for pre-processing?)
refine_gpu = ""
# How many MPI processes to use
refine_mpi = 5
# How many threads to use
refine_threads = 6
# Skip padding?
refine_skip_padding = False
# Submit jobs to the cluster?
refine_submit_to_queue = True
# Use fast subsets in 2D/3D classification when batch_size is bigger than this
refine_batchsize_for_fast_subsets = 100000


### 2D classification parameters
# Wait with the first 2D classification batch until at least this many particles are extracted
minimum_batch_size = 250
# Number of iterations to perform in 2D classification
class2d_nr_iter = 20
# Rotational search step (in degrees)
class2d_angle_step = 6
# Offset search range (in pixels)
class2d_offset_range = 5
# Offset search step (in pixels)
class2d_offset_step = 1
# Option to ignore the CTFs until their first peak (try this if all particles go into very few classes)
class2d_ctf_ign1stpeak = False
# Additional arguments to pass to relion-refine
class2d_other_args = ""


### 3D classification parameters
# Number of iterations to perform in 3D classification
class3d_nr_iter = 20
# Reference mask
class3d_reference_mask = ""
# Option to ignore the CTFs until their first peak (try this if all particles go into very few classes)
class3d_ctf_ign1stpeak = False
# Regularisation parameter (T)
class3d_T_value = 4
# Angular sampling step
class3d_angle_step = "7.5 degrees"
# Offset search range (in pixels)
class3d_offset_range = 5
# Offset search step (in pixels)
class3d_offset_step = 1
# Additional arguments to pass to relion-refine
class3d_other_args = ""


## SGD initial model generation
# Number of models to generate simulatenously (K>1 may be useful for getting rid of outliers in the particle images)
inimodel_nr_classes = 4
# Ignore CTFs until first peak?
inimodel_ctf_ign1stpeak = False
# Enforce non-negative solvent?
inimodel_solvent_flatten = True
# Initial angular sampling
inimodel_angle_step = "15 degrees"
# Initial search range (in pixels)
inimodel_offset_range = 6
# Initial offset search step (in pixels)
inimodel_offset_step = 2
# Number of initial iterations
inimodel_nr_iter_initial = 50
# Number of in-between iterations
inimodel_nr_iter_inbetween = 200
# Number of final iterations
inimodel_nr_iter_final = 50
# Frequency to write out information
inimodel_freq_writeout = 10
# Initial resolution (in A)
inimodel_resol_ini = 35
# Final resolution (in A)
inimodel_resol_final = 15
# Initial mini-batch size
inimodel_batchsize_ini = 100
# Final mini-batch size
inimodel_batchsize_final = 500
# Increased noise variance half-life (off, i.e. -1, by default; values of ~1000 have been observed to be useful in difficult cases)
inimodel_sigmafudge_halflife = -1
# Additional arguments to pass to relion_refine (skip annealing to get rid of outlier particles)
inimodel_other_args = " --sgd_skip_anneal "


### Cluster submission settings
# Name of the queue to which to submit the job
queue_name = "openmpi"
# Name of the command used to submit scripts to the queue
queue_submit_command = "qsub"
# The template for your standard queue job submission script
queue_submission_template = "/dls_sw/apps/EM/relion/qsub_template_hamilton"
# Minimum number of dedicated cores that need to be requested on each node
queue_minimum_dedicated = 1
