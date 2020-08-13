# not being used atm

import MotionCorrection

MC = MotionCorrection.MotionCorrection(
    "/dls/ebic/data/staff-scratch/ccpem/Relion31TutorialPrecalculatedResults/MotionCorr/relioncor2/corrected_micrographs.star"
)

MC.total_motion()
MC.av_motion_per_frame()
