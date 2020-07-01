from gemmi import cif


class MotionCorrection:
    def __init__(self, file_path):
        print("\nINIT")
        self.doc = cif.read_file(file_path)
        self.motion_vals = None
        self.number_of_blocks = len(self.doc)
        print("number of blocks = {}".format(self.number_of_blocks))
        print("first block = {}".format(self.doc[0]))
        print("second block = {}".format(self.doc[1]))

    def total_motion(self):
        print("\nTOTAL MOTION")
        micrographs_block = self.doc[1]
        self.motion_vals = micrographs_block.find_loop("_rlnAccumMotionTotal")
        for value in self.motion_vals:
            print(value)

        # testing if the loop columns can be indexed
        print("first value = ", self.motion_vals[0])
        return self.motion_vals[0]

    # Currently unsure how this is supposed to be calculated - maybe (Early - Late)/2 ?
    # Or all of the frames' motions averaged?
    # Is it 'the average motion of a given frame' or 'the average of all the frames motions'?

    def av_motion_per_frame(self):
        print("\nAVERAGE MOTION")
        sum_total_motion = 0
        for value in self.motion_vals:
            sum_total_motion += float(value)
        print("total motion sum = ", sum_total_motion)
        print("number of frames = ", len(self.motion_vals))
        average = sum_total_motion / len(self.motion_vals)
        print(
            "average motion per frame (sum of all total motions divided by number of frames) = ",
            average,
        )
