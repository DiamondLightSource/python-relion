from workflows.transport.stomp_transport import StompTransport
import relion.FindData as FD


default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-testing.cfg"
StompTransport.load_configuration_file(default_configuration)

stomp = StompTransport()
stomp.connect()
stomp.send(
    "ispyb_connector", {"content": "hello", "parameters": {"ispyb_command": "thing"}}
)

input = {
    1: {
        "name": "MotionCorr/job002",
        1: [
            "total_motion",
            "star",
            "corrected_micrographs.star",
            1,
            "_rlnAccumMotionTotal",
        ],
        2: [
            "early_motion",
            "star",
            "corrected_micrographs.star",
            1,
            "_rlnAccumMotionEarly",
        ],
        3: [
            "late_motion",
            "star",
            "corrected_micrographs.star",
            1,
            "_rlnAccumMotionLate",
        ],
    },
    2: {
        "name": "CtfFind/job003",
        1: ["astigmatism", "star", "micrographs_ctf.star", 1, "_rlnCtfAstigmatism"],
        2: ["defocusU", "star", "micrographs_ctf.star", 1, "_rlnDefocusU"],
        3: ["defocusV", "star", "micrographs_ctf.star", 1, "_rlnDefocusV"],
        4: ["astigmatism_angle", "star", "micrographs_ctf.star", 1, "_rlnDefocusAngle"],
        5: [
            "max_resolution",
            "star",
            "micrographs_ctf.star",
            1,
            "_rlnCtfMaxResolution",
        ],
        6: [
            "cc/fig_of_merit",
            "star",
            "micrographs_ctf.star",
            1,
            "_rlnCtfFigureOfMerit",
        ],
        7: ["cc/fig_doesnt_exist", "star", "micrographs_ctf.star", 1, "_rlnCtfFigure"],
    },
}

input_test_folder = "/dls/ebic/data/staff-scratch/ccpem/Relion31TutorialPrecalculatedResults"  # Path(dials_data("relion_tutorial_data"))
FDobject = FD.FindData(input_test_folder, input)
data = FDobject.get_data()

stomp.send(
    "ispyb_connector", {"content": data, "parameters": {"ispyb_command": "thing"}}
)
