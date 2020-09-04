import sys
from optparse import SUPPRESS_HELP, OptionParser

from workflows.transport.stomp_transport import StompTransport
import relion
from pprint import pprint


def run():
    parser = OptionParser()

    parser.add_option("-?", action="help", help=SUPPRESS_HELP)

    parser.add_option(
        "--test",
        action="store_true",
        dest="test",
        help="Run in ActiveMQ testing namespace (zocdev, default)",
    )
    parser.add_option(
        "--live",
        action="store_true",
        dest="test",
        help="Run in ActiveMQ live namespace (zocalo)",
    )

    parser.add_option(
        "--d",
        "--dir",
        "--directory",
        action="store",
        dest="relion_directory",
        help="Path to directory containing Relion data. Defaults to current directory",
        default=".",
    )
    # change settings when in live mode
    default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-testing.cfg"
    if "--live" in sys.argv:
        default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-live.cfg"

    StompTransport.load_configuration_file(default_configuration)
    StompTransport.add_command_line_options(parser)
    (options, args) = parser.parse_args(sys.argv[1:])
    stomp = StompTransport()
    stomp.connect()

    project = relion.Project(options.relion_directory)
    result = collect_ctffind(project)
    for item in result:
        message = {"parameters": item, "content": "dummy_content"}
        pprint(message)
        stomp.send("ispyb_connector", message)


def collect_ctffind(project):
    ctf_dictionary_list = []
    for job in project.ctffind.values():
        for item in job:
            ctf_dictionary_list.append(
                {
                    "ispyb_command": "insert_ctf",
                    "astigmatism": item.astigmatism,
                    "astigmatism_angle": item.defocus_angle,
                    "max_estimated_resolution": item.max_resolution,
                    "estimated_defocus": (float(item.defocus_u) + float(item.defocus_v))
                    / 2,
                    "micrograph_name": item.micrograph_name,
                    "cc_value": item.fig_of_merit,
                }
            )
    return ctf_dictionary_list


def collect_motion_correction(project):
    motion_corr_dictionary_list = []
    for job in project.motioncorrection.values():
        for item in job:
            motion_corr_dictionary_list.append(
                {
                    "ispyb_command": "insert_motion_corr",
                    "micrograph_name": item.micrograph_name,
                    "total_motion": item.accum_motion_total,
                    "early_motion": item.accum_motion_early,
                    "late_motion": item.accum_motion_late,
                    "average_motion_per_frame": (
                        float(item.accum_motion_total)
                    ),  # / number of frames
                }
            )
    return motion_corr_dictionary_list


if __name__ == "__main__":
    run()
