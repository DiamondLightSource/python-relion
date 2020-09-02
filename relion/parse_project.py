import sys
from optparse import SUPPRESS_HELP, OptionParser

# import workflows
from workflows.transport.stomp_transport import StompTransport
import relion
from pprint import pprint
from pathlib import Path


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

    # change settings when in live mode
    default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-testing.cfg"
    if "--live" in sys.argv:
        default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-live.cfg"

    StompTransport.load_configuration_file(default_configuration)
    StompTransport.add_command_line_options(parser)
    (options, args) = parser.parse_args(sys.argv[1:])
    stomp = StompTransport()
    stomp.connect()

    # create the project object based on the current directory
    current_directory = Path.cwd()
    project = relion.Project(current_directory)

    # find out what sort of messages we could send

    # ideally every message is created in a separate function

    # print them instead

    pprint(collect_ctffind(project))


def collect_ctffind(project):
    ctf_dictionary_list = []
    for job in project.ctffind.values():
        for item in job:
            ctf_dictionary_list.append(
                {
                    "ispyb_command": "insert_ctf",
                    "max_resolution": item.max_resolution,
                    "astigmatism": item.astigmatism,
                    "astigmatism_angle": item.defocus_angle,
                    "estimated_resolution": item.max_resolution,
                    "estimated_defocus": (float(item.defocus_u) + float(item.defocus_v))
                    / 2,
                    "cc_value": item.fig_of_merit,
                }
            )
    return ctf_dictionary_list


if __name__ == "__main__":
    run()
