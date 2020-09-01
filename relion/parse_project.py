import sys
from optparse import SUPPRESS_HELP, OptionParser

import workflows
from workflows.transport.stomp_transport import StompTransport


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
    project = ...

    # find out what sort of messages we could send

    # ideally every message is created in a separate function

    # print them instead

    pprint(something_motioncorr(project))


def something_motioncorr(project):
    ...

    return [ { ... }, ... ]


if __name__ == "__main__":
    run()
