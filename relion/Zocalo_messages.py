from workflows.transport.stomp_transport import StompTransport
import relion.FindData as FD
import relion.input_dict

default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-testing.cfg"
StompTransport.load_configuration_file(default_configuration)

stomp = StompTransport()
stomp.connect()
stomp.send(
    "ispyb_connector", {"content": "hello", "parameters": {"ispyb_command": "thing"}}
)

input_test_folder = "/dls/ebic/data/staff-scratch/ccpem/Relion31TutorialPrecalculatedResults"  # Path(dials_data("relion_tutorial_data"))
FDobject = FD.FindData(input_test_folder, relion.input_dict.input_star)
data = FDobject.get_data()

stomp.send(
    "ispyb_connector", {"content": data, "parameters": {"ispyb_command": "thing"}}
)
