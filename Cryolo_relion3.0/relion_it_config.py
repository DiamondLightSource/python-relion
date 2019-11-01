"""
Options needed for making cryolo_relion_it facility specific
"""

# Location of the cryolo_relion_it files
cryolo_relion_directory = "/dls_sw/apps/EM/relion_cryolo/CryoloRelion-master/"

# Location of the cryolo specific files
cryolo_config = "/dls_sw/apps/EM/crYOLO/cryo_phosaurus/config.json"
cryolo_gmodel = "/dls_sw/apps/EM/crYOLO/cryo_phosaurus/gmodel_phosnet_20190516.h5"

# Cluster options for cryolo
use_cluster = True
qsub_file = "/dls_sw/apps/EM/relion_cryolo/CryoloRelion-master/qsub.sh"
qtemplate_file = "/home/yig62234/Documents/pythonEM/Cryolo_relion3.0/qtemplate.sh"
