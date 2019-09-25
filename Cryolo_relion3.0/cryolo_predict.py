#!/dls_sw/apps/python/anaconda/4.6.14/64/envs/cryolo/bin/python
# -*- coding: utf-8 -*-
import re
import sys

from cryolo.predict import _main_

if __name__ == "__main__":
    sys.argv[0] = re.sub(r"(-script\.pyw?|\.exe)?$", "", sys.argv[0])
    with open(".cry_predict_done", "w+"):
        pass
    sys.exit(_main_())
