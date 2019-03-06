# btrdb4
# Package stub for the btrdb4 database library.
#
# Author:   PingThings
# Created:  Fri Feb 22 11:48:05 2019 -0500
#
# For license information, see LICENSE.txt
# ID: __init__.py [] allen@pingthings.io $

"""
Package stub for the btrdb4 database library.
"""

##########################################################################
## Imports
##########################################################################

import warnings

##########################################################################
## Warning
##########################################################################

MESSAGE = ("The btrdb4 package is not available in this version. To install "
          "with pip use `pip install btrdb==4.*`")

# raise exception on ImportWarning
warnings.simplefilter('error', ImportWarning)

# warn user of version issue
warnings.warn(MESSAGE, ImportWarning)
