# btrdb.version
# Maintains version and package information for deployment.
#
# Author:   PingThings
# Created:  Tue Dec 18 14:50:05 2018 -0500
#
# For license information, see LICENSE.txt
# ID: version.py [] allen@pingthings.io $

"""
Maintains version and package information for deployment.
"""

##########################################################################
## Module Info
##########################################################################

__version_info__ = {
    'major': 5,
    'minor': 10,
    'micro': 1,
    'releaselevel': 'final',
    'serial': 15,
}

##########################################################################
## Helper Functions
##########################################################################

def get_version(short=False):
    """
    Prints the version.
    """
    assert __version_info__['releaselevel'] in ('alpha', 'beta', 'final')
    vers = ["%(major)i.%(minor)i" % __version_info__, ]
    if __version_info__['micro']:
        vers.append(".%(micro)i" % __version_info__)
    if __version_info__['releaselevel'] != 'final' and not short:
        vers.append('%s%i' % (__version_info__['releaselevel'][0],
                              __version_info__['serial']))
    return ''.join(vers)
