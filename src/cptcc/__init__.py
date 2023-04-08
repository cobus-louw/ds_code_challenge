# read version from installed package
from cptcc.cptcc import *
import cptcc.distance as distance
import cptcc.utils as utils
import cptcc.wind as wind

from importlib.metadata import version
__version__ = version("cptcc")
