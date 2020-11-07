import os
import sys
import logging
from numba import NumbaDeprecationWarning
import warnings

from numba import NumbaDeprecationWarning
warnings.simplefilter('ignore', category=FutureWarning)
warnings.simplefilter('ignore', category=NumbaDeprecationWarning)

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from core import data

logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logger.info("downloading 'ata audiencia'")
    data.download_file(label='ata_audiencia')
    logger.info("downloading 'laudo pericial'")
    data.download_file(label='laudo_pericial')
