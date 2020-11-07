#import pickle
import os
import re
import sys
import subprocess
os.path.join(os.getcwd(), '../../../..')
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'../../../..'))
from src.core import utils
import spacy


# spacy download
def download_model_spacy(size='sm'):
    '''
    download from spacy portuguese repo.
    '''
    # download to model/spacy.bin
    cmd = 'python -m spacy download pt_core_news_' + size
    p = subprocess.Popen(cmd, stdin=None, stdout=None, shell=True).wait()
    if p == 0:
        print("\npt_core_news_"+ size + "successfully downloaded.\n")
    else:
        print("\nfailed to download pt_core_news_+size\n")

# load spacy
def load_model(filename='model_cnn_', size='sm', path='/models', type_= '.bin'):


    root_path = utils.get_path_to_project_dir(os.getcwd())
    full_path = root_path + path + '/' + filename + size + type_

    if (os.path.isfile(full_path)):
        nlp = utils.read_obj(filename + size, path='/models', type_=type_)
    else:
        nlp = spacy.load("pt_core_news_" + size)
        utils.write_obj(nlp, filename + size, path='/models',type_=type_)

    return nlp
