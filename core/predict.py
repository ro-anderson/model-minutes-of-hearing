import joblib
import logging
import numpy as np
import os
import pandas as pd
import sys
import pickle
import spacy
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'../..'))
from src.core import utils
from src.core.estimators.cnn import cnn

logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
logger = logging.getLogger(__name__)

class Predictor:


    def __init__(self, size='sm', type_='.bin', sample_size=None):
        self.size = size
        self.type = type_
        self.sample_size = sample_size
        self.model_path = '/models'
        self.data_interim = 'data/interim'
        self.data_external_txt = 'data/external/sample/ata_audiencia/txt'


    def load_model(self):
        cnn.download_model_spacy(size=self.size)
        ml = cnn.load_model(size=self.size, path=self.model_path,type_=self.type)
        return ml
    
    def predict(self):
        # generating prediction object.
        self.ml = Predictor.load_model(self)
        

        # read files, create list of docs and save the obj.
        list_docs = utils.create_min_of_hearing_list(path_txt=self.data_external_txt, sample_size=self.sample_size)
        utils.write_obj(list_docs, 'list_docs', path=self.data_interim, type_=self.type)
        
        # get key testemunhas
        list_docs_testemunhas = utils.get_testemunhas(list_docs) 
        utils.write_obj(list_docs_testemunhas, 'list_docs_testemunhas', path=self.data_interim, type_=self.type)

        # get df_testemunhas by doc
        df = utils.get_df_testemunhas_by_doc(list_docs_testemunhas) 
        utils.write_obj(df, 'df_excerpts', path=self.data_interim, type_=self.type)
        
        # get df with union of testemunhas by doc
        df_ = utils.df_union_testemunhas(df)
        utils.write_obj(df_, 'df_witnesses', path=self.data_interim, type_=self.type)
        return df_


if __name__ =="__main__":
    pred = Predictor(size='sm', type_='.bin', sample_size=6)
    pred.predict()
    print(f'tipo do obj:{type(pred)}')
    
    # print df_excerpts
    df_ = utils.read_obj('df_excerpts', path='data/interim', type_='.bin')
    print(df_.shape)
    print(df_)

    # print df_witnesses
    df_wit = utils.read_obj('df_witnesses', path='data/interim', type_='.bin')
    print(df_wit.shape)
    print(df_wit)

