import os
import spacy
import sys
import logging
from numba import NumbaDeprecationWarning
import pandas as pd
import warnings
warnings.simplefilter('ignore', category=FutureWarning)
warnings.simplefilter('ignore', category=NumbaDeprecationWarning)

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..'))
from src.core import utils
from src.core.predict import Predictor
from src.core.estimators.cnn import cnn

def test_clean_str():
    assert utils.clean_str("Ra/!") == "ra!"

def test_write_obj():
    lista = ['a','b']
    lista2 = utils.write_obj(lista,"lista")
    str_ = utils.get_path_to_project_dir(os.getcwd()) 
    project_root_path = utils.get_path_to_project_dir(os.getcwd())
    path = '/data/interim'
    full_path = project_root_path + '/' + path + '/' + "lista" + ".bin"
    assert os.path.isfile(full_path) == True


def test_read_obj():
    lista = ['a','b']
    utils.write_obj(lista,"lista")
    lista2 = utils.read_obj("lista")
    assert lista == lista2

def test_load_model():
    cnn.download_model_spacy()
    nlp = cnn.load_model(filename="model_cnn_",size='sm', path='/models', type_=".bin")
    nlp2 = spacy.load("pt_core_news_"+ 'sm')
    assert type(nlp) == type(nlp2)

def test_create_min_of_hearing_list():
    dict_ = {'file':'a','text':'b'}
    doc = utils.create_min_of_hearing_list()[0]
    assert doc.keys() == dict_.keys() 

def test_list_findall_match():
    str_ = 'testemunhas rodrigo didier e carlos almeida'
    matches = utils.list_findall_match(str_) 
    test = ['testemunhas rodrigo didier e carlos almeida']
    assert matches == test

def test_get_name():
    str_ = 'testemunhas rodrigo didier e carlos almeida'
    names = utils.get_name(str_) 
    test = {'rodrigo didier','carlos almeida'}
    
def test_get_names_list():
    str1 = 'testemunhas rodrigo didier e carlos almeida'
    str2 = 'testemunhas rodrigo silva e maria mendonça'
    list_str = [str1,str2]
    names = utils.get_names_list(list_str) 

    test = [{'rodrigo didier','carlos almeida'},{'rodrigo silva','maria mendonça'}]
    assert names == test

def test_get_testemunhas():
    doc = utils.create_min_of_hearing_list()[0:4]
    testemunhas_doc = utils.get_testemunhas(doc)
    dict_ = {'file':'a','lista_excertos_regex':'b','testemunhas_por_excerto':'c'}
    assert testemunhas_doc[0].keys() ==dict_.keys()

def test_df_from_dict(): 
    dic_ = {'A':['B','C','D']}
    df = utils.df_from_dic(dic_)
    assert type(df) == type(pd.DataFrame())

def test_get_df_testemunhas_by_doc(): 
    doc = utils.create_min_of_hearing_list()[0:4]
    testemunhas_doc = utils.get_testemunhas(doc)
    df_final = utils.get_df_testemunhas_by_doc(testemunhas_doc)
    assert type(df_final) == type(pd.DataFrame())

def test_df_union_testemunhas():
    doc = utils.create_min_of_hearing_list()[0:4]
    testemunhas_doc = utils.get_testemunhas(doc)
    df_final = utils.get_df_testemunhas_by_doc(testemunhas_doc)
    df = utils.df_union_testemunhas(df_final)
    assert type(df) == type(pd.DataFrame())

def test_load_model():
    pred = Predictor(sample_size=6)
    ml = pred.load_model()
    ml2 = utils.read_obj('model_cnn_sm', '/test/core', type_='.bin')
    assert type(ml) == type(ml2)

def test_predict():
    pred = Predictor(sample_size=6)
    df_wit = pred.predict()
    df_test2 = utils.read_obj('df_witnesses', '/test/core', type_='.bin')
    assert df_wit.columns == df_test2.columns

if __name__ =="__main__":
    print(utils.clean_str('ra/!'))
    pred = Predictor(size='sm', type_='.bin', sample_size=6)
    print(type(pred))
                        
