import os
import pandas as pd
import sys
import pickle
from unidecode import unidecode
import re
import logging
from numba import NumbaDeprecationWarning
import warnings
warnings.simplefilter('ignore', category=FutureWarning)
warnings.simplefilter('ignore', category=NumbaDeprecationWarning)
from unidecode import unidecode
from tqdm import tqdm

os.path.join(os.getcwd(), '../..')
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'../..'))
from src.core.estimators.cnn import cnn


dict_re = {
    'testemunha':r'testem\w*.{0,90}\w*'
    }

def get_path_to_project_dir(os_getcwd):
    '''
    get the major project dirpath to model 
    '''
    req = r'\w*.{0,100}model-minutes-of-hearing-data-extraction'
    
    path = re.findall(req, os_getcwd)[0]    
    #path.replace('model-minutes-of-hearing-data-extraction')
    return path

def write_obj(obj_to_write, filename, path='/data/interim', type_ = '.bin'):
    project_root_path = get_path_to_project_dir(os.getcwd())
    full_path = project_root_path + '/' + path + '/' + filename + type_
    # open a file, where you ant to store the data
    file_obj = open(full_path, 'wb')
    pickle.dump(obj_to_write, file_obj)

def read_obj(filename,path='/data/interim', type_ ='.bin'):
    project_root_path = get_path_to_project_dir(os.getcwd())
    full_path = project_root_path + '/' + path + '/' + filename + type_
    if (os.path.isfile(full_path)):
        file_obj = open(full_path,'rb')           # dump information to that file
    else:
        print(f'file {full_path} dosent exist')
    return(pickle.load(file_obj))


def clean_str(str_):
    '''
    clean text.
    '''
    punc = '.;/"()'
    for j in punc:
        str_ = unidecode(str_.replace(j, '')).lower()
    return str_


def create_min_of_hearing_list(path_txt='/data/external/sample/ata_audiencia/txt', sample_size=None):

    '''
    read all files in txt format and return (dict) list of files.
    '''

    root_path = get_path_to_project_dir(os.getcwd()) 
    os.chdir(root_path + '/' + path_txt)
 
    list_ = []
    idx = 0
    if sample_size == None:
        sample_size = len(os.listdir())

    for file_name in os.listdir():

        if file_name[-4:] == '.txt' and idx < sample_size:
            with open(file_name,'r') as myfile:
                text = clean_str(myfile.read().replace('\n', ' '))
                list_.append({'file':file_name,'text':text})
        idx = idx + 1

    os.chdir(root_path)
    return list_


def list_findall_match(text, req=dict_re['testemunha']):

    """
    aplica o regex no texto proveniente de um campo do laudo
    """ 

    matches = re.findall(req, text)
    return matches


def get_name(str_):
#    nlp = spacy.load("pt_core_news_sm")
    nlp = cnn.load_model()
    doc = nlp(str_)
    set_names = set()
    for ent in doc.ents:
        if ent.label_ == 'PER':
            set_names.add(ent.text)
    return set_names


def get_names_list(list_str):
    return [get_name(x) for x in list_str]

def get_testemunhas(list_files):

    '''
    retorna a lista de atas com os  'excertos pegos no regex' e os nomes encontrados.
    '''

    for i in tqdm(range(len(list_files))):
        list_files[i]['lista_excertos_regex'] = list_findall_match(list_files[i]['text'],dict_re['testemunha'])
        list_files[i]['testemunhas_por_excerto'] =  get_names_list(list_files[i]['lista_excertos_regex'])
        del list_files[i]['text']
        write_obj(list_files, 'list_get_testemunha_output')
    return list_files


def df_from_dic(dic_):

    '''
    input dictionary returns a dataframe object.
    '''

    return pd.DataFrame().from_dict(dic_)

def union_serie_of_sets(serie):
    a = set()
    for x in list(serie):
        a = a.union(x)
    return a

def get_df_testemunhas_by_doc(list_files):
    '''
    input list of dicts with doc variables and returns a concatenated dataframe.
    '''
    df = pd.DataFrame()
    for i in tqdm(range(len(list_files))):
        if (i == 0):
            df = pd.concat([df_from_dic(list_files[i]),df_from_dic(list_files[i+1])])
        elif((i + 1) < len(list_files)):
            df = pd.concat([df, df_from_dic(list_files[i+1])])
        else:
            pass

    return df

def df_union_testemunhas(df):
    dict_agg = {'testemunhas_por_excerto':union_serie_of_sets}
    df_ = df.groupby(['file']).agg(dict_agg)
    df_.rename(columns={'file':'file', 'testemunhas_por_excerto':'witnesses'},inplace = True)
    return df_
