# -*- coding: utf-8 -*-

"""
autores: Aron Ifanger
versão: 1.0
"""

import os

# Connection
from pyathenajdbc import connect
#from pyathena import connect
import fs_s3fs
import fastparquet
import botocore
import boto3

# Time
from datetime import datetime
from pytz import timezone

# Files
import csv
import pickle

from io import StringIO, BytesIO

# Data
import pandas as pd
import re
from urllib.parse import quote, unquote

# S3
DB_BUCKET = "aijus-databases"
S3_KEY = os.environ.get("S3_KEY")
S3_SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY")

# Local Stage from file download
LOCAL_STAGE = "data/external/"
S3_SEP = "/"
LOCAL_SEP = "_-_"

import logging

logger = logging.getLogger(__name__)


class FileManager(object):
    """
    Classe para gerenciar a movimentacao dos arquivos no S3
    """

    def __init__(self):
        self.s3 = boto3.resource("s3", aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET_ACCESS_KEY)

        self.s3_client = boto3.client("s3", aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET_ACCESS_KEY)

        self.conn = connect(access_key=S3_KEY, secret_key=S3_SECRET_ACCESS_KEY, region_name='us-east-1',
                            s3_staging_dir='s3://{bucket}/athena/aijus/query_stage/'.format(bucket=DB_BUCKET))

    def get_url(self, bucket, key):
        
        return 'https://s3.console.aws.amazon.com/s3/object/%s/%s?region=us-east-1&tab=overview' % (bucket, key)

    def list_s3_files(self, prefix, bucket=DB_BUCKET, full_path=False):
        """
        Descricao.

        Lista os arquivos de um diretório do S3.

        Retorno
        -------
        :return: list
            Lista com os endereços.
        """

        my_bucket = self.s3.Bucket(bucket)

        bucket_content = my_bucket.objects.filter(Prefix=prefix)

        path_list = []

        if full_path:
            for element in bucket_content:
                path_list.append(element.key)
            return path_list

        else:
            for element in bucket_content:
                path = [s for s in element.key.split('/') if s][(len(prefix.split('/')) - 1):]
                if path:
                    path_list.append(path)

            return path_list
        

    def read_txt_from_s3(self, key, bucket=DB_BUCKET, errors="ignore"):
        """
        Descricao.

        Lê o arquivo a partir do endereco.

        Retorno
        -------

        :param key: endereco do arquivo no S3
        :param bucket: bucket do S  3 onde o arquivo esta armazenado
        :return: str
            String com o texto da peticao
        """

        object = self.s3_client.get_object(Bucket=bucket, Key=key)

        return object['Body'].read().decode('utf-8', errors=errors)


    def read_s3(self, key, bucket=DB_BUCKET):
        """
        Lê o arquivo a partir do endereco.

        Retorno
        -------

        :param key: endereco do arquivo no S3
        :param bucket: bucket do S3 onde o arquivo esta armazenado
        :return: object
        """
        object = self.s3_client.get_object(Bucket=bucket, Key=key)
  
        return pickle.loads(object['Body'].read())

    def save_s3(self, obj, key, bucket=DB_BUCKET):
        """
        Lê o arquivo a partir do endereco.

        Retorno
        -------
        :param obj: python object
        :param key: endereco do arquivo no S3
        :param bucket: bucket do S3 onde o arquivo esta armazenado
        :return: object
        """
        s3_object = self.s3.Object(bucket, key)
        s3_object.put(Body=pickle.dumps(obj))
        

    def read_table_from_s3(self, key, sep="|", bucket=DB_BUCKET, header='infer', names=None):

        """
        Descricao.

        Lê uma tabela a partir do endereco.

        Retorno
        -------
        :return: DataFrame
            Uma tabela do pandas
        """

        text = self.read_txt_from_s3(key, bucket)
        file_csv = StringIO(text)
        df = pd.read_csv(file_csv, sep=sep, header=header, names=names) if names else pd.read_csv(file_csv, sep=sep, header=header) 
        return df
    

    def read_excel_from_s3(self, key, sheetname=0, bucket=DB_BUCKET):
        """
        Descricao.

        Lê uma tabela a partir do endereco.

        Retorno
        -------
        :return: DataFrame
            Uma tabela do pandas
        """
        
        obj = self.s3_client.get_object(Bucket=bucket, Key=key)
        bytes_stram = BytesIO(obj['Body'].read())
        df = pd.read_excel(bytes_stram, sheetname=sheetname)
        
        return df


    def read_sql(self, query):
        """
        Descricao.

        Lê uma tabela do athena

        Retorno
        -------
        :return: DataFrame
            Uma tabela do pandas
        """

        return pd.read_sql(query, self.conn)


    def save_to_s3(self, binary, key, bucket=DB_BUCKET):
        """
        Descricao.

        Salva o arquivo no endereco.

        Argumentos
        ----------
        :arg binary: bin
            arquivo no formato binario
        :arg key: strin
            endereco do arquivo no S3

        """
        s3_object = self.s3.Object(bucket, key)
        s3_object.put(Body=binary)


    def save_table_to_s3(self, df, directory, bucket=DB_BUCKET, index=False, header=False, sep=",", 
                         add_timestamp=True, file_name=None, quoted=False):
        """
        Descricao.

        Salva a tabela no endereco.

        Argumentos
        ----------
        :param bucket:
        :param index: boolean que indica se o indice da tabela deve ser salvo
        :param df: tabela do pandas
        :param directory: endereco do arquivo no S3
        """

        if directory[-1] == "/":
            directory = directory[:-1]

        timestamp = str(pd.Timestamp.now())

        if file_name is None:
            key = "{directory}/{timestamp}.csv".format(directory=directory, timestamp=timestamp)
        else:
            key = "{directory}/{name}.csv".format(directory=directory, name=file_name)
        
        if add_timestamp:
            df["timesatmp"] = timestamp
            
        quoting = csv.QUOTE_ALL if quoted else None

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=index, header=header, sep=sep, quoting=quoting)
        self.save_to_s3(csv_buffer.getvalue(), key, bucket)
        
        return key
    
    def save_excel_to_s3(self, df, key, bucket=DB_BUCKET):
        """
        Descricao.

        Salva uma tabela no endereco.

        Retorno
        -------
        :return: DataFrame
            Uma tabela do pandas
        """
        
        buffer = BytesIO()
        df.to_excel(buffer, engine='openpyxl', index=False)
        self.save_to_s3(buffer.getvalue(), key, bucket)
        
        return self.get_url(bucket, key)

    
    
    def save_parquet_table_to_s3(self, df, directory, bucket=DB_BUCKET, add_timestamp=True, file_name=None, append=True, partition_on=[]):
        """
        Descricao.

        Salva a tabela no endereco.

        Argumentos
        ----------
        :param bucket:
        :param index: boolean que indica se o indice da tabela deve ser salvo
        :param df: tabela do pandas
        :param directory: endereco do arquivo no S3
        """

        if directory[-1] == "/":
            directory = directory[:-1]

        if add_timestamp:
            df["timesatmp"] = str(pd.Timestamp.now())
        
        s3 = fs_s3fs.S3FS(bucket_name=bucket, aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET_ACCESS_KEY)
        
        fastparquet.write(directory, df, file_scheme='hive', open_with=s3.open, append=append, partition_on=partition_on)
        
        return directory


    def save_table(self, df, directory, index=False, header=False):
        """
        Descricao.

        Salva a tabela no endereco.

        Argumentos
        ----------
        :param bucket:
        :param index: boolean que indica se o indice da tabela deve ser salvo
        :param df: tabela do pandas
        :param directory: endereco do arquivo no S3
        """

        if directory[-1] == "/":
            directory = directory[:-1]

        timestamp = str(pd.Timestamp.now())

        directory = "{directory}/{timestamp}.csv".format(directory=directory, timestamp=timestamp)

        df.to_csv(directory, index=index, header=header)


    def download_s3(self, key, bucket=DB_BUCKET, local_prefix='', use_stage=True, rename=''):
        """
        Descricao.

        Baixa um arquivo para a area de stage com o nome igual ao key com as barras substituidas pelo
        separador definido no arquivo de configuracao (LOCAL_SEP)

        Argumentos
        ----------
        :param bucket:
        :param key: caminho para o arquivo no S3

        """

        try:
            prefix = LOCAL_STAGE + local_prefix if use_stage else local_prefix
            filename = prefix + rename if rename else prefix + key.replace(S3_SEP, LOCAL_SEP)
            file = self.s3.Bucket(bucket).download_file(Key=key, Filename=filename)
            return filename

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.error("The object does not exist.")
            else:
                raise


    def upload_s3(self, key, filename, bucket=DB_BUCKET):
        """
        Descricao.

        Faz o upload do arquivo salvo no endereco indicado pelo filename para o S3 no endereco indicado
        pela key.

        Argumentos
        ----------
        :param bucket:
        :param key: caminho para o arquivo no S3
        :param filename: endereco do arquivo local

        """
        try:
            self.s3_client.upload_file(filename, bucket, key)
        except Exception as e:
            logger.error("Upload error: %s"% str(e))

    def delete_file(self, filename):
        try:
            os.remove(filename)
        except:
            logger.error("Delete error.")


    def copy_s3_file(self, from_bucket, to_bucket, to_prefix, file_name):
        """
        Copiar arquivo de um bucket diferente do padrao.
        :param from_bucket:
        :param to_bucket:
        :param file_name: nome do arquivo
        :return: nada
        """
        copy_source = {
            'Bucket': from_bucket,
            'Key': file_name
        }
        self.s3.Object(to_bucket, to_prefix + file_name).copy(copy_source)


    def sync_file_list(self, file_list, from_bucket, to_bucket, to_prefix):

        total_pendent = len(file_list)
        for i, file_name in enumerate(file_list):
            logger.info("(%d / %d) Copying file '...%s' to aijus-databases bucket" % (i, total_pendent, file_name[-50:]))
            self.copy_s3_file(from_bucket, to_bucket, to_prefix, file_name)
            

    def get_file_relation_table(self, raw_bucket, txt_bucket, supplier):
        """
        Returns the table with relation between the raw file path and the text file path.

        :param raw_bucket: raw file bucket;
        :param raw_prefix: raw file prefix;
        :param txt_bucket: text file bucket;
        :param txt_prefix: text file prefix;
        :return: dataframe file relation
        """

        # Listing bucket files
        logger.info("Loading txt keys from table file_manager.storage_current_files_view")
        txt_list = self.read_sql("""SELECT key FROM file_manager.storage_current_files_view
            WHERE bucket = '{bucket}' AND supplier = '{supplier}'""".format(bucket=txt_bucket, supplier=supplier))["key"]
        
        logger.info("Loading raw keys from table file_manager.storage_current_files_view")
        raw_list = self.read_sql("""SELECT key FROM file_manager.storage_current_files_view
            WHERE bucket = '{bucket}' AND supplier = '{supplier}'""".format(bucket=raw_bucket, supplier=supplier))["key"]

        # Generate dict with file relations from file lists
        logger.info("Generatind keys dict")
        file_dict = dict([(get_file_name(f), {'txt_path':f}) for f in txt_list])
        keys = file_dict.keys()

        for f in raw_list:
            file_name = get_file_name(f)

            if file_name in keys:
                file_dict[file_name]['pdf_path'] = f
            else:
                file_dict[file_name] = {'pdf_path':f}

        # Generate dataframe from dict            
        col_name = ["raw_bucket", "raw_quoted_key", "txt_bucket", "txt_quoted_key"]

        file_relation_df = pd.DataFrame([("csn-bruno", value.get('pdf_path', ''), "csn-txt", value.get('txt_path', '')) 
                                         for key, value in file_dict.items()], columns=col_name)

        return file_relation_df


    def get_file_relation_delta_table(self, file_relation_df):
        """
        Filter processed files from the relational table.

        :param file_relation_df: table with files relationship;
        :return: filtered file relation
        """
        processed_files = self.read_sql("SELECT txt_quoted_path from file_manager.file_relation")["txt_quoted_path"]

        ft = file_relation_df.txt_quoted_key.map(lambda x: (x != '') & (x not in processed_files))
        file_relation_delta_df = file_relation_df[ft]

        return file_relation_delta_df

    
    def update_file_relation_table(self, raw_bucket, txt_bucket, supplier):
        """
        List all files, filter processed from list, save pendent files

        :param raw_bucket: raw file bucket;
        :param raw_prefix: raw file prefix;
        :param txt_bucket: text file bucket;
        :param txt_prefix: text file prefix.
        """

        create_table_dll = """
            CREATE EXTERNAL TABLE file_relation (
                `raw_bucket` string, 
                `raw_quoted_path` string, 
                `txt_bucket` string, 
                `txt_quoted_path` string, 
                `timestamp` timestamp)
            ROW FORMAT DELIMITED "
                FIELDS TERMINATED BY ',' 
            STORED AS INPUTFORMAT 
                'org.apache.hadoop.mapred.TextInputFormat' 
            OUTPUTFORMAT 
                'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION
                's3://aijus-databases/athena/file-manager-db/file_relation'
            TBLPROPERTIES (
                'has_encrypted_data'='false', 
                'transient_lastDdlTime'='1555347314')"
            """
        
        logger.info("Linsting all files - params: %s, %s, %s" %(raw_bucket, txt_bucket, supplier))
        file_relation_df = self.get_file_relation_table(raw_bucket, txt_bucket, supplier)
        logger.info("Filtering processed files - params: %s, %s, %s" %(raw_bucket, txt_bucket, supplier))
        file_relation_delta_df = self.get_file_relation_delta_table(file_relation_df)

        logger.info("Saving new table - params: %s, %s, %s" %(raw_bucket, txt_bucket, supplier))
        self.save_table_to_s3(df=file_relation_delta_df, directory="athena/file-manager-db/file_relation")
    
    
    def update_all_relation_table(self, bucket_file_list=None, append=False):

        default_bucket_file_list = [
            ("aijus-labor-pdfs", "aijus-labor-txts", "jusbrasil"),
            ("aijus-labor-pdfs", "aijus-labor-txts", "digesto"),
            ("csn-bruno", "csn-txt", "hd")
        ]

        bucket_file_list = bucket_file_list + default_bucket_file_list if append else default_bucket_file_list

        for raw_bucket, raw_prefix, txt_bucket, txt_prefix in bucket_file_list:
            self.update_file_relation_table(raw_bucket, txt_bucket, supplier)

    

    def get_object_info(self, bucket, key, company, suplier):
        
        """
        gathered together all the info of the object in S3

        :param bucket: S3 object
        :param key: file path form an object of S3
        :param company: company name
        :param supplier: supplier name
        :return: info dict
        """

        logger.debug("Getting info from object %s/%s"% (bucket, key) )
        s3_object = [e for e in self.s3.Bucket(bucket).objects.filter(Prefix=key)][0]

        uploadtime = upload_time(s3_object)
        file_size = s3_object.size
        bucket_full_file_path = "s3://{bucket}/{key}" 
        athena_upload_ts = datetime.today().strftime("%Y-%m-%d")

        return {
            "bucket_upload_ts": uploadtime,
            "file_size": file_size,
            "bucket_full_file_path": quote("s3://{}/{}".format(bucket,key)),
            "athena_upload_ts": athena_upload_ts,
            "company": company,
            "supplier": suplier
        }


    def get_pendent_object_info(self, bucket, prefix, company, supplier):
        """
        gathered together all the info of the object in S3

        :param bucket: S3 object
        :param prefix: root directory from S3-bucket
        :param company: company name
        :param supplier: supplier name
        :return: info-data-frame
        """

        logger.info("Listing all S3 files")
        file_list = self.list_s3_files(bucket=bucket, prefix=prefix, full_path=True)
        total_files = len(file_list)
        logger.info("%d files found."% total_files)
        
        delta_files = self.get_object_list_delta(pd.Series(file_list))
        total_files = len(delta_files)
        logger.info("%d pendent files."% total_files)
        
        logger.info("Getting info for all files")
        info_buck = []
        for i, file_name in enumerate(delta_files):
            info_buck.append(self.get_object_info(bucket, unquote(file_name), company, supplier))
            if i % 1000 == 0:
                logger.info("Getting info %08d / %08d"%(i, total_files))
            
            
        if len(info_buck) == 0:
            logger.info("Nenhum objeto pendente encontrado.")
            return None
            
        info_buck = pd.DataFrame(info_buck)
        coloumn_names = ["bucket_upload_ts", "file_size", "bucket_full_file_path", "athena_upload_ts", "company", "supplier"]

        return info_buck[coloumn_names]
    
    
    def get_object_list_delta(self, all_files):
        """
        gathered together all the info of the object in S3

        :param all_objects_df: info delta data frame
        :return: delta_files
        """
        
        logger.info("Listing files from view file_manager.storage_current_files_view")
        processed_files = self.read_sql("""SELECT key, bucket FROM file_manager.storage_current_files_view""")
        
        logger.info("Filtering files")
        all_files_df = pd.DataFrame({'key':all_files.map(quote)})
        
        joinned = processed_files.set_index("key").join(all_files_df.set_index("key"), how='right', lsuffix="_l")
        delta_files = joinned[joinned.bucket.isnull()].index.values

        return delta_files
    
    
    def update_current_files_table(self, bucket, prefix, company, supplier):
        """
        available_extensions = ['.doc', '.docx', '.pdf', '.png', '.jpg', '.jpeg']
        return any([ext in filename.lower() for ext in available_extensions])
        List all files, filter processed from list, save pendent files
        """
        
        create_table_dll = """
            CREATE EXTERNAL TABLE `storage_current_files`(
              `bucket_upload_ts` timestamp, 
              `file_size` double, 
              `bucket_full_file_path` string, 
              `athena_upload_dt` string, 
              `company` string, 
              `supplier` string)
            ROW FORMAT DELIMITED 
              FIELDS TERMINATED BY ',' 
            STORED AS INPUTFORMAT 
              'org.apache.hadoop.mapred.TextInputFormat' 
            OUTPUTFORMAT 
              'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION
              's3://aijus-databases/athena/file-manager-db/storage_current_files'
            TBLPROPERTIES (
              'has_encrypted_data'='false', 
              'transient_lastDdlTime'='1562180925')
            """
        
        logger.info("Listing all files - params: %s, %s, %s, %s" %(bucket, prefix, company, supplier))
        pendent_files_df = self.get_pendent_object_info(bucket, prefix, company, supplier)
        
        if pendent_files_df is not None:
            logger.info("Saving new table - params: %s, %s, %s, %s" %(bucket, prefix, company, supplier))
            self.save_table_to_s3(df=pendent_files_df, directory="athena/file-manager-db/storage_current_files_v1")          

            
    def update_all_current_files_table(self, bucket_file_list=None, append=False, folder=0):

        default_bucket_file_list = [
            ("aijus-csn", "files/txts/jusbrasil/", "csn", "jusbrasil"),
            ("aijus-csn", "files/originals/jusbrasil/", "csn", "jusbrasil"),
            ("aijus-csn", "files/txts/digesto/", "csn", "digesto"), 
            ("aijus-csn", "files/originals/digesto/", "csn", "digesto"),
            ("aijus-csn", "files/txts/CSN/", "csn", "hd"), 
            ("aijus-csn", "files/originals/CSN/", "csn", "hd")
        ]

        bucket_file_list = bucket_file_list + default_bucket_file_list if append else default_bucket_file_list

        
        (bucket, prefix, company, supplier) = bucket_file_list[folder]
        self.update_current_files_table(bucket, prefix, company, supplier)

            
def get_file_name(file_path):
    """
    Extracts the file name from file path
    
    :param file_path: full file path;
    :return: file name.
    """
    
    file_name = file_path.split("/")[-1]
    file_name = file_name[:file_name.rfind(".")]
    
    return file_name



def remove_ext(filename):
    """
    Funcao para remover a extensao do arquivo
    :param filename: string com o nome do arquivo
    :return: string com nome sem extensao
    """
    position = filename.rfind('.')
    return filename[:position]


def get_ext(filename):
    """
    Funcao para extrair apenas a extensao do arquivo
    :param filename: string com o nome do arquivo
    :return: string com a extensao do arquivo
    """
    position = filename.rfind('.')
    return filename[position:]


def check_ext(filename):
    """
    Funcao para verificar se o diretório eh valido
    :param filename: string com o nome do arquivo
    :return: boolean indicado se eh valido ou nao
    """
    available_extensions = ['.doc', '.docx', '.pdf', '.png', '.jpg', '.jpeg']
    return any([ext in filename.lower() for ext in available_extensions])


def get_ts(only_numbers=False):
    ts = str(pd.Timestamp.now())
    if only_numbers:
        return re.sub(" |-|:|\.", "", ts)
    else:
        return ts

    
def get_file_name(file_path):
    
    file_name = file_path.split("/")[-1]
    return file_name[:file_name.rfind(".")]
    
    
def upload_time(item):
    hora = item.last_modified
    inputTime = hora.astimezone(timezone('America/Sao_Paulo'))
    return inputTime.strftime("%Y-%m-%d %H:%M:%S.%f")
