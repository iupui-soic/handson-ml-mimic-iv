"""
This modules reads postgre sql database for MIMIC-IV and returns table as pandas dataframe.

Author: Priyanshu Sinha (prisinha@iu.edu)
"""
import os
import psycopg2
import pandas as pd
import json

class MIMIC:
    def __init__(self, cred_file) -> None:
        self._credential_file = os.path.abspath(cred_file)
        self._db_name = None
        self._user = None
        self._password = None
        self._queries = None
        self._mimic_structure = {
            'mimic_core': ['admissions', 'patients', 'transfers'],
            'mimic_hosp': ['d_hcpcs', 'd_icd_diagnoses', 'd_icd_procedures', 'd_labitems', 'diagnoses_icd', 'drgcodes', 'emar',
                            'emar_detail', 'hcpcsevents', 'labevents', 'microbiologyevents', 'pharmacy', 'poe', 'poe_detail',
                            'prescriptions', 'procedures_icd', 'services'],
            'mimic_icu': ['chartevents', 'd_items', 'datetimeevents', 'icustays', 'inputevents', 'outputevents', 'procedureevents']
        }
        self.set_queries()
        self.read_and_set_credentials()

    @property
    def credential_file(self):
        return self._credential_file

    @credential_file.setter
    def credential_file(self, new_cred_file):
        self._credential_file = os.path.abspath(new_cred_file)

    def read_and_set_credentials(self):
        with open(self._credential_file, 'r') as cred_data:
            credentials = json.load(cred_data)

        self._db_name = credentials['dbname']
        self._user = credentials['user']
        self._password = credentials['password']

    def get_postgre_connection(self):
        conn_string = f"dbname='{self._db_name}' user='{self._user}' password='{self._password}'"
        try:
            connection = psycopg2.connect(conn_string)
            cursor = connection.cursor()
            return connection, cursor
        except Exception as ex:
            print(f"Exception Occured : {ex.message}")

    def get_query_res_as_pandas_df(self, query_str):
        """
        Right now doing it without chunksize. 

        TO-DO: Read data in chunksize and then work.
        """
        db_conn, db_cur = self.get_postgre_connection()
        try:
            table_df = pd.read_sql_query(query_str, db_conn)
            return table_df
        except Exception as ex:
            print(f"Exception Occured : {ex.message}")
            db_conn.rollback()
        db_conn.close()

    def make_select_queries(self, schema_name, relation_name):
        return f'SELECT * from {schema_name}.{relation_name}'

    def set_queries(self):
        self._queries = {
            'mimic_core_admissions': self.make_select_queries('mimic_core', self._mimic_structure['mimic_core'][0]),
            'mimic_core_patients': self.make_select_queries('mimic_core', self._mimic_structure['mimic_core'][1]),
            'mimic_core_transfers': self.make_select_queries('mimic_core', self._mimic_structure['mimic_core'][2]),
            'mimic_hosp_d_hcpcs': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][0]),
            'mimic_hosp_d_icd_diagnoses': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][1]),
            'mimic_hosp_d_icd_procedures': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][2]),
            'mimic_hosp_d_labitems': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][3]),
            'mimic_hosp_diagnoses_icd': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][4]),
            'mimic_hosp_drgcodes': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][5]),
            'mimic_hosp_emar': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][6]),
            'mimic_hosp_emar_detail': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][7]),
            'mimic_hosp_hcpcsevents': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][8]),
            'mimic_hosp_labevents': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][9]),
            'mimic_hosp_microbiologyevents': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][10]),
            'mimic_hosp_pharmacy': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][11]),
            'mimic_hosp_poe': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][12]),
            'mimic_hosp_poe_detail': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][13]),
            'mimic_hosp_prescriptions': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][14]),
            'mimic_hosp_procedures_icd': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][15]),
            'mimic_hosp_services': self.make_select_queries('mimic_hosp', self._mimic_structure['mimic_hosp'][16]),
            'mimic_icu_chartevents': self.make_select_queries('mimic_icu', self._mimic_structure['mimic_icu'][0]),
            'mimic_icu_d_items': self.make_select_queries('mimic_icu', self._mimic_structure['mimic_icu'][1]),
            'mimic_icu_datetimeevents': self.make_select_queries('mimic_icu', self._mimic_structure['mimic_icu'][2]),
            'mimic_icu_icustays': self.make_select_queries('mimic_icu', self._mimic_structure['mimic_icu'][3]),
            'mimic_icu_inputevents': self.make_select_queries('mimic_icu', self._mimic_structure['mimic_icu'][4]),
            'mimic_icu_outputevents': self.make_select_queries('mimic_icu', self._mimic_structure['mimic_icu'][5]),
            'mimic_icu_procedureevents': self.make_select_queries('mimic_icu', self._mimic_structure['mimic_icu'][6]),
        }

    def get_mimic_core_addissions(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_core_admissions'])
    
    def get_mimic_core_patients(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_core_patients'])

    def get_mimic_core_transfers(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_core_transfers'])

    def get_mimic_hosp_d_hcps(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_d_hcpcs'])

    def get_mimic_hosp_d_icd_diagnoses(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_d_icd_diagnoses'])

    def get_mimic_hosp_d_icd_procedures(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_d_icd_procedures'])

    def get_mimic_hosp_d_labitems(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_d_labitems'])

    def get_mimic_hosp_diagnoses_icd(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_diagnoses_icd'])

    def get_mimic_hosp_drgcodes(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_drgcodes'])

    def get_mimic_hosp_emar(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_emar'])

    def get_mimic_hosp_emar_detail(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_emar_detail'])

    def get_mimic_hosp_hcpcsevents(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_hcpcsevents'])

    def get_mimic_hosp_labevents(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_labevents'])

    def get_mimic_hosp_microbiologyevents(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_microbiologyevents'])

    def get_mimic_hosp_pharmacy(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_pharmacy'])

    def get_mimic_hosp_poe(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_poe'])

    def get_mimic_hosp_poe_detail(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_poe_detail'])

    def get_mimic_hosp_prescriptions(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_prescriptions'])

    def get_mimic_hosp_procedures_icd(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_procedures_icd'])

    def get_mimic_hosp_services(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_hosp_services'])

    def get_mimic_icu_chartevents(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_icu_chartevents'])

    def get_mimic_icu_d_items(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_icu_d_items'])

    def get_mimic_icu_datetimeevents(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_icu_datetimeevents'])

    def get_mimic_icu_icustays(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_icu_icustays'])

    def get_mimic_icu_inputevents(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_icu_inputevents'])

    def get_mimic_icu_outputevents(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_icu_outputevents'])

    def get_mimic_icu_procedureevents(self):
        return self.get_query_res_as_pandas_df(self._queries['mimic_icu_procedureevents'])

    def get_mimic_tables(self):
        query_str = "select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';"
        return self.get_query_res_as_pandas_df(query_str)

class MIMICManipulations:
    def __init__(self, credential_file):
        self._mimic_instance = MIMIC(credential_file)

    def get_df(self):
        """
        This method returns final dataframe after merging following tables:
        - mimic_core_patients
        - mimic_core_admissions
        - mimic_core_transfers
        - mimic_hosp_d_icd_procedures
        - mimic_hosp_procedres_icd
        - mimic_hosp_drgcodes
        - mimic_hosp_microbiologyevents
        - mimic_icu_procedurevents
        - mimic_icu_icustays
        """
        df_admission = self._mimic_instance.get_mimic_core_addissions()
        df_patients = self._mimic_instance.get_mimic_core_patients()
        df_transfers = self._mimic_instance.get_mimic_core_transfers()
        pass


    

    
        