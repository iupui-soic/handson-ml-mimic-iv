"""
This modules reads postgre sql database for MIMIC-IV and returns table as pandas dataframe.

Author: Priyanshu Sinha (prisinha@iu.edu)
"""
import os
from matplotlib.pyplot import axis
from numpy import dtype
import pandas as pd
import dask.dataframe as dd
from datetime import datetime
import glob2
import sys

sys.path.append('..')

from utils.util import generic_utils

class MIMIC:
    def __init__(self, dir_path) -> None:
        self._dir_path = os.path.abspath(dir_path)
        self._mimic_structure = {
            'mimic_core': ['admissions', 'patients', 'transfers'],
            'mimic_hosp': ['d_hcpcs', 'd_icd_diagnoses', 'd_icd_procedures', 'd_labitems', 'diagnoses_icd', 'drgcodes', 'emar',
                            'emar_detail', 'hcpcsevents', 'labevents', 'microbiologyevents', 'pharmacy', 'poe', 'poe_detail',
                            'prescriptions', 'procedures_icd', 'services'],
            'mimic_icu': ['chartevents', 'd_items', 'datetimeevents', 'icustays', 'inputevents', 'outputevents', 'procedureevents']
        }

        try:
            assert os.path.exists(self._dir_path) #Technically I should match all the file names and their size. Will do it later.
            print(f"MIMIC dataset found at path : {self._dir_path}")
        except AssertionError:
            print(f"Dataset directory is empty. Exiting system.")
            sys.exit(0)

    def read_data(self, file_path, dtype):
        if os.path.exists(file_path):
            df = dd.read_csv(file_path, dtype = dtype)
            return df
        else:
            print(f"Given file path {file_path} don't exist.")
            
    def join_path(self, file_path):
        return os.path.join(self._dir_path, file_path)

    def read_core_admissions_csv(self):
        df_admission = self.read_data(self.join_path('core/admissions.csv'), dtype={'admission_location': 'object',
                                                                                        'deathtime': 'object',
                                                                                        'edouttime': 'object',
                                                                                        'edregtime': 'object'})

        df_admission['hadm_id'] = df_admission['hadm_id'].astype('float64')
        df_admission['subject_id'] = df_admission['subject_id'].astype('float64')
        return df_admission

    def read_core_patients_csv(self):
        df_patients = self.read_data(self.join_path('core/patients.csv'), dtype={'dod': 'object'})
        df_patients['subject_id'] = df_patients['subject_id'].astype('float64')
        return df_patients

    def read_core_transfers_csv(self):
        df_transfers = self.read_data(self.join_path('core/transfers.csv'), dtype={'careunit': 'object',
                                                                                    'hadm_id': 'float64'})
        df_transfers['hadm_id'] = df_transfers['hadm_id'].astype('float64')
        df_transfers['subject_id'] = df_transfers['subject_id'].astype('float64')
        return df_transfers

    def read_hosp_d_hcpcs_csv(self):
        df_d_hcpcs = self.read_data(self.join_path('hosp/d_hcpcs.csv'), dtype=None)
        return df_d_hcpcs

    def read_hosp_d_icd_diagnoses_csv(self):
        df_d_icd_diagnoses = self.read_data(self.join_path('hosp/d_icd_diagnoses.csv'), dtype={'icd_code': 'object'})
        return df_d_icd_diagnoses

    def read_hosp_d_icd_procedures_csv(self):
        df_d_icd_procedures = self.read_data(self.join_path('hosp/d_icd_procedures.csv'), dtype={'icd_code': 'object'})
        return df_d_icd_procedures

    def read_hosp_d_labitems_csv(self):
        df_d_labitems = self.read_data(self.join_path('hosp/d_labitems.csv'), dtype={'loinc_code': 'object'})
        return df_d_labitems

    def read_hosp_diagnoses_icd_csv(self):
        df_diagnoses_icd = self.read_data(self.join_path('hosp/diagnoses_icd.csv'), dtype=None)
        df_diagnoses_icd['subject_id'] = df_diagnoses_icd['subject_id'].astype('float64')
        df_diagnoses_icd['hadm_id'] = df_diagnoses_icd['hadm_id'].astype('float64')
        return df_diagnoses_icd

    def read_hosp_drgcodes_csv(self):
        df_drgcodes = self.read_data(self.join_path('hosp/drgcodes.csv'), dtype=None)
        df_drgcodes['subject_id'] = df_drgcodes['subject_id'].astype('float64')
        df_drgcodes['hadm_id'] = df_drgcodes['hadm_id'].astype('float64')
        return df_drgcodes

    def read_hosp_emar_csv(self):
        df_emar = self.read_data(self.join_path('hosp/emar.csv'), dtype=None)
        df_emar['subject_id'] = df_emar['subject_id'].astype('float64')
        df_emar['hadm_id'] = df_emar['hadm_id'].astype('float64')
        return df_emar

    def read_hosp_emar_detail_csv(self):
        df_emar_detail = self.read_data(self.join_path('hosp/emar_detail.csv'), dtype={'completion_interval': 'object',
                                                                                        'continued_infusion_in_other_location': 'object',
                                                                                        'dose_due': 'object',
                                                                                        'dose_given': 'object',
                                                                                        'infusion_complete': 'object',
                                                                                        'infusion_rate_adjustment': 'object',
                                                                                        'infusion_rate_unit': 'object',
                                                                                        'new_iv_bag_hung': 'object',
                                                                                        'non_formulary_visual_verification': 'object',
                                                                                        'product_amount_given': 'object',
                                                                                        'product_description_other': 'object',
                                                                                        'reason_for_no_barcode': 'object',
                                                                                        'restart_interval': 'object',
                                                                                        'route': 'object',
                                                                                        'side': 'object',
                                                                                        'site': 'object'})

        df_emar_detail['subject_id'] = df_emar_detail['subject_id'].astype('float64')

        return df_emar_detail

    def read_hosp_hcpcsevents_csv(self):
        df_hcpcsevents = self.read_data(self.join_path('hosp/hcpcsevents.csv'), dtype={'hcpcs_cd': 'object'})
        df_hcpcsevents['subject_id'] = df_hcpcsevents['subject_id'].astype('float64')
        df_hcpcsevents['hadm_id'] = df_hcpcsevents['hadm_id'].astype('float64')
        return df_hcpcsevents

    def read_hosp_labevents_csv(self):
        df_labevents = self.read_data(self.join_path('hosp/labevents.csv'), dtype={'comments': 'object',
                                                                                    'flag': 'object',
                                                                                    'value': 'object'})
        df_labevents['subject_id'] = df_labevents['subject_id'].astype('float64')
        df_labevents['hadm_id'] = df_labevents['hadm_id'].astype('float64')
        return df_labevents

    def read_hosp_microbiologyevents_csv(self):
        df_microbiologyevents = self.read_data(self.join_path('hosp/microbiologyevents.csv'), dtype={'comments': 'object',
                                                                                                    'isolate_num': 'float64',
                                                                                                    'org_itemid': 'float64',
                                                                                                    'quantity': 'object'})
        df_microbiologyevents['subject_id'] = df_microbiologyevents['subject_id'].astype('float64')
        df_microbiologyevents['hadm_id'] = df_microbiologyevents['hadm_id'].astype('float64')
        return df_microbiologyevents

    def read_hosp_pharmacy_csv(self):
        df_pharmacy = self.read_data(self.join_path('hosp/pharmacy.csv'), dtype={'expirationdate': 'object'})
        df_pharmacy['subject_id'] = df_pharmacy['subject_id'].astype('float64')
        df_pharmacy['hadm_id'] = df_pharmacy['hadm_id'].astype('float64')
        return df_pharmacy

    def read_hosp_poe_csv(self):
        df_poe = self.read_data(self.join_path('hosp/poe.csv'), dtype={'discontinue_of_poe_id': 'object',
                                                                        'discontinued_by_poe_id': 'object',
                                                                        'order_status': 'object'})

        df_poe['subject_id'] = df_poe['subject_id'].astype('float64')
        df_poe['hadm_id'] = df_poe['hadm_id'].astype('float64')
        return df_poe

    def read_hosp_poe_detail_csv(self):
        df_poe_detail = self.read_data(self.join_path('hosp/poe_detail.csv'), dtype=None)
        df_poe_detail['subject_id'] = df_poe_detail['subject_id'].astype('float64')
        return df_poe_detail

    def read_hosp_prescriptions_csv(self):
        df_prescriptions = self.read_data(self.join_path('hosp/prescriptions.csv'), dtype={'form_rx': 'object',
                                                                                            'gsn': 'object',
                                                                                            'ndc': 'float64'})
        df_prescriptions['subject_id'] = df_prescriptions['subject_id'].astype('float64')
        df_prescriptions['hadm_id'] = df_prescriptions['hadm_id'].astype('float64')
        return df_prescriptions

    def read_hosp_procedures_icd_csv(self):
        df_procedures_icd = self.read_data(self.join_path('hosp/procedures_icd.csv'), dtype={'icd_code': 'object'})
        df_procedures_icd['subject_id'] = df_procedures_icd['subject_id'].astype('float64')
        df_procedures_icd['hadm_id'] = df_procedures_icd['hadm_id'].astype('float64')
        return df_procedures_icd

    def read_hosp_services_csv(self):
        df_services = self.read_data(self.join_path('hosp/services.csv'), dtype={'prev_service': 'object'})
        df_services['subject_id'] = df_services['subject_id'].astype('float64')
        df_services['hadm_id'] = df_services['hadm_id'].astype('float64')
        return df_services
        
    def read_icu_chartevents_csv(self):
        df_chartevents = self.read_data(self.join_path('icu/chartevents.csv'), dtype={'value': 'object',
                                                                                    'valuenum': 'float64',
                                                                                    'valueuom': 'object'})
        df_chartevents['subject_id'] = df_chartevents['subject_id'].astype('float64')
        df_chartevents['hadm_id'] = df_chartevents['hadm_id'].astype('float64')                                                            
        return df_chartevents

    def read_icu_d_items_csv(self):
        df_d_items = self.read_data(self.join_path('icu/d_items.csv'), dtype=None)
        return df_d_items

    def read_icu_datetimeevents_csv(self):
        df_datetimeevents = self.read_data(self.join_path('icu/datetimeevents.csv'), dtype=None)
        df_datetimeevents['subject_id'] = df_datetimeevents['subject_id'].astype('float64')
        df_datetimeevents['hadm_id'] = df_datetimeevents['hadm_id'].astype('float64')
        return df_datetimeevents

    def read_icu_icustays_csv(self):
        df_icustays = self.read_data(self.join_path('icu/icustays.csv'), dtype=None)
        df_icustays['subject_id'] = df_icustays['subject_id'].astype('float64')
        df_icustays['hadm_id'] = df_icustays['hadm_id'].astype('float64')
        return df_icustays

    def read_icu_inputevents_csv(self):
        df_inputevents = self.read_data(self.join_path('icu/inputevents.csv'), dtype={'originalamount': 'float64',
                                                                                    'totalamount': 'float64'})
        df_inputevents['subject_id'] = df_inputevents['subject_id'].astype('float64')
        df_inputevents['hadm_id'] = df_inputevents['hadm_id'].astype('float64')
        return df_inputevents

    def read_icu_outputevents_csv(self):
        df_outputevents = self.read_data(self.join_path('icu/outputevents.csv'), dtype={'value': 'float64'})
        df_outputevents['subject_id'] = df_outputevents['subject_id'].astype('float64')
        df_outputevents['hadm_id'] = df_outputevents['hadm_id'].astype('float64')
        return df_outputevents

    def read_icu_procedureevents_csv(self):
        df_procedureevents = self.read_data(self.join_path('icu/procedureevents.csv'), dtype={'originalamount': 'float64',
                                                                                                'value': 'float64'})

        df_procedureevents['subject_id'] = df_procedureevents['subject_id'].astype('float64')
        df_procedureevents['hadm_id'] = df_procedureevents['hadm_id'].astype('float64')
        return df_procedureevents

class MIMICManipulations:
    def __init__(self, dir_path):
        self._mimic_instance = MIMIC(dir_path)
        self._util_instance = generic_utils()

    def filter_core_admission(self, df):
        df_admission = df.drop(['hadm_id', 'edregtime', 'edouttime', 'deathtime'], axis=1)
        df_admission['admittime'] = dd.to_datetime(df_admission['admittime'])
        df_admission['dischtime'] = dd.to_datetime(df_admission['dischtime'])
        df_admission['los_admission'] = (df_admission['dischtime'] - df_admission['admittime']).dt.total_seconds()/86400
        df_admission = df_admission.drop(['admittime', 'dischtime'], axis=1)
        df_admission = df_admission[df_admission['los_admission'] > 0]
        df_admission = self._util_instance.remove_duplicates_and_re_index(df_admission, 'subject_id')
        return df_admission

    def filter_core_patients(self, df):
        df_patient = df.drop(['anchor_year', 'anchor_year_group', 'dod'], axis=1)
        df_patient = self._util_instance.remove_duplicates_and_re_index(df_patient, 'subject_id')
        return df_patient

    def filter_core_transfers(self, df):
        df_transfer = df.drop(['hadm_id', 'transfer_id', 'intime', 'outtime'], axis=1)
        df_transfer = self._util_instance.remove_duplicates_and_re_index(df_transfer, 'subject_id')
        return df_transfer

    def merge_core_tables(self):
        df_admission = self.filter_core_admission(self._mimic_instance.read_core_admissions_csv())
        df_patient = self.filter_core_patients(self._mimic_instance.read_core_patients_csv())
        df_transfers = self.filter_core_transfers(self._mimic_instance.read_core_transfers_csv())
        df_core_merge = df_admission.merge(df_patient, how='left', on=['subject_id'])\
                                    .merge(df_transfers, how='outer', on=['subject_id', 'hadm_id'])
        return df_core_merge

    def filter_hosp_diagnoses_icd(self, df):
        df_diagnoses_icd = df.drop(['hadm_id', 'seq_num', 'icd_version'], axis=1)
        df_diagnoses_icd = df_diagnoses_icd.rename(columns = {'icd_code':'diagnosis_icd_code'})
        df_diagnoses_icd = self._util_instance.remove_duplicates_and_re_index(df_diagnoses_icd, 'subject_id')
        return df_diagnoses_icd

    def filter_hosp_procedures_icd(self, df):
        df_procedures_icd = df.drop(['hadm_id', 'seq_num', 'chartdate', 'icd_version'], axis = 1)
        df_procedures_icd = df_procedures_icd.rename(columns = {'icd_code':'procedures_icd_code'})
        df_procedures_icd = self._util_instance.remove_duplicates_and_re_index(df_procedures_icd, 'subject_id')
        return df_procedures_icd

    def filter_hosp_lab_events(self, df):
        df_lab_events = df.drop(['labevent_id', 'specimen_id', 'itemid', 'charttime', 'storetime', 'valueuom', 
                                    'ref_range_lower', 'ref_range_upper', 'comments', 'valuenum', 'hadm_id'], axis=1)
        df_lab_events = self._util_instance.remove_duplicates_and_re_index(df_lab_events, 'subject_id')                            
        return df_lab_events

    def merge_hosp_lab_events_d_labitems(self):
        df_lab_events = self.filter_hosp_lab_events(self._mimic_instance.read_hosp_labevents_csv())
        df_d_lab_items = self._mimic_instance.read_hosp_d_labitems_csv()
        df_lab_events_merge = df_lab_events.merge(df_d_lab_items, how='left', on=['itemid'])
        return df_lab_events_merge

    def filter_hosp_drgcodes(self, df):
        df_drgcodes = df.drop(['hadm_id', 'description', 'drg_severity', 'drg_mortality'], axis=1)
        df_drgcodes = self._util_instance.remove_duplicates_and_re_index(df_drgcodes, 'subject_id')
        return df_drgcodes

    def filter_hosp_emar(self, df):
        df_emar = df[['subject_id', 'medication', 'event_txt']]
        df_emar = self._util_instance.remove_duplicates_and_re_index(df_emar, 'subject_id')
        return df_emar

    def filter_hosp_poe(self, df):
        df_poe = df[['subject_id', 'order_type', 'transaction_type']]
        df_poe = self._util_instance.remove_duplicates_and_re_index(df_poe, 'subject_id')
        return df_poe

    def merge_hosp_emar_poe(self):
        df_emar = self.filter_hosp_emar(self._mimic_instance.read_hosp_emar_csv())
        df_poe = self.filter_hosp_poe(self._mimic_instance.read_hosp_poe_csv())
        df_merge_emar_poe = df_emar.merge(df_poe, how='outer', on=['poe_id', 'subject_id', 'hadm_id'])
        df_merge_emar_poe.drop(['emar_id', 'poe_id'], axis=1)
        return df_merge_emar_poe

    def filter_hosp_microbiologyevents(self, df):
        df_microbiologyevents = df[['subject_id', 'org_name', 'test_name', 'quantity', 'ab_name']]
        df_microbiologyevents = self._util_instance.remove_duplicates_and_re_index(df_microbiologyevents, 'subject_id')
        return df_microbiologyevents

    def filter_hosp_prescriptions(self, df):
        df_prescriptions = df[['subject_id', 'drug', 'route']]
        df_prescriptions = self._util_instance.remove_duplicates_and_re_index(df_prescriptions, 'subject_id')
        return df_prescriptions

    def filter_hosp_service(self, df):
        df_service = df[['subject_id', 'curr_service']]
        df_service = self._util_instance.remove_duplicates_and_re_index(df_service, 'subject_id')
        return df_service

    def merge_core_hosp_tables(self):
        df_core_merged = self.merge_core_tables()
        df_hosp_lab_events_merge = self.merge_hosp_lab_events_d_labitems()
        df_hosp_emar_poe_merge = self.merge_hosp_emar_poe()
        df_core_diagnoses_merge = df_core_merged.merge(self.filter_hosp_diagnoses_icd(self._mimic_instance.read_hosp_diagnoses_icd_csv()), how='outer', on=['subject_id', 'hadm_id'])\
                                                .merge(self.filter_hosp_procedures_icd(self._mimic_instance.read_hosp_procedures_icd_csv()), how='outer', on=['subject_id', 'hadm_id', 'icd_code', 'icd_version'])
        df_core_hosp_merge = df_core_diagnoses_merge.merge(self.filter_hosp_drgcodes(self._mimic_instance.read_hosp_drgcodes_csv()), how='outer', on=['subject_id', 'hadm_id'])
        df_core_hosp_merge = df_core_hosp_merge.merge(df_hosp_lab_events_merge, how='outer', on=['subject_id', 'hadm_id'])
        df_core_hosp_merge = df_core_hosp_merge.merge(df_hosp_emar_poe_merge, how='outer', on=['subject_id', 'hadm_id'])\
                                               .merge(self.filter_hosp_prescriptions(self._mimic_instance.read_hosp_prescriptions_csv()), how='outer', on=['subject_id', 'hadm_id'])\
                                               .merge(self.filter_hosp_microbiologyevents(self._mimic_instance.read_hosp_microbiologyevents_csv()), how='outer', on=['subject_id', 'hadm_id'])\
                                               .merge(self.filter_hosp_service(self._mimic_instance.read_hosp_services_csv()), how='outer', on=['subject_id', 'hadm_id'])
        return df_core_hosp_merge
    
    def filter_icu_icustays(self, df):
        df_icustays = df.drop(['hadm_id', 'stay_id', 'intime', 'outtime'], axis=1)
        df_icustays = self._util_instance.remove_duplicates_and_re_index(df_icustays, 'subject_id')
        return df_icustays

    def filter_icu_chartevents(self, df):
        df_chartevents = df.drop(['hadm_id', 'stay_id', 'charttime', 'storetime', 'value', 'valuenum', 'valueuom', 'warning'], axis=1)
        df_chartevents = self._util_instance.remove_duplicates_and_re_index(df_chartevents, 'subject_id')
        return df_chartevents

    def merge_core_hosp_icu_tables(self):
        df_core_hosp_merged = self.merge_core_hosp_tables()
        df_core_hosp_icu_merged = df_core_hosp_merged.merge(self.filter_icu_icustays(self._mimic_instance.read_icu_icustays_csv()), how='outer', on=['subject_id', 'hadm_id', ])\
                                                     .merge(self.filter_icu_chartevents(self._mimic_instance.read_icu_chartevents_csv()), how='outer', on=['subject_id', 'hadm_id', 'stay_id'])

        df_core_hosp_icu_merged.drop(['stay_id'], axis=1)
        return df_core_hosp_icu_merged
