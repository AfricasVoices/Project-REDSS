# Note: This module project-specific and not yet suitable for migration to Core
from core_data_modules.cleaners import somali

from project_redss.lib.redss_code_translators import *


class CodingPlan(object):
    def __init__(self, raw_field, coded_field, coda_filename, cleaner=None, code_translator=None, time_field=None):
        self.raw_field = raw_field
        self.coded_field = coded_field
        self.coda_filename = coda_filename
        self.cleaner = cleaner
        self.code_translator = code_translator
        self.time_field = time_field
        self.id_field = "{}_id".format(self.raw_field)


class DatasetSpecification(object):
    RQA_CODING_PLANS = [
        CodingPlan(raw_field="rqa_s01e01_raw",
                   coded_field="rqa_s01_e01_coded",
                   time_field="sent_on",
                   coda_filename="s01e01",
                   cleaner=None,
                   code_translator=S01E01Translator),

        CodingPlan(raw_field="rqa_s01e02_raw",
                   coded_field="rqa_s01_e02_coded",
                   time_field="sent_on",
                   coda_filename="s01e02",
                   cleaner=None,
                   code_translator=S01E01Translator),  # TODO: Use S01E02 when available

        CodingPlan(raw_field="rqa_s01e02_raw",
                   coded_field="rqa_s01_e02_coded",
                   time_field="sent_on",
                   coda_filename="s01e03",
                   cleaner=None,
                   code_translator=S01E01Translator),  # TODO: Use S01E03 when available

        CodingPlan(raw_field="rqa_s01e02_raw",
                   coded_field="rqa_s01_e02_coded",
                   time_field="sent_on",
                   coda_filename="s01e04",
                   cleaner=None,
                   code_translator=S01E01Translator)  # TODO: Use S01E04 when available
    ]

    SURVEY_CODING_PLANS = [
        CodingPlan(raw_field="gender_raw",
                   coded_field="gender_coded",
                   time_field="gender_time",
                   coda_filename="gender",
                   cleaner=somali.DemographicCleaner.clean_gender,
                   code_translator=GenderTranslator)
    ]
