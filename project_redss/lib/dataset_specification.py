# Note: This module project-specific and not yet suitable for migration to Core
from core_data_modules.cleaners import somali

from project_redss.lib.redss_code_translators import *
from project_redss.lib.redss_schemes import CodeSchemes


class CodingPlan(object):
    def __init__(self, raw_field, coded_field, coda_filename, cleaner=None, code_scheme=None, time_field=None,
                 run_id_field=None, icr_filename=None):
        self.raw_field = raw_field
        self.coded_field = coded_field
        self.coda_filename = coda_filename
        self.icr_filename = icr_filename
        self.cleaner = cleaner
        self.code_scheme = code_scheme
        self.time_field = time_field
        self.run_id_field = run_id_field
        self.id_field = "{}_id".format(self.raw_field)


class DatasetSpecification(object):
    DEV_MODE = True

    RQA_CODING_PLANS = [
        CodingPlan(raw_field="rqa_s01e01_raw",
                   coded_field="rqa_s01e01_coded",
                   time_field="sent_on",
                   coda_filename="s01e01",
                   icr_filename="s01e01",
                   run_id_field="rqa_s01e01_run_id",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E01),

        CodingPlan(raw_field="rqa_s01e02_raw",
                   coded_field="rqa_s01e02_coded",
                   time_field="sent_on",
                   coda_filename="s01e02",
                   icr_filename="s01e02",
                   run_id_field="rqa_s01e02_run_id",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E01),  # TODO: Use S01E02 when available

        CodingPlan(raw_field="rqa_s01e03_raw",
                   coded_field="rqa_s01e03_coded",
                   time_field="sent_on",
                   coda_filename="s01e03",
                   icr_filename="s01e03",
                   run_id_field="rqa_s01e03_run_id",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E01),  # TODO: Use S01E03 when available

        CodingPlan(raw_field="rqa_s01e04_raw",
                   coded_field="rqa_s01e04_coded",
                   time_field="sent_on",
                   coda_filename="s01e04",
                   icr_filename="s01e04",
                   run_id_field="rqa_s01e04_run_id",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E01)  # TODO: Use S01E04 when available
    ]

    # If in production mode, check that the above TODOs have been dealt with
    if not DEV_MODE:
        s01e01_uses = 0
        for plan in RQA_CODING_PLANS:
            if plan.code_scheme == CodeSchemes.S01E01:
                s01e01_uses += 1
        assert s01e01_uses == 1

    SURVEY_CODING_PLANS = [
        CodingPlan(raw_field="gender_raw",
                   coded_field="gender_coded",
                   time_field="gender_time",
                   coda_filename="gender",
                   cleaner=somali.DemographicCleaner.clean_gender,
                   code_scheme=CodeSchemes.GENDER),

        CodingPlan(raw_field="mogadishu_sub_district_raw",
                   coded_field="mogadishu_sub_district_coded",
                   time_field="mogadishu_sub_district_time",
                   coda_filename="mogadishu_sub_district",
                   cleaner=somali.DemographicCleaner.clean_mogadishu_sub_district,
                   code_scheme=CodeSchemes.MOGADISHU_SUB_DISTRICT),

        CodingPlan(raw_field="mogadishu_sub_district_raw",
                   coded_field="district_coded",
                   time_field="mogadishu_sub_district_time",
                   coda_filename="district",
                   cleaner=somali.DemographicCleaner.clean_somalia_district,
                   code_scheme=CodeSchemes.DISTRICT)
    ]
