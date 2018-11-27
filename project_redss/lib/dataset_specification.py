# Note: This module project-specific and not yet suitable for migration to Core
from core_data_modules.cleaners import somali

from project_redss.lib.redss_code_translators import *
from project_redss.lib.redss_schemes import CodeSchemes


class CodingPlan(object):
    def __init__(self, raw_field, coded_field, coda_filename, cleaner=None, code_scheme=None, time_field=None):
        self.raw_field = raw_field
        self.coded_field = coded_field
        self.coda_filename = coda_filename
        self.cleaner = cleaner
        self.code_scheme = code_scheme
        self.time_field = time_field
        self.id_field = "{}_id".format(self.raw_field)


class DatasetSpecification(object):
    DEV_MODE = True

    RQA_CODING_PLANS = [
        CodingPlan(raw_field="rqa_s01e01_raw",
                   coded_field="rqa_s01_e01_coded",
                   time_field="sent_on",
                   coda_filename="s01e01",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E01),

        CodingPlan(raw_field="rqa_s01e02_raw",
                   coded_field="rqa_s01_e02_coded",
                   time_field="sent_on",
                   coda_filename="s01e02",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E01),  # TODO: Use S01E02 when available

        CodingPlan(raw_field="rqa_s01e02_raw",
                   coded_field="rqa_s01_e02_coded",
                   time_field="sent_on",
                   coda_filename="s01e03",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E01),  # TODO: Use S01E03 when available

        CodingPlan(raw_field="rqa_s01e02_raw",
                   coded_field="rqa_s01_e02_coded",
                   time_field="sent_on",
                   coda_filename="s01e04",
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
                   code_scheme=CodeSchemes.DISTRICT),

        CodingPlan(raw_field="mogadishu_sub_district_raw",
                   coded_field="region_coded",
                   time_field="mogadishu_sub_district_time",
                   coda_filename="region",
                   cleaner=None,
                   code_scheme=CodeSchemes.REGION),

        CodingPlan(raw_field="mogadishu_sub_district_raw",
                   coded_field="state_coded",
                   time_field="mogadishu_sub_district_time",
                   coda_filename="state",
                   cleaner=None,
                   code_scheme=CodeSchemes.STATE),

        CodingPlan(raw_field="mogadishu_sub_district_raw",
                   coded_field="zone_coded",
                   time_field="mogadishu_sub_district_time",
                   coda_filename="zone",
                   cleaner=None,
                   code_scheme=CodeSchemes.ZONE),

        CodingPlan(raw_field="age_raw",
                   coded_field="age_coded",
                   time_field="age_time",
                   coda_filename="age",
                   cleaner=lambda text: str(somali.DemographicCleaner.clean_age(text)),  # TODO: NC data out of range
                   code_scheme=CodeSchemes.AGE),

        CodingPlan(raw_field="idp_camp_raw",
                   coded_field="idp_camp_coded",
                   time_field="idp_camp_time",
                   coda_filename="idp_camp",
                   cleaner=somali.DemographicCleaner.clean_yes_no,
                   code_scheme=CodeSchemes.IDP_CAMP),

        CodingPlan(raw_field="recently_displaced_raw",
                   coded_field="recently_displaced_coded",
                   time_field="recently_displaced_time",
                   coda_filename="recently_displaced",
                   cleaner=somali.DemographicCleaner.clean_yes_no,
                   code_scheme=CodeSchemes.RECENTLY_DISPLACED),

        CodingPlan(raw_field="hh_language_raw",
                   coded_field="hh_language_coded",
                   time_field="hh_language_time",
                   coda_filename="hh_language",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E01)
    ]
