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
                   coda_filename="s01e01.json",
                   icr_filename="s01e01.csv",
                   run_id_field="rqa_s01e01_run_id",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E01),

        CodingPlan(raw_field="rqa_s01e02_raw",
                   coded_field="rqa_s01e02_coded",
                   time_field="sent_on",
                   coda_filename="s01e02.json",
                   icr_filename="s01e02.csv",
                   run_id_field="rqa_s01e02_run_id",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E02),

        CodingPlan(raw_field="rqa_s01e03_raw",
                   coded_field="rqa_s01e03_coded",
                   time_field="sent_on",
                   coda_filename="s01e03.json",
                   icr_filename="s01e03.csv",
                   run_id_field="rqa_s01e03_run_id",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E03),

        CodingPlan(raw_field="rqa_s01e04_raw",
                   coded_field="rqa_s01e04_coded",
                   time_field="sent_on",
                   coda_filename="s01e04.json",
                   icr_filename="s01e04.csv",
                   run_id_field="rqa_s01e04_run_id",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E04)
    ]

    @staticmethod
    def redss_clean_age(text):
        age = somali.DemographicCleaner.clean_age(text)
        if type(age) == int and 10 <= age < 100:
            return str(text)
            # TODO: Once the cleaners are updated to not return Codes.NOT_CODED, this should be updated to still return
            #       NC in the case where age is an int but is out of range
        else:
            return Codes.NOT_CODED

    LOCATION_CODING_PLANS = [
        CodingPlan(raw_field="mogadishu_sub_district_raw",
                   coded_field="mogadishu_sub_district_coded",
                   time_field="mogadishu_sub_district_time",
                   coda_filename="mogadishu_sub_district.json",
                   cleaner=somali.DemographicCleaner.clean_mogadishu_sub_district,
                   code_scheme=CodeSchemes.MOGADISHU_SUB_DISTRICT),

        CodingPlan(raw_field="mogadishu_sub_district_raw",
                   coded_field="district_coded",
                   time_field="mogadishu_sub_district_time",
                   coda_filename="district.json",
                   cleaner=somali.DemographicCleaner.clean_somalia_district,
                   code_scheme=CodeSchemes.DISTRICT),

        CodingPlan(raw_field="mogadishu_sub_district_raw",
                   coded_field="region_coded",
                   time_field="mogadishu_sub_district_time",
                   coda_filename="region.json",
                   cleaner=None,
                   code_scheme=CodeSchemes.REGION),

        CodingPlan(raw_field="mogadishu_sub_district_raw",
                   coded_field="state_coded",
                   time_field="mogadishu_sub_district_time",
                   coda_filename="state.json",
                   cleaner=None,
                   code_scheme=CodeSchemes.STATE),

        CodingPlan(raw_field="mogadishu_sub_district_raw",
                   coded_field="zone_coded",
                   time_field="mogadishu_sub_district_time",
                   coda_filename="zone.json",
                   cleaner=None,
                   code_scheme=CodeSchemes.ZONE),
    ]

    SURVEY_CODING_PLANS = [
        CodingPlan(raw_field="gender_raw",
                   coded_field="gender_coded",
                   time_field="gender_time",
                   coda_filename="gender.json",
                   cleaner=somali.DemographicCleaner.clean_gender,
                   code_scheme=CodeSchemes.GENDER)
    ]
    SURVEY_CODING_PLANS.extend(LOCATION_CODING_PLANS)
    SURVEY_CODING_PLANS.extend([
        CodingPlan(raw_field="age_raw",
                   coded_field="age_coded",
                   time_field="age_time",
                   coda_filename="age.json",
                   cleaner=lambda text: DatasetSpecification.redss_clean_age(text),
                   code_scheme=CodeSchemes.AGE),

        CodingPlan(raw_field="idp_camp_raw",
                   coded_field="idp_camp_coded",
                   time_field="idp_camp_time",
                   coda_filename="idp_camp.json",
                   cleaner=somali.DemographicCleaner.clean_yes_no,
                   code_scheme=CodeSchemes.IDP_CAMP),

        CodingPlan(raw_field="recently_displaced_raw",
                   coded_field="recently_displaced_coded",
                   time_field="recently_displaced_time",
                   coda_filename="recently_displaced.json",
                   cleaner=somali.DemographicCleaner.clean_yes_no,
                   code_scheme=CodeSchemes.RECENTLY_DISPLACED),

        CodingPlan(raw_field="hh_language_raw",
                   coded_field="hh_language_coded",
                   time_field="hh_language_time",
                   coda_filename="hh_language.json",
                   cleaner=None,
                   code_scheme=CodeSchemes.HH_LANGUAGE)
    ])
