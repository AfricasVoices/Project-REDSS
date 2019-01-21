# Note: This module project-specific and not yet suitable for migration to Core
from core_data_modules.cleaners import somali, Codes

from project_redss.lib.redss_schemes import CodeSchemes


class CodingPlan(object):
    def __init__(self, raw_field, coded_field, coda_filename, cleaner=None, code_scheme=None, time_field=None,
                 run_id_field=None, icr_filename=None, analysis_file_key=None, id_field=None):
        self.raw_field = raw_field
        self.coded_field = coded_field
        self.coda_filename = coda_filename
        self.icr_filename = icr_filename
        self.cleaner = cleaner
        self.code_scheme = code_scheme
        self.time_field = time_field
        self.run_id_field = run_id_field
        self.analysis_file_key = analysis_file_key

        if id_field is None:
            id_field = "{}_id".format(self.raw_field)
        self.id_field = id_field


class PipelineConfiguration(object):
    DEV_MODE = False

    RQA_CODING_PLANS = [
        CodingPlan(raw_field="rqa_s01e01_raw",
                   coded_field="rqa_s01e01_coded",
                   time_field="sent_on",
                   coda_filename="s01e01.json",
                   icr_filename="s01e01.csv",
                   run_id_field="rqa_s01e01_run_id",
                   analysis_file_key="rqa_s01e01_",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E01),

        CodingPlan(raw_field="rqa_s01e02_raw",
                   coded_field="rqa_s01e02_coded",
                   time_field="sent_on",
                   coda_filename="s01e02.json",
                   icr_filename="s01e02.csv",
                   run_id_field="rqa_s01e02_run_id",
                   analysis_file_key="rqa_s01e02_",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E02),

        CodingPlan(raw_field="rqa_s01e03_raw",
                   coded_field="rqa_s01e03_coded",
                   time_field="sent_on",
                   coda_filename="s01e03.json",
                   icr_filename="s01e03.csv",
                   run_id_field="rqa_s01e03_run_id",
                   analysis_file_key="rqa_s01e03_",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E03_REASONS),

        CodingPlan(raw_field="rqa_s01e04_raw",
                   coded_field="rqa_s01e04_coded",
                   time_field="sent_on",
                   coda_filename="s01e04.json",
                   icr_filename="s01e04.csv",
                   run_id_field="rqa_s01e04_run_id",
                   analysis_file_key="rqa_s01e04_",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E04)
    ]

    @staticmethod
    def redss_clean_age(text):
        age = somali.DemographicCleaner.clean_age(text)
        if type(age) == int and 10 <= age < 100:
            return str(age)
            # TODO: Once the cleaners are updated to not return Codes.NOT_CODED, this should be updated to still return
            #       NC in the case where age is an int but is out of range
        else:
            return Codes.NOT_CODED

    LOCATION_CODING_PLANS = [
        CodingPlan(raw_field="mogadishu_sub_district_raw",
                   id_field="mogadishu_sub_district_raw_id",
                   coded_field="mogadishu_sub_district_coded",
                   time_field="mogadishu_sub_district_time",
                   coda_filename="location.json",
                   analysis_file_key="mogadishu_sub_district",
                   cleaner=somali.DemographicCleaner.clean_mogadishu_sub_district,
                   code_scheme=CodeSchemes.MOGADISHU_SUB_DISTRICT),

        CodingPlan(raw_field="mogadishu_sub_district_raw",
                   id_field="mogadishu_sub_district_raw_id",
                   coded_field="district_coded",
                   time_field="mogadishu_sub_district_time",
                   coda_filename="location.json",
                   analysis_file_key="district",
                   cleaner=somali.DemographicCleaner.clean_somalia_district,
                   code_scheme=CodeSchemes.DISTRICT),

        CodingPlan(raw_field="mogadishu_sub_district_raw",
                   id_field="mogadishu_sub_district_raw_id",
                   coded_field="region_coded",
                   time_field="mogadishu_sub_district_time",
                   coda_filename="location.json",
                   analysis_file_key="region",
                   cleaner=None,
                   code_scheme=CodeSchemes.REGION),

        CodingPlan(raw_field="mogadishu_sub_district_raw",
                   id_field="mogadishu_sub_district_raw_id",
                   coded_field="state_coded",
                   time_field="mogadishu_sub_district_time",
                   coda_filename="location.json",
                   analysis_file_key="state",
                   cleaner=None,
                   code_scheme=CodeSchemes.STATE),

        CodingPlan(raw_field="mogadishu_sub_district_raw",
                   id_field="mogadishu_sub_district_raw_id",
                   coded_field="zone_coded",
                   time_field="mogadishu_sub_district_time",
                   coda_filename="location.json",
                   analysis_file_key="zone",
                   cleaner=None,
                   code_scheme=CodeSchemes.ZONE),
    ]

    SURVEY_CODING_PLANS = [
        CodingPlan(raw_field="gender_raw",
                   coded_field="gender_coded",
                   time_field="gender_time",
                   coda_filename="gender.json",
                   analysis_file_key="gender",
                   cleaner=somali.DemographicCleaner.clean_gender,
                   code_scheme=CodeSchemes.GENDER)
    ]
    SURVEY_CODING_PLANS.extend(LOCATION_CODING_PLANS)
    SURVEY_CODING_PLANS.extend([
        CodingPlan(raw_field="age_raw",
                   coded_field="age_coded",
                   time_field="age_time",
                   coda_filename="age.json",
                   analysis_file_key="age",
                   cleaner=lambda text: PipelineConfiguration.redss_clean_age(text),
                   code_scheme=CodeSchemes.AGE),

        CodingPlan(raw_field="idp_camp_raw",
                   coded_field="idp_camp_coded",
                   time_field="idp_camp_time",
                   coda_filename="idp_camp.json",
                   analysis_file_key="idp_camp",
                   cleaner=somali.DemographicCleaner.clean_yes_no,
                   code_scheme=CodeSchemes.IDP_CAMP),

        CodingPlan(raw_field="recently_displaced_raw",
                   coded_field="recently_displaced_coded",
                   time_field="recently_displaced_time",
                   coda_filename="recently_displaced.json",
                   analysis_file_key="recently_displaced",
                   cleaner=somali.DemographicCleaner.clean_yes_no,
                   code_scheme=CodeSchemes.RECENTLY_DISPLACED),

        CodingPlan(raw_field="hh_language_raw",
                   coded_field="hh_language_coded",
                   time_field="hh_language_time",
                   coda_filename="hh_language.json",
                   analysis_file_key="hh_language",
                   cleaner=None,
                   code_scheme=CodeSchemes.HH_LANGUAGE),

        CodingPlan(raw_field="repeated_raw",
                   coded_field="repeated_coded",
                   time_field="repeated_time",
                   coda_filename="repeated.json",
                   analysis_file_key="repeated",
                   cleaner=somali.DemographicCleaner.clean_yes_no,
                   code_scheme=CodeSchemes.REPEATED),

        CodingPlan(raw_field="involved_raw",
                   coded_field="involved_coded",
                   time_field="involved_time",
                   coda_filename="involved.json",
                   analysis_file_key="involved",
                   cleaner=somali.DemographicCleaner.clean_yes_no,
                   code_scheme=CodeSchemes.INVOLVED)
    ])
