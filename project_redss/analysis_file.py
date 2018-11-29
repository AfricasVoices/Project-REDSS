import sys
import time

from core_data_modules.cleaners import Codes
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCSVIO
from core_data_modules.traced_data.util import FoldTracedData
from core_data_modules.pipeline_utils.consent_utils import ConsentUtils

from project_redss.lib import AnalysisKeys
from project_redss.lib.dataset_specification import DatasetSpecification


class AnalysisFile(object):
    @staticmethod
    def generate(user, data, csv_by_message_output_path, csv_by_individual_output_path):
        # Serializer is currently overflowing
        # TODO: Investigate/address the cause of this.
        sys.setrecursionlimit(10000)

        demog_keys = []
        for plan in DatasetSpecification.SURVEY_CODING_PLANS:
            if plan.analysis_file_key not in demog_keys:
                demog_keys.append(plan.analysis_file_key)
            if plan.raw_field not in demog_keys:
                demog_keys.append(plan.raw_field)

        for td in data:
            td.append_data(
                {plan.analysis_file_key: plan.code_scheme.get_code_with_id(td[plan.coded_field]["CodeID"]).string_value
                 for plan in DatasetSpecification.SURVEY_CODING_PLANS},
                Metadata(user, Metadata.get_call_location(), time.time())
            )

        evaluation_keys = [
            # "repeated",
            # "repeated_raw",
            # "involved",
            # "involved_raw"
        ]

        # Translate keys to final values for analysis
        show_keys = set()  # of all radio show matrix keys
        # AnalysisKeys.set_matrix_keys(
        #     user, data, show_keys, "S07E01_Humanitarian_Priorities (Text) - esc4jmcna_activation_coded",
        #     "humanitarian_priorities"
        # )

        show_keys = list(show_keys)
        show_keys.sort()

        equal_keys = ["uid", "operator"]
        equal_keys.extend(demog_keys)
        equal_keys.extend(evaluation_keys)
        concat_keys = [
            "rqa_s01e01_raw",
            "rqa_s01e02_raw",
            "rqa_s01e03_raw",
            "rqa_s01e04_raw"
        ]
        matrix_keys = show_keys
        bool_keys = [
            # avf_consent_withdrawn_key,

            # "sms_ad",
            # "radio_promo",
            # "radio_show",
            # "non_logical_time",
            # "radio_participation_s01e01",
            # "radio_participation_s01e02",
            # "radio_participation_s01e03",
            # "radio_participation_s01e04"
        ]

        # Export to CSV
        export_keys = ["uid", "operator"]
        export_keys.extend(bool_keys)
        export_keys.extend(show_keys)
        export_keys.extend(concat_keys)
        export_keys.extend(demog_keys)
        export_keys.extend(evaluation_keys)

        # TODO: Delete in time?
        # export_keys = [
        #     "uid",
        #     "rqa_s01e01_raw",
        #     "rqa_s01e02_raw",
        #     "rqa_s01e03_raw",
        #     "rqa_s01e04_raw",
        #     "gender_raw",
        #     "mogadishu_sub_district_raw",
        #     "age_raw",
        #     "idp_camp_raw",
        #     "recently_displaced_raw",
        #     "hh_language_raw",
        # ]

        # Set consent withdrawn based on presence of data coded as "stop"
        # ConsentUtils.determine_consent_withdrawn(user, data, export_keys, avf_consent_withdrawn_key)

        # Set consent withdrawn based on stop codes from humanitarian priorities.
        # TODO: Update Core Data to set 'stop's instead of '1's?
        # for td in data:
        #     if td.get("humanitarian_priorities_stop") == Codes.MATRIX_1:
        #         td.append_data({avf_consent_withdrawn_key: Codes.TRUE},
        #                        Metadata(user, Metadata.get_call_location(), time.time()))
        #
        # # Set consent withdrawn based on auto-categorisation in Rapid Pro
        # for td in data:
        #     if td.get(rapid_pro_consent_withdrawn_key) == "yes":  # Not using Codes.YES because this is from Rapid Pro
        #         td.append_data({avf_consent_withdrawn_key: Codes.TRUE},
        #                        Metadata(user, Metadata.get_call_location(), time.time()))
        #
        # for td in data:
        #     if avf_consent_withdrawn_key not in td:
        #         td.append_data({avf_consent_withdrawn_key: Codes.FALSE},
        #                        Metadata(user, Metadata.get_call_location(), time.time()))

        # Fold data to have one respondent per row
        to_be_folded = []
        for td in data:
            to_be_folded.append(td.copy())

        folded_data = FoldTracedData.fold_iterable_of_traced_data(
            user, data, fold_id_fn=lambda td: td["uid"],
            equal_keys=equal_keys, concat_keys=concat_keys, matrix_keys=matrix_keys, bool_keys=bool_keys
        )

        # Process consent
        # ConsentUtils.set_stopped(user, data, avf_consent_withdrawn_key)
        # ConsentUtils.set_stopped(user, folded_data, avf_consent_withdrawn_key)

        # Output to CSV with one message per row
        with open(csv_by_message_output_path, "w") as f:
            TracedDataCSVIO.export_traced_data_iterable_to_csv(data, f, headers=export_keys)

        with open(csv_by_individual_output_path, "w") as f:
            TracedDataCSVIO.export_traced_data_iterable_to_csv(folded_data, f, headers=export_keys)

        return data
