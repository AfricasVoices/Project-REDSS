import sys
import time

from core_data_modules.cleaners import Codes
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCSVIO
from core_data_modules.traced_data.util import FoldTracedData
from core_data_modules.util import TimeUtils

from project_redss.lib import AnalysisKeys, MessageFilters
from project_redss.lib.dataset_specification import DatasetSpecification
from project_redss.lib.redss_schemes import CodeSchemes


class ConsentUtils(object):
    # TODO: This used to be in Core but has been duplicated then modified here in order to test the updates
    #       needed to support Labels instead of strings.
    @staticmethod
    def td_has_stop_code(td, coding_plans):
        """
        Returns whether any of the values for the given keys are Codes.STOP in the given TracedData object.

        :param td: TracedData object to search for stop codes.
        :type td: TracedData
        :param coding_plans:
        :type coding_plans: iterable of CodingPlan
        :return: Whether td contains Codes.STOP in any of the keys in 'keys'.
        :rtype: bool
        """
        for plan in coding_plans:
            if plan.code_scheme.get_code_with_id(td[plan.coded_field]["CodeID"]).control_code == Codes.STOP:
                return True
        return False

    @classmethod
    def determine_consent_withdrawn(cls, user, data, coding_plans, withdrawn_key="consent_withdrawn"):
        """
        Determines whether consent has been withdrawn, by searching for Codes.STOP in the given list of keys.

        TracedData objects where a stop code is found will have the key-value pair <withdrawn_key>: Codes.TRUE
        appended. Objects where a stop code is not found are not modified.

        Note that this does not actually set any other keys to Codes.STOP. Use Consent.set_stopped for this purpose.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to determine consent for.
        :type data: iterable of TracedData
        :param coding_plans:
        :type coding_plans: iterable of CodingPlan
        :param withdrawn_key: Name of key to use for the consent withdrawn field.
        :type withdrawn_key: str
        """
        for td in data:
            if cls.td_has_stop_code(td, coding_plans):
                td.append_data(
                    {withdrawn_key: Codes.TRUE},
                    Metadata(user, Metadata.get_call_location(), time.time())
                )

    @staticmethod
    def set_stopped(user, data, withdrawn_key="consent_withdrawn"):
        """
        For each TracedData object in an iterable whose 'withdrawn_key' is Codes.True, sets every other key to
        Codes.STOP. If there is no withdrawn_key or the value is not Codes.True, that TracedData object is not modified.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to set to stopped if consent has been withdrawn.
        :type data: iterable of TracedData
        :param withdrawn_key: Key in each TracedData object which indicates whether consent has been withdrawn.
        :type withdrawn_key: str
        """
        for td in data:
            if td.get(withdrawn_key) == Codes.TRUE:
                stop_dict = {key: Codes.STOP for key in td.keys() if key != withdrawn_key}
                td.append_data(stop_dict, Metadata(user, Metadata.get_call_location(), time.time()))


class AnalysisFile(object):
    @staticmethod
    def generate(user, data, csv_by_message_output_path, csv_by_individual_output_path, production_csv_output_path):
        # Serializer is currently overflowing
        # TODO: Investigate/address the cause of this.
        sys.setrecursionlimit(10000)

        consent_withdrawn_key = "consent_withdrawn"
        for td in data:
            td.append_data({consent_withdrawn_key: Codes.FALSE},
                           Metadata(user, Metadata.get_call_location(), time.time()))

        # Set the list of raw/coded keys which
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

        for td in data:
            td.append_data(
                {"operator": CodeSchemes.OPERATOR.get_code_with_id(td["operator_coded"]["CodeID"]).string_value},
                Metadata(user, Metadata.get_call_location(), time.time())
            )

        evaluation_keys = [
            # "repeated",
            # "repeated_raw",
            # "involved",
            # "involved_raw"
        ]

        # Translate keys to final values for analysis
        matrix_keys = []

        for plan in DatasetSpecification.RQA_CODING_PLANS:
            show_matrix_keys = set()
            for code in plan.code_scheme.codes:
                show_matrix_keys.add(f"{plan.analysis_file_key}{code.string_value}")

            AnalysisKeys.set_matrix_keys(
                user, data, show_matrix_keys, plan.code_scheme, plan.coded_field, plan.analysis_file_key)

            matrix_keys.extend(show_matrix_keys)

        matrix_keys.sort()

        equal_keys = ["uid", "operator"]
        equal_keys.extend(demog_keys)
        equal_keys.extend(evaluation_keys)
        concat_keys = [
            "rqa_s01e01_raw",
            "rqa_s01e02_raw",
            "rqa_s01e03_raw",
            "rqa_s01e04_raw"
        ]
        bool_keys = [
            consent_withdrawn_key,

            "sms_ad",
            "radio_promo",
            "radio_show",
            "non_logical_time",
            "radio_participation_s01e01",
            "radio_participation_s01e02",
            "radio_participation_s01e03",
            "radio_participation_s01e04"
        ]

        # Export to CSV
        export_keys = ["uid", "operator"]
        export_keys.extend(bool_keys)
        export_keys.extend(matrix_keys)
        export_keys.extend(concat_keys)
        export_keys.extend(demog_keys)
        export_keys.extend(evaluation_keys)

        # Set consent withdrawn based on presence of data coded as "stop"
        ConsentUtils.determine_consent_withdrawn(
            user, data, DatasetSpecification.SURVEY_CODING_PLANS, consent_withdrawn_key)

        # Set consent withdrawn based on stop codes from radio question answers
        for td in data:
            for plan in DatasetSpecification.RQA_CODING_PLANS:
                if td[f"{plan.analysis_file_key}{Codes.STOP}"] == Codes.MATRIX_1:
                    td.append_data({consent_withdrawn_key: Codes.TRUE},
                                   Metadata(user, Metadata.get_call_location(), time.time()))

        # Fold data to have one respondent per row
        to_be_folded = []
        for td in data:
            to_be_folded.append(td.copy())

        folded_data = FoldTracedData.fold_iterable_of_traced_data(
            user, data, fold_id_fn=lambda td: td["uid"],
            equal_keys=equal_keys, concat_keys=concat_keys, matrix_keys=matrix_keys, bool_keys=bool_keys
        )

        # Fix-up _NA and _NC keys, which are currently being set incorrectly by
        # FoldTracedData.fold_iterable_of_traced_data when there are multiple radio shows
        # TODO: Update FoldTracedData to handle NA and NC correctly under multiple radio shows
        for td in folded_data:
            for plan in DatasetSpecification.RQA_CODING_PLANS:
                if td.get(plan.raw_field, "") != "":
                    td.append_data({f"{plan.analysis_file_key}{Codes.TRUE_MISSING}": Codes.MATRIX_0},
                                   Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

                contains_non_nc_key = False
                for key in matrix_keys:
                    if key.startswith(plan.analysis_file_key) and not key.endswith(Codes.NOT_CODED) \
                            and td.get(key) == Codes.MATRIX_1:
                        contains_non_nc_key = True
                if not contains_non_nc_key:
                    td.append_data({f"{plan.analysis_file_key}{Codes.NOT_CODED}": Codes.MATRIX_1},
                                   Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

        # Process consent
        ConsentUtils.set_stopped(user, data, consent_withdrawn_key)
        ConsentUtils.set_stopped(user, folded_data, consent_withdrawn_key)

        # Output to CSV with one message per row
        with open(csv_by_message_output_path, "w") as f:
            TracedDataCSVIO.export_traced_data_iterable_to_csv(data, f, headers=export_keys)

        with open(csv_by_individual_output_path, "w") as f:
            TracedDataCSVIO.export_traced_data_iterable_to_csv(folded_data, f, headers=export_keys)

        production_keys = [
            "uid",
            "rqa_s01e01_raw",
            "rqa_s01e02_raw",
            "rqa_s01e03_raw",
            "rqa_s01e04_raw",
            "gender_raw",
            "mogadishu_sub_district_raw",
            "age_raw",
            "idp_camp_raw",
            "recently_displaced_raw",
            "hh_language_raw",
        ]
        not_noise = MessageFilters.filter_noise(data, "noise", lambda x: x)
        with open(production_csv_output_path, "w") as f:
            TracedDataCSVIO.export_traced_data_iterable_to_csv(not_noise, f, headers=production_keys)

        return data
