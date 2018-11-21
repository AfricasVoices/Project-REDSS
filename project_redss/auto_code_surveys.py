import os
import time
from os import path

from core_data_modules.cleaners import Codes, PhoneCleaner
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaIO, TracedDataCoda2IO
from core_data_modules.util import IOUtils

from project_redss.lib import Channels
from project_redss.lib.dataset_specification import DatasetSpecification


class AutoCodeSurveys(object):
    @staticmethod
    def auto_code_surveys(user, data, phone_uuid_table, coda_output_dir):
        for td in data:
            labels_dict = dict()
            for plan in DatasetSpecification.SURVEY_CODING_PLANS:
                if plan.raw_field not in td:
                    na_label = CleaningUtils.make_label(
                        plan.code_translator.scheme_id, plan.code_translator.code_id(Codes.TRUE_MISSING),
                        Metadata.get_call_location(), control_code=Codes.TRUE_MISSING
                    )
                    labels_dict[plan.coded_field] = na_label
                else:
                    coded_label = CleaningUtils.apply_cleaner_to_traced_data_iterable(
                        user, data, plan.raw_field, plan.coded_field, plan.cleaner, plan.code_translator
                    )
                    labels_dict[plan.coded_field] = coded_label
            td.append_data(labels_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        # # Label each message with the operator of the sender
        # for td in data:
        #     phone_number = phone_uuid_table.get_phone(td["avf_phone_id"])
        #     operator = PhoneCleaner.clean_operator(phone_number)
        #
        #     td.append_data(
        #         {"operator": operator},
        #         Metadata(user, Metadata.get_call_location(), time.time())
        #     )
        #
        # # Label each message with channel keys
        # for td in data:
        #     Channels.set_channel_keys(user, td)

        # Output for manual verification + coding
        IOUtils.ensure_dirs_exist(coda_output_dir)
        for plan in DatasetSpecification.SURVEY_CODING_PLANS:
            TracedDataCoda2IO.add_message_ids(user, data, plan.raw_field, plan.id_field)

            output_path = path.join(coda_output_dir, "{}.json".format(plan.coda_name))
            with open(output_path, "w") as f:
                TracedDataCoda2IO.export_traced_data_iterable_to_coda_2(
                    data, plan.raw_field, plan.time_field, plan.id_field, {plan.coded_field}, f
                )

        return data
