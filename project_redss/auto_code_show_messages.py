import os
import random
import time
from os import path

from core_data_modules.cleaners import somali, Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaIO, TracedDataCSVIO, TracedDataCoda2IO
from core_data_modules.util import IOUtils
from dateutil.parser import isoparse

from project_redss.lib import ICRTools
from project_redss.lib import MessageFilters
from project_redss.lib.dataset_specification import DatasetSpecification


class AutoCodeShowMessages(object):
    RQA_KEYS = []
    for plan in DatasetSpecification.RQA_CODING_PLANS:
        RQA_KEYS.append(plan.raw_field)

    SENT_ON_KEY = "sent_on"
    NOISE_KEY = "noise"
    PROJECT_START_DATE = isoparse("2010-01-01T00+03:00")  # TODO: Set when known
    PROJECT_END_DATE = isoparse("2030-01-01T00+03:00")  # TODO: Set when known
    ICR_MESSAGES_COUNT = 200

    @classmethod
    def auto_code_show_messages(cls, user, data, icr_output_dir, coda_output_dir):
        # Filter out test messages sent by AVF.
        # TODO: Re-enable before entering production mode
        # data = MessageFilters.filter_test_messages(data)

        # Filter for runs which don't contain a response to any week's question
        data = MessageFilters.filter_empty_messages(data, cls.RQA_KEYS)

        # Filter out runs sent outwith the project start and end dates
        data = MessageFilters.filter_time_range(data, cls.SENT_ON_KEY, cls.PROJECT_START_DATE, cls.PROJECT_END_DATE)

        # Tag messages which are noise as being noise
        for td in data:
            is_noise = True
            for rqa_key in cls.RQA_KEYS:
                if rqa_key in td and not somali.DemographicCleaner.is_noise(td[rqa_key], min_length=1):
                    is_noise = False
            td.append_data({cls.NOISE_KEY: is_noise}, Metadata(user, Metadata.get_call_location(), time.time()))

        # Label missing data
        # TODO: Set scheme/code ids once we have a code scheme for these
        for td in data:
            missing_dict = dict()
            for plan in DatasetSpecification.RQA_CODING_PLANS:
                if plan.raw_field not in td:
                    na_label = CleaningUtils.make_label(
                        plan.code_translator.scheme_id, plan.code_translator.code_id(Codes.TRUE_MISSING),
                        Metadata.get_call_location(), control_code=Codes.TRUE_MISSING
                    )
                    missing_dict[plan.coded_field] = na_label.to_dict()
            td.append_data(missing_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        # Filter for messages which aren't noise (in order to export to Coda and export for ICR)
        not_noise = MessageFilters.filter_noise(data, cls.NOISE_KEY, lambda x: x)

        # Output messages which aren't noise to Coda
        IOUtils.ensure_dirs_exist(coda_output_dir)
        for plan in DatasetSpecification.RQA_CODING_PLANS:
            TracedDataCoda2IO.add_message_ids(user, not_noise, plan.raw_field, plan.id_field)

            output_path = path.join(coda_output_dir, "{}.json".format(plan.coda_name))
            with open(output_path, "w") as f:
                TracedDataCoda2IO.export_traced_data_iterable_to_coda_2(
                    not_noise, plan.raw_field, cls.SENT_ON_KEY, plan.id_field, {}, f
                )

        # Randomly select some messages to export for ICR
        # TODO: ICR
        icr_messages = ICRTools.generate_sample_for_icr(not_noise, cls.ICR_MESSAGES_COUNT, random.Random(0))

        # Output ICR data to a CSV file
        # run_id_key = "{} (Run ID) - {}".format(cls.VARIABLE_NAME, cls.FLOW_NAME)
        # raw_text_key = "{} (Text) - {}".format(cls.VARIABLE_NAME, cls.FLOW_NAME)
        # IOUtils.ensure_dirs_exist_for_file(icr_output_path)
        with open(icr_output_dir, "w") as f:
            f.write("")
            # TracedDataCSVIO.export_traced_data_iterable_to_csv(icr_messages, f, headers=[run_id_key, raw_text_key])

        return data
