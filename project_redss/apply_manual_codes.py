import io
import time
from io import BytesIO
from os import path

import pytz
from core_data_modules.cleaners import CharacterCleaner, Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.cleaners.codes import SomaliaCodes
from core_data_modules.cleaners.location_tools import SomaliaLocations
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaIO, TracedDataTheInterfaceIO, TracedDataCoda2IO
from core_data_modules.util import IOUtils
from dateutil.parser import isoparse

from project_redss.lib.dataset_specification import DatasetSpecification


class ApplyManualCodes(object):
    VARIABLE_NAME = "S07E01_Humanitarian_Priorities"
    FLOW_NAME = "esc4jmcna_activation"

    @classmethod
    def apply_manual_codes(cls, user, data, coda_input_dir, interface_output_dir):
        # merge manually coded radio show files into the cleaned dataset
        for plan in DatasetSpecification.RQA_CODING_PLANS:
            nr_label = CleaningUtils.make_label(
                plan.code_translator.scheme_id, plan.code_translator.code_id(Codes.NOT_REVIEWED),
                Metadata.get_call_location(), control_code=Codes.NOT_REVIEWED
            )

            coda_input_path = path.join(coda_input_dir, "{}.json".format(plan.coda_name))
            if path.exists(coda_input_path):
                with open(coda_input_path, "r") as f:
                    TracedDataCoda2IO.import_coda_2_to_traced_data_iterable_multi_coded(
                        user, data, plan.id_field, {plan.coded_field: plan.code_translator.scheme_id}, nr_label, f)
            else:
                # Read from simulated empty file
                TracedDataCoda2IO.import_coda_2_to_traced_data_iterable_multi_coded(
                    user, data, plan.id_field, {plan.coded_field: plan.code_translator.scheme_id}, nr_label,
                    io.StringIO("[]"))

        # Merge manually coded survey files into the cleaned dataset
        for plan in DatasetSpecification.SURVEY_CODING_PLANS:
            nr_label = CleaningUtils.make_label(
                plan.code_translator.scheme_id, plan.code_translator.code_id(Codes.NOT_REVIEWED),
                Metadata.get_call_location(), control_code=Codes.NOT_REVIEWED
            )

            coda_input_path = path.join(coda_input_dir, "{}.json".format(plan.coda_name))
            if path.exists(coda_input_path):
                with open(coda_input_path, "r") as f:
                    TracedDataCoda2IO.import_coda_2_to_traced_data_iterable(
                        user, data, plan.id_field, {plan.coded_field: plan.code_translator.scheme_id}, nr_label, f)
            else:
                # Read from simulated empty file
                TracedDataCoda2IO.import_coda_2_to_traced_data_iterable(
                    user, data, plan.id_field, {plan.coded_field: plan.code_translator.scheme_id}, nr_label,
                    io.StringIO("[]"))

        # TODO: Districts clean-up?
        # # Set district/region/state/zone codes from the coded district field.
        # for td in data:
        #     if td["district_review"] in {Codes.TRUE_MISSING, Codes.STOP}:
        #         td.append_data({
        #             "district_coded": td["district_review"],
        #             "region_coded": td["district_review"],
        #             "state_coded": td["district_review"],
        #             "zone_coded": td["district_review"],
        #             "district_coda": td["district_review"]
        #         }, Metadata(user, Metadata.get_call_location(), time.time()))
        #     else:
        #         td.append_data({
        #             "district_coded": SomaliaLocations.district_for_location_code(td["district_coded"]),
        #             "region_coded": SomaliaLocations.region_for_location_code(td["district_coded"]),
        #             "state_coded": SomaliaLocations.state_for_location_code(td["district_coded"]),
        #             "zone_coded": SomaliaLocations.zone_for_location_code(td["district_coded"]),
        #             "district_coda": Codes.TRUE_MISSING if td["district_review"] == Codes.TRUE_MISSING else td[
        #                 "district_coded"]
        #         }, Metadata(user, Metadata.get_call_location(), time.time()))
        #
        #     # If we failed to find a zone after searching location codes, try inferring from the operator code instead
        #     if td["zone_coded"] not in SomaliaCodes.ZONES:
        #         td.append_data({
        #             "zone_coded": SomaliaLocations.zone_for_operator_code(td["operator"])
        #         }, Metadata(user, Metadata.get_call_location(), time.time()))

        return data
