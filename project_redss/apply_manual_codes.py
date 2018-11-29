import io
import time
from io import BytesIO
from os import path

import pytz
from core_data_modules.cleaners import CharacterCleaner, Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.cleaners.codes import SomaliaCodes
from core_data_modules.cleaners.location_tools import SomaliaLocations
from core_data_modules.data_models import Code
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaIO, TracedDataTheInterfaceIO, TracedDataCoda2IO
from core_data_modules.util import IOUtils
from dateutil.parser import isoparse

from project_redss.lib import MessageFilters
from project_redss.lib.dataset_specification import DatasetSpecification
from project_redss.lib.redss_schemes import CodeSchemes


class ApplyManualCodes(object):
    @classmethod
    def apply_manual_codes(cls, user, data, coda_input_dir, interface_output_dir):
        # Merge manually coded radio show files into the cleaned dataset
        for plan in DatasetSpecification.RQA_CODING_PLANS:
            rqa_messages = [td for td in data if plan.raw_field in td]

            nr_label = CleaningUtils.make_label(
                plan.code_scheme, plan.code_scheme.get_code_with_control_code(Codes.NOT_REVIEWED),
                Metadata.get_call_location()
            )

            coda_input_path = path.join(coda_input_dir, "{}.json".format(plan.coda_filename))
            if path.exists(coda_input_path):
                with open(coda_input_path, "r") as f:
                    TracedDataCoda2IO.import_coda_2_to_traced_data_iterable_multi_coded(
                        user, rqa_messages, plan.id_field, {plan.coded_field: plan.code_scheme.scheme_id}, nr_label, f)
            else:
                # Read from simulated empty file
                TracedDataCoda2IO.import_coda_2_to_traced_data_iterable_multi_coded(
                    user, rqa_messages, plan.id_field, {plan.coded_field: plan.code_scheme.scheme_id}, nr_label,
                    io.StringIO("[]"))

        # Merge manually coded survey files into the cleaned dataset
        for plan in DatasetSpecification.SURVEY_CODING_PLANS:
            nr_label = CleaningUtils.make_label(
                plan.code_scheme, plan.code_scheme.get_code_with_control_code(Codes.NOT_REVIEWED),
                Metadata.get_call_location()
            )

            coda_input_path = path.join(coda_input_dir, "{}.json".format(plan.coda_filename))
            if path.exists(coda_input_path):
                with open(coda_input_path, "r") as f:
                    TracedDataCoda2IO.import_coda_2_to_traced_data_iterable(
                        user, data, plan.id_field, {plan.coded_field: plan.code_scheme.scheme_id}, nr_label, f)
            else:
                # Read from simulated empty file
                TracedDataCoda2IO.import_coda_2_to_traced_data_iterable(
                    user, data, plan.id_field, {plan.coded_field: plan.code_scheme.scheme_id}, nr_label,
                    io.StringIO("[]"))

        # Set district/region/state/zone codes from the coded district field.
        for td in data:
            # Up to 1 location code should have been assigned in Coda. Search for that code,
            # ensuring that only 1 has been assigned or, if multiple have been assigned, that they are non-conflicting
            # control codes
            location_code = None

            for plan in DatasetSpecification.LOCATION_CODING_PLANS:
                coda_coda = plan.code_scheme.get_code_with_id(td[plan.coded_field]["CodeID"])
                if location_code is not None:
                    assert coda_coda.code_id == location_code.code_id or coda_coda.control_code == Codes.NOT_REVIEWED
                if coda_coda.control_code != Codes.NOT_REVIEWED:
                    location_code = coda_coda

            # If no code was found, then this location is still not reviewed.
            # Synthesise a NOT_REVIEWED code accordingly.
            if location_code is None:
                location_code = Code()
                location_code.code_type = "Control"
                location_code.control_code = Codes.NOT_REVIEWED

            # If a control code was found, set all other location keys to that control code,
            # otherwise convert the provided location to the other locations in the hierarchy.
            if location_code.code_type == "Control":
                for plan in DatasetSpecification.LOCATION_CODING_PLANS:
                    td.append_data({
                        plan.coded_field: CleaningUtils.make_label(
                            plan.code_scheme,
                            plan.code_scheme.get_code_with_control_code(location_code.control_code),
                            Metadata.get_call_location()
                        ).to_dict()
                    }, Metadata(user, Metadata.get_call_location(), time.time()))
            else:
                location = location_code.match_values[0]

                def make_location_code(scheme, clean_value):
                    if clean_value == Codes.NOT_CODED:
                        return scheme.get_code_with_control_code(Codes.NOT_CODED)
                    else:
                        return scheme.get_code_with_match_value(clean_value)

                td.append_data({
                    "mogadishu_sub_district_coded": CleaningUtils.make_label(
                        CodeSchemes.MOGADISHU_SUB_DISTRICT,
                        make_location_code(CodeSchemes.MOGADISHU_SUB_DISTRICT,
                                           SomaliaLocations.mogadishu_sub_district_for_location_code(location)),
                        Metadata.get_call_location()).to_dict(),
                    "district_coded": CleaningUtils.make_label(
                        CodeSchemes.DISTRICT,
                        make_location_code(CodeSchemes.DISTRICT,
                                           SomaliaLocations.district_for_location_code(location)),
                        Metadata.get_call_location()).to_dict(),
                    "region_coded": CleaningUtils.make_label(
                        CodeSchemes.REGION,
                        make_location_code(CodeSchemes.REGION,
                                           SomaliaLocations.region_for_location_code(location)),
                        Metadata.get_call_location()).to_dict(),
                    "state": CleaningUtils.make_label(
                        CodeSchemes.STATE,
                        make_location_code(CodeSchemes.STATE,
                                           SomaliaLocations.state_for_location_code(location)),
                        Metadata.get_call_location()).to_dict(),
                    "zone": CleaningUtils.make_label(
                        CodeSchemes.ZONE,
                        make_location_code(CodeSchemes.ZONE,
                                           SomaliaLocations.zone_for_location_code(location)),
                        Metadata.get_call_location()).to_dict()
                }, Metadata(user, Metadata.get_call_location(), time.time()))

        return data
