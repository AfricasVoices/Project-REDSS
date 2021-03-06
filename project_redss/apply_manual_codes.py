import time
from os import path

from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.cleaners.location_tools import SomaliaLocations
from core_data_modules.data_models import Code
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaV2IO

from project_redss.lib.pipeline_configuration import PipelineConfiguration
from project_redss.lib.redss_schemes import CodeSchemes


class ApplyManualCodes(object):
    @staticmethod
    def make_location_code(scheme, clean_value):
        if clean_value == Codes.NOT_CODED:
            return scheme.get_code_with_control_code(Codes.NOT_CODED)
        else:
            return scheme.get_code_with_match_value(clean_value)

    @classmethod
    def apply_manual_codes(cls, user, data, coda_input_dir):
        # Merge manually coded radio show files into the cleaned dataset
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            rqa_messages = [td for td in data if plan.raw_field in td]
            coda_input_path = path.join(coda_input_dir, plan.coda_filename)

            f = None
            try:
                if path.exists(coda_input_path):
                    f = open(coda_input_path, "r")
                TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable_multi_coded(
                    user, rqa_messages, plan.id_field, {plan.coded_field: plan.code_scheme}, f)

                if plan.binary_code_scheme is not None:
                    if f is not None:
                        f.seek(0)
                    TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable(
                        user, rqa_messages, plan.id_field, {plan.binary_coded_field: plan.binary_code_scheme}, f)
            finally:
                if f is not None:
                    f.close()

        # Mark data that is noise as Codes.NOT_CODED
        for td in data:
            if td["noise"]:
                nc_dict = dict()
                for plan in PipelineConfiguration.RQA_CODING_PLANS:
                    if plan.coded_field not in td:
                        nc_label = CleaningUtils.make_label_from_cleaner_code(
                            plan.code_scheme, plan.code_scheme.get_code_with_control_code(Codes.NOT_CODED),
                            Metadata.get_call_location()
                        )
                        nc_dict[plan.coded_field] = [nc_label.to_dict()]

                        if plan.binary_code_scheme is not None:
                            nc_label = CleaningUtils.make_label_from_cleaner_code(
                                plan.binary_code_scheme, plan.binary_code_scheme.get_code_with_control_code(Codes.NOT_CODED),
                                Metadata.get_call_location()
                            )
                            nc_dict[plan.binary_coded_field] = nc_label.to_dict()

                td.append_data(nc_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        # Merge manually coded survey files into the cleaned dataset
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            f = None
            try:
                coda_input_path = path.join(coda_input_dir, plan.coda_filename)
                if path.exists(coda_input_path):
                    f = open(coda_input_path, "r")
                TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable(
                    user, data, plan.id_field, {plan.coded_field: plan.code_scheme}, f)
            finally:
                if f is not None:
                    f.close()

        # Set district/region/state/zone codes from the coded district field.
        for td in data:
            # Up to 1 location code should have been assigned in Coda. Search for that code,
            # ensuring that only 1 has been assigned or, if multiple have been assigned, that they are non-conflicting
            # control codes
            location_code = None

            for plan in PipelineConfiguration.LOCATION_CODING_PLANS:
                coda_code = plan.code_scheme.get_code_with_id(td[plan.coded_field]["CodeID"])
                if location_code is not None:
                    if not (coda_code.code_id == location_code.code_id or coda_code.control_code == Codes.NOT_REVIEWED):
                        location_code = Code(None, "Control", None, None, None, None, control_code=Codes.NOT_INTERNALLY_CONSISTENT)
                elif coda_code.control_code != Codes.NOT_REVIEWED:
                    location_code = coda_code

            # If no code was found, then this location is still not reviewed.
            # Synthesise a NOT_REVIEWED code accordingly.
            if location_code is None:
                location_code = Code(None, "Control", None, None, None, None, control_code=Codes.NOT_REVIEWED)

            # If a control code was found, set all other location keys to that control code,
            # otherwise convert the provided location to the other locations in the hierarchy.
            if location_code.code_type == "Control":
                for plan in PipelineConfiguration.LOCATION_CODING_PLANS:
                    td.append_data({
                        plan.coded_field: CleaningUtils.make_label_from_cleaner_code(
                            plan.code_scheme,
                            plan.code_scheme.get_code_with_control_code(location_code.control_code),
                            Metadata.get_call_location()
                        ).to_dict()
                    }, Metadata(user, Metadata.get_call_location(), time.time()))
            else:
                location = location_code.match_values[0]

                td.append_data({
                    "mogadishu_sub_district_coded": CleaningUtils.make_label_from_cleaner_code(
                        CodeSchemes.MOGADISHU_SUB_DISTRICT,
                        cls.make_location_code(CodeSchemes.MOGADISHU_SUB_DISTRICT,
                                               SomaliaLocations.mogadishu_sub_district_for_location_code(location)),
                        Metadata.get_call_location()).to_dict(),
                    "district_coded": CleaningUtils.make_label_from_cleaner_code(
                        CodeSchemes.DISTRICT,
                        cls.make_location_code(CodeSchemes.DISTRICT,
                                               SomaliaLocations.district_for_location_code(location)),
                        Metadata.get_call_location()).to_dict(),
                    "region_coded": CleaningUtils.make_label_from_cleaner_code(
                        CodeSchemes.REGION,
                        cls.make_location_code(CodeSchemes.REGION,
                                               SomaliaLocations.region_for_location_code(location)),
                        Metadata.get_call_location()).to_dict(),
                    "state_coded": CleaningUtils.make_label_from_cleaner_code(
                        CodeSchemes.STATE,
                        cls.make_location_code(CodeSchemes.STATE,
                                               SomaliaLocations.state_for_location_code(location)),
                        Metadata.get_call_location()).to_dict(),
                    "zone_coded": CleaningUtils.make_label_from_cleaner_code(
                        CodeSchemes.ZONE,
                        cls.make_location_code(CodeSchemes.ZONE,
                                               SomaliaLocations.zone_for_location_code(location)),
                        Metadata.get_call_location()).to_dict()
                }, Metadata(user, Metadata.get_call_location(), time.time()))

        return data
