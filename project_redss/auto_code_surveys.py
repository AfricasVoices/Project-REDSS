import os
import time
from os import path

from core_data_modules.cleaners import Codes, PhoneCleaner
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaIO, TracedDataCoda2IO
from core_data_modules.util import IOUtils

from project_redss.lib import Channels
from project_redss.lib.dataset_specification import DatasetSpecification, OperatorTranslator
from project_redss.lib.redss_schemes import CodeSchemes


class AutoCodeSurveys(object):
    SENT_ON_KEY = "sent_on"

    @classmethod
    def auto_code_surveys(cls, user, data, phone_uuid_table, coda_output_dir):
        # Label missing data
        for td in data:
            missing_dict = dict()
            for plan in DatasetSpecification.SURVEY_CODING_PLANS:
                if plan.raw_field not in td:
                    na_label = CleaningUtils.make_label(
                        plan.code_scheme, plan.code_scheme.get_code_with_control_code(Codes.TRUE_MISSING),
                        Metadata.get_call_location()
                    )
                    missing_dict[plan.coded_field] = na_label.to_dict()
            td.append_data(missing_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        # Auto-code remaining data
        for plan in DatasetSpecification.SURVEY_CODING_PLANS:
            if plan.cleaner is not None:
                CleaningUtils.apply_cleaner_to_traced_data_iterable(user, data, plan.raw_field, plan.coded_field,
                                                                    plan.cleaner, plan.code_scheme)

        # For any locations where the cleaners assigned a code to a sub district, set the district code to NC
        # (this is because only one column should have a value set in Coda)
        for td in data:
            if "mogadishu_sub_district_coded" in td:
                mogadishu_code_id = td["mogadishu_sub_district_coded"]["CodeID"]
                if CodeSchemes.MOGADISHU_SUB_DISTRICT.get_code_with_id(mogadishu_code_id).code_type == "Normal":
                    nc_label = CleaningUtils.make_label(
                        CodeSchemes.MOGADISHU_SUB_DISTRICT,
                        CodeSchemes.MOGADISHU_SUB_DISTRICT.get_code_with_control_code(Codes.NOT_CODED),
                        Metadata.get_call_location(),
                    )
                    td.append_data({"district_coded": nc_label.to_dict()},
                                   Metadata(user, Metadata.get_call_location(), time.time()))

        # Set operator from phone number
        for td in data:
            operator_clean = PhoneCleaner.clean_operator(phone_uuid_table.get_phone(td["uid"]))
            if operator_clean == Codes.NOT_CODED:
                label = CleaningUtils.make_label(
                    CodeSchemes.OPERATOR, CodeSchemes.OPERATOR.get_code_with_control_code(Codes.NOT_CODED),
                    Metadata.get_call_location()
                )
            else:
                label = CleaningUtils.make_label(
                    CodeSchemes.OPERATOR, CodeSchemes.OPERATOR.get_code_with_match_value(operator_clean),
                    Metadata.get_call_location()
                )
            td.append_data({"operator_coded": label.to_dict()}, Metadata(user, Metadata.get_call_location(), time.time()))

        # Label each message with channel keys
        Channels.set_channel_keys(user, data, cls.SENT_ON_KEY)

        # Output single-scheme answers to coda for manual verification + coding
        IOUtils.ensure_dirs_exist(coda_output_dir)
        for plan in DatasetSpecification.SURVEY_CODING_PLANS:
            if plan.raw_field == "mogadishu_sub_district_raw":
                continue
            
            TracedDataCoda2IO.add_message_ids(user, data, plan.raw_field, plan.id_field)

            output_path = path.join(coda_output_dir, f"{plan.coda_filename}.json")
            with open(output_path, "w") as f:
                TracedDataCoda2IO.export_traced_data_iterable_to_coda_2(
                    data, plan.raw_field, plan.time_field, plan.id_field, {plan.coded_field}, f
                )

        # Output location scheme to coda for manual verification + coding
        output_path = path.join(coda_output_dir, "location.json")
        with open(output_path, "w") as f:
            TracedDataCoda2IO.add_message_ids(user, data, "mogadishu_sub_district_raw", "mogadishu_sub_district_id")
            TracedDataCoda2IO.export_traced_data_iterable_to_coda_2(
                data, "mogadishu_sub_district_raw", "mogadishu_sub_district_time", "mogadishu_sub_district_id",
                {"mogadishu_sub_district_coded", "district_coded", "region_coded", "state_coded", "zone_coded"}, f
            )

        return data
