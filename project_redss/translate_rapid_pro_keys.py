import time
from os import path

from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCoda2IO
from dateutil.parser import isoparse

from project_redss.lib.redss_schemes import CodeSchemes


class TranslateRapidProKeys(object):
    RAPID_PRO_KEY_MAP = [
        # List of (new_key, old_key)
        ("uid", "avf_phone_id"),

        ("rqa_s01e01_raw", "Rqa_S01E01 (Value) - csap_s01e01_activation"),
        ("rqa_s01e02_raw", "Rqa_S01E02 (Value) - csap_s01e02_activation"),
        # Not setting week 3 key here because it contains some week 4 messages which require special handling.
        # That special handling is performed in cls.translate_rapid_pro_keys()
        ("rqa_s01e04_raw", "Rqa_S01E04 (Value) - csap_s01e04_activation"),

        ("rqa_s01e01_run_id", "Rqa_S01E01 (Run ID) - csap_s01e01_activation"),
        ("rqa_s01e02_run_id", "Rqa_S01E02 (Run ID) - csap_s01e02_activation"),
        ("rqa_s01e03_run_id", "Rqa_S01E03 (Run ID) - csap_s01e03_activation"),
        ("rqa_s01e04_run_id", "Rqa_S01E04 (Run ID) - csap_s01e04_activation"),

        ("sent_on", "Rqa_S01E01 (Time) - csap_s01e01_activation"),
        ("sent_on", "Rqa_S01E02 (Time) - csap_s01e02_activation"),
        ("sent_on", "Rqa_S01E03 (Time) - csap_s01e03_activation"),
        ("sent_on", "Rqa_S01E04 (Time) - csap_s01e04_activation"),

        ("gender_raw", "Gender (Value) - csap_demog"),
        ("gender_time", "Gender (Time) - csap_demog"),
        ("mogadishu_sub_district_raw", "Mog_Sub_District (Value) - csap_demog"),
        ("mogadishu_sub_district_time", "Mog_Sub_District (Time) - csap_demog"),
        ("age_raw", "Age (Value) - csap_demog"),
        ("age_time", "Age (Time) - csap_demog"),
        ("idp_camp_raw", "Idp_Camp (Value) - csap_demog"),
        ("idp_camp_time", "Idp_Camp (Time) - csap_demog"),
        ("recently_displaced_raw", "Recently_Displaced (Value) - csap_demog"),
        ("recently_displaced_time", "Recently_Displaced (Time) - csap_demog"),
        ("hh_language_raw", "Hh_Language (Value) - csap_demog"),
        ("hh_language_time", "Hh_Language (Time) - csap_demog"),

        ("repeated_raw", "Repeated (Value) - csap_evaluation"),
        ("repeated_time", "Repeated (Time) - csap_evaluation"),
        ("involved_raw", "Involved (Value) - csap_evaluation"),
        ("involved_time", "Involved (Time) - csap_evaluation")
    ]

    WEEK_3_TIME_KEY = "Rqa_S01E03 (Time) - csap_s01e03_activation"
    WEEK_3_VALUE_KEY = "Rqa_S01E03 (Value) - csap_s01e03_activation"
    WEEK_4_START = isoparse("2018-12-23T00:00:00+03:00")

    @classmethod
    def build_message_to_s01e02_dict(cls, user, data, coda_input_dir):
        # Duplicate the input list because reading the file requires appending data to the TracedData,
        # and we don't actually want to modify the input at this stage of the pipeline.
        data = [td.copy() for td in data]

        # Apply the week 3 codes from Coda.
        message_id_key = "radio_show_3_message_id"
        coded_ws_key = "radio_show_3_ws"
        TracedDataCoda2IO.add_message_ids(user, data, cls.WEEK_3_VALUE_KEY, message_id_key)
        coda_input_path = path.join(coda_input_dir, "s01e03.json")
        with open(coda_input_path) as f:
            TracedDataCoda2IO.import_coda_2_to_traced_data_iterable(
                user, data, message_id_key, {coded_ws_key: CodeSchemes.WS_CORRECT_DATASET}, f)

        # Parse the loaded codes into a look-up table of raw message string -> is ws boolean.
        message_to_ws_dict = dict()
        for td in data:
            label = td.get(coded_ws_key)
            if label is not None:
                message_to_ws_dict[td.get(cls.WEEK_3_VALUE_KEY)] = \
                    label["CodeID"] == CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01e02").code_id

        return message_to_ws_dict

    @classmethod
    def translate_rapid_pro_keys(cls, user, data, coda_input_dir):
        # Build a map of raw week 3 messages to wrong scheme data
        message_to_s01e02_dict = cls.build_message_to_s01e02_dict(user, data, coda_input_dir)
        
        # Do the actual key mapping
        for td in data:
            mapped_dict = dict()
            
            if cls.WEEK_3_TIME_KEY in td:
                # Redirect any week 3 messages coded as s01e02 in the WS - Correct Dataset scheme to week 2
                if message_to_s01e02_dict.get(td[cls.WEEK_3_VALUE_KEY], False):
                    print(f"redirected '{td[cls.WEEK_3_VALUE_KEY]}'")
                    mapped_dict["rqa_s01e02_raw"] = td[cls.WEEK_3_VALUE_KEY]
                # Redirect any week 4 messages which were in the week 3 flow due to a late flow change-over.
                elif isoparse(td[cls.WEEK_3_TIME_KEY]) > cls.WEEK_4_START:
                    mapped_dict["rqa_s01e04_raw"] = td[cls.WEEK_3_VALUE_KEY]
                else:
                    mapped_dict["rqa_s01e03_raw"] = td[cls.WEEK_3_VALUE_KEY]

            # Translate all other keys
            for new_key, old_key in cls.RAPID_PRO_KEY_MAP:
                if old_key in td:
                    mapped_dict[new_key] = td[old_key]

            td.append_data(mapped_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        return data
