import time
from os import path

from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaV2IO
from dateutil.parser import isoparse

from project_redss.lib.redss_schemes import CodeSchemes


class TranslateRapidProKeys(object):
    RAPID_PRO_KEY_MAP = [
        # List of (new_key, old_key)
        ("uid", "avf_phone_id"),

        ("rqa_s01e01_raw", "Rqa_S01E01 (Value) - csap_s01e01_activation"),
        ("rqa_s01e02_raw", "Rqa_S01E02 (Value) - csap_s01e02_activation"),
        # Not setting weeks 3 or 4 key here because they contain some messages from the other weeks.
        # Special handling is performed in cls.translate_rapid_pro_keys()

        ("rqa_s01e01_run_id", "Rqa_S01E01 (Run ID) - csap_s01e01_activation"),
        ("rqa_s01e02_run_id", "Rqa_S01E02 (Run ID) - csap_s01e02_activation"),
        ("rqa_s01e03_run_id", "Rqa_S01E03 (Run ID) - csap_s01e03_activation"),
        ("rqa_s01e04_run_id", "Rqa_S01E04 (Run ID) - csap_s01e04_activation"),

        ("sent_on", "Rqa_S01E01 (Time) - csap_s01e01_activation"),
        ("sent_on", "Rqa_S01E02 (Time) - csap_s01e02_activation"),
        ("sent_on", "Rqa_S01E03 (Time) - csap_s01e03_activation"),

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

    WEEK_4_TIME_KEY = "Rqa_S01E04 (Time) - csap_s01e04_activation"
    WEEK_4_VALUE_KEY = "Rqa_S01E04 (Value) - csap_s01e04_activation"

    THURSDAY_BURST_START = "2019-01-17T12:03:11+03:00"
    THURSDAY_BURST_END = "2019-01-17T12:24:42+03:00"
    THURSDAY_CORRECTION_TIME = "2018-12-13T00:00:00+03:00"

    FRIDAY_BURST_START = "2019-01-12T09:45:12+03:00"
    FRIDAY_BURST_END = "2019-01-12T09:51:57+03:00"
    FRIDAY_CORRECTION_TIME = "2018-12-14T00:00:00+03:00"

    @classmethod
    def build_message_to_s01e02_dict(cls, user, data, coda_input_dir):
        # Duplicate the input list because reading the file requires appending data to the TracedData,
        # and we don't actually want to modify the input at this stage of the pipeline.
        data = [td.copy() for td in data]

        # Apply the week 3 codes from Coda.
        message_id_key = "radio_show_3_message_id"
        coded_ws_key = "radio_show_3_ws"
        TracedDataCodaV2IO.compute_message_ids(user, data, cls.WEEK_3_VALUE_KEY, message_id_key)
        coda_input_path = path.join(coda_input_dir, "s01e03.json")
        with open(coda_input_path) as f:
            TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable(
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
        """
        Uses the cls.RAPID_PRO_KEY_MAP to rename the keys exported by Rapid Pro to keys which are easier to work
        with in the pipeline. 
        
        Also performs several project-specific redirects of radio show question messages which went into the wrong
        activation flow when running the project. These redirects are described in the comments below. These
        redirects are performed here so that the rest of the pipeline can be given a dataset where it looked like
        nothing went wrong, which means operational corrections shouldn't be needed so much elsewhere.
        """
        
        # Build a map of raw week 3 messages to wrong scheme data
        message_to_s01e02_dict = cls.build_message_to_s01e02_dict(user, data, coda_input_dir)
        
        # Do the actual key mapping
        for td in data:
            mapped_dict = dict()
            
            if cls.WEEK_3_TIME_KEY in td:
                # Redirect any week 3 messages coded as s01e02 in the WS - Correct Dataset scheme to week 2
                if message_to_s01e02_dict.get(td[cls.WEEK_3_VALUE_KEY], False):
                    mapped_dict["rqa_s01e02_raw"] = td[cls.WEEK_3_VALUE_KEY]
                # Redirect any week 4 messages which were in the week 3 flow due to a late flow change-over.
                elif isoparse(td[cls.WEEK_3_TIME_KEY]) > cls.WEEK_4_START:
                    mapped_dict["rqa_s01e04_raw"] = td[cls.WEEK_3_VALUE_KEY]
                else:
                    mapped_dict["rqa_s01e03_raw"] = td[cls.WEEK_3_VALUE_KEY]

            # Redirect any week 2 messages which were in the week 4 flow, due to undelivered messages being delivered
            # in two bursts after the end of the radio shows.
            if cls.WEEK_4_TIME_KEY in td:
                if isoparse(cls.THURSDAY_BURST_START) <= isoparse(td[cls.WEEK_4_TIME_KEY]) < isoparse(cls.THURSDAY_BURST_END):
                    mapped_dict["rqa_s01e02_raw"] = td[cls.WEEK_4_VALUE_KEY]
                    mapped_dict["sent_on"] = cls.THURSDAY_CORRECTION_TIME
                elif isoparse(cls.FRIDAY_BURST_START) <= isoparse(td[cls.WEEK_4_TIME_KEY]) < isoparse(cls.FRIDAY_BURST_END):
                    mapped_dict["rqa_s01e02_raw"] = td[cls.WEEK_4_VALUE_KEY]
                    mapped_dict["sent_on"] = cls.FRIDAY_CORRECTION_TIME
                else:
                    mapped_dict["rqa_s01e04_raw"] = td[cls.WEEK_4_VALUE_KEY]
                    mapped_dict["sent_on"] = td[cls.WEEK_4_TIME_KEY]

            # Translate all other keys
            for new_key, old_key in cls.RAPID_PRO_KEY_MAP:
                if old_key in td:
                    mapped_dict[new_key] = td[old_key]

            if cls.WEEK_3_TIME_KEY in td and message_to_s01e02_dict.get(td[cls.WEEK_3_VALUE_KEY], False):
                # Fake the timestamp of redirected week 3 messages to make it look like they arrived on the day
                # before the incorrect sms ad was sent, i.e. the last day of week 2.
                # This is super yucky, but works because (a) timestamps are never exported, and (b) this date
                # is being set to non_logical anyway in channels.py.
                mapped_dict["sent_on"] = "2018-12-15T00:00:00+03:00"

            td.append_data(mapped_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        return data
