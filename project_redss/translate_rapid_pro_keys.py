import time

from core_data_modules.traced_data import Metadata
from dateutil.parser import isoparse


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
    def translate_rapid_pro_keys(cls, user, data):
        for td in data:
            mapped_dict = dict()

            # Redirect any week 4 messages which were in the week 3 flow due to a late flow change-over.
            if cls.WEEK_3_TIME_KEY in td:
                if isoparse(td[cls.WEEK_3_TIME_KEY]) > cls.WEEK_4_START:
                    mapped_dict["rqa_s01e04_raw"] = td[cls.WEEK_3_VALUE_KEY]
                else:
                    mapped_dict["rqa_s01e03_raw"] = td[cls.WEEK_3_VALUE_KEY]

            # Translate all other keys
            for new_key, old_key in cls.RAPID_PRO_KEY_MAP:
                if old_key in td:
                    mapped_dict[new_key] = td[old_key]

            td.append_data(mapped_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        return data
