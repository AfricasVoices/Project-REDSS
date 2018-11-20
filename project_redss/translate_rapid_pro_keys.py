import time

from core_data_modules.traced_data import Metadata


class TranslateRapidProKeys(object):
    RAPID_PRO_KEY_MAP = [
            ("rqa_s01e01_raw", "Rqa_S01E01 (Value) - csap_s01e01_activation"),
            ("rqa_s01e02_raw", "Rqa_S01E02 (Value) - csap_s01e02_activation"),
            ("rqa_s01e03_raw", "Rqa_S01E04 (Value) - csap_s01e03_activation"),
            ("rqa_s01e04_raw", "Rqa_S01E04 (Value) - csap_s01e04_activation"),

            ("sent_on", "Rqa_S01E01 (Time) - csap_s01e01_activation"),
            ("sent_on", "Rqa_S01E02 (Time) - csap_s01e02_activation"),
            ("sent_on", "Rqa_S01E03 (Time) - csap_s01e03_activation"),
            ("sent_on", "Rqa_S01E04 (Time) - csap_s01e04_activation")
        ]

    @classmethod
    def translate_rapid_pro_keys(cls, user, data):
        for td in data:
            mapped_dict = dict()
            for new_key, old_key in cls.RAPID_PRO_KEY_MAP:
                if old_key in td:
                    mapped_dict[new_key] = td[old_key]
            td.append_data(mapped_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        return data
