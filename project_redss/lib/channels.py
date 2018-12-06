import time

from core_data_modules.cleaners import Codes
from core_data_modules.traced_data import Metadata
from dateutil.parser import isoparse


class Channels(object):
    SMS_AD_KEY = "sms_ad"
    RADIO_PROMO_KEY = "radio_promo"
    RADIO_SHOW_KEY = "radio_show"
    NON_LOGICAL_KEY = "non_logical_time"
    S01E01_KEY = "radio_participation_s01e01"
    S01E02_KEY = "radio_participation_s01e02"
    S01E03_KEY = "radio_participation_s01e03"
    S01E04_KEY = "radio_participation_s01e04"

    SMS_AD_RANGES = [
        # TODO
    ]
    RADIO_PROMO_RANGES = [
        # TODO
    ]
    RADIO_SHOW_RANGES = [
        # TODO
    ]

    S01E01_RANGES = [
        # TODO
    ]

    S01E02_RANGES = [
        # TODO
    ]

    S01E03_RANGES = [
        # TODO
    ]

    S01E04_RANGES = [
        # TODO
    ]

    RANGES = {
        SMS_AD_KEY: SMS_AD_RANGES,
        RADIO_PROMO_KEY: RADIO_PROMO_RANGES,
        RADIO_SHOW_KEY: RADIO_SHOW_RANGES,
        S01E01_KEY: S01E01_RANGES,
        S01E02_KEY: S01E02_RANGES,
        S01E03_KEY: S01E03_RANGES,
        S01E04_KEY: S01E04_RANGES
    }

    @staticmethod
    def timestamp_is_in_ranges(timestamp, ranges):
        for range in ranges:
            if isoparse(range[0]) <= timestamp < isoparse(range[1]):
                return True
        return False

    @classmethod
    def set_channel_keys(cls, user, data, time_key):
        for td in data:
            timestamp = isoparse(td[time_key])

            channel_dict = dict()
            time_range_matches = 0
            for key, ranges in cls.RANGES.items():
                if cls.timestamp_is_in_ranges(timestamp, ranges):
                    time_range_matches += 1
                    channel_dict[key] = Codes.TRUE
                else:
                    channel_dict[key] = Codes.FALSE

            assert time_range_matches <= 1, f"Time '{td[time_key]}' matches multiple time ranges"

            if time_range_matches == 0:
                channel_dict[cls.NON_LOGICAL_KEY] = Codes.TRUE
            else:
                channel_dict[cls.NON_LOGICAL_KEY] = Codes.FALSE

            td.append_data(channel_dict, Metadata(user, Metadata.get_call_location(), time.time()))
