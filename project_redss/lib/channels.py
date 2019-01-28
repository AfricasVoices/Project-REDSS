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

    # Time ranges expressed in format (start_of_range_inclusive, end_of_range_exclusive)
    SMS_AD_RANGES = [
        ("2018-12-02T19:25:19+03:00", "2018-12-03T00:00:00+03:00"),  # (not using ...-02T24:... due to an isoparse bug)
        ("2018-12-09T19:00:30+03:00", "2018-12-10T00:00:00+03:00"),
        ("2018-12-16T19:00:55+03:00", "2018-12-17T00:00:00+03:00"),
        ("2018-12-23T19:00:45+03:00", "2018-12-24T00:00:00+03:00")
    ]

    RADIO_PROMO_RANGES = [
        ("2018-12-02T00:00:00+03:00", "2018-12-02T19:25:19+03:00"),
        ("2018-12-03T00:00:00+03:00", "2018-12-05T00:00:00+03:00"),

        ("2018-12-09T00:00:00+03:00", "2018-12-09T19:00:30+03:00"),
        ("2018-12-10T00:00:00+03:00", "2018-12-12T00:00:00+03:00"),

        ("2018-12-16T00:00:00+03:00", "2018-12-16T19:00:55+03:00"),
        ("2018-12-17T00:00:00+03:00", "2018-12-19T00:00:00+03:00"),

        ("2018-12-23T00:00:00+03:00", "2018-12-23T19:00:45+03:00"),
        ("2018-12-24T00:00:00+03:00", "2018-12-26T00:00:00+03:00")
    ]

    RADIO_SHOW_RANGES = [
        ("2018-12-06T00:00:00+03:00", "2018-12-07T00:00:00+03:00"),
        ("2018-12-13T00:00:00+03:00", "2018-12-14T00:00:00+03:00"),
        ("2018-12-20T00:00:00+03:00", "2018-12-21T00:00:00+03:00"),
        ("2018-12-27T00:00:00+03:00", "2018-12-28T00:00:00+03:00")
    ]

    S01E01_RANGES = [
        ("2018-12-02T00:00:00+03:00", "2018-12-09T00:00:00+03:00")
    ]

    S01E02_RANGES = [
        ("2018-12-09T00:00:00+03:00", "2018-12-16T00:00:00+03:00")
    ]

    S01E03_RANGES = [
        ("2018-12-16T00:00:00+03:00", "2018-12-23T00:00:00+03:00")
    ]

    S01E04_RANGES = [
        ("2018-12-23T00:00:00+03:00", "2018-12-31T00:00:00+03:00")
    ]

    CHANNEL_RANGES = {
        SMS_AD_KEY: SMS_AD_RANGES,
        RADIO_PROMO_KEY: RADIO_PROMO_RANGES,
        RADIO_SHOW_KEY: RADIO_SHOW_RANGES
    }

    SHOW_RANGES = {
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

            # Set channel ranges
            time_range_matches = 0
            for key, ranges in cls.CHANNEL_RANGES.items():
                if cls.timestamp_is_in_ranges(timestamp, ranges):
                    time_range_matches += 1
                    channel_dict[key] = Codes.TRUE
                else:
                    channel_dict[key] = Codes.FALSE

            # Set time as NON_LOGICAL if it doesn't fall in range of the **sms ad/radio promo/radio_show**
            if time_range_matches == 0:
                # Assert in range of project
                assert isoparse("2018-12-02T00:00:00+03:00") <= timestamp < isoparse("2018-12-31T00:00:00+03:00"), \
                    f"Timestamp {td[time_key]} out of range of project"
                channel_dict[cls.NON_LOGICAL_KEY] = Codes.TRUE
            else:
                assert time_range_matches == 1, f"Time '{td[time_key]}' matches multiple time ranges"
                channel_dict[cls.NON_LOGICAL_KEY] = Codes.FALSE

            # Set show ranges
            for key, ranges in cls.SHOW_RANGES.items():
                if cls.timestamp_is_in_ranges(timestamp, ranges):
                    channel_dict[key] = Codes.TRUE
                else:
                    channel_dict[key] = Codes.FALSE

            td.append_data(channel_dict, Metadata(user, Metadata.get_call_location(), time.time()))
