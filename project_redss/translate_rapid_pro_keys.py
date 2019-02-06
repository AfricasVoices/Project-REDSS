from os import path

from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCoda2IO
from core_data_modules.util import TimeUtils
from dateutil.parser import isoparse

from project_redss.lib.redss_schemes import CodeSchemes


class TranslateRapidProKeys(object):
    # TODO: Move the constants in this file to configuration json
    RAPID_PRO_KEY_MAP = [
        # List of (new_key, old_key)
        ("uid", "avf_phone_id"),

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

    SHOW_ID_MAP = {
        "Rqa_S01E01 (Value) - csap_s01e01_activation": 1,
        "Rqa_S01E02 (Value) - csap_s01e02_activation": 2,
        "Rqa_S01E03 (Value) - csap_s01e03_activation": 3,
        "Rqa_S01E04 (Value) - csap_s01e04_activation": 4
    }

    RAW_ID_MAP = {
        1: "rqa_s01e01_raw",
        2: "rqa_s01e02_raw",
        3: "rqa_s01e03_raw",
        4: "rqa_s01e04_raw"
    }

    WEEK_3_TIME_KEY = "Rqa_S01E03 (Time) - csap_s01e03_activation"
    WEEK_3_VALUE_KEY = "Rqa_S01E03 (Value) - csap_s01e03_activation"
    WEEK_4_START = "2018-12-23T00:00:00+03:00"

    WEEK_4_TIME_KEY = "Rqa_S01E04 (Time) - csap_s01e04_activation"
    WEEK_4_VALUE_KEY = "Rqa_S01E04 (Value) - csap_s01e04_activation"

    THURSDAY_BURST_START = "2019-01-17T12:03:11+03:00"
    THURSDAY_BURST_END = "2019-01-17T12:24:42+03:00"
    THURSDAY_CORRECTION_TIME = "2018-12-13T00:00:00+03:00"

    FRIDAY_BURST_START = "2019-01-12T09:45:12+03:00"
    FRIDAY_BURST_END = "2019-01-12T09:51:57+03:00"
    FRIDAY_CORRECTION_TIME = "2018-12-14T00:00:00+03:00"

    @classmethod
    def _build_message_to_s01e02_dict(cls, user, data, coda_input_dir):
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
    def set_show_ids(cls, user, data, show_id_map):
        """
        Sets a show_id for each message, using the presence of Rapid Pro value keys to determine which show each message
        belongs to.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to set the show ids of.
        :type data: iterable of TracedData
        :param show_id_map: Dictionary of Rapid Pro value key to show id.
        :type show_id_map: dict of str -> int
        """
        for td in data:
            show_dict = dict()

            for message_key, show_id in show_id_map.items():
                if message_key in td:
                    assert "rqa_message" not in show_dict
                    show_dict["rqa_message"] = td[message_key]
                    show_dict["show_id"] = show_id

            td.append_data(show_dict, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    @classmethod
    def remap_radio_shows(cls, user, data, coda_input_dir):
        """
        Remaps radio shows which were in the wrong flow, and therefore have the wrong key/values set, to have the
        key/values they would have had if they had been received by the correct flow.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to move the radio show messages in.
        :type data: iterable of TracedData
        :param coda_input_dir: Directory to read coded coda files from.
        :type coda_input_dir: str
        """
        # TODO: Convert the show remapping code here into reusable functions for each case that they handle.
        #       Note that ultimately we probably don't want to handle the 'WS' show remapping here,
        #       because we get that for free when we implement 'WS' handling properly.

        # Build a map of raw week 3 messages to wrong scheme data
        message_to_s01e02_dict = cls._build_message_to_s01e02_dict(user, data, coda_input_dir)

        for td in data:
            mapped_dict = dict()

            if cls.WEEK_3_TIME_KEY in td:
                # Redirect any week 3 messages coded as s01e02 in the WS - Correct Dataset scheme to week 2
                # Also, fake the timestamp of redirected week 3 messages to make it look like they arrived on the day
                # before the incorrect sms ad was sent, i.e. the last day of week 2.
                # This is super yucky, but works because (a) timestamps are never exported, and (b) this date
                # is being set to non_logical anyway in channels.py.
                if message_to_s01e02_dict.get(td["rqa_message"], False):
                    mapped_dict["show_id"] = 2
                    mapped_dict["sent_on"] = "2018-12-15T00:00:00+03:00"

                # Redirect any week 4 messages which were in the week 3 flow due to a late flow change-over.
                elif isoparse(td[cls.WEEK_3_TIME_KEY]) > isoparse(cls.WEEK_4_START):
                    mapped_dict["show_id"] = 4

            # Redirect any week 2 messages which were in the week 4 flow, due to undelivered messages being delivered
            # in two bursts after the end of the radio shows.
            if cls.WEEK_4_TIME_KEY in td:
                if isoparse(cls.THURSDAY_BURST_START) <= isoparse(td[cls.WEEK_4_TIME_KEY]) < isoparse(cls.THURSDAY_BURST_END):
                    mapped_dict["show_id"] = 2
                    mapped_dict["sent_on"] = cls.THURSDAY_CORRECTION_TIME
                elif isoparse(cls.FRIDAY_BURST_START) <= isoparse(td[cls.WEEK_4_TIME_KEY]) < isoparse(cls.FRIDAY_BURST_END):
                    mapped_dict["show_id"] = 2
                    mapped_dict["sent_on"] = cls.FRIDAY_CORRECTION_TIME

            td.append_data(mapped_dict, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    @classmethod
    def remap_key_names(cls, user, data, key_map):
        """
        Remaps key names.
        
        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to remap the key names of.
        :type data: iterable of TracedData
        :param key_map: Iterable of (new_key, old_key).
        :type key_map: iterable of (str, str)
        """
        for td in data:
            remapped = dict()
               
            for new_key, old_key in key_map:
                if old_key in td and new_key not in td:
                    remapped[new_key] = td[old_key]

            td.append_data(remapped, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    @classmethod
    def set_rqa_raw_keys_from_show_ids(cls, user, data, raw_id_map):
        """
        Despite the earlier phases of this pipeline stage using a common 'rqa_message' field and then a 'show_id'
        field to identify which radio show a message belonged to, the rest of the pipeline still uses the presence
        of a raw field for each show to determine which show a message belongs to. This function translates from
        the new 'show_id' method back to the old 'raw field presence` method.
        
        TODO: Update the rest of the pipeline to use show_ids, and/or perform remapping before combining the datasets.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to set raw radio show message fields for.
        :type data: iterable of TracedData
        :param raw_id_map: Dictionary of show id to the rqa message key to assign each td[rqa_message} to.
        :type raw_id_map: dict of int -> str
        """
        for td in data:
            for show_id, message_key in raw_id_map.items():
                if "rqa_message" in td and td.get("show_id") == show_id:
                    td.append_data({message_key: td["rqa_message"]},
                                   Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    @classmethod
    def translate_rapid_pro_keys(cls, user, data, coda_input_dir):
        """
        Remaps the keys of rqa messages in the wrong flow into the correct one, and remaps all Rapid Pro keys to
        more usable keys that can be used by the rest of the pipeline.
        
        TODO: Break this function such that the show remapping phase happens in one class, and the Rapid Pro remapping
              in another?
        """
        # Set a show id field for each message, using the presence of Rapid Pro value keys in the TracedData.
        # Show ids are necessary in order to be able to remap radio shows and key names separately (because data
        # can't be 'deleted' from TracedData).
        cls.set_show_ids(user, data, cls.SHOW_ID_MAP)

        # Move rqa messages which ended up in the wrong flow to the correct one.
        cls.remap_radio_shows(user, data, coda_input_dir)

        # Remap the keys used by Rapid Pro to more usable key names that will be used by the rest of the pipeline.
        cls.remap_key_names(user, data, cls.RAPID_PRO_KEY_MAP)

        # Convert from the new show id format to the raw field format still used by the rest of the pipeline.
        cls.set_rqa_raw_keys_from_show_ids(user, data, cls.RAW_ID_MAP)

        return data
