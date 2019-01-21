import json

from core_data_modules.data_models import Scheme


def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return Scheme.from_firebase_map(firebase_map)


class CodeSchemes(object):
    S01E01 = _open_scheme("s01e01.json")
    S01E02 = _open_scheme("s01e02.json")
    S01E03_REASONS = _open_scheme("s01e03_reasons.json")
    S01E04 = _open_scheme("s01e04.json")

    OPERATOR = _open_scheme("operator.json")

    GENDER = _open_scheme("gender.json")
    MOGADISHU_SUB_DISTRICT = _open_scheme("mogadishu_sub_district.json")
    DISTRICT = _open_scheme("district.json")
    REGION = _open_scheme("region.json")
    STATE = _open_scheme("state.json")
    ZONE = _open_scheme("zone.json")
    AGE = _open_scheme("age.json")
    IDP_CAMP = _open_scheme("idp_camp.json")
    RECENTLY_DISPLACED = _open_scheme("recently_displaced.json")
    HH_LANGUAGE = _open_scheme("hh_language.json")

    REPEATED = _open_scheme("repeated.json")
    INVOLVED = _open_scheme("involved.json")
