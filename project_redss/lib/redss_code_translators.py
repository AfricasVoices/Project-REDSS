from core_data_modules.cleaners import CodeTranslator, Codes

S01E01Translator = CodeTranslator("Scheme-d7eb83ef", {
    # TODO once set in code scheme

    Codes.TRUE_MISSING: "code-NA-f93d3eb7",
    Codes.SKIPPED: "code-NS-2c11b7c9",
    Codes.NOT_CODED: "code-NC-42f1d983",
    Codes.NOT_REVIEWED: "code-NR-5e3eee23",
    Codes.STOP: "code-STOP-08b832a8",
    Codes.WRONG_SCHEME: "code-WS-adb25603b7af"
})

GenderTranslator = CodeTranslator("Scheme-12cb6f95", {
    Codes.MALE: "code-63dcde9a",
    Codes.FEMALE: "code-86a4602c",

    Codes.TRUE_MISSING: "code-NA-f93d3eb7",
    Codes.SKIPPED: "code-NS-2c11b7c9",
    Codes.NOT_CODED: "code-NC-42f1d983",
    Codes.NOT_REVIEWED: "code-NR-5e3eee23",
    Codes.STOP: "code-STOP-08b832a8",
    Codes.WRONG_SCHEME: "code-WS-adb25603b7af"
})
