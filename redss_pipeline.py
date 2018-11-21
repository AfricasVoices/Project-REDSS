import argparse
import time

from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils, PhoneNumberUuidTable

from project_redss import AnalysisFile
from project_redss import ApplyManualCodes
from project_redss import AutoCodeShowMessages
from project_redss import AutoCodeSurveys
from project_redss import CombineRawDatasets
from project_redss.lib import AnalysisKeys
from project_redss.translate_rapid_pro_keys import TranslateRapidProKeys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runs the post-fetch phase of the REDSS pipeline")

    parser.add_argument("user", help="User launching this program")

    parser.add_argument("phone_number_uuid_table_path", metavar="phone-number-uuid-table-path",
                        help="JSON file containing the phone number <-> UUID lookup table for the messages/surveys "
                             "datasets")
    parser.add_argument("s01e01_input_path", metavar="s01e01-input-path",
                        help="Path to the episode 1 raw messages JSON file, containing a list of serialized TracedData "
                             "objects")
    parser.add_argument("s01e02_input_path", metavar="s01e02-input-path",
                        help="Path to the episode 2 raw messages JSON file, containing a list of serialized TracedData "
                             "objects")
    parser.add_argument("s01e03_input_path", metavar="s01e03-input-path",
                        help="Path to the episode 3 raw messages JSON file, containing a list of serialized TracedData "
                             "objects")
    parser.add_argument("s01e04_input_path", metavar="s01e04-input-path",
                        help="Path to the episode 4 raw messages JSON file, containing a list of serialized TracedData "
                             "objects")
    parser.add_argument("demog_input_path", metavar="demog-input-path",
                        help="Path to the raw demographics JSON file, containing a list of serialized TracedData "
                             "objects")
    parser.add_argument("evaluation_input_path", metavar="evaluation-input-path",
                        help="Path to the raw evaluation JSON file, containing a list of serialized TracedData "
                             "objects")
    parser.add_argument("prev_coded_dir_path", metavar="prev-coded-dir-path",
                        help="Directory containing Coda files generated by a previous run of this pipeline. "
                             "New data will be appended to these files.")

    parser.add_argument("json_output_path", metavar="json-output-path",
                        help="Path to a JSON file to write TracedData for final analysis file to")
    parser.add_argument("interface_output_dir", metavar="interface-output-dir",
                        help="Path to a directory to write The Interface files to")
    parser.add_argument("icr_output_dir", metavar="icr-output-dir",
                        help="Directory to write CSV files to, each containing 200 messages and message ids for use " 
                             "in inter-code reliability evaluation"),
    parser.add_argument("coded_dir_path", metavar="coded-dir-path",
                        help="Directory to write coded Coda files to")
    parser.add_argument("csv_by_message_output_path", metavar="csv-by-message-output-path",
                        help="Analysis dataset where messages are the unit for analysis (i.e. one message per row)")
    parser.add_argument("csv_by_individual_output_path", metavar="csv-by-individual-output-path",
                        help="Analysis dataset where respondents are the unit for analysis (i.e. one respondent "
                             "per row, with all their messages joined into a single cell).")

    args = parser.parse_args()
    user = args.user

    phone_number_uuid_table_path = args.phone_number_uuid_table_path
    s01e01_input_path = args.s01e01_input_path
    s01e02_input_path = args.s01e02_input_path
    s01e03_input_path = args.s01e03_input_path
    s01e04_input_path = args.s01e04_input_path
    demog_input_path = args.demog_input_path
    evaluation_input_path = args.evaluation_input_path
    prev_coded_dir_path = args.prev_coded_dir_path

    json_output_path = args.json_output_path
    interface_output_dir = args.interface_output_dir
    icr_output_dir = args.icr_output_dir
    coded_dir_path = args.coded_dir_path
    csv_by_message_output_path = args.csv_by_message_output_path
    csv_by_individual_output_path = args.csv_by_individual_output_path

    message_paths = [s01e01_input_path, s01e02_input_path, s01e03_input_path, s01e04_input_path]

    # Load phone number <-> UUID table
    print("Loading Phone Number <-> UUID Table...")
    with open(phone_number_uuid_table_path, "r") as f:
        phone_number_uuid_table = PhoneNumberUuidTable.load(f)

    # Load messages
    messages_datasets = []
    for i, path in enumerate(message_paths):
        print("Loading Episode {}/{}...".format(i + 1, len(message_paths)))
        with open(path, "r") as f:
            messages_datasets.append(TracedDataJsonIO.import_json_to_traced_data_iterable(f))

    # Load surveys
    print("Loading Demographics...")
    with open(demog_input_path, "r") as f:
        demographics = TracedDataJsonIO.import_json_to_traced_data_iterable(f)

    print("Loading Evaluation...")
    with open(evaluation_input_path, "r") as f:
        evaluation = TracedDataJsonIO.import_json_to_traced_data_iterable(f)

    # Add survey data to the messages
    print("Combining Datasets...")
    data = CombineRawDatasets.combine_raw_datasets(user, messages_datasets, [demographics, evaluation])

    print("Translating Rapid Pro Keys...")
    data = TranslateRapidProKeys.translate_rapid_pro_keys(user, data)

    print("Auto Coding Messages...")
    data = AutoCodeShowMessages.auto_code_show_messages(user, data, icr_output_dir, coded_dir_path)

    # print("Auto Coding Surveys...")
    # data = AutoCodeSurveys.auto_code_surveys(user, data, phone_number_uuid_table, coded_dir_path, prev_coded_dir_path)
    #
    # print("Applying Manual Codes from Coda...")
    # data = ApplyManualCodes.apply_manual_codes(user, data, prev_coded_dir_path, interface_output_dir)
    #
    # print("Generating Analysis CSVs...")
    # data = AnalysisFile.generate(user, data, csv_by_message_output_path, csv_by_individual_output_path)

    # Write json output
    print("Writing TracedData to file...")
    IOUtils.ensure_dirs_exist_for_file(json_output_path)
    with open(json_output_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(data, f, pretty_print=True)
