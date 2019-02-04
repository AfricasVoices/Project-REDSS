import argparse
import json
import os
import subprocess
import tempfile

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetches all the raw data for this project from Rapid Pro. "
                                                 "This script must be run from its parent directory.")

    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file")
    parser.add_argument("rapid_pro_tools_dir", metavar="rapid-pro-tools-dir",
                        help="Path to a directory to checkout the required version of RapidProTools to")
    parser.add_argument("root_data_dir", metavar="root-data-dir",
                        help="Path to the root of the project data directory")

    args = parser.parse_args()

    user = args.user
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    rapid_pro_tools_dir = args.rapid_pro_tools_dir
    root_data_dir = os.path.abspath(args.root_data_dir)

    uuid_table_path = f"{root_data_dir}/UUIDs/phone_uuids.json"

    SHOWS = [
        "csap_s01e01_activation",
        "csap_s01e02_activation",
        "csap_s01e03_activation",
        "csap_s01e04_activation"
    ]

    SURVEYS = [
        "csap_demog",
        "csap_evaluation"
    ]

    TEST_CONTACTS_PATH = os.path.abspath("./test_contacts.json")

    # Read the settings from the configuration file
    with open(pipeline_configuration_file_path) as f:
        pipeline_config = json.load(f)

        rapid_pro_base_url = pipeline_config["RapidProBaseURL"]
        rapid_pro_token_file_url = pipeline_config["RapidProTokenFileURL"]

    # Download/checkout the appropriate version of RapidProTools
    exit_code = subprocess.call(["./checkout_rapid_pro_tools.sh", rapid_pro_tools_dir])
    assert exit_code == 0, f"./checkout_rapid_pro_tools.sh failed with exit_code {exit_code}"

    # Fetch the Rapid Pro Token from the Google Cloud URL and load it into memory
    temp_dir = tempfile.mkdtemp()
    rapid_pro_token_local_file_url = f"{temp_dir}/rapid_pro_token.txt"
    exit_code = subprocess.call(["gsutil", "cp", rapid_pro_token_file_url, rapid_pro_token_local_file_url])
    assert exit_code == 0, f"gsutil failed with exit_code {exit_code}"
    with open(rapid_pro_token_local_file_url) as f:
        rapid_pro_token = f.read().strip()

    # Download all the runs for each of the radio shows
    for show in SHOWS:
        output_file_path = f"{root_data_dir}/Raw Data/{show}.json"
        print(f"Exporting show '{show}' to '{output_file_path}'...")

        exit_code = subprocess.call([
            "./docker-run.sh",
            "--flow-name", show,
            "--test-contacts-path", TEST_CONTACTS_PATH,
            rapid_pro_base_url,
            rapid_pro_token,
            user,
            "all",
            uuid_table_path,
            output_file_path
        ], cwd=f"{rapid_pro_tools_dir}/fetch_runs")

        assert exit_code == 0, f"{rapid_pro_tools_dir}/fetch_runs/fetch_runs.py failed with exit_code {exit_code}"

    # Download all the runs for each of the surveys
    for survey in SURVEYS:
        output_file_path = f"{root_data_dir}/Raw Data/{survey}.json"
        print(f"Exporting survey '{survey}' to '{output_file_path}'...")

        exit_code = subprocess.call([
            "./docker-run.sh",
            "--flow-name", survey,
            "--test-contacts-path", TEST_CONTACTS_PATH,
            rapid_pro_base_url,
            rapid_pro_token,
            user,
            "latest-only",
            uuid_table_path,
            output_file_path
        ], cwd=f"{rapid_pro_tools_dir}/fetch_runs")

        assert exit_code == 0, f"{rapid_pro_tools_dir}/fetch_runs/fetch_runs.py failed with exit_code {exit_code}"
