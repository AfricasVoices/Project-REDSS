# Project REDSS
Data pipeline for REDSS/CSAP.

This pipeline fetches all project data from a Rapid Pro instance, and processes it to produce CSV files suitable
for downstream analysis.

## Pre-requisites
Before the pipeline can be run, the following tools must be installed:
 - Docker
 - Bash
 
Development requires the following additional tools:
 - Python 3.6+
 - pipenv
 - git

## Usage
Running the pipeline requires (1) creating a phone number <-> UUID table to support de-identification of 
respondents, (2) fetching all the relevant data from Rapid Pro, and (3) processing the raw data to
produce the outputs required for coding and then for analysis. 

To simplify the configuration and execution of these stages, this project includes a `run_scripts`
directory, which contains shell scripts for driving each of the stages. 
More detailed descriptions of the functions of each of those stages, and instructions for using
the run scripts, are provided below. 

### 1. Phone Number <-> UUID Table
First, create an empty phone number <-> UUID table by running the following command in 
the `run_scripts` directory:

```
$ ./1_create_uuid_table.sh <data-root> 
```

where `data-root` is an absolute path to the directory in which all pipeline data should be stored. 
The UUID table will be saved to a file in the directory `<data-root>/UUIDs`.

### 2. Fetch Raw Data
Next, fetch all the raw data required by the pipeline from Rapid Pro by running the following command in 
the `run_scripts` directory:

`$ ./2_fetch_raw_data.sh <user> <rapid-pro-root> <rapid-pro-server> <rapid-pro-token> <data-root>`.

where:
 - `user` is the identifier of the person running the script, for use in the TracedData Metadata 
   e.g. `user@africasvoices.org`
 - `rapid-pro-root` is an absolute path to the directory to store a local clone of 
   [RapidProTools](https://github.com/AfricasVoices/RapidProTools) in.
   The RapidProTools project hosts the re-usable RapidPro data fetchers.
   The exact version required by this project is checked out automatically.
 - `rapid-pro-server` is the root address of the RapidPro server to retrieve data from e.g. `https://app.rapidpro.io`.
 - `rapid-pro-token` is the access token for this instance of RapidPro. The access token may be found by logging into 
   RapidPro's web interface, then navigating to your organisation page (via the button in the top-right), then copying
   the hexadecimal string given after "Your API Token is ..."
 - `data-root` is an absolute path to the directory in which all pipeline data should be stored.
   Raw data will be saved to TracedData JSON files in `<data-root>/Raw Data`. 

### 3. Generate Outputs
Finally, process the raw data to produce outputs for The Interface, ICR, Coda, and messages/individuals/production
CSVs for final analysis, by running the following command in the `run_scripts` directory.

```
$ ./3_generate_outputs.sh [--drive-upload <drive-service-account-credentials-url> <drive-upload-dir>] <user> <data-root>
```

where:
 - `--drive-upload` is an optional flag for uploading the messages, individuals, and production CSVs to Drive.
   If this flag set, pass the arguments:
  - `drive-service-account-credentials-url`, a gs URL to the private credentials file of a Google Drive service account.
    This service account will be used to upload outputted data for analysis to a directory on Google Drive.
  - `drive-upload-dir`, the path to a directory in Google Drive to upload the messages, individuals, and production 
    CSVs to. Before files can be uploaded to a directory, the directory must be shared with the service account's 
    email address (which can be found in the `client_email` field of the service account's credentials file).
 - `user` is the identifier of the person running the script, for use in the TracedData Metadata 
   e.g. `user@africasvoices.org`.
 - `data-root` is an absolute path to the directory in which all pipeline data should be stored.
   Updated Coda files containing new data to be coded will be saved in `<data-root>/Raw Data`.
   All other output files will be saved in `<data-root>/Outputs`.
   
As well as uploading the messages, individuals, and production CSVs to Drive, this stage outputs the following to
`<data-root>/Outputs`:
 - Local copies of the messages, individuals, and production CSVs (`csap_mes.csv`, `csap_ind.csv`, `csap_prod.csv`)
 - A serialized export of the list of TracedData objects representing all the data that was exported for analysis 
   (`traced_data.json`)
 - For each week of radio shows, a random sample of 200 messages that weren't classified as noise, for use in ICR (`ICR/`)
 - Coda V2 messages files for each dataset (`Coda Files/<dataset>.json`). To upload these datasets to Coda, use the 
   `set.py` or `add.py` tools in the [Coda V2](https://github.com/AfricasVoices/CodaV2/tree/master/data_tools) repository.
 
To make coded data available to the pipeline, use the `get.py` script in `CodaV2/data_tools` to export coded messages
to `<data-root>/Coded Coda Files/<dataset>.json`
