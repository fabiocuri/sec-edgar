import ast
import concurrent.futures
import os
import re
from collections import defaultdict
from itertools import chain

import numpy as np
import pandas as pd
import spacy
import yaml
from geotext import GeoText
from google.cloud import bigquery, secretmanager, storage
from sec_api import QueryApi, RenderApi
from tqdm import tqdm

from handlers import upload_table_to_bigquery

nlp = spacy.load("en_core_web_sm")

from html_parsers import get_fields_10K, get_fields_20F

if __name__ == "__main__":

    config = yaml.load(open("../config.yaml"), Loader=yaml.FullLoader)

    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(config["bigquery_dataset"])

    SEC_API_KEY = (
        secretmanager.SecretManagerServiceClient()
        .access_secret_version(
            request={"name": "projects/294962085016/secrets/SECRET_KEY_SEC/versions/6"}
        )
        .payload.data.decode("UTF-8")
    )

    # Define buckets/folders

    buckets = {}

    client = storage.Client()

    for bucket_name in [
        "bucket-10k-filings-html",
        "bucket-20f-filings-html",
        "bucket-debug-10k-filings-html",
        "bucket-debug-20f-filings-html",
        "bucket-fields-10k-filings-html",
        "bucket-fields-20f-filings-html",
    ]:
        buckets[bucket_name] = client.get_bucket(config["buckets"][bucket_name])

    buckets["bucket-files"] = "../" + config["buckets"]["bucket-files"]
    buckets["bucket-filings"] = "../" + config["buckets"]["bucket-filings"]
    buckets["bucket-results"] = "../" + config["buckets"]["bucket-results"]

    # Import reference files

    with open(f'{buckets["bucket-files"]}/country_mapping.txt', "r") as file1, open(
        f'{buckets["bucket-files"]}/state_to_acronym.txt', "r"
    ) as file2, open(f'{buckets["bucket-files"]}/AI_keywords.txt', "r") as file3:
        country_mapping, state_list, AI_keywords = (
            ast.literal_eval(file1.read()),
            ast.literal_eval(file2.read()),
            ast.literal_eval(file3.read()),
        )

    actions = [
        "retrieve-filings-current-year",
        "download-html-current-year",
        "parse-htmls-current-year",
        "create-dataframe",
        "clean-buckets",
        "generate-visualisation-data",
    ]
    tasks = {action: config[action] for action in actions}

    form_queries = {
        "20F": 'formType:"20-F" AND ',
        "10K": 'formType:("10-K", "10-KT", "10KSB", "10KT405", "10KSB40", "10-K405") AND ',
    }

    year, batch_size = config["year"], config["batch-size"]

    def query(buckets, file_name, SEC_API_KEY, form_type, year, form_query):

        queryApi = QueryApi(api_key=SEC_API_KEY)
        log_file = list()

        for month in range(1, 13):
            universe_query = (
                form_query + f"filedAt:[{year}-{month:02d}-01 TO {year}-{month:02d}-31]"
            )
            base_query = {
                "query": {"query_string": {"query": universe_query}},
                "from": 0,
            }
            while True:
                response = queryApi.get_filings(base_query)
                if len(response["filings"]) == 0:
                    break
                urls_list = [
                    filing["linkToFilingDetails"] for filing in response["filings"]
                ]
                log_file += urls_list
                base_query["from"] += len(response["filings"])
            print(f"Filing URLs downloaded for {form_type} {year}-{month:02d}")

        file_contents_str = "\n".join(log_file)

        with open(f'{buckets["bucket-filings"]}/{file_name}', "w") as file:
            file.write(file_contents_str)

        print("All URLs downloaded")

    # Retrieve URLs of filings

    if tasks["retrieve-filings-current-year"]:

        for form_type in form_queries.keys():

            print(f"Generating list of {form_type} URLs ...")

            # Delete the list of filings for the current year, as it will be replaced or created for the first time.

            file_name = f"{form_type}_{year}_fillings.txt"

            if os.path.exists(f'{buckets["bucket-filings"]}/{file_name}'):
                os.remove(f'{buckets["bucket-filings"]}/{file_name}')

            query(
                buckets=buckets,
                file_name=file_name,
                SEC_API_KEY=SEC_API_KEY,
                form_type=form_type,
                year=year,
                form_query=form_queries[form_type],
            )

    def download_filings(buckets, SEC_API_KEY, form_type, year, batch_size):

        file_name = f"{form_type}_{year}_fillings.txt"

        in_bucket = buckets["bucket-filings"]
        out_bucket = buckets[f"bucket-{form_type.lower()}-filings-html"]

        blobs = [blob.name for blob in out_bucket.list_blobs()]

        with open(f"{in_bucket}/{file_name}", "r") as file:

            urls = file.read().split("\n")

        urls = list(sorted(set(urls)))
        intervals = [urls[i : i + batch_size] for i in range(0, len(urls), batch_size)]

        renderApi = RenderApi(api_key=SEC_API_KEY)

        for interval in tqdm(intervals):

            for url in interval:

                file_name = "_".join(url.split("/")[-3:])

                if file_name not in blobs:

                    try:

                        filing = renderApi.get_filing(url)

                        blob_file = out_bucket.blob(file_name)
                        blob_file.upload_from_string(filing)

                    except Exception as e:
                        print(f"Problem with {url}: {e}")

    # Download filings HTMLs

    if tasks["download-html-current-year"]:

        for form_type in form_queries.keys():

            print(f"Downloading {form_type} HTMLs...")

            download_filings(
                buckets=buckets,
                SEC_API_KEY=SEC_API_KEY,
                form_type=form_type,
                year=year,
                batch_size=batch_size,
            )

    def parse_html(in_bucket, debug_bucket, files_bucket, html_file, debug_files):

        html_name = html_file

        in_bucket = buckets[f"bucket-{form_type.lower()}-filings-html"]
        debug_bucket = buckets[f"bucket-debug-{form_type.lower()}-filings-html"]
        files_bucket = buckets[f"bucket-fields-{form_type.lower()}-filings-html"]

        if html_file not in debug_files:

            tags = ["p", "span", "div", "b"]
            results = []
            values = []

            blob = in_bucket.blob(html_file)
            html_bytes = blob.download_as_bytes()

            # Select the parser that scores best in retrieving the CFN and addresses

            for tag in tags:

                if form_type == "20F":

                    result, parsed_l, original_l = get_fields_20F(html_bytes, tag)

                if form_type == "10K":

                    result, parsed_l, original_l = get_fields_10K(html_bytes, tag)

                results.append(len([x for x in result[1:3] if x]))
                values.append((result, parsed_l, original_l))

                if result[1] and result[2]:
                    break

            idx_best_parser = max(range(len(results)), key=results.__getitem__)

            best_values = values[idx_best_parser]
            start_items = [best_values[0][i] for i in list(range(4))]
            parsed_l, original_l = best_values[1:]

            blob_file = debug_bucket.blob(html_name)
            blob_file.upload_from_string("Parsed")

            # Removing small results, which are usually noise
            if len(original_l) > 200:

                root, _ = os.path.splitext(html_name)

                for i, start_item in enumerate(start_items):
                    if start_item:
                        if i == 0:
                            section = original_l[start_item - 1 : start_item + 1]

                            if len(section) > 0 and len(section) < 10:

                                section = ". ".join(section)

                                html_name = root + "_CN_" + ".txt"

                                blob_file = files_bucket.blob(html_name)
                                blob_file.upload_from_string(section)

                        if i == 1:
                            section = original_l[start_item - 1 : start_item + 2]

                            if len(section) > 0 and len(section) < 10:

                                section = ". ".join(section)

                                html_name = root + "_CFN_" + ".txt"

                                blob_file = files_bucket.blob(html_name)
                                blob_file.upload_from_string(section)

                        if i == 2:
                            section = original_l[start_item - 4 : start_item + 1]

                            if len(section) > 0 and len(section) < 10:

                                section = ". ".join(section)

                                html_name = root + "_ADDRESS_" + ".txt"

                                blob_file = files_bucket.blob(html_name)
                                blob_file.upload_from_string(section)

                        if i == 3:

                            AI_match = [
                                item
                                for item in AI_keywords
                                if item in " ".join(parsed_l)
                            ]

                            if len(AI_match) > 0:

                                section = str(AI_match)

                                html_name = root + "_AI_KEYWORDS_" + ".txt"

                                blob_file = files_bucket.blob(html_name)
                                blob_file.upload_from_string(section)

    # Parses filings HTMLs

    if tasks["parse-htmls-current-year"]:

        for form_type in form_queries.keys():

            in_bucket = buckets[f"bucket-{form_type.lower()}-filings-html"]
            debug_bucket = buckets[f"bucket-debug-{form_type.lower()}-filings-html"]
            files_bucket = buckets[f"bucket-fields-{form_type.lower()}-filings-html"]

            print(f"Parsing {form_type} HTMLs...")

            html_files = [blob.name for blob in in_bucket.list_blobs()]
            debug_files = [blob.name for blob in debug_bucket.list_blobs()]

            for html_file in tqdm(html_files):

                parse_html(
                    in_bucket=in_bucket,
                    debug_bucket=debug_bucket,
                    files_bucket=files_bucket,
                    html_file=html_file,
                    debug_files=debug_files,
                )

    def extract_commission_file_numbers(text):
        pattern = r"\d{3}-\d{5}"
        file_numbers = re.findall(pattern, text)

        if len(file_numbers) > 0:
            return file_numbers[0]
        else:
            return ""

    def extract_state_code(state_list, address):

        doc = nlp(address)
        state_codes = set()
        for ent in doc.ents:
            if ent.text in state_list.values():
                state_codes.add(ent.text)
            elif ent.text in state_list:
                state_codes.add(state_list[ent.text])

        state_codes = list(state_codes)

        if len(state_codes) == 1:

            state_code = state_codes[0]

        else:

            state_code = ""

        return state_code

    # Creates final dataframe for the given year

    if tasks["create-dataframe"]:

        print("Creating dataframe...")

        for form_type in form_queries.keys():

            filings_bucket = buckets[f"bucket-{form_type.lower()}-filings-html"]
            debug_bucket = buckets[f"bucket-debug-{form_type.lower()}-filings-html"]
            files_bucket = buckets[f"bucket-fields-{form_type.lower()}-filings-html"]
            results_bucket = buckets["bucket-results"]

            bucket_files = files_bucket.list_blobs()
            files = [blob.name for blob in bucket_files]

            data = defaultdict(lambda: defaultdict(str))

            for file in tqdm(files):

                content_type = [
                    content
                    for content in ["_ADDRESS_", "_CFN_", "_CN_", "_AI_KEYWORDS_"]
                    if content in file
                ]

                if len(content_type) > 0:

                    assert len(content_type) == 1

                    content_type = content_type[0]

                    defaultdict_key = file.replace(content_type, "")

                    content = files_bucket.blob(file)
                    content = content.download_as_text()

                    if content_type == "_ADDRESS_":

                        if form_type == "20F":

                            content = (
                                GeoText(content).countries[0]
                                if len(GeoText(content).countries) > 0
                                else ""
                            )
                            content = (
                                country_mapping[content]
                                if content in list(country_mapping.keys())
                                else content
                            )

                        if form_type == "10K":

                            content = extract_state_code(state_list, content)

                    if content_type == "_CFN_":

                        content = extract_commission_file_numbers(content)

                    if content_type == "_AI_KEYWORDS_":

                        content = content.split(". ")
                        content = list(set([c.capitalize() for c in content if c]))

                    data[defaultdict_key][content_type] = content

            df = pd.DataFrame()

            df["Filing"] = list(set(sorted(list(data.keys()))))
            df["Location"] = [
                data[filing]["_ADDRESS_"] for filing in list(df["Filing"])
            ]
            df["Comission File Number"] = [
                data[filing]["_CFN_"] for filing in list(df["Filing"])
            ]
            df["Company Name"] = [data[filing]["_CN_"] for filing in list(df["Filing"])]
            df["AI Keywords"] = [
                data[filing]["_AI_KEYWORDS_"] for filing in list(df["Filing"])
            ]

            df.to_excel(f"{results_bucket}/{form_type}_{year}.xlsx", index=None)

    def delete_blob(blob):
        blob.delete()

    # Empty the buckets/folders

    if tasks["clean-buckets"]:

        print("Emptying buckets/folders...")

        for form_type in form_queries.keys():

            filings_bucket = buckets[f"bucket-{form_type.lower()}-filings-html"]
            debug_bucket = buckets[f"bucket-debug-{form_type.lower()}-filings-html"]
            files_bucket = buckets[f"bucket-fields-{form_type.lower()}-filings-html"]

            for bucket_to_be_deleted in [filings_bucket, debug_bucket, files_bucket]:

                blobs_to_be_deleted = bucket_to_be_deleted.list_blobs()

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.map(delete_blob, blobs_to_be_deleted)

    if tasks["generate-visualisation-data"]:

        print("Generating charts...")

        results_bucket = buckets["bucket-results"]
        results_files = os.listdir(results_bucket)

        results_files = [
            file
            for file in results_files
            if file.startswith("10K") or file.startswith("20F")
        ]

        years = [file.split("_")[1].replace(".xlsx", "") for file in results_files]

        df = pd.DataFrame()

        for year in years:

            for form_type in form_queries.keys():

                df_form_type_year = pd.read_excel(
                    f"{results_bucket}/{form_type}_{year}.xlsx"
                )
                df_form_type_year["Form Type"] = form_type
                df_form_type_year["Year"] = year

                df = pd.concat([df, df_form_type_year], axis=0)

        df.fillna(np.nan, inplace=True)

        # Chart "AI foreign public companies registered in SEC"

        df_plot = df.copy()
        df_plot = df_plot.dropna(subset=["AI Keywords"])

        df_plot = df_plot[df_plot["Form Type"] == "20F"]
        df_plot = df_plot[["Location", "Year"]]
        df_plot = df_plot.dropna(subset=["Location"])
        df_plot = df_plot.groupby(["Location", "Year"]).size().reset_index(name="Count")
        df_plot = df_plot.rename(columns={"Location": "Country"})

        df_plot = df_plot[df_plot["Country"] != "United States"]

        df_plot.to_excel(
            f"{results_bucket}/CHART_EVOLUTION_OF_US_REGISTERED_AI_PUBLIC_COMPANIES_PER_COUNTRY.xlsx",
            index=None,
        )

        upload_table_to_bigquery(
            bigquery_client,
            dataset_ref,
            df_plot,
            "CHART_EVOLUTION_OF_US_REGISTERED_AI_PUBLIC_COMPANIES_PER_COUNTRY",
        )

        # Chart "Top concepts in SEC filings"

        df_plot = df.copy()
        df_plot = df_plot.dropna(subset=["AI Keywords"])

        df_plot = df[["Location", "Year", "AI Keywords", "Form Type"]]
        df_plot["Location"] = [
            "United States" if form_type == "10K" else location
            for location, form_type in zip(df_plot["Location"], df_plot["Form Type"])
        ]
        del df_plot["Form Type"]
        df_plot = df_plot.dropna(subset=["Location"])
        df_plot = df_plot.fillna("")
        df_plot = df_plot.rename(columns={"Location": "Country"})

        df_plot = df_plot[df_plot["AI Keywords"] != ""]

        df_plot["AI Keywords"] = [
            kw.replace('["', "").replace('"]', "") for kw in df_plot["AI Keywords"]
        ]
        df_plot["AI Keywords"] = [ast.literal_eval(kw) for kw in df_plot["AI Keywords"]]

        df_plot = (
            df_plot.groupby(["Country", "Year"])["AI Keywords"]
            .agg(lambda x: list(chain.from_iterable(x)))
            .reset_index()
        )

        threshold = [
            len(list(set(x))) >= int(config["threshold_concepts"])
            for x in df_plot["AI Keywords"]
        ]

        df_plot = df_plot[threshold]
        df_plot = df_plot.explode("AI Keywords")

        r = list()

        for year in list(set(df_plot["Year"])):

            for country in list(set(df_plot["Country"])):

                subset = df_plot[df_plot["Year"] == year]
                subset = subset[subset["Country"] == country]

                if len(subset) == 0:

                    kws = []
                    diff = 0

                else:

                    kws = list(subset["AI Keywords"])
                    diff = int(len(set(kws)))

                r.append([country, year, kws, diff])

        df_plot = pd.DataFrame(
            r, columns=["Country", "Year", "AI Keywords", "Number of concepts"]
        )
        df_plot = df_plot.explode("AI Keywords")

        df_plot.to_excel(
            f"{results_bucket}/CHART_TOP_CONCEPTS_IN_AI_PUBLIC_COMPANIES_BUSINESS_DESCRIPTIONS.xlsx",
            index=None,
        )

        upload_table_to_bigquery(
            bigquery_client,
            dataset_ref,
            df_plot,
            "CHART_TOP_CONCEPTS_IN_AI_PUBLIC_COMPANIES_BUSINESS_DESCRIPTIONS",
        )

        # Chart "AI public companies registered in SEC"

        df_plot = df.copy()
        df_plot = df_plot.dropna(subset=["AI Keywords"])

        df_plot = df_plot[df_plot["Form Type"] == "10K"]
        df_plot = df_plot[["Location", "Year"]]
        df_plot = df_plot.dropna(subset=["Location"])
        df_plot = df_plot.groupby(["Location", "Year"]).size().reset_index(name="Count")
        df_plot = df_plot.rename(columns={"Location": "US State"})

        df_plot.to_excel(
            f"{results_bucket}/CHART_AI_PUBLIC_COMPANIES_PER_US_STATE.xlsx", index=None
        )

        upload_table_to_bigquery(
            bigquery_client,
            dataset_ref,
            df_plot,
            "CHART_AI_PUBLIC_COMPANIES_PER_US_STATE",
        )
