import argparse
import csv
import gzip
import json
import os
import shutil
from pathlib import Path
from random import sample
from time import perf_counter
from typing import TypeVar

import jsonlines
import urllib3
import urllib3.exceptions
import xmltodict

_Path = TypeVar("_Path", str, Path)


def fast_line_count(file: _Path, has_header: bool = False) -> int:
    """Counts the number of newlines in a file using an 8MB buffer

    file: file to count lines of
    header: whether the file has a header (to not count)
    """

    line_count = 0

    with open(file, "rb") as file_buffer:
        while 1:
            buffer = file_buffer.read(8192 * 1024)
            if not buffer:
                break
            line_count += buffer.count(b"\n")
    if has_header:
        line_count -= 1
    return line_count


def upw_request(doi: str, email: str = "unpaywall_01@example.com") -> dict:
    """Performs a request via the UPW REST API

    doi: digital identifier of the paper
    email: address required for API requests
    """
    url: str = f"https://api.unpaywall.org/v2/{doi}?email={email}"
    http = urllib3.PoolManager()
    response = http.request("GET", url)

    json_data: dict = json.loads(response.data)

    if "error" in json_data:
        return {}
    return json_data


def pmc_request(pmc_id: str) -> dict:
    """Performs a request via the OA API

    pmcid: PubMedCentral ID
    """

    url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id={pmc_id}&format=pdf"
    retry = True
    http = urllib3.PoolManager()
    data_dict = {}

    while retry:
        try:
            response = http.request("GET", url)
            data_dict = xmltodict.parse(response.data)
            retry = False
        except urllib3.exceptions.MaxRetryError:
            pass
    return data_dict


def unified_processor(
    field: str,
    year: int,
    sample_size: int = 800,
    email: str = "unpaywall_01@example.com",
):
    """Produces UPW and PMC format files for BGH

    csvfile: path to PubMed format csv files.
    email: email address for UPW calls
    samplesize: sampling size
    """

    if not os.path.exists(Path(f"./output/{field}")):
        os.makedirs(Path(f"./output/{field}"))

    if not os.path.exists(Path(f"./reports/{field}")):
        os.makedirs(Path(f"./reports/{field}"))

    csv_file = Path(f"./input/{field}/{field}{year}.csv")
    jsonl_file = Path(f"./output/{field}/{field}{year}-UPW.jsonl")
    txt_file = Path(f"./output/{field}/{field}{year}-PMC.txt")
    dump_path = Path(f"./reports/{field}/{field}{year}-dump.csv")

    line_count = fast_line_count(csv_file, True)

    print(f"{line_count} entries.")

    # creates a sorted list of line numbers to avoid storing entire csv files to memory
    sample_size = sample_size if sample_size < line_count else (line_count - 1)
    selection: list[int] = sample(range(1, line_count + 2), sample_size)
    selection.sort()

    print(f"Processing {sample_size} samples of {csv_file}")
    start_time = perf_counter()

    with open(csv_file, "r", encoding="utf-8") as csv_file, jsonlines.open(
        jsonl_file, mode="w"
    ) as jsonl_writer, open(dump_path, "w", encoding="utf-8") as dump_file, open(
        txt_file, "w", encoding="ascii"
    ) as txt_writer:
        # Creates a dummy line for biblio-glutton-harvester
        txt_writer.write("DatePlaceholder\n")

        # initialize counts
        count: dict[str, int] = {
            "upw": 0,
            "pmc": 0,
            "discard": 0,
            "no_pdf": 0,
            "total": 0,
        }

        position = 0
        sample_position = 0

        dump_writer = csv.writer(dump_file, dialect="unix")

        for row in csv.reader(csv_file):
            if sample_position == sample_size:
                break
            # skips header
            if position == 0:
                position += 1
                continue
            # if sampling, skips non samples
            if selection is not None and position != selection[sample_position]:
                position += 1
                continue
            count["total"] += 1

            # sets up fields for unpaywall and PMC API
            pmc_id = row[8]
            pubmed_id = row[0]
            doi = row[10]

            print(f"Processing entry {count['total']}", end="\r")

            if bool(doi):
                json_data = upw_request(doi, email)
                if bool(json_data):
                    jsonl_writer.write(json_data)

                    count["upw"] += 1

                    sample_position += 1
                    position += 1
                    continue

            # discards PMC ids that didn't return pmids
            if pubmed_id == "":
                count["discard"] += 1

                sample_position += 1
                position += 1
                continue
            data_dict = pmc_request(pmc_id)

            path = None
            if "error" not in data_dict["OA"]:
                path = data_dict["OA"]["records"]["record"]["link"]["@href"]
            # discards papers with no pdf available
            if path is None:
                count["no_pdf"] += 1
                dump_list = [doi, pmc_id]
                dump_writer.writerow(dump_list)

                position += 1
                sample_position += 1
                continue
            # remove ftp prefix from filepath
            subpath: str = path.removeprefix("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/")

            # line formatting for entry into output txt file
            output_line = (
                f"{subpath}\t"
                "CitationPlaceholder\t"
                f"{pmc_id}\t"
                f"PMID:{pubmed_id}\t"
                "LicensePlaceholder\n"
            )

            txt_writer.write(output_line)
            count["pmc"] += 1

            sample_position += 1
            position += 1
        print("Finished writing to output files.")
        end_time = perf_counter()

        print(
            f"""{count['total']} entries processed in 

            {(end_time - start_time):.2f} seconds."""
        )

        print(f"{count['upw']} entries saved to {jsonl_file}")
        print(f"{count['pmc']} entries saved to {txt_file}")
        print(f"{count['discard']} entries discarded due to missing information")
        print(f"{count['no_pdf']} entries discarded due to unavailable pdf downloads")
    # compresses jsonl to jsonl.gz
    with open(jsonl_file, "rb") as to_compress:
        jsonlgz_file = f"{jsonl_file}.gz"
        with gzip.open(jsonlgz_file, "wb") as compress_file:
            shutil.copyfileobj(to_compress, compress_file)
    print(f"{os.path.basename(jsonl_file)} compressed to .gz")

    # deletes jsonl file
    os.remove(jsonl_file)

    # removes txt file if empty
    if count["pmc"] == 0:
        os.remove(txt_file)
    # removes dump file if empty
    if count["no_pdf"] == 0:
        os.remove(dump_path)

    return count.values()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Processes PubMed CSV files for biblio-glutton harvesting"
    )

    parser.add_argument("field", type=str)
    parser.add_argument(
        "--start", dest="startyear", type=int, default=2012, required=False
    )
    parser.add_argument("--end", dest="endyear", type=int, default=2022, required=False)
    parser.add_argument("-s", "--samples", dest="samples", type=int, default=850)
    parser.add_argument(
        "-e", "--email", dest="email", type=str, default="unpaywall_01@example.com"
    )
    args = parser.parse_args()

    if not os.path.exists(Path(f"./reports/{args.field}")):
        os.makedirs(Path(f"./reports/{args.field}"))

    csv_path = Path(f"./reports/{args.field}/{args.field}Report.csv")

    with open(csv_path, mode="w", encoding="utf-8") as report_file:
        csv_writer = csv.writer(report_file, dialect="unix")
        header = ["Year", "UPW", "PMC", "NoPubMed", "NoPDF", "Total"]
        csv_writer.writerow(header)
        for y in range(args.startyear, args.endyear + 1, 1):
            print_list = [y]

            return_list = unified_processor(
                args.field,
                y,
                sample_size=args.samples,
                email=args.email,
            )
            print_list.extend(return_list)
            csv_writer.writerow(print_list)
