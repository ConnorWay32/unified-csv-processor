import argparse
import csv
import gzip
import json
import os
import shutil
import string
from pathlib import Path
from random import sample
from time import perf_counter
from typing import TypeVar

import jsonlines
import urllib3
import urllib3.exceptions
import xmltodict

_OpenFile = TypeVar("_OpenFile", str, Path)


def fast_line_count(file: _OpenFile, has_header: bool = False) -> int:
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

    jsondata: dict = json.loads(response.data)

    if "error" in jsondata:
        return {}
    return jsondata


def pmc_request(pmcid: str) -> dict:
    """Performs a request via the OA API

    pmcid: PubMedCentral ID
    """

    url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id={pmcid}&format=pdf"
    retry = True
    http = urllib3.PoolManager()
    datadict = {}

    while retry:
        try:
            response = http.request("GET", url)
            datadict = xmltodict.parse(response.data)
            retry = False
        except urllib3.exceptions.MaxRetryError:
            pass
    return datadict


def unified_processor(
    csvfile, samplesize: int = 800, email: str = "unpaywall_01@example.com"
):
    """Produces UPW and PMC format files for BGH

    csvfile: path to PubMed format csv files.
    email: email address for UPW calls
    samplesize: sampling size
    """
    outfile = str(os.path.basename(csvfile)).removesuffix(".csv")
    field = outfile.translate(str.maketrans("", "", string.digits))

    if not os.path.exists(Path(f"./output/{field}")):
        os.makedirs(Path(f"./output/{field}"))

    if not os.path.exists(Path(f"./reports/{field}")):
        os.makedirs(Path(f"./reports/{field}"))

    jsonlfile = Path(f"./output/{field}/{outfile}-UPW.jsonl")
    txtfile = Path(f"./output/{field}/{outfile}-PMC.txt")
    dump_path = Path(f"./reports/{field}/{outfile}-dump.csv")

    line_count = fast_line_count(csvfile, True)

    print(f"{line_count} entries.")

    # samples k = samplesize positions from the number of lines
    if samplesize is not None:
        samplesize = samplesize if samplesize < line_count else (line_count - 1)
        selection = sample(range(1, line_count + 2), samplesize)
        selection.sort()
    print(f"Processing {samplesize} samples of {csvfile}")
    startprocess = perf_counter()
    with open(csvfile, "r", encoding="utf-8") as csv_file, jsonlines.open(
        jsonlfile, mode="w"
    ) as jsonlwriter, open(dump_path, "w", encoding="utf-8") as dumpfile, open(
        txtfile, "w", encoding="ascii"
    ) as txtwriter:
        # Creates a dummy line for biblio-glutton-harvester
        txtwriter.write("DatePlaceholder\n")

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

        csvreader = csv.reader(csv_file)
        dumpwriter = csv.writer(dumpfile, dialect="unix")

        for row in csvreader:
            if sample_position == samplesize:
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
            pmcid = row[8]
            pmid = row[0]
            doi = row[10]

            print(f"Processing entry {count['total']}", end="\r")

            if bool(doi):
                jsondata = upw_request(doi, email)
                if bool(jsondata):
                    jsonlwriter.write(jsondata)

                    count["upw"] += 1

                    sample_position += 1
                    position += 1
                    continue

            # discards PMC ids that didn't return pmids
            if pmid == "":
                count["discard"] += 1

                sample_position += 1
                position += 1
                continue
            datadict = pmc_request(pmcid)

            path = None
            if "error" not in datadict["OA"]:
                path = datadict["OA"]["records"]["record"]["link"]["@href"]
            # discards papers with no pdf available
            if path is None:
                count["no_pdf"] += 1
                dumplist = [doi, pmcid]
                dumpwriter.writerow(dumplist)

                position += 1
                sample_position += 1
                continue
            # remove ftp prefix from filepath
            subpath: str = path.removeprefix("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/")

            # line formatting for entry into output txt file
            outputline = (
                f"{subpath}\t"
                "CitationPlaceholder\t"
                f"{pmcid}\t"
                f"PMID:{pmid}\t"
                "LicensePlaceholder\n"
            )

            txtwriter.write(outputline)
            count["pmc"] += 1

            sample_position += 1
            position += 1
        print("Finished writing to output files.")
        endprocess = perf_counter()

        print(
            f"""{count['total']} entries processed in 

            {(endprocess - startprocess):.2f} seconds."""
        )

        print(f"{count['upw']} entries saved to {jsonlfile}")
        print(f"{count['pmc']} entries saved to {txtfile}")
        print(f"{count['discard']} entries discarded due to missing information")
        print(f"{count['no_pdf']} entries discarded due to unavailable pdf downloads")
    # compresses jsonl to jsonl.gz
    with open(jsonlfile, "rb") as tocompress:
        jsonlgzfile = f"{jsonlfile}.gz"
        with gzip.open(jsonlgzfile, "wb") as compressfile:
            shutil.copyfileobj(tocompress, compressfile)
    print(f"{os.path.basename(jsonlfile)} compressed to .gz")

    # deletes jsonl file
    os.remove(jsonlfile)

    # removes txt file if empty
    if count["pmc"] == 0:
        os.remove(txtfile)
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

    with open(csv_path, mode="w", encoding="utf-8") as reportfile:
        csvwriter = csv.writer(reportfile, dialect="unix")
        header = ["Year", "UPW", "PMC", "NoPubMed", "NoPDF", "Total"]
        csvwriter.writerow(header)
        for y in range(args.startyear, args.endyear + 1, 1):
            printlist = [y]

            returnlist = unified_processor(
                Path(f"./input/{args.field}/{args.field}{y}.csv"),
                samplesize=args.samples,
                email=args.email,
            )
            printlist.extend(returnlist)
            csvwriter.writerow(printlist)
