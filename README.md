# unified-csv-processor
Python utility to create file lists for [biblio-glutton-harvester](https://github.com/kermitt2/biblio-glutton-harvester) based on PubMed open access search results.
* Uses CSV files available from [PubMed](https://pubmed.ncbi.nlm.nih.gov/) search results
* Produces .jsonl.gz file lists to harvest files from the Unpaywall database
* As a fallback, produces .txt file lists to harvest files from the PubMedCentral set for articles not available on the Unpaywall database
* Can be restricted to a set maximum sample size to randomly select a given number of articles from the CSV file(default 850)

## Requirements
Requires Python 3, along with the `jsonlines`, `urllib3` and `xmltodict` packages. 
The utility makes API calls to the [PMC OA Web Service API](https://www.ncbi.nlm.nih.gov/pmc/tools/oa-service/) and the 
[Unpaywall REST API](https://unpaywall.org/products/api), so an internet connection is required during runtime.

## Installation
While in the desired current working directory,
```
git clone https://github.com/ConnorWay32/unified-csv-processor
```
To create a new virtual environment (recommended):
### conda
```
conda create -n csv-processor
conda activate csv-processor
conda install jsonlines urllib3 xmltodict
```
### pyenv-virtualenv
```
pyenv install 3.11.2
pyenv virtualenv 3.11.2 csv-processor
pyenv activate csv-processor
pip install jsonlines urllib3 xmltodict
```
When inside the **unified-csv-processor** directory, you can set the pyenv environment to be local to that directory with:
```
pyenv local csv-processor
```
## Usage
The requisite CSV files can be obtained from the [PubMed](https://pubmed.ncbi.nlm.nih.gov/) website.
1. Using the Search or Advanced Search option, search the PubMed database with your desired search/filtering terms.
2. Below the search bar, click **Save**
   - Set **Selection** to **All results**
   - Set **Format** to **CSV**
3. Click **Create File**

The tool can be used as a python module, or from the command line.
### As a Module
From a python file within the same directory,
```python
from unified-csv-processor import unified_processor

unified_processor(csv_path = "/path/to/csvfile", sample_size = 850, email = "validemail@address.com")
```
#### Arguments:
```
csv_path: path to the csv file, can be any string-like path object (string, pathlib.Path, etc.)
sample_size: maximum sample size. Default is 850
email: a valid email address (required by the Unpaywall API for requests)
```
### As a Command
The command is designed for article lists organized by field and year, so CSV files should be supplied with the following directory structure and naming scheme: 
```
.
├── input
│   └─── Cardiothoracic
│       ├── Cardiothoracic2012.csv
│       ├── Cardiothoracic2013.csv
│       ├── Cardiothoracic2014.csv
│       ├── Cardiothoracic2015.csv
│       ├── Cardiothoracic2016.csv
│       ├── Cardiothoracic2017.csv
│       ├── Cardiothoracic2018.csv
│       ├── Cardiothoracic2019.csv
│       ├── Cardiothoracic2020.csv
│       ├── Cardiothoracic2021.csv
│       └── Cardiothoracic2022.csv
│ 
├── output
│
├── reports
│ 
└── unifiedprocessor.py
```
To use the command (requires being in the same working directory):
```
python unified-csv-processor.py [field] [--start STARTYEAR] [--end ENDYEAR] [-s or --samples SAMPLES] [-e or --email EMAIL]

    field (required): the name of the field/category to process.

    Optional Arguments:
    --start         : first year of the field's CSV files. Defaults to 2012
    --end           : last year of the field's CSV files. Defaults to 2022
    -s or --samples : maximum sample size. Defaults to 850
    -e or --email   : email address for use in the Unpaywall API. Defaults to 'unpaywall_01@example.com'
```
In the above example, the Cardiothoracic field would be processed with the following:
```
python unified-csv-processor.py Cardiothoracic --start 2012 --end 2022 --samples 850 --email unpaywall_01@example.com
```
### Harvesting
Output files can be found in the unified-csv-processor/output directory.

When using [biblio-glutton-harvester](https://github.com/kermitt2/biblio-glutton-harvester), the `--unpaywall` argument should link to the .jsonl.gz file created.
If a .txt file was created, use the `--pmc` argument in a new command to link to the .txt file created.
## Notes
The random sampling uses `random.sample` from the `random` module from the Python standard library.

## License
Distributed under the [MIT license](https://opensource.org/license/mit/).
