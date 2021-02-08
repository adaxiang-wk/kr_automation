# Automation Tool for DCM Korean Market

## Steps
### Step 0: Input & Setting
- create and activate python environment
    - create conda environment `conda create --name <env> --file requirements.txt`
    - activate the environment `conda activate <env>`
- prepare input file: 
    - the format is specified in `./data/dataframe/example.xlsx`
    - name the file name as `input.xlsx` and put the file under the directory`/data/dataframe` 
- browser driver:
    - the program uses chrome driver
    - [download](https://chromedriver.chromium.org/downloads): *choose the version that corresponds to the chrome version on your computer*
    - put the downloaded driver `chromedriver.exe` under `/dependencies`
### Step 1: Scraping 
`python main.py -action getinfo -date [start_date] [end_date]`
- **action**: `getinfo`
- **date**: start and end date for the deals in the input file
- e.g.: the deals in the input files were announced between 2019 Jan 01 and 2019 Dec 31
    - `python main.py -action getinfo -date 20190101 20191231`

### Step 2: Parse to JSON and Post to database
#### Running as two steps
1. Running in pie: `python main.py -action parse_batch -env pie`; Running in prd: `python main.py -action parse_batch -env prd`
2. Running in pie: `python main.py -action post_batch -env pie`; Running in prd: `python main.py -action post_batch -env prd`

#### Running as one step
Running in pie: `python main.py -action parse_post_batch -env pie`; Running in prd: `python main.py -action parse_post_batch -env prd`

## Examine Results
- **scraping results**: in directory `data/dataframe/final_df.csv`
- **parsing results**: in directory `logs/[env]_parse_log.csv`
- **posting results**: in directory `logs/[env]_post_log.csv`



