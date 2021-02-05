# Automation Tool for DCM Korean Market

## Environment

## Arguements
- `-action`: choose actions from `getinfo`, `parse_batch`, `post_batch`, `parse_post_batch` 
- `-date`: start and end date for the deals in the input file with the format *yyyymmdd*
- `-env`: choose to run in pie or prd

## Steps
### Step 1: Scraping 
`python main.py -action getinfo -date [start_date] [end_date]`
- **action**: `getinfo`
- **date**: start and end date for the deals in the input file
- e.g.: the deals in the input files were announced between 2019 Jan 01 and 2019 Dec 31
    - `python main.py -action getinfo -date 20190101 20191231`

### Step 2: Parse to JSON and Post to database
#### Running as two steps
1. `python main.py -action parse_batch -env pie`
2. `python main.py -action post_batch -env pie`

#### Running as one step
`python main.py -action parse_post_batch -env pie`

## Examine Results
- **scraping results**: in directory `data/dataframe/brk_df.csv`
- **parsing results**: in directory `logs/[env]_parse_log.csv`
- **posting results**: in directory `logs/[env]_post_log.csv`

