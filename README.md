# team_aragon

## Overview
This project includes Python scripts designed to load data into an SQL database. The scripts are tailored for two specific datasets: Weekly updates from HHS (Health and Human Services) and CMS (Centers for Medicare & Medicaid Services) quality data. The README provides instructions on using these scripts and outlines their functionality.

## Requirements
Python 3.x
An SQL database (e.g., MySQL, PostgreSQL)

## Installation
1. Clone this repository to your local machine:
```
git clone https://github.com/your_username/team_aragon.git
cd team_aragon
```
2. Install the required Python packages:
```
pip install pandas
pip install numpy 
pip install psycopg
```
## Usage
Weekly Updates (HHS Data)
To load the HHS data, use the following command:

```
python load-hhs.py <file_name>
```

Replace <file_name> with the name of the CSV file containing the weekly updates, e.g., 2022-01-04-hhs-data.csv.


