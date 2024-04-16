# Population bining

Project used to wrangle the population in various Vilnius districts. 

The repo that tracks the raw level data is [here](https://github.com/vilnius/gyventojai) 

After cloning the above project, run the command: 

```bash
git log --pretty=format:%h,%ad,%an,%ae,%s > gitlogs.csv
```

Then rename the columns to: 

* commit 
* datetime
* user
* email
* message

Then, put the file in the `input/` directory.

The ETL command to do the wrangling is as follows: 

```bash
python etl_population.py
```

The output file is in the `output/` directory. 

The columns of the output file are: 

* **age_bin** - the age interval of the population.

* **gender** - gender indicator 

* **as_of_date** - the time of the data collection.

* **district** - Vilnius district ID. 

* **count** - the total amount of people in the given slice of the population.