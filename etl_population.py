# Data wrangling 
import pandas as pd 

# Paths 
import os 

# Iteration tracking 
from tqdm import tqdm

# Defining the age bins to put the population into
# The bins are defined as the lower bound of the age range
# The last bin is defined as the upper bound of the age range
AGE_BINS = [-1, 7, 12, 18, 21, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80]  
#AGE_BINS_LABELS = [f"{AGE_BINS[i]}-{AGE_BINS[i+1]}" for i in range(len(AGE_BINS)-1)]
#AGE_BINS_LABELS.append(f"{AGE_BINS[-1]}+")

if __name__ == '__main__':
    # Infer the path to this file 
    path = os.path.dirname(os.path.abspath(__file__))

    # Reading the input data 
    input = pd.read_csv(os.path.join(path, 'input', 'commits.csv'))

    # Parsing the datetime column 
    input['datetime'] = pd.to_datetime(input['datetime'])

    # Adding the year column 
    input['year'] = [x.year for x in input['datetime']]

    # Creating a placeholder dataframe 
    output = pd.DataFrame()

    # Iterating over the input rows 
    for index, row in tqdm(input.iterrows(), total=len(input)):
        # Reading the commit data
        d = pd.read_csv(f'https://raw.githubusercontent.com/vilnius/gyventojai/{row["commit"]}/registered_people_n_streets.csv', on_bad_lines='skip')

        # Dropping rows with missing SENIUNNR values
        d = d.dropna(subset=['SENIUNNR'])

        # Filling missing values in the SEIMOS_PADETIS with 'O'
        d['SEIMOS_PADETIS'] = d['SEIMOS_PADETIS'].fillna('O')

        # Calculating the age of an individual 
        d['age'] = row['year'] - d['GIMIMO_METAI']

        # Assigning the age bin to the individual
        d['age_bin'] = pd.cut(d['age'], bins=AGE_BINS)

        # The missing age bin will be the last AGE_BINS member
        d['age_bin'] = d['age_bin'].cat.add_categories(f"{AGE_BINS[-1]}+") 
        d['age_bin'] = d['age_bin'].fillna(f"{AGE_BINS[-1]}+")

        # Aggregating by age bucket and SENIUNNR 
        # The aggregated stats are: 
        # Total number of individuals in the age bucket
        # Total number of SEIMOS_PADETIS in the age bucket 
        # Totla number of LYTIS in the age bucket
        agg = d.groupby(['age_bin', 'SENIUNNR', 'SEIMOS_PADETIS', 'LYTIS']).size().reset_index(name='count')

        # Converting the age_bin to a string
        agg['age_bin'] = agg['age_bin'].astype(str)

        # Changing (-1 to (0 
        agg['age_bin'] = agg['age_bin'].str.replace('-1', '0')

        # Adding the date of the data 
        agg['as_of_date'] = row['datetime'].strftime('%Y-%m-%d')

        # Changing the names of certain columns 
        agg.rename(columns={'SENIUNNR': 'district', 'SEIMOS_PADETIS': 'family_status', 'LYTIS': 'gender'}, inplace=True)

        # Adding to output 
        output = pd.concat([output, agg])

    # Saving the output
    output.to_csv(os.path.join(path, 'output', 'population_VILNIUS.csv'), index=False)