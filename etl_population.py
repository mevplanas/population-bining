# Data wrangling 
import pandas as pd 

# Paths 
import os 

# Iteration tracking 
from tqdm import tqdm

# Date wrangling 
import datetime

# Defining the age bins to put the population into
# The bins are defined as the lower bound of the age range
# The last bin is defined as the upper bound of the age range
# The final bins are: 
# 0-5
# 6-10
# 11-18
# 19-35
# 35-65
# 65+
AGE_BINS = [-1, 5, 10, 18, 35, 65]  

def clean_str(x: str) -> str: 
    # Removing the leading and trailing whitespace
    x = x.strip()

    # Converting to lowercase
    x = x.lower()

    # Removing more than one whitespace
    x = ' '.join(x.split())

    return x

# Defining the function for aggregation of the data 
def aggregate_data(d, as_of_date: datetime.datetime) -> pd.DataFrame:
    # If the SENIUNNR is missing, we continue to the next row
    if 'SENIUNIJA' not in d.columns:
        return pd.DataFrame()

    # Dropping rows with missing SENIUNNR values
    d = d.dropna(subset=['SENIUNIJA'])

    # Cleaning certain strings
    d['SENIUNIJA'] = d['SENIUNIJA'].apply(clean_str)
    d['LYTIS'] = d['LYTIS'].apply(clean_str)

    # Calculating the age of an individual 
    if 'GIMIMO_METAI' in d.columns and 'year' in d.columns:
        d['age'] = d['year'] - d['GIMIMO_METAI']
    elif 'AMZIUS' in d.columns:
        d['age'] = d['AMZIUS']

    # Assigning the age bin to the individual
    d['age_bin'] = pd.cut(d['age'], bins=AGE_BINS)

    # The missing age bin will be the last AGE_BINS member
    d['age_bin'] = d['age_bin'].cat.add_categories(f"{AGE_BINS[-1]}+") 
    d['age_bin'] = d['age_bin'].fillna(f"{AGE_BINS[-1]}+")

    # Aggregating by age bucket and SENIUNNR 
    # The aggregated stats are: 
    # Total number of individuals in the age bucket
    # Totla number of LYTIS in the age bucket
    agg = d.groupby(['age_bin', 'SENIUNIJA', 'LYTIS']).size().reset_index(name='count')

    # Converting the age_bin to a string
    agg['age_bin'] = agg['age_bin'].astype(str)

    # Changing (-1 to (0 
    agg['age_bin'] = agg['age_bin'].str.replace('-1', '0')

    # Adding the date of the data 
    agg['as_of_date'] = as_of_date

    # Changing the names of certain columns 
    agg.rename(columns={'SENIUNIJA': 'district', 'SEIMOS_PADETIS': 'family_status', 'LYTIS': 'gender'}, inplace=True)

    return agg

if __name__ == '__main__':
    # Infer the path to this file 
    path = os.path.dirname(os.path.abspath(__file__))

    # Reading the input data 
    input = pd.read_excel(os.path.join(path, 'input', 'gitlogs.xlsx'))

    # Parsing the datetime column 
    input['datetime'] = pd.to_datetime(input['datetime'])

    # Adding the year column 
    input['year'] = [x.year for x in input['datetime']]

    # Creating a placeholder dataframe 
    output = pd.DataFrame()

    # Creating the output directory
    output_dir = os.path.join(path, 'output')
    os.makedirs(output_dir, exist_ok=True)

    # Iterating over the input rows 
    for index, row in tqdm(input.iterrows(), total=len(input)):
        try:
            # Reading the commit data
            d = pd.read_csv(f'https://raw.githubusercontent.com/vilnius/gyventojai/{row["commit"]}/registered_people_n_streets.csv', on_bad_lines='skip')

            # Adding the year column
            d['year'] = row['year']

            # Converting the row['datetime'] to a datetime object
            as_of_date = pd.to_datetime(row['datetime'])

            # Aggregating the data
            agg = aggregate_data(d, row['datetime'])

            # Adding to output 
            output = pd.concat([output, agg])
        except Exception as e:
            print(f'Error processing commit {row["commit"]}: {e}')

    # Reading the additional data 
    additional_data_dir = os.path.join(path, 'additional_data')

    # Listing the data in the directory
    additional_data_files = os.listdir(additional_data_dir)

    # Iterating over the files
    for file in additional_data_files:
        # Reading the data
        d = pd.read_csv(os.path.join(additional_data_dir, file), sep=';')

        # Gettiing the YYYYMM format datetime from the name 
        as_of_date = datetime.datetime.strptime(file.split('.')[0].split('GYV_')[1], '%Y%m')

        # Aggregating the data
        agg = aggregate_data(d, as_of_date)

        # Adding to output
        output = pd.concat([output, agg])

    # Converting the as of date to a YYYY-MM format 
    output['as_of_date'] = [x.strftime('%Y-%m') for x in output['as_of_date']]

    # Creating an encoded column called district_id
    output['district_id'] = pd.factorize(output['district'])[0]

    # Creating an id for the age bin
    output['age_bin_id'] = pd.factorize(output['age_bin'])[0]

    # Getting the minimum and maximum month of the data
    min_month = output['as_of_date'].min()
    max_month = output['as_of_date'].max()

    # Creating a date range of monthly data
    date_range = pd.date_range(start=min_month, end=max_month, freq='M')

    # Creating a dataframe where one column is the date range and the other is an incrementing integer
    date_range_df = pd.DataFrame({'as_of_date': date_range, 'as_of_date_id': range(len(date_range))})
    date_range_df['as_of_date'] = [x.strftime('%Y-%m') for x in date_range_df['as_of_date']]

    # Merging the date range with the output
    output = output.merge(date_range_df, on='as_of_date')

    # Creating the gender_id column 
    output['gender_id'] = pd.factorize(output['gender'])[0]

    # Creating the dataframes for decoding the _id columns for later use 
    district_df = output[['district_id', 'district']].drop_duplicates()
    age_bin_df = output[['age_bin_id', 'age_bin']].drop_duplicates()
    gender_df = output[['gender_id', 'gender']].drop_duplicates()
    as_of_date_df = output[['as_of_date_id', 'as_of_date']].drop_duplicates()

    # Saving the decoders to output 
    district_df.to_csv(os.path.join(output_dir, 'district_decoder.csv'), index=False)
    age_bin_df.to_csv(os.path.join(output_dir, 'age_bin_decoder.csv'), index=False)
    gender_df.to_csv(os.path.join(output_dir, 'gender_decoder.csv'), index=False)  
    as_of_date_df.to_csv(os.path.join(output_dir, 'as_of_date_decoder.csv'), index=False)

    # Leaving only the needed columns in the output dataframe 
    output = output[['district_id', 'age_bin_id', 'gender_id', 'as_of_date_id', 'count']]

    # Dropping duplicates
    output = output.drop_duplicates()

    # Asserting that the age_bin, district_id, gender and as_of_date combinations are unique
    grouped = output.groupby(['district_id', 'age_bin_id', 'gender_id', 'as_of_date_id', 'count']).size()
    assert grouped.max() == 1, f"Duplicate rows found: {grouped[grouped > 1]}"

    # Converting to appropriate types
    output.district_id = output.district_id.astype(str)
    output.age_bin_id = output.age_bin_id.astype(str)
    output.gender_id = output.gender_id.astype(str)
    output.as_of_date_id = output.as_of_date_id.astype(int)

    # Saving the output
    output.to_csv(os.path.join(output_dir, f'population_VILNIUS_{datetime.datetime.now().date()}.csv'), index=False)

    # Creating the train and test frames in the following way: 
    # We will keep the last 12 as_of_date_id values as the test set
    # The rest will be the training set
    train = output[output['as_of_date_id'] < output['as_of_date_id'].max() - 12]
    test = output[output['as_of_date_id'] >= output['as_of_date_id'].max() - 12]

    # Saving the train and test dataframes
    train.to_csv(os.path.join(output_dir, f'population_train_VILNIUS_{datetime.datetime.now().date()}.csv'), index=False)

    # Reseting the test id 
    test.reset_index(drop=True, inplace=True)
    test['ID'] = test.index

    # Rearanging the columns 
    test = test[['ID', 'district_id', 'age_bin_id', 'gender_id', 'as_of_date_id', 'count']]
    solution = test.copy()
    test.drop(columns='count', inplace=True)
    test.to_csv(os.path.join(output_dir, f'population_test_VILNIUS_{datetime.datetime.now().date()}.csv'), index=False)

    # Creating a solution entry for the participants in the kaggle competition
    # Random 50% of the rows will have the value for "Usage" as "Public"
    solution['Usage'] = 'Private'
    solution.loc[solution.sample(frac=0.5).index, 'Usage'] = 'Public'
    
    solution.to_csv(os.path.join(output_dir, f'population_solution_VILNIUS_{datetime.datetime.now().date()}.csv'), index=False)