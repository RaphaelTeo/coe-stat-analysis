import pandas as pd
path_coe = 'data/COEBiddingResultsPrices.csv'
path_processed = 'data_processed/processed_coe.csv'
coe = pd.read_csv(path_coe, thousands=',') #thousands separator causing issues

# HELPFUL FUNCTIONS
def transpose_df(df):
    df_t = df.transpose()
    df_t.columns = df_t.iloc[0] #declare header
    df_t = df_t[1:] #remove extra header
    df_t.reset_index(inplace=True)
    df_t.rename_axis(None, axis=1, inplace=True) #index was called "Data Series"
    return df_t

# Function to convert date format to YYYY-MM in digits
def convert_date_format(x, original_months, converted_months, index_monthstart, index_yearend):
    ref_dict = dict(zip(original_months, converted_months))
    this_date = x[index_monthstart:] #start the search from the 5th character to skip the year and space
    return f'{x[:index_yearend]}-{ref_dict[this_date]}'

def rebase_index_monthly(row, staticdf, year_base, label_time, label_value, label_category):
    value = row[label_value]
    month_base = row[label_time][5:]    
    value_base = float(staticdf.loc[(staticdf[label_time] == f'{year_base}-{month_base}') & (staticdf[label_category] == row[label_category]), label_value].iloc[0])
    return round(value/value_base*100,3)

def rebase_index_baseperiod(row, staticdf, value_base, label_time, label_value, label_category): 
    value = row[label_value]
    value_base = float(staticdf.loc[(staticdf[label_time] == f'{value_base}') & (staticdf[label_category] == row[label_category]), label_value].iloc[0])
    return round(value/value_base*100,3)


# PROCESS DATA
# Cut off at last date of Nov 2025
index_of_2025Nov = coe[coe['month'] == '2025-11'].index[-1] #get the last occurrence
coe_nov25 = coe.loc[:index_of_2025Nov]

# Average bids 1 and 2
coe_nov25_bidavg = coe_nov25.groupby(['month','vehicle_class']).mean().reset_index().drop(columns=['bidding_no'])

# Flatten multiindex columns
coe_nov25_bidavg.columns = coe_nov25_bidavg.columns.to_flat_index()
#coe_nov25_bidavg.to_csv('temp.csv')

# Index COE premiums to 2010 values
staticdf = coe_nov25_bidavg.copy()
coe_nov25_bidavg['coe_indexed_monthly'] = coe_nov25_bidavg.apply(rebase_index_monthly, axis=1, args=(staticdf, 2010, 'month', 'premium', 'vehicle_class'))

# Index COE premiums to 2010-01
coe_nov25_bidavg['coe_indexed_2010-01'] = coe_nov25_bidavg.apply(rebase_index_baseperiod, axis=1, args=(staticdf, '2010-01', 'month', 'premium', 'vehicle_class'))

# Convert month column to datetime
coe_nov25_bidavg['month'] = pd.to_datetime(coe_nov25_bidavg['month'], format='%Y-%m')


coe_nov25_bidavg.to_csv(path_processed, index=False)
