import pandas as pd
path_cpi = 'data/outputFile.xlsx'
path_processed = 'data_processed/processed_cpi.csv'
cpi = pd.read_excel(path_cpi, sheet_name='T7', skiprows=10) #the only xlsx file

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

def rebase_index_monthly(row, staticdf, year_base, label_time, label_value):
    value = row[label_value]
    month_base = row[label_time][5:]
    print(f'{year_base}-{month_base}: {value}')
    
    value_base = float(staticdf.loc[staticdf[label_time] == f'{year_base}-{month_base}', label_value].iloc[0])
    return round(value/value_base*100,3)

def rebase_index_baseperiod(row, label_value, value_base): 
    value = row[label_value]
    return round(value/value_base*100,3)


# PROCESS DATA
cpi_t = transpose_df(cpi)
cpi_t.rename(columns={'index': 'month'}, inplace=True)
cpi_mainmetrics = cpi_t.loc[:, ['month','All Items']] #only interested in 'All Items'
cpi_mainmetrics_sorted = cpi_mainmetrics[::-1] #reverse the order to have earliest month first

# Cut off at earliest date of coe dataset
index_of_earliestdate = cpi_mainmetrics_sorted[cpi_mainmetrics_sorted['month'] == '2010 Jan'].index[0]
cpi_mainmetrics_sorted_earliestdate = cpi_mainmetrics_sorted.loc[index_of_earliestdate:]

# Convert date format to YYYY-MM
text_months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
numerical_months = ['01','02','03','04','05','06','07','08','09','10','11','12']
cpi_mainmetrics_sorted_earliestdate.loc[:,'month'] = cpi_mainmetrics_sorted_earliestdate.loc[:,'month'].apply(convert_date_format, args=(text_months, numerical_months, 5, 4))

# Re-base to 2010 months
#cpi_mainmetrics_sorted_earliestdate_rebased = rebase_index_old(cpi_mainmetrics_sorted_earliestdate, 2010, 2010, 2026, 'month', 'All Items', numerical_months)
cpi_mainmetrics_sorted_earliestdate['cpi_reindexed_monthly'] = cpi_mainmetrics_sorted_earliestdate.apply(rebase_index_monthly, axis=1, args=(cpi_mainmetrics_sorted_earliestdate.copy(), 2010, 'month', 'All Items'))

# Re-base to 2010-01
value_base = float(cpi_mainmetrics_sorted_earliestdate.loc[cpi_mainmetrics_sorted_earliestdate['month'] == f'2010-01', 'All Items'].iloc[0])
cpi_mainmetrics_sorted_earliestdate['cpi_reindexed_2010-01'] = cpi_mainmetrics_sorted_earliestdate.apply(rebase_index_baseperiod, axis=1, args=( 'All Items', value_base))

# Convert month column to datetime
cpi_mainmetrics_sorted_earliestdate['month'] = pd.to_datetime(cpi_mainmetrics_sorted_earliestdate['month'], format='%Y-%m')

cpi_mainmetrics_sorted_earliestdate.to_csv(path_processed, index=False)
