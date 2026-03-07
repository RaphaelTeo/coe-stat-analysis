import pandas as pd
path_deregistered = 'data/MotorVehiclesDeRegisteredUnderVehicleQuotaSystemMonthly.csv'
path_registered = 'data/NewRegistrationOfMotorVehiclesUnderVehicleQuotaSystemMonthly.csv'
path_processed = 'data_processed/processed_net_registrations.csv'
deregistered = pd.read_csv(path_deregistered)
registered = pd.read_csv(path_registered)

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

def basicprep_vehregs(df, index_monthstart, index_yearend):
    df_t = transpose_df(df)
    df_t.rename(columns={'index': 'month', '    Category A: Cars':'Category A', '    Category B: Cars':'Category B', '    Category D: Motorcycles & Scooters':'Category D'}, inplace=True)
    #print(df_t.columns) #discovered the leading spaces
    df_mainmetrics = df_t.loc[:, ['month','Category A', 'Category B', 'Category D']]

    # Convert date format to YYYY-MM
    text_months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    numerical_months = ['01','02','03','04','05','06','07','08','09','10','11','12']
    df_mainmetrics.loc[:,'month'] = df_mainmetrics.loc[:,'month'].apply(convert_date_format, args=(text_months, numerical_months, index_monthstart, index_yearend))
    
    # Convert month column to datetime and other columns to int
    #df_mainmetrics = df_mainmetrics.astype({'month': 'datetime64[ns]', 'Category A': int, 'Category B': int, 'Category D': int}) #error caused by a '-' value
    df_mainmetrics = df_mainmetrics.replace('-', 0) # for the specific error case
    df_mainmetrics['month'] = pd.to_datetime(df_mainmetrics['month'], format='%Y-%m')
    df_mainmetrics['Category A'] = df_mainmetrics['Category A'].fillna(0).astype(int)
    df_mainmetrics['Category B'] = df_mainmetrics['Category B'].fillna(0).astype(int)
    df_mainmetrics['Category D'] = df_mainmetrics['Category D'].fillna(0).astype(int)
    return df_mainmetrics

# PROCESS DATA
# Process vehicle registration and deregistration datasets
registered_prep = basicprep_vehregs(registered, 4, 4)
deregistered_prep = basicprep_vehregs(deregistered, 4, 4)

# Combined both datasets to get net registrations
net_registrations = pd.merge(registered_prep,deregistered_prep, how='inner', on='month', suffixes=('_registered', '_deregistered'))

for category in ['Category A', 'Category B', 'Category D']:
    net_registrations[f'Net {category}'] = net_registrations[f'{category}_registered'] - net_registrations[f'{category}_deregistered']
    net_registrations.drop(columns=[f'{category}_registered', f'{category}_deregistered'], inplace=True)

# Cut off at 2010 01
index_of_jan2010 = net_registrations[net_registrations['month'] == '1/1/2010'].index[-1] #get the last occurrence
net_registrations = net_registrations.loc[:index_of_jan2010]
net_registrations = net_registrations[::-1]

net_registrations.to_csv(path_processed, index=False)