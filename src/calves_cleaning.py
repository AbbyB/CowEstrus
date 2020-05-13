import pandas as pd
pd.set_option('mode.chained_assignment', None)

import os



columns = ['cow_id', 'lactation_number', 'event', 'days_in_milk', 'date', 'remark', 'r', 't', 'b']

breedings = pd.read_csv(
    'sample/breedings.csv',
    header=0,
    names=columns,
    converters={'event': lambda x: x.strip()})

breedings.days_in_milk = pd.to_timedelta(breedings.days_in_milk, unit='days')
breedings.date = pd.to_datetime(breedings.date, errors='coerce')
breedings['calving_date'] = breedings.date - breedings.days_in_milk

breedings.drop(['remark', 'r', 't', 'b', 'days_in_milk', 'date'], axis=1, inplace=True)
breedings = breedings.drop_duplicates().reset_index(drop=True)

# find weird entries, remove, and log
cow_groups = breedings.sort_values(by=['cow_id', 'event', 'calving_date']).groupby(by=['cow_id', 'event'])
duplicate_births = breedings[cow_groups.calving_date.diff() <= pd.Timedelta(days=280)]
indices = (duplicate_births.index-1).append(duplicate_births.index).sort_values()
duplicate_births = breedings.loc[indices, :]
if len(duplicate_births) > 0:
    print('Duplicate births:')
    print(duplicate_births)
breedings = breedings[~breedings.isin(duplicate_births)].dropna()

breedings = breedings.sort_values(by=['cow_id'])
breedings.to_csv('sample cleaned/breedings.csv', index=False)

cows = dict()
columns = ['act_heat_index', 'act_heat_index_smart', 'activity', 'heat_index', 'activity_1day_avg', '60percentile_of_5day_temp', 'temp', 'temp_dec_index', 'temp_height_index', 'temp_inc_index', 'temp_without_drink_cycles', 'time', 'cow_id']

for file_name in os.listdir('sample/'):
    if file_name != 'breedings.csv' and file_name.split('.')[1] == 'csv':
        data = pd.read_csv('sample/' + file_name)

        data.columns = columns
        data.time = pd.to_datetime(data.time, errors='coerce')
        data = data[~data.activity_1day_avg.isnull()]
        data = data[['activity', 'temp_without_drink_cycles', 'time', 'cow_id']]
        data = data.reset_index(drop=True)

        # checks for missing data at beginning/ end and removes it
        for i, row in data[data.time.diff() > pd.Timedelta(hours=1, minutes=10)].iterrows():
            if i < 14*24*6:
                data = data[data.index >= i]
            elif len(df) - i < 14*24*6:
                data = data[data.index < i]
        data.set_index(data.time, inplace=True)
        data.drop(['time'], axis=1, inplace=True)

        # smoothes outliers
        data.loc[(data.activity < data.activity.mean() - 2 * data.activity.std()), 'activity'] = data.activity.mean() - 2 * data.activity.std()
        data.loc[(data.activity > data.activity.mean() + 2 * data.activity.std()), 'activity'] = data.activity.mean() + 2 * data.activity.std()
        data.loc[(data.temp_without_drink_cycles < data.temp_without_drink_cycles.mean() - 2 * data.temp_without_drink_cycles.std()), 'temp_without_drink_cycles'] = data.temp_without_drink_cycles.mean() - 2 * data.temp_without_drink_cycles.std()
        data.loc[(data.temp_without_drink_cycles > data.temp_without_drink_cycles.mean() + 2 * data.temp_without_drink_cycles.std()), 'temp_without_drink_cycles'] = data.temp_without_drink_cycles.mean() + 2 * data.temp_without_drink_cycles.std()

        # smoothing
        # TODO: make the rolling based on time
        data.temp_without_drink_cycles = data.temp_without_drink_cycles.rolling(12*6, min_periods=1, center=True).mean()
        data.activity = data.activity.rolling(24*6, min_periods=1, center=True).mean()

        # fill in missing data
        data.temp_without_drink_cycles.interpolate(method='time', inplace=True)
        data.activity.interpolate(method='time', inplace=True)

        cow_id = data.cow_id.unique()[0]
        print(file_name, cow_id)
        plot_data(data, cow_id)
#         cows[cow_id] = data
#         data.to_csv('sample cleaned/' + str(cow_id) + '.csv', index=True)
