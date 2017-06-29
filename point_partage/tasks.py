import csv

import pandas as pd
from sklearn.externals import joblib
from sklearn.ensemble.forest import RandomForestRegressor
from sklearn.feature_selection.variance_threshold import VarianceThreshold
from sklearn.preprocessing.imputation import Imputer

from point_partage import transformers
from point_partage.column_names import BID, CLICKS, INDEX_COLUMNS, TARGET

COLS_TO_DROP = ["Last Pushed"]


def add_date_to_csv(input_target, output_target, date):
    with output_target.open("w") as output_stream:
        writer = csv.writer(output_stream)
        with input_target.open("r") as input_stream:
            reader = csv.reader(input_stream,
                                delimiter=';')
            new_rows = []
            header = next(reader)
            new_header = header + ['date']
            new_rows.append(new_header)
            string_date = date.isoformat()
            for row in reader:
                new_row = row + [string_date]
                new_rows.append(new_row)
            writer.writerows(new_rows)


def train_and_save_model(input_targets, output_target):
    data = read_data(input_targets)
    model = train_model(data)
    save_model(model, output_target)


def train_model(data):
    indexed_data = data.set_index(INDEX_COLUMNS)
    data_with_next_values = transformers.NextValueAdder(cols_to_process=[CLICKS, BID]) \
        .fit_transform(indexed_data)
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    numeric_data = data_with_next_values.select_dtypes(include=numerics)
    numeric_data.loc[numeric_data[TARGET].isnull(), TARGET] = 0
    y = numeric_data[TARGET]
    numeric_data = numeric_data.drop([TARGET] + COLS_TO_DROP, axis=1)
    imputed_data = Imputer(strategy='median', axis=1).fit_transform(numeric_data)
    vt = VarianceThreshold(threshold=0.02)
    x = vt.fit_transform(imputed_data)
    rf = RandomForestRegressor(n_estimators=50, n_jobs=4)
    rf.fit(x, y)
    return rf


def read_data(input_targets):
    input_dfs = []
    for target in input_targets:
        input_dfs.append(pd.read_csv(target.path))
    data = pd.concat(input_dfs, ignore_index=True)
    return data


def save_model(model, output_target):
    joblib.dump(model, output_target.path)
