from sklearn.base import BaseEstimator, TransformerMixin


class NextValueAdder(BaseEstimator, TransformerMixin):
    def __init__(self, cols_to_process):
        self.cols_to_process = cols_to_process
        self.new_cols = None

    def fit(self, df):
        return self

    def transform(self, df):
        sorted_df = df.sort_index(ascending=True)
        self.new_cols = {x: sorted_df[x].groupby(level=0).shift(-1) for x in self.cols_to_process}
        for col in self.new_cols.keys():
            sorted_df["next_" + col] = self.new_cols[col]
        return sorted_df


class NumericalFeatureImputation(TransformerMixin, BaseEstimator):
    def __init__(self, strategy="median"):
        self.strategy = strategy
        self.numerical_columns = []
        self.fill_na_dict = {}

    def fit(self, df):
        self.numerical_columns = df.select_dtypes(include=[int, float]).columns
        if self.strategy == "median":
            self.fill_na_dict = {c: df[c].median() for c in self.numerical_columns}
        elif self.strategy == "mean":
            self.fill_na_dict = {c: df[c].median() for c in self.numerical_columns}
        else:
            raise Exception("Unknown strategy " + self.strategy)
        return self

    def transform(self, df):
        df_imputed = df.fillna(self.fill_na_dict, inplace=True)
        print df_imputed.isnull().sum()
        return df_imputed


class PandasDfToNpArrayConverter(TransformerMixin, BaseEstimator):
    def __init__(self):
        self.features = []

    def fit(self, df):
        self.features = df.sort_index(axis=1).columns
        return self

    def transform(self, df):
        return df.sort_index(axis=1).as_matrix()

