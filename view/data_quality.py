import pandas as pd


class DataQualityChecker:
    def __init__(self, df):
        self.df = df
        self.report = {}

    def generate_report(self):
        self._check_missing_values()
        self._check_data_types()
        self._check_duplicates()
        return self.report

    def fix_data(self):
        # Example fixes
        self.df = self.df.dropna()  # Handle missing values
        self.df = self.df.drop_duplicates()
        return self.df

    def _check_missing_values(self):
        self.report['missing_values'] = self.df.isnull().sum().to_dict()

    def _check_data_types(self):
        self.report['data_types'] = self.df.dtypes.astype(str).to_dict()

    def _check_duplicates(self):
        self.report['duplicates'] = int(self.df.duplicated().sum())