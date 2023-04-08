import pandas as pd
import re


class WindData():

    @staticmethod
    def get_df(url):
        '''
        A function to extract wind data from a given url

        Parameters
        ----------
        url : str

        Returns
        -------
        pd.DataFrame: A dataframe of wind data
        '''
        return pd.read_excel(url, skiprows=2)

    @staticmethod
    def _filter_site(site_name, df, cols_to_keep=['date_time']):
        for col in df.columns:
            if site_name in col:
                cols_to_keep.append(col)
        return df[cols_to_keep]

    @staticmethod
    def _remove_duplicates(my_list):
        seen = set()
        return [x for x in my_list if not (x in seen or seen.add(x))]

    @staticmethod
    def _clean_column_names(column_names):
        cols_clean = [col.replace('&', '').replace(
            ' ', '_').lower() for col in column_names]
        cols_clean = [re.sub('_+', '_', col) for col in cols_clean]
        return cols_clean

    @classmethod
    def clean_wind_data(cls, df):
        '''
        A function to clean wind data

        Parameters
        ----------
        df : pd.DataFrame

        Returns
        -------
        pd.DataFrame: A cleaned dataframe of wind data
        '''

        df_tmp = df.copy()
        df_tmp.columns = cls._clean_column_names(df_tmp.columns)
        df_tmp.set_index('date_time', inplace=True)

        def replace_col_names(columns):
            locations = cls._remove_duplicates(
                ([col.replace('.1', '') for col in columns]))
            metrics = cls._clean_column_names(
                cls._remove_duplicates(list(df_tmp.iloc[0].values)))
            units = cls._clean_column_names(
                cls._remove_duplicates(list(df_tmp.iloc[1].values)))
            met_unit = [f'{met}({unit})' for met,
                        unit in list(zip(metrics, units))]
            return pd.MultiIndex.from_product([locations, met_unit])

        df_tmp.columns = replace_col_names(df_tmp.columns)
        df_tmp = df_tmp.iloc[2:-8]  # remove first 2 rows and last 8 rows
        # convert index to datetime
        df_tmp.index = pd.to_datetime(
            df_tmp.index, format='%d/%m/%Y %H:%M', utc=True)
        # convert all values to numeric
        df_tmp = df_tmp.apply(pd.to_numeric, errors='coerce')
        df_tmp.reset_index(inplace=True)

        return df_tmp
