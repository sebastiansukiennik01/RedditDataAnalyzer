"""
    This file contains class which main purpouse is to clean, standaraize data, merge posts and comments together
and overall prepare collected data for further analysis.

"""

import pandas as pd
import datetime
import re

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

class Preprocessing:

    def find_url(self, string):
        regex = r'(https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}[-a-zA-Z0-9()@:%_+.~#?&/=]*)'
        url = re.findall(regex, string)
        new_url = [u.replace(")", "") for u in url]
        return new_url

    def clean_com_df(self, file_path):
        """
            Takes filepath to comments dataframe,
        :param file_path:
        :return: Dataframe
        """
        df = pd.read_csv(file_path)
        df['DateTime'] = df['DateTime'].apply(datetime.datetime.utcfromtimestamp)

        df.applymap(self.find_url)
        print(df.head())


    def clean_post_df(self):
        pass

    def merge_pot_com(self):
        pass

    def extrac_information(self):
        pass

if __name__ == '__main__':
    prep = Preprocessing()
    prep.clean_com_df("/Users/sebastiansukiennik/Desktop/PycharmProjects/RedditDataAnalyzer/Data/metaverse_hot_comments.csv")
