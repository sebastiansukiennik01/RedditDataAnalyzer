import requests
import pandas as pd
import numpy as np
import datetime


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


class MyDataFrame():

    def create_dataframe(self, json_file):
        """
        Creates pandas DataFrame object containing posts characteristics: datetime, author name, number of upvotes etc.
        :param json_file:
        :return: Pandas DataFrame containing posts
        """
        df = pd.DataFrame()
        for post in json_file['data']['children']:
            df = df.append({"Time": datetime.datetime.utcfromtimestamp(post['data']['created_utc']),
                                "Author": post['data']['author'],
                                "Title": post['data']['title'],
                                "Subreddit": post['data']['subreddit'],
                                "Downs": post['data']['downs'],
                                "name": post['data']['name'],
                                "Upvote ratio": post['data']["upvote_ratio"],
                                "Is original": post['data']['is_original_content'],
                                "Category": post['data']['category'],
                                "Score": post['data']['score'],
                                "likes": post['data']['likes'],
                                "Visited": post['data']['visited'],
                                "Id": post['data']['id'],
                                "Author": post['data']['author'],
                                "Number of comments": post['data']['num_comments'],
                                "URL": post['data']['url'],
                                }, ignore_index=True)
        return df

    def create_comments_df(self, json_file):
        """
        Creates pandas DataFrame object containing comments connected to each post
        :param json_file:
        :return: Pandas DataFrame
        """
        df = pd.DataFrame()
        file = json_file[0]['data']['children'][0]['data']
        print(file['num_comments'])
        for i in json_file:
            for j in i['data']['children']:
                print(j['data'].get('body'))

        print(type(file), file)
        print(file.keys())
        return df

class Reddit:
    """
    Main class, contains methods which connect to Reddit via requests package.
    """

    data = {'grant_type': 'password',
                'username': 'Quick-Wear-6539',
                'password': 'nawvoj-Xahkij-xotdu0'}
    params = {'limit': '15', 'after': ''}
    headers = {'User-Agent': 'TemporaryTesting/0.0.1'}

    def __init__(self, client_id="Lr8XDPt5VBjHpMofvjqamA", client_secret="7t6-4401z7KIOC5i5pbjcqNYMtlayg", user_agent="TemporaryTesting"):
        """
        Constructor setting basic credentials, allows connecting to Reddit API
        :param client_id:
        :param client_secret:
        :param user_agent:
        """
        self.my_client_id = client_id
        self.my_client_secret = client_secret
        self.my_user_agent = user_agent

    def connect(self):
        """
        Sends authorization request with credentials, connects to Reddit API
        :return: None
        """
        auth = requests.auth.HTTPBasicAuth(self.my_client_id, self.my_client_secret)
        res = requests.post('https://www.reddit.com/api/v1/access_token',
                            auth=auth, data=self.data, headers=self.headers)
        TOKEN = res.json()['access_token']
        self.headers = {**self.headers, **{'Authorization': f"bearer {TOKEN}"}}
        res = requests.get('https://oauth.reddit.com/api/v1/me', headers=self.headers)

    def subreddit_request(self, subreddit, sort_type):
        """
        Makes request with specified subreddit and sort type
        :param subreddit: Name of subreddit to be explored
        :param sort_type: Type of post sorting: Hot/New/Top/Rising
        :return: Pandas DataFrame containing posts
        """
        res = requests.get(f"https://oauth.reddit.com/r/{subreddit}/{sort_type}", headers=self.headers, params=self.params)
        res.raise_for_status()
        if res.status_code != 204:
            try:
                return MyDataFrame().create_dataframe(res.json())
            except ValueError:
                print("Błąd podczas twoerzenia Dataframe'u")

    def search_subreddits(self, subreddits_list, limits_list, sorts_list):
        """
        Iterates through provided subreddits, limits, sort types and calls function subreddit_request.
        Saves collected data from every searched subreddit to independent data to file.
        :param subreddits_list: List of subreddit names
        :param limits_list: List of number of elements to collect from each subreddit.
        :param sorts_list: List of sorting types: Hot/New/Top/Rising
        :return: None
        """
        x = [subreddits_list, limits_list, sorts_list]
        lenght = len(subreddits_list)
        infos = np.array([xi + [None]*(lenght - len(xi)) for xi in x])
        infos = infos.transpose()

        for info in infos:
            print(info)
            limit = int(info[1])

            df = pd.DataFrame()
            while np.divmod(limit, 100)[0] >= 0:
                limit -= 100
                df = df.append(self.subreddit_request(info[0], info[2]), ignore_index=True)
                self.params['after'] = df.iat[-1, 5]
                self.params['limit'] = limit

            self.save_to_csv(df, info[0], info[2])
            self.params['after'] = ""
            self.params['limit'] = '100'

    def reddit_request(self, phrase, sort_type):
        """
        Searches a specified phrase in all of subreddits
        :param phrase: Searched phrase
        :param sort_type: Best/Hot/New/Top/Rising
        :return: Pandas DataFrame containing posts
        """
        res = requests.get(f"https://oauth.reddit.com/search/?q={phrase}&sort={sort_type}", headers=self.headers, params=self.params)
        res.raise_for_status()
        if res.status_code != 204:
            try:
                return MyDataFrame().create_dataframe(res.json())
            except ValueError:
                print("Błąd podczas twoerzenia Dataframe'u")

    def comments_request(self, names):
        """
        Collects comments connected with each post.
        :param names: Names of post (strings which are basically posts ID)
        :return: Pandas DataFrame containing comments
        """
        print(names)
        data = pd.DataFrame()
        for name in names:
            res = requests.get(f"https://www.reddit.com/comments/{name}.json", headers={'User-Agent': 'TemporaryTesting/0.0.1'})
            res.raise_for_status()
            if res.status_code != 204:
                data = data.append(MyDataFrame().create_comments_df(res.json()))
        return data

    def search_all_reddit(self, search_list, limits_list, sorts_list):
        """
        Iterates through provided phrases, limits, sort type lists and calls function reddit_request.
        Saves collected data from every searched subreddit to independent data to file.
        :param search_list:
        :param limits_list:
        :param sorts_list:
        :return: None
        """
        x = [search_list, limits_list, sorts_list]
        length = len(search_list)
        infos = np.array([xi + [None]*(length - len(xi)) for xi in x])
        infos = infos.transpose()

        for info in infos:
            print(info)
            df = pd.DataFrame()
            df_comm = pd.DataFrame()
            limit = int(info[1])

            while np.divmod(limit, 100)[0] >= 0:
                limit -= 100
                df = df.append(self.reddit_request(info[0], info[2]), ignore_index=True)

                df_comm = df_comm.append(self.comments_request(df['Id'].array))


                self.params['after'] = df.iat[-1, 5]
                self.params['limit'] = limit
            self.save_to_csv(df, info[0], info[2])
            df_comm.to_csv('Data/komentarze.csv')
            self.params['limit'] = '100'
            self.params['after'] = ''


    def save_to_csv(self, dataframe, file_name, sort_type):
        """
        Saves dataframe to file name: 'file_name_sort_type.csv'
        :param dataframe:
        :param file_name:
        :param sort_type:
        :return: None
        """
        if isinstance(dataframe, pd.DataFrame):
            dataframe.to_csv(f'Data/{file_name}_{sort_type}.csv')


if __name__ == '__main__':

    red = Reddit()
    red.connect()

    search_sort_types = ["relevance", "new", "hot", "top", "comments"]
    subreddit_sort_types = ["hot", "new", "top", "rising"]

    phrases = ["USDJPY", "EURUSD", "GBPUSD"]
    sub = ["forex", "Python", "Polska"]
    lim = [2200, 175, 100]
    sor = ["new", "new", "hot"]
    red.search_subreddits(sub, lim, sor)

    '''
    res = requests.get("https://www.reddit.com/comments/q1omt5.json", headers={'User-Agent': 'TemporaryTesting/0.0.1'})
    res.raise_for_status()
    lista = []
    if res.status_code != 204:
        for x in res.json():
            for k in x['data']['children']:
                lista.append(k['data'].get("body"))

    for l in lista:
        print(l, "\n\n\n")'''













