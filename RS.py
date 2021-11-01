import requests
import pandas as pd
import numpy as np
import datetime


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


class MyDataFrame:

    def create_post_df(self, json_file):
        """
        Creates pandas DataFrame object containing posts characteristics: datetime, author name, number of upvotes etc.
        :param json_file:
        :return: Pandas DataFrame containing posts
        """
        df = pd.DataFrame()
        for post in json_file['data']['children']:
            df = df.append({"Time": datetime.datetime.utcfromtimestamp(post['data']['created_utc']),
                                "User Name": post['data']['author'],
                                "User Id": post['data']['author_fullname'],
                                "Title": post['data']['title'],
                                "Body": post['data']['selftext'],
                                "Subreddit Name": post['data']['subreddit'],
                                "Subreddit Id": post['data']['subreddit_id'],
                                "Downs": post['data']['downs'],
                                "Ups": post['data']['ups'],
                                "Post full Id": post['data']['name'],
                                "Id": post['data']['id'],
                                "Upvote ratio": post['data']["upvote_ratio"],
                                "Is original": post['data']['is_original_content'],
                                "Category": post['data']['category'],
                                "Score": post['data']['score'],
                                "Number of comments": post['data']['num_comments'],
                                "Media url": post['data']['url'],
                                "Total awards received": post['data']['total_awards_received'],
                                "URL": "https://www.reddit.com" + post['data']['permalink'],
                                }, ignore_index=True)

        return df

    def create_comments_df(self, json_file):
        """
        Creates pandas DataFrame object containing comments connected to each post
        :param json_file:
        :return: Pandas DataFrame
        """
        df = pd.DataFrame()

        info = json_file[0]['data']['children'][0]['data']
        print(info['num_comments'])

        df = df.append({"PostId": info.get('name'),
                        "Comment": info.get('selftext'),
                        "DateTime": info.get('created_utc'),
                        "Subreddit": info.get('subreddit'),
                        "SubredditID": info.get('subreddit_id'),
                        "AuthorID": info.get('author_fullname'),
                        "Author Name": info.get('author'),
                        "CommentId": info.get('name'),
                        "ParentId": info.get('parent_id'),
                        "Upvotes": info.get('ups'),
                        "Score": info.get('score'),
                        "Downvotes": info.get('downs'),
                        "URL": info.get('url')},
                       ignore_index=True)

        for i in json_file:
            for j in i['data']['children']:
                temp = j['data'].get('replies')

                while isinstance(temp, dict):
                    info = temp['data']['children'][0]['data']
                    df = df.append({"PostId": info.get('link_id'),
                                "Comment": info.get('body'),
                                "DateTime": info.get('created'),
                                "Subreddit": info.get('subreddit'),
                                "SubredditID": info.get('subreddit_id'),
                                "AuthorID": info.get('author_fullname'),
                                "Author Name": info.get('author'),
                                "CommentId": info.get('name'),
                                "ParentId": info.get('parent_id'),
                                "Upvotes": info.get('ups'),
                                "Score": info.get('score'),
                                "Downvotes": info.get('downs'),
                                "URL": info.get('url')},
                               ignore_index=True)
                    if isinstance(temp['data']['children'][0]['data'].get('replies'), dict):
                        temp = temp['data']['children'][0]['data'].get('replies')
                    else:
                        break


                df = df.append({"PostId": j['data'].get('link_id'),
                                "Comment": j['data'].get('body'),
                                "DateTime": j['data'].get('created'),
                                "Subreddit": j['data'].get('subreddit'),
                                "SubredditID": j['data'].get('subreddit_id'),
                                "AuthorID": j['data'].get('author_fullname'),
                                "Author Name": j['data'].get('author'),
                                "CommentId": j['data'].get('name'),
                                "ParentId": j['data'].get('parent_id'),
                                "Upvotes": j['data'].get('ups'),
                                "Score": j['data'].get('score'),
                                "Downvotes": j['data'].get('downs'),
                                "URL": j['data'].get('url')},
                               ignore_index=True)
        df.dropna(subset=["PostId"], inplace=True)
        return df


class Reddit:
    """
    Main class, contains methods which connect to Reddit via requests package.
    """

    data = {'grant_type': 'password',
                'username': 'Quick-Wear-6539',
                'password': 'nawvoj-Xahkij-xotdu0'}
    params = {'limit': '100', 'after': ''}
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
                return MyDataFrame().create_post_df(res.json())
            except ValueError:
                print(f"Encountered error during creating dataframe of subreddit: {subreddit} posts!")

    def posts_request(self, phrase, sort_type):
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
                return MyDataFrame().create_post_df(res.json())
            except ValueError:
                print(f"Encountered error during creating dataframe of phrase: {phrase} posts!")

    def users_request(self, name, sort_type):
        """
        Extracts user names from post and comment dataframes, makes requests to posses information about users.
        Writes it down to csv file.

        :param post_users: Dataframe containing posts info
        :param comm_users: Dataframe containing comments info
        :return: Pandas DataFrame containing users information
        """

        res = requests.get(f"https://oauth.reddit.com/search/?q={name}&sort={sort_type}", headers=self.headers, params=self.params)
        res.raise_for_status()
        if res.status_code != 204:
            try:
                return MyDataFrame().create_post_df(res.json())
            except ValueError:
                print(f"Encountered error during creating dataframe of user: {name} posts!")

    def comments_request(self, names):
        """
        Collects comments connected with each post.

        :param names: Names of post (strings which are basically posts ID)
        :return: Pandas DataFrame containing comments
        """
        print(names)
        data = pd.DataFrame()
        for name in names:
            print(name)
            res = requests.get(f"https://www.reddit.com/comments/{name}.json", headers={'User-Agent': 'TemporaryTesting/0.0.1'})
            res.raise_for_status()
            if res.status_code != 204:
                data = data.append(MyDataFrame().create_comments_df(res.json()))
        return data

    def search_users_posts(self, users_list, limits_list, sorts_list):
        """
        Iterates through provided users names, limits, sort types and calls user_request function.
        Saves collected posts from every user to an independent data file.

        :param users_list: List of users names
        :param limits_list: List of number of elements to be collected from each user
        :param sorts_list: List of post sorting types for each user: Hot/New/Top
        :return: None
        """
        x = [users_list, limits_list, sorts_list]
        print(x)
        length = len(users_list)
        infos = np.array([xi + [None]*(length - len(xi)) for xi in x])
        infos = infos.transpose()

        for info in infos:
            df = pd.DataFrame()
            df_com = pd.DataFrame()
            limit = int(info[1])

            while np.divmod(limit, 100)[0] >= 0:
                if(np.divmod(limit, 100)[0] == 0):
                    self.params['limit'] =  np.divmod(limit, 100)[1]
                    df = df.append(self.users_request(info[0], info[2]), ignore_index=True)
                    df_com = df_com.append(self.comments_request(df.loc[df['Number of comments'] > 0, 'Id'].array))
                else:
                    self.params['limit'] = '100'
                    df = df.append(self.users_request(info[0], info[2]), ignore_index=True)
                    df_com = df_com.append(self.comments_request(df.loc[df['Number of comments'] > 0, 'Id'].array))
                limit -= 100
                self.params['after'] = df.iat[-1, 5]

            self.save_to_csv(df, info[0], info[2], 'posts')
            self.save_to_csv(df_com, info[0], info[2], 'comments')
            self.params['after'] = ''
            self.params['limit'] = '100'

    def search_subreddits_posts(self, subreddits_list, limits_list, sorts_list):
        """
        Iterates through provided subreddits, limits, sort types and calls function subreddit_request.
        Saves collected posts from every searched subreddit to independent data file.

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
            df = pd.DataFrame()
            df_com = pd.DataFrame()
            limit = int(info[1])

            while np.divmod(limit, 100)[0] >= 0:
                if (np.divmod(limit, 100)[0] == 0):
                    self.params['limit'] = np.divmod(limit, 100)[1]
                    df = df.append(self.subreddit_request(info[0], info[2]), ignore_index=True)
                    df_com = df_com.append(self.comments_request(df.loc[df['Number of comments'] > 0, 'Id'].array))
                else:
                    self.params['limit'] = '100'
                    df = df.append(self.subreddit_request(info[0], info[2]), ignore_index=True)
                    df_com = df_com.append(self.comments_request(df.loc[df['Number of comments'] > 0, 'Id'].array))
                limit -= 100
                self.params['after'] = df.iat[-1, 5]

            self.save_to_csv(df, info[0], info[2], 'posts')
            self.save_to_csv(df_com, info[0], info[2], 'comments')
            self.params['after'] = ''
            self.params['limit'] = '100'

    def search_whole_reddit(self, search_list, limits_list, sorts_list):
        """
        Iterates through provided phrases, limits, sort type lists and calls function reddit_request.
        Saves collected posts from whole reddit to independent data to file for each phrase.

        :param search_list: list of phrases to look for
        :param limits_list: list containing number of posts to be extracted for each phrase
        :param sorts_list: list containing sorting types for each phrase
        :return: None
        """
        x = [search_list, limits_list, sorts_list]
        length = len(search_list)
        infos = np.array([xi + [None]*(length - len(xi)) for xi in x])
        infos = infos.transpose()

        for info in infos:
            print(info)
            df = pd.DataFrame()
            df_com = pd.DataFrame()
            limit = int(info[1])

            while np.divmod(limit, 100)[0] >= 0:
                if (np.divmod(limit, 100)[0] == 0):
                    self.params['limit'] = np.divmod(limit, 100)[1]
                    df = df.append(self.posts_request(info[0], info[2]), ignore_index=True)
                    df_com = df_com.append(self.comments_request(df.loc[df['Number of comments'] > 0, 'Id'].array))
                else:
                    self.params['limit'] = '100'
                    df = df.append(self.posts_request(info[0], info[2]), ignore_index=True)
                    df_com = df_com.append(self.comments_request(df.loc[df['Number of comments'] > 0, 'Id'].array))
                limit -= 100
                self.params['after'] = df.iat[-1, 5]

            self.save_to_csv(df, info[0], info[2], 'posts')
            self.save_to_csv(df_com, info[0], info[2], 'comments')
            #self.save_to_csv(self.users_requests(df, df_com), info[0], info[2], 'users')
            self.params['limit'] = '100'
            self.params['after'] = ''

    def save_to_csv(self, dataframe, file_name, sort_type, suffix):
        """
        Saves dataframe to file name: 'file_name_sort_type.csv'

        :param dataframe:
        :param file_name:
        :param sort_type:
        :return: None
        """
        if isinstance(dataframe, pd.DataFrame):
            dataframe.to_csv(f'Data/{file_name}_{sort_type}_{suffix}.csv')


if __name__ == '__main__':

    red = Reddit()
    red.connect()

    search_sort_types = ["relevance", "new", "hot", "top", "comments"]
    subreddit_sort_types = ["hot", "new", "top", "rising"]

    user_names = ["Tradingwaves1", "Anonymous_Crispy", "csfxmarketnews"]
    phrases = ["USDJPY", "EURUSD", "GBPUSD"]
    sub = ["forex", "Python", "Polska"]
    lim = [10, 15, 17]
    sor = ["new", "new", "hot"]
    #red.search_subreddits(sub, lim, sor)
    #red.search_whole_reddit(['USDJPY'], [50], ['hot'])
    red.search_users_posts(user_names, lim, sor)

    '''df = pd.read_csv("Data/USDJPY_hot_posts.csv")
    names = list(df['User Name'].unique())
    print(type(names))
    limits = [20]*len(names)
    sorts = ['new']*len(names)
    red.search_users_posts(names, limits, sorts)'''














