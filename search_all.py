import pandas as pd
import requests
import json
import re
import ast
import datetime
import time
import os
import pickle
import json
from pandas import json_normalize
import signal
import sys

BEARER_TOKEN = os.environ.get("BEARER_TOKEN")
MAX_RESULTS = 500  # A number between 10 and 500.
# DATE = '2020-01-01'
year = '2022'
start = f'{year}-01-01'
end = f'{year}-12-31'
file_name = f'./data/#stressed{year}_0109.pkl'
# file_name = f'./data/opt/{DATE}.pkl'

def sigint_handler(signal, frame):
    #Ctrl+Cをした時に実行する
    # print ('KeyboardInterrupt is caught')
    global df
    if os.path.exists(file_name):
        with open(file_name, 'rb') as handle:
            bef_df = pickle.load(handle)
        df = pd.concat([bef_df, df])

    df.drop_duplicates(subset='text', keep='last', inplace=True)
    print(df,f'量：{len(df)}')
    df.reset_index(drop=True, inplace=True)
    df.to_pickle(file_name)
    sys.exit(0)
signal.signal(signal.SIGINT, sigint_handler)

def create_url(QUERY, MAX_RESULTS):
    # クエリ条件：指定のワードを含む、リツイートを除く、botと思われるユーザーのツイートを除く（自分で追加する)
    query = QUERY
    tweet_fields = 'tweet.fields=author_id,id,text,created_at'
    max_results = f'max_results={MAX_RESULTS}'
    # start_time = f'start_time={DATE}T00:00:00Z'
    # end_time = f'end_time={DATE}T23:59:59Z'
    start_time = f'start_time={start}T00:00:00Z'
    end_time = f'end_time={end}T23:59:59Z'

    # url = f"https://api.twitter.com/2/tweets/search/all?query={query}&{tweet_fields}&{max_results}"
    url = f"https://api.twitter.com/2/tweets/search/all?query={query}&{tweet_fields}&{max_results}&{start_time}&{end_time}"
    return url

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2FullArchiveSearchPython"
    return r

def connect_to_endpoint(url):
    response = requests.request("GET", url, auth=bearer_oauth)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def get_tweet(BEARER_TOKEN, MAX_RESULTS, QUERY):
    bearer_token = BEARER_TOKEN
    url = create_url(QUERY, MAX_RESULTS)
    json_response = connect_to_endpoint(url)
    json_dumps = json.dumps(json_response, indent=4, sort_keys=True)
    return json_dumps
    #return ast.literal_eval(re.sub('\\n\s+', '', json_dumps))

def utc_to_jst(timestamp_utc):
    datetime_utc = datetime.datetime.strptime(
        timestamp_utc + "+0000", "%Y-%m-%d %H:%M:%S.%f%z")
    datetime_jst = datetime_utc.astimezone(
        datetime.timezone(datetime.timedelta(hours=+9)))
    timestamp_jst = datetime.datetime.strftime(
        datetime_jst, '%Y-%m-%d %H:%M:%S')
    return timestamp_jst

def shape_data(data):
    for i, d in enumerate(data):
        # URLの削除
        #data[i]['text'] = re.sub('[ 　]https://t\.co/[a-zA-Z0-9]+', '', d['text'])
        data[i]['text'] = re.sub('https://t\.co/[a-zA-Z0-9]+', '', d['text'])
        # ユーザー名の削除
        data[i]['text'] = re.sub('[ 　]?@[a-zA-Z0-9_]+[ 　]', '', d['text'])
        # # 絵文字の除去
        # data[i]['text'] = d['text'].encode('cp932',errors='ignore').decode('cp932')
        # # ハッシュタグの削除
        # data[i]['text'] = re.sub('#.+ ', '', d['text'])
        # 全角スペース、タブ、改行を削除
        data[i]['text'] = re.sub(r"[\u3000\t\n]", "", d['text'])
        # 日付時刻の変換（UTCからJST）
        # data[i]['created_at'] = utc_to_jst(
        #     d['created_at'].replace('T', ' ')[:-1])
    return data

#ここからメインのところ
df = pd.DataFrame()
iterator, request_iterator = 0, 0


#"(love OR thank OR look OR want OR hope) "
#'(hate OR fuck OR kill OR try OR cry)'


#ハッシュタグの検索の場合には注意　#を%23と書く必要がある
# target_word = "(love OR thank OR look OR want OR hope OR great OR loved OR wonderful OR gload OR kind) "
# target_word = "#stressed "
target_word = "%23stressed "
query_ = "lang:en -is:retweet"
next_token = ''
break_flag = False

while True:
    try:
        info['meta']['next_token']
    except KeyError:  # 次ページがない(next_tokenがない)場合はループを抜ける
        del data
        break_flag = True
    except NameError:  # TARGET_WORDS内の各要素で初めてAPIを取得するとき
        query = query_
    else:  # 2ページめ以降の処理
        next_token = info['meta']['next_token']
        query = query_ + '&next_token=' + next_token
    finally:
        # 次ページがないときにはbreak
        if break_flag:
            break

        # request error 429 対策，sleepを少しだけはさむ．
        time.sleep(1)
        QUERY = f'{target_word}{query}'
        data = get_tweet(BEARER_TOKEN, MAX_RESULTS, QUERY)
        info = json.loads(data)
        temp_df = json_normalize(shape_data(info['data']))
        print(temp_df)
        df = pd.concat([df, temp_df])

        iterator += info['meta']['result_count']

        request_iterator += 1
        print(f'{request_iterator}回目のリクエストです{iterator}取得しました')

        if request_iterator >= 180: # 180requestを超えたら止める
            print('180リクエストを超えるため、15分間停止します...')
            time.sleep(15.01*60) # 15分間（余裕をみてプラス1秒弱）中断
            request_iterator = 0

print(os.path.exists(file_name))
if os.path.exists(file_name):
    with open(file_name, 'rb') as handle:
        bef_df = pickle.load(handle)
        df = pd.concat([bef_df, df])

df.drop_duplicates(subset='text', keep='last', inplace=True)
df.reset_index(drop=True, inplace=True)
df.to_pickle(file_name)
