import config
import praw
import datetime
import pandas as pd
import os
import datetime as dt
from psaw import PushshiftAPI

submission_id_file = 'submission_id_parse.txt'
SUBREDDIT_NAME = 'CBD'


def authenticate():
    print('Authenticating User....')
    reddit = praw.Reddit(client_id=config.client_id,
                         client_secret=config.client_secret,
                         user_agent=config.user_agent,
                         username=config.username,
                         password=config.password)
    print("User '{user}' Authenticated".format(user=reddit.user.me()))
    return reddit


def parsed_ids(file):
    if not os.path.isfile(file):
        replied_ids = []
    else:
        with open(file, 'r') as fin:
            replied_ids = fin.read()
            replied_ids = replied_ids.split('\n')
            replied_ids = list(filter(None, replied_ids))
    return replied_ids


def save_data(data_frame):
    if os.path.exists('./results.csv'):
        with open('results.csv', 'a') as fout:
            data_frame.to_csv(fout, index=False, header=False)
    else:
        print('Creating File...')
        data_frame.to_csv('results.csv', index=False)
        print('File Created')


def log_id(id_num, id_list):
    with open(id_list, 'a') as fout:
        fout.write(id_num + '\n')


def scrape_data(submission_id_list, api):

    def get_date(created):
        return datetime.datetime.fromtimestamp(created)

    comment_dct = {
        "username": [],
        "created at": [],
        "comment content": [],
        "comment link": [],
        "post title": [],
        "post link": [],
    }

    start_epoch = int(dt.datetime(2017, 1, 1).timestamp())
    submission_ids = list(api.search_submissions(after=start_epoch,
                                                 subreddit='CBD',
                                                 filter=['url', 'author', 'title', 'subreddit']))
    print(len(submission_ids))
    try:
        for submission in submission_ids:#  subreddit.hot(limit=number_of_posts):
            if submission.id not in submission_id_list:
                print('Submission found: {} {}'.format(submission.id,dt.datetime.now()))
                log_id(submission.id, submission_id_file)

                submission.comments.replace_more()
                for comment in submission.comments.list():
                    if comment.author:
                        comment_dct['username'].append(comment.author)
                        comment_dct['created at'].append(get_date(comment.created_utc))
                        comment_dct['comment content'].append(comment.body)
                        comment_dct['comment link'].append('https://www.reddit.com/' + comment.permalink)
                        comment_dct['post title'].append(submission.title)
                        comment_dct['post link'].append('https://www.reddit.com/' + submission.permalink)
                    
    except Exception as e:
        print('Error! {}'.format(e))
        
    finally:
        data_frame = pd.DataFrame(comment_dct, columns=['username',
                                                    'created at',
                                                    'comment content',
                                                    'comment link',
                                                    'post title',
                                                    'post link'])
        save_data(data_frame)


def main():
    submission_id_list = parsed_ids(submission_id_file)

    reddit = authenticate()
    api = PushshiftAPI(reddit)
    scrape_data(submission_id_list,api)


if __name__ == '__main__':
    main()
