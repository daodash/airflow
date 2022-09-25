import requests
import pandas as pd
import sqlalchemy as db
from typing import Optional, Dict, List
from airflow.models import Variable

# load env variables
discourse_url = Variable.get("DISCOURSE_URL")
discourse_api_key = Variable.get("DISCOURSE_API_KEY")
discourse_api_username = Variable.get("DISCOURSE_API_USERNAME")
db_string = Variable.get('DB_STRING')
db_schema = Variable.get('DB_SCHEMA') #daodash_ny

# create db engine
db_engine = db.create_engine(db_string)

def get_table_max_id(table_name: str) -> int:
    """
    Get MAX(id) for given table_name
    """
    sql = 'SELECT MAX(id) AS max_id FROM {}.{};'.format(db_schema, table_name)
    with db_engine.connect() as conn:
        result = conn.execute(statement=sql)
        for row in result:
            max_id = row.max_id
        if max_id is None:
            max_id = 0
        #print(table_name, 'max_id:', max_id)
    return max_id

# source: https://blog.alexparunov.com/upserting-update-and-insert-with-pandas
def create_upsert_method(meta: db.MetaData, extra_update_fields: Optional[Dict[str, str]]):
    """
    Create upsert method that satisfied the pandas's to_sql API.
    """
    def method(table, conn, keys, data_iter):
        # select table that data is being inserted to (from pandas's context)
        sql_table = db.Table(table.name, meta, autoload=True)

        # list of dictionaries {col_name: value} of data to insert
        values_to_insert = [dict(zip(keys, data)) for data in data_iter]

        # create insert statement using postgresql dialect
        insert_stmt = db.dialects.postgresql.insert(sql_table, values_to_insert)

        # create update statement for excluded fields on conflict
        update_stmt = {exc_k.key: exc_k for exc_k in insert_stmt.excluded if exc_k.key != 'created_at'}
        if extra_update_fields:
            update_stmt.update(extra_update_fields)

        # create upsert statement
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=sql_table.primary_key.columns, # index elements are primary keys of a table
            set_=update_stmt # the SET part of an INSERT statement
        )

        # execute upsert statement
        conn.execute(upsert_stmt)

    return method

def data_transform_and_load(
    df_to_load: pd.DataFrame,
    table_name: str,
    list_of_col_names: List,
    rename_mapper: Optional[Dict[str, str]] = None,
    extra_update_fields: Optional[Dict[str, str]] = None
):
    """
    Prep given df_to_load and load it to table_name
    """
    # check if DataFrame contains any data, if it doesn't - skip the rest
    if df_to_load.empty:
        return False

    # change json column names to match table column names
    if rename_mapper:
        df_to_load = df_to_load.rename(columns=rename_mapper, inplace=False)

    # include only necessary columns
    df_to_load = df_to_load.filter(list_of_col_names)

    # create DB metadata object that can access table names, primary keys, etc.
    meta = db.MetaData(db_engine, schema=db_schema)

    # create upsert method that is accepted by pandas API
    upsert_method = create_upsert_method(meta, extra_update_fields)

    # perform upsert of DataFrame values to the given table
    df_to_load.to_sql(
        name=table_name,
        con=db_engine,
        schema=db_schema,
        index=False,
        if_exists='append',
        chunksize=200, # it's recommended to insert data in chunks
        method=upsert_method
    )

    # if it got that far without any errors - notify a successful completion
    return True


########################################################
# Categories
def pull_categories():
    api_query = '{}/categories.json'.format(discourse_url)
    table_name = 'discourse_categories'

    # retrieve json results and convert to dataframe
    result = requests.get(api_query).json()
    df = pd.json_normalize(result['category_list']['categories'])

    # prep and upsert data
    data_transform_and_load(
        df_to_load=df,
        table_name=table_name,
        list_of_col_names=['id', 'name', 'slug'],
        extra_update_fields={"updated_at": "NOW()"}
    )

########################################################
# Users
def pull_users():
    for page_n in range(1000):
        api_query = '{}/directory_items.json?period=all&order=topic_count&page={}'.format(discourse_url, page_n)
        table_name = 'discourse_users'

        # retrieve json results and convert to dataframe
        result = requests.get(api_query).json()
        df = pd.json_normalize(result['directory_items'])

        # prep and upsert data
        isLoaded = data_transform_and_load(
            df_to_load=df,
            table_name=table_name,
            list_of_col_names=[
                'id', 'username', 'name', 'days_visited', 'time_read', 'topics_entered',
                'topic_count', 'posts_read', 'post_count', 'likes_received', 'likes_given'
            ],
            rename_mapper={
                'user.username': 'username',
                'user.name': 'name'
            },
            extra_update_fields={"updated_at": "NOW()"}
        )

        # check if current page contains any users, exit loop if it doesn't
        if not isLoaded:
            break


########################################################
# Topics
def pull_topics():
    for page_n in range(1000): # check for more_topics_url instead?
        api_query = '{}/latest.json?order=created&ascending=true&api_key={}&api_username={}&page={}'.format(
            discourse_url, discourse_api_key, discourse_api_username, page_n
        )
        table_name = 'discourse_topics'

        # retrieve json results
        result = requests.get(api_query).json()
        df = pd.json_normalize(result['topic_list']['topics'])

        # extract original and most recent poster using list comprehansion
        #[print(n, x['description'], x['user_id']) for n in range(0, len(df)) for x in df['posters'][n] if 'Original Poster' in x['description']]
        df['user_id'] = [
            x['user_id']
                for n in range(0, len(df))
                    for x in df['posters'][n]
                        if 'Original Poster' in x['description']
        ]
        df['last_post_user_id'] = [
            x['user_id']
                for n in range(0, len(df))
                    for x in df['posters'][n]
                        if 'Most Recent Poster' in x['description']
        ]

        # prep and upsert data
        isLoaded = data_transform_and_load(
            df_to_load=df,
            table_name=table_name,
            list_of_col_names=[
                'id', 'title', 'slug', 'user_id', 'category_id', 'excerpt', 'created_at',
                'last_post_user_id', 'last_posted_at', 'views_count', 'posts_count', 'reply_count',
                'like_count', 'is_pinned', 'is_visible', 'is_closed', 'is_archived'
            ],
            rename_mapper={
                'views': 'views_count',
                'pinned_globally': 'is_pinned',
                'visible': 'is_visible',
                'closed': 'is_closed',
                'archived': 'is_archived'
            },
            extra_update_fields={"updated_at": "NOW()"}
        )

        # check if current page contains any users, exit loop if it doesn't
        if not isLoaded:
            break


########################################################
# Posts & Polls
def pull_posts_and_polls():
    polls = []
    votes = []

    sql = 'SELECT id AS topic_id FROM {}.{} ORDER BY 1;'.format(db_schema, 'discourse_topics')
    with db_engine.connect() as conn:
        result = conn.execute(statement=sql)
        for row in result:
            topic_id = row.topic_id

            api_query = '{}/t/{}.json?api_key={}&api_username={}'.format(
                discourse_url, topic_id, discourse_api_key, discourse_api_username
            )
            table_name = 'discourse_posts'

            # retrieve json results for posts
            result = requests.get(api_query).json()
            posts = result['post_stream']['posts']
            df = pd.json_normalize(posts)

            # prep and upsert data
            isLoaded = data_transform_and_load(
                df_to_load=df,
                table_name=table_name,
                list_of_col_names=[
                    'id', 'topic_id', 'content', 'reply_count', 'reads_count', 'readers_count',
                    'user_id', 'created_at', 'updated_at', 'deleted_at'
                ],
                rename_mapper={
                    'cooked': 'content',
                    'reads': 'reads_count'
                },
                extra_update_fields=None
            )

            # check if current topic contains any data, if it doesn't - skip and continue
            if not isLoaded:
                continue

            # check if there are polls attached to the posts and pull them into saparate DataFrame
            for p in posts:
                if 'polls' in p:
                    # collect polls
                    dfp = pd.json_normalize(p['polls'])
                    dfp['id'] = int(p['id']) * 1000000000 + dfp.index + 1
                    dfp['post_id'] = p['id']
                    dfp['title'] = str(p['topic_slug']).replace('-', ' ') + ' - ' + dfp['name']
                    polls.append(dfp)

                    # collect poll votes
                    for poll_n in range(0, len(p['polls'])):
                        dfv = pd.json_normalize(p['polls'][poll_n]['options'])
                        dfv['post_idx'] = p['id']
                        dfv['poll_idx'] = poll_n + 1
                        dfv['vote_idx'] = dfv.index + 1
                        votes.append(dfv)


    # Polls
    table_name = 'discourse_polls'
    df_polls = pd.concat(polls)

    # prep and upsert data
    data_transform_and_load(
        df_to_load=df_polls,
        table_name=table_name,
        list_of_col_names=[
            'id', 'post_id', 'title', 'status', 'voters_count'
        ],
        rename_mapper={
            'voters': 'voters_count'
        },
        extra_update_fields={"updated_at": "NOW()"}
    )

    # Poll votes
    table_name = 'discourse_poll_votes'
    df_votes = pd.concat(votes)
    df_votes['poll_id'] = df_votes['post_idx'] * 1000000000 + df_votes['poll_idx']
    df_votes['id'] = df_votes['poll_id'] * 100 + df_votes['vote_idx']

    # prep and upsert data
    data_transform_and_load(
        df_to_load=df_votes,
        table_name=table_name,
        list_of_col_names=[
            'id', 'poll_id', 'vote_option', 'votes_count'
        ],
        rename_mapper={
            'html': 'vote_option',
            'votes': 'votes_count'
        },
        extra_update_fields={"updated_at": "NOW()"}
    )


########################################################
# main run
pull_categories()
pull_users()
pull_topics()
pull_posts_and_polls()


########################################################
# final clean up
db_engine.dispose()
