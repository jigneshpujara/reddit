import sqlite3


class RedditPipeline:
    def __init__(self):
        self.create_connection()
        self.create_table()

    def create_connection(self):
        """
        This function will create the sqlite3 database connection.
        """
        self.conn = sqlite3.connect("reddit.db", timeout=10)
        self.curr = self.conn.cursor()

    def create_table(self):
        """
        This function is for creating tables.
        """
        self.curr.execute("""CREATE TABLE IF NOT EXISTS user (user_name TEXT, karma TEXT, cake_day TEXT)""")
        self.curr.execute(
            """CREATE TABLE IF NOT EXISTS post (id INTEGER PRIMARY KEY AUTOINCREMENT, domain TEXT, title TEXT, post_id TEXT, votes TEXT, 
            num_comments TEXT, description TEXT, time TEXT, post_url TEXT, community_name TEXT )""")
        self.curr.execute(
            """CREATE TABLE IF NOT EXISTS comments (id TEXT PRIMARY KEY, parent_id TEXT, text TEXT, post_id TEXT, 
            FOREIGN  KEY (post_id) REFERENCES post (post_id))""")

    def process_item(self, item, spider):
        self.store_db(item)
        return item

    def store_db(self, item):
        reddit_post = item['reddit_post']
        comment_data = item['comment_data']
        user = item['user']

        user_check_query = f"SELECT user_name from user where user_name = '{user['user_name']}';"
        self.curr.execute(user_check_query)
        sel_query = self.curr.fetchall()
        if not sel_query:
            user_query = "INSERT INTO user ('user_name', 'karma' , 'cake_day') VALUES (?,?,?)"
            self.curr.execute(user_query, (user["user_name"], user["karma"], user["cake_day"]))

        post_check_query = f"select post_id from post where post_id = '{reddit_post['post_id']}';"
        self.curr.execute(post_check_query)
        sel_query = self.curr.fetchall()
        if not sel_query:
            post_query = "INSERT INTO post ('domain', 'title' , 'post_id', 'votes', 'num_comments', 'description'" \
                         ",'time' , 'post_url' , 'community_name') VALUES (?,?,?,?,?,?,?,?,?)"
            self.curr.execute(post_query, (reddit_post['domain'], reddit_post['title'], reddit_post['post_id'],
                                           reddit_post['votes'], reddit_post['num_comments'],
                                           reddit_post['description'], reddit_post['time'], reddit_post['post_url'],
                                           reddit_post['community_name']))

        for comment in comment_data:
            post_id_query = f"SELECT id FROM post where post_id = '{comment['post_id']}';"
            self.curr.execute(post_id_query)
            post_id_index = self.curr.fetchall()
            print(f"ZZZ: {post_id_index}")
            try:
                comment['post_id'] = post_id_index[0][0]
            except IndexError:
                comment['post_id'] = 'NA'


            comment_check_query = f"SELECT id from comments where id = '{comment['id']}';"
            self.curr.execute(comment_check_query)
            sel_query = self.curr.fetchall()
            if not sel_query:
                try:
                    comment["text"] = comment["text"].replace('"', '')
                except AttributeError:
                    print("its an attribute error.")

                comment_query = "INSERT INTO comments (id, parent_id, text, post_id) VALUES (?,?,?,?)"
                self.curr.execute(comment_query,
                                  (comment['id'], comment['parent_id'], comment['text'], comment['post_id']))

        self.conn.commit()

    def close_spider(self, spider):
        self.conn.close()
        print("Connection Closed!!")
