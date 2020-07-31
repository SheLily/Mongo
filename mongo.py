import pandas as pd

import re

from pprint import pprint



from pymongo import MongoClient

import pymongo





def read_data(csv_file, db):

    data = pd.read_csv(csv_file)

    posts = db.posts

    for i, row in data.iterrows():

        posts.insert_one(row.to_dict()).inserted_id 

    

    





def find_cheapest(db):

    """

    Sort tickets from the database in ascending order of price

    Documentation: https://docs.mongodb.com/manual/reference/method/cursor.sort/

    """

    pprint(db.posts.find().sort('Цена', pymongo.ASCENDING)[0])

    



def find_by_name(name, db):

    """

    Find tickets by artist name (including substring, for instance "Seconds to"),

    and return them in ascending price

    """

    name = '.*'+re.escape(name)+'.*'

    regex = re.compile(name, re.IGNORECASE)

    return list(db.posts.find({'Исполнитель': regex}).sort('Цена', pymongo.ASCENDING))



    





if __name__ == '__main__':

    csv_file = 'artists.csv'

    connect = MongoClient()

    db = connect['local-database']

    db = db['test-collecction']

    read_data(csv_file, db)

    find_cheapest(db)

    pprint(find_by_name('Миха', db))

    
