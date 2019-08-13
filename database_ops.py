from pymongo import MongoClient
from tqdm import tqdm

from credentials import creds


def transfer_from_local_to_remote():
    """
    Transfers all analytical360 collections from local to remote
    """
    local_client = MongoClient()
    local_db = local_client['analytical360']
    collections = local_db.list_collection_names()

    uname = creds['username']
    pwd = creds['password']
    server = creds['server']

    conn_str = 'mongodb+srv://{}:{}@{}.mongodb.net/test?retryWrites=true&w=majority'.format(uname, pwd, server)
    remote_client = MongoClient(conn_str)
    remote_db = remote_client['products']

    for c in collections:
        print(c)
        coll = local_db[c]
        for doc in tqdm(list(coll.find())):
            existing = list(remote_db[c].find({**doc}))
            if len(existing) == 0:
                remote_db[c].insert_one(doc)
