# load modules
import sqlite3
import pandas as pd
import os
import shutil

# create clean output directory
output_dir = 'uuid'
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)
os.makedirs(output_dir)

# connect to index.db
conn = sqlite3.connect('index.db')

# get UUIDs
uuids = pd.read_sql_query("SELECT DISTINCT uuid FROM archive", conn)['uuid'].tolist()
uuids_len = len(uuids)

# create and save JSON for each UUID
for uuid in uuids:
    # get table
    q = f"SELECT * FROM archive WHERE uuid = '{uuid}'"
    df = pd.read_sql_query(q, conn)
    # save JSON
    out = os.path.join(output_dir, f"{uuid}.json")
    df.to_json(out, orient = 'records')
    # print progress every 25 tables
    i = uuids.index(uuid) + 1
    if i % 25 == 0:
        print(f"Processed {i} of {uuids_len} tables")

# close connection
conn.close()
