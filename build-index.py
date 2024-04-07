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
    # get table, creating a new column 'file_name_url' that is 'file_name' if 'file_duplicate' is 0,
    # otherwise the 'file_name' where 'file_duplicate' = 0 for the same 'file_md5'
    # this is to point duplicate files to the URL of the original non-duplicate file
    q = f"SELECT a.uuid, a.file_name, a.file_timestamp, a.file_date, a.file_duplicate, a.file_md5, a.file_size, b.file_name AS file_name_url FROM archive a LEFT JOIN (SELECT file_md5, file_name FROM archive WHERE uuid = '{uuid}' AND file_duplicate = 0 GROUP BY file_md5) AS b ON a.file_md5 = b.file_md5 WHERE a.uuid = '{uuid}';"
    df = pd.read_sql_query(q, conn)
    # ensure above process worked
    df2 = pd.read_sql_query(f"SELECT * FROM archive WHERE uuid = '{uuid}';", conn)
    assert df.shape[0] == df2.shape[0]
    assert df['file_name_url'].isnull().sum() == 0
    # save JSON
    out = os.path.join(output_dir, f"{uuid}.json")
    df.to_json(out, orient = 'records')
    # print progress every 25 tables
    i = uuids.index(uuid) + 1
    if i % 25 == 0:
        print(f"Processed {i} of {uuids_len} tables")

# close connection
conn.close()
