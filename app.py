import os
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

load_dotenv()

DEV_URI = os.getenv("dev_mongo_image")
LOCAL_URI = os.getenv("local_mongo_image")
DB_NAME = "pdf_extraction"
BATCH_SIZE = 1000

def copy_collection(src_db, dst_db, coll_name):
    src_coll = src_db[coll_name]
    dst_coll = dst_db[coll_name]

    print(f"🔄 Copying collection: {coll_name}")

    # Drop destination collection for fresh copy
    dst_coll.drop()

    # Copy indexes (except _id default)
    for index in src_coll.list_indexes():
        index_dict = dict(index)
        index_dict.pop("v", None)  # remove internal version
        index_keys = index_dict.pop("key")
        if index_keys != {"_id": 1}:  # skip default _id
            dst_coll.create_index(list(index_keys.items()), **index_dict)

    # Copy documents in batches
    cursor = src_coll.find({})
    batch, count = [], 0

    for doc in cursor:
        batch.append(doc)
        if len(batch) >= BATCH_SIZE:
            dst_coll.insert_many(batch)
            count += len(batch)
            print(f"   Inserted {count} docs...")
            batch.clear()

    if batch:
        dst_coll.insert_many(batch)
        count += len(batch)

    print(f"✅ Finished {coll_name}: {count} docs copied.")


def main():
    try:
        src_client = MongoClient(DEV_URI)
        dst_client = MongoClient(LOCAL_URI)

        src_db = src_client[DB_NAME]
        dst_db = dst_client[DB_NAME]

        collections = src_db.list_collection_names()
        for coll in collections:
            if coll.startswith("system."):
                continue
            copy_collection(src_db, dst_db, coll)

        print("\n🎉 Migration completed successfully.")

    except PyMongoError as e:
        print(f"❌ Mongo error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        src_client.close()
        dst_client.close()


if __name__ == "__main__":
    main()
