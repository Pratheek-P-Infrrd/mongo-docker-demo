from pymongo.errors import PyMongoError
from application_config import app_config
from mongo_config import get_mongo_clients

def copy_collection(src_db, dst_db, coll_name):
    src_coll = src_db[coll_name]
    dst_coll = dst_db[coll_name]

    print(f"🔄 Copying collection: {coll_name}")
    dst_coll.drop()

    # Copy indexes
    for index in src_coll.list_indexes():
        index_dict = dict(index)
        index_dict.pop("v", None)
        index_keys = index_dict.pop("key")
        if index_keys != {"_id": 1}:
            dst_coll.create_index(list(index_keys.items()), **index_dict)

    # Copy docs in batches
    cursor = src_coll.find({})
    batch, count = [], 0
    for doc in cursor:
        batch.append(doc)
        if len(batch) >= app_config.BATCH_SIZE:
            dst_coll.insert_many(batch)
            count += len(batch)
            print(f"   Inserted {count} docs...")
            batch.clear()
    if batch:
        dst_coll.insert_many(batch)
        count += len(batch)

    print(f"✅ Finished {coll_name}: {count} docs copied.")

def main():
    src_client, dst_client = None, None
    try:
        src_client, dst_client = get_mongo_clients()
        src_db = src_client[app_config.DB_NAME]
        dst_db = dst_client[app_config.DB_NAME]

        for coll in src_db.list_collection_names():
            if coll.startswith("system."):
                continue
            copy_collection(src_db, dst_db, coll)

        print("\n🎉 Migration completed successfully.")

    except PyMongoError as e:
        print(f"❌ Mongo error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        if src_client:
            src_client.close()
        if dst_client:
            dst_client.close()

if __name__ == "__main__":
    main()
