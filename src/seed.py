import json
import pyinputplus as pyinput
from utilities import listFiles, getFileName
from dotenv import dotenv_values

config = {
    **dotenv_values("db.env")
}

paths = {
    "config/": "config"
}

# Certificates for staging + production databases
server_certificates = {
    "Production": "./production_certificate.json",
    "Staging": "./staging_certificate.json"
}

def populate(db, client):
    for path, collection_prefix in paths.items():
        seed_arr = []
        files = listFiles(path)
        for file in files:
            with open(file, "r") as inputFile:
                inputFile_json = json.load(inputFile)
                if client == "MongoDB":
                    file_name = inputFile_json["name"]
                    inputFile_json.pop('name', None)
                    seed_arr.append({"_id": file_name, **inputFile_json})
                else:
                    seed_arr.append(inputFile_json)
        try:
            match client:
                case "MongoDB":
                    db[collection_prefix].insert_many(seed_arr)
                    db[collection_prefix].create_index("name")
                case "Firebase":
                    batch = db.batch()
                    for seed in seed_arr:
                        doc_ref = db.collection(collection_prefix).document(seed["name"])
                        seed.pop('name', None)
                        batch.set(doc_ref, seed)
                    batch.commit()
        except Exception as e:
            print(f'Failed to seed {client} with error {e}')
            return
    print(f'Seeded {client}')
    
            
def clear_db(db, client):
    for path, collection_prefix in paths.items():
        try:
            match client:
                case "MongoDB":
                    db[collection_prefix].delete_many({})
                case "Firebase":
                    batch = db.batch()
                    collection = db.collection(collection_prefix)
                    docs = collection.stream()
                    for doc in docs:
                        print(f"Deleting {doc.id}")
                        doc_ref = collection.document(doc.id)
                        batch.delete(doc_ref)
                    batch.commit()
        except Exception as e:
             print(f'Failed to clear {client} with error {e}')
             return
        
    print(f'Cleared {client}')

def test_db(db, database_selection, database_environment):
    db = None
    try:
        match database_selection:
            case "MongoDB":
                import pymongo
                client = pymongo.MongoClient("mongodb://%s:%s@%s" % (config["MONGODB_USER"], config["MONGODB_PASSWORD"], config["MONGODB_URL"]))
                client["rentalreviews"]
            case "Firebase":
                import firebase_admin
                from firebase_admin import credentials, firestore
                cred = credentials.Certificate(server_certificates[database_environment])
                firebase_admin.initialize_app(cred)
                db = firestore.client()
        
        return {"status": True, "exception": None}
    except Exception as e:
        return {"status": False, "exception": e}


def main(database_selection, database_action, database_environment):
    db = None
    try:
        match database_selection:
            case "MongoDB":
                import pymongo
                client = pymongo.MongoClient("mongodb://%s:%s@%s" % (config["MONGODB_USER"], config["MONGODB_PASSWORD"], config["MONGODB_URL"]))
                db = client["rentalreviews"]
            case "Firebase":
                import firebase_admin
                from firebase_admin import credentials, firestore
                cred = credentials.Certificate(server_certificates[database_environment])
                firebase_admin.initialize_app(cred)
                db = firestore.client()
        print(f'Initialized connection to {database_selection}')
    except Exception as e:
        print(f'Failed to initialize source {database_selection} with exception {e}')
        return

    match database_action:
        case "Test":
            ret = test_db(db, None, database_action)
            if ret['status']:
                print(f'Connection successfully initialized to {database_selection}')
            else:
                 print(f'Failed to initialize source {database_selection} with exception {ret["exception"]}')
        case "Seed":
            populate(db, database_selection)
        case "Clear":
            clear_db(db, database_selection)
            clear_db(db, database_selection)
        case "Re-seed":
            clear_db(db, database_selection)
            populate(db, database_selection)
            clear_db(db, database_selection)
            populate(db, database_selection)

if __name__ == "__main__":
    database_selection = pyinput.inputMenu(["MongoDB", "Firebase"], lettered=True, numbered=False)
    database_action = pyinput.inputMenu(["Test", "Seed", "Clear", "Re-seed"], lettered=True, numbered=False)
    database_environment = ""
    if database_selection != "MongoDB":
        database_environment = pyinput.inputMenu(["Production", "Staging"], lettered=True, numbered=False)

    main(database_selection, database_action, database_environment)
