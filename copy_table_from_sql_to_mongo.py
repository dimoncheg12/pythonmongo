import psycopg2, traceback, sys, settings_local
from pymongo import MongoClient

rows = {}
try:
    print "Connecting DB..."
    conn = psycopg2.connect(
        "dbname='" + settings_local.DB_NAME + "' user='" + settings_local.DB_USER + "' host='" + settings_local.DB_HOST + "' password='" + settings_local.DB_PASS + "'")
    print "Connected."
except Exception, e:
    print "I am unable to connect to the database"
    print str(e)
    sys.exit()
try:
    print "Connecting MongoDB..."
    client = MongoClient()
    client = MongoClient("mongodb://"+settings_local.MONGO_HOST+":"+settings_local.MONGO_PORT)
    print "Connected."
except Exception, e:
    print "I am unable to connect to the mongodb"
    print str(e)
    sys.exit()
try:
    print "Getting merchants from database..."
    cur = conn.cursor()
    cur.execute("""SELECT id,name_en,url from merchants where istest = true limit 10""")
    rows = cur.fetchall()
    print "\nTable merchants:\n"
    for row in rows:
        print "   ", row[0]
except Exception, e:
    print "I am unable get merchants:"
    print str(e)
    sys.exit()
try:
    print "Copping merchants from database to mongodb..."
    db = client.test
    collection = db.merchants
    for row in rows:
        merchant = {
            "merchant_name": row[1],
            "id": row[0],
            "url": row[2]
        }

        db.merchant.insert(merchant)
        print "Successfully inserted document: %s" % merchant
except Exception, e:
    print "I am unable copy merchants"
    print str(e)
    sys.exit()
