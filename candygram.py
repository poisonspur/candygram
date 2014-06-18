import pymongo
import feedparser
import calendar

def main():

    feed_name = 'PoisonSpurBlog'
    mongo_feed = get_mongo_feed(feed_name)
    #DCJ1
    print mongo_feed['feed']['_id']
    incoming_feed = {}
    if mongo_feed['feed']:
        feed_url = mongo_feed['feed'].get('feed_url', '')
        if feed_url:
            incoming_feed = get_incoming_feed(feed_url)

    if incoming_feed:
        if incoming_feed['pub_ts'] > mongo_feed['feed']['pub_ts']:
            entries = incoming_feed.get('entries', {})
            if entries:
                update_feed_entries(mongo_feed, entries)
                collection = mongo_feed['db']['feeds']
                collection.update( {'name' : feed_name }, { '$set' : { 'pub_ts' : incoming_feed['pub_ts'] } }) 
    
    


def get_mongo_feed(feed_name):

    from pymongo import MongoClient

    client = MongoClient('mongodb://localhost:27017')
    db = client['platypus']
    collection = db['feeds']
    # collection.update( {'name' : feed_name }, { '$set' : { 'feed_url' : 'http://www.poisonspur.com/rss.xml' } }) 
    feed = collection.find_one(  {'name' : feed_name } )

    mongo_feed = { 'feed' : {} }
    mongo_feed['client'] = client
    mongo_feed['db'] = db
    mongo_feed['feed'] = feed
    
    return mongo_feed


def get_incoming_feed(feed_url):

    # parsed_feed = feedparser.parse('http://localhost/feeds/rss.xml')
    incoming_feed = feedparser.parse(feed_url)
    for k in incoming_feed.keys():
        print k
    #print incoming_feed['updated_parsed']
    incoming_feed['pub_ts'] = calendar.timegm(incoming_feed['updated_parsed'])
    return incoming_feed


def update_feed_entries(mongo_feed, entries):

    # TODO - deleted blog entry handling
    collection = mongo_feed['db'].feed_items
    print collection
    mongo_entries = []
    for entry in entries:
        doc = { }
        doc['feed_id'] = mongo_feed['feed']['_id']
        doc['title'] = entry['title']
        doc['url'] = entry['link']
        doc['body'] = entry['summary']
        doc['pub_ts'] = calendar.timegm(entry['published_parsed'])
        print str(doc['pub_ts'])
        id = collection.update({'url' : doc['url']}, doc, upsert=True)

    print collection.count()


if __name__ == '__main__':
    main()

