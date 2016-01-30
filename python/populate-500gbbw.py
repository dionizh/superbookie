
import sys
import re
import csv
import json
import copy

from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk

INDEX_NAME = 'superbookie'

UNUSED_FIELDS = ['dummy', 'AUTHOR FIRST', 'AUTHOR LAST', "AUTHOR'S OTHER NAME"]

def index_authors(data, list_name):
   for line in data.splitlines():
        info = re.split("[,-.\[\]]+", line)
        info = [i.strip() for i in info]
        
        print line
        if len(line) < 4:
            continue # not enough info
        print info

        yield {
                '_id': info[3],                
                'name': info[2],
                'gender': info[4]
            }

def index_books(data, list_name):
   for line in data.splitlines():
        info = re.split("[,-.\[\]]+", line)
        info = [i.strip() for i in info]
        
        print line
        if len(line) < 4:
            continue # not enough info
        print info

        yield {
                '_id': info[0],
                'title': info[1],
                'publication': datetime(int(info[5]), 1, 1),
                'author': info[3]
            }

def parse_list(data, list_name):
    # return index_authors(data, list_name)
    #return index_books(data, list_name)
    pass

def get_headers(row):
    # get clean headers
    headers = []
    i = 0
    for field in row:
        # m = re.match(r' [":]+([\w .\']+)["]*', field)
        # if i == 0:
        #     header = 'id'
        # else:
        #     header = 'dummy'
        #     if m:
        #         #print '__match', m.groups()[0]
        #         header = m.groups()[0]
        # headers.append(header)
        headers.append(field)
        i += 1
    return headers

def get_bookdict(headers, row):
    bookdict = {}
    i = 0
    for header in headers: 
        val = row[i]
        #print val
        # if i == 0:
        #     m = re.match(r'[ ":{]+([\d]+)', val)
        # else:
        #     m = re.match(r' [":]+([\w .:\']+)["]*', val)
        # if m:
        #     val = m.groups()[0]

        #print header, '*', val
        bookdict[header] = val
        i += 1

    # for field in UNUSED_FIELDS:
    #     bookdict.pop(field)
    return bookdict

def load_data(es, listname, datafile):

    f = open(datafile, "r")
    #data = json.load(f)
    #data = json.loads(f.read())
    #f.close()

    csvreader = csv.reader(f)
    
    # headers or single book dictionary keys
    headers = []

    # master list
    booklist = []

    i = 0
    for row in csvreader:
        #print row
        if i == 6:
            headers = get_headers(row)
                
        if i >= 8:
            bookdict = get_bookdict(headers, row)
            if not bookdict['#']:
                continue
            bookdict['raw'] = row
            booklist.append(bookdict)

        i += 1
        # if i > 10:
        #     break

    # a list of dictionary of content
    actions = []

    print '******** BOOK LIST *******'
    
    for book in booklist:
        print 100*'*'
        #print book
        # for h in headers:
        #     if h not in UNUSED_FIELDS:
        #         print h, '=', book.get(h)
        #print book['raw']

        author = '%s %s' % (book['Author First Name'], book['Author Last Name'])
        author = author.decode('utf-8')

        action = {'_type': 'book', 
                  '_id': book['#'], 
                  '_source': {
                    'id': int(book['#']),
                    'title': book['Book Title'],
                    'author': author,
                    'lists': [{
                        'id': '500gbbw', 
                        'name': '500 Great Books by Women'
                        }],
                    'publication': book['Date'],
                    'countries': [book['Country']]
                    }
                }
        print action
        actions.append(action)
    
    success, _ = bulk(es, actions, index=INDEX_NAME, raise_on_error=True)
    print('Performed %d actions' % success)

    # and now we can count the documents
    print(es.count(index=INDEX_NAME)['count'], 'documents in index')

    return

    # we let the streaming bulk continuously process the commits as they come
    # in - since the `parse_commits` function is a generator this will avoid
    # loading all the commits into memory
    # for ok, result in streaming_bulk(
    #         es,
    #         parse_list(data, listname),
    #         index=INDEX_NAME,
    #         doc_type='book',
    #         chunk_size=50 # keep the batch sizes small for appearances only
    #     ):

    #     action, result = result.popitem()
    #     doc_id = '/%s/%s' % (INDEX_NAME, result['_id'])
    #     # process the information from ES whether the document has been
    #     # successfully indexed
    #     if not ok:
    #         print('Failed to %s document %s: %r' % (action, doc_id, result))
    #     else:
    #         print('Successfully indexed %s' % doc_id)


if __name__ == '__main__':
    listname = sys.argv[1]
    datafile = sys.argv[2]

    print '**************************************'
    print 'LIST:', listname
    print 'DATA:', datafile
    print '**************************************'

    # instantiate es client, connects to localhost:9200 by default
    es = Elasticsearch()

    load_data(es, listname, datafile)

