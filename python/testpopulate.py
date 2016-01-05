
import re

from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk


data = '''
89, Frankenstein - Mary Wollstonecraft Shelley. 1818. Read in 2013. [1,2,3,4]
109, The Fall of the House of Usher - Edgar Allan Poe. 1839. Read in 2013. [1,2,3,4]
113, A Christmas Carol - Charles Dickens. 1843. Read in 2009. [1]
'''


def create_index(client, index):
    # create empty index
    client.indices.create(
        index=index,
        body={
            'settings': {
                # just one shard, no replicas for testing
                'number_of_shards': 1,
                'number_of_replicas': 0
            }
        },
        # ignore already existing index
        ignore=400
    )

    # we will use user on several places
    author_mapping = {
        'properties': {
            'name': {
                'type': 'multi_field',
                'fields': {
                    'raw': {'type' : 'string', 'index' : 'not_analyzed'},
                    'name': {'type' : 'string'}
                }
            }
        }
    }

    client.indices.put_mapping(
        index=index,
        doc_type='book',
        body={
            'book': {
                'properties': {
                    'author': author_mapping,
                    'publication': {'type': 'date'},
                    'title': {'type': 'string'}
                }
            }
        }
    )


def parse_list(data, list_name):
   """
   Go through the book log and generate a document per book
   containing all the metadata.
   """
   for line in data.splitlines():
        info = re.split("[,-.\[\]]+", line)
        info = [i.strip() for i in info]
        
        print line
        if len(line) < 4:
            continue # not enough info
        print info

        yield {
            #'_parent': list_name,
            'author': {
                'name': info[2]
            },            
            'title': info[1],
            'publication': datetime(int(info[3]), 1, 1)
       }


def load_data(client, data, index='booklist'):

	create_index(client, index)

	list_name = '1001books'

	# we let the streaming bulk continuously process the commits as they come
    # in - since the `parse_commits` function is a generator this will avoid
    # loading all the commits into memory
	for ok, result in streaming_bulk(
            client,
            parse_list(data, list_name),
            index=index,
            doc_type='book',
            chunk_size=50 # keep the batch sizes small for appearances only
		):

		action, result = result.popitem()
        doc_id = '/%s/%s' % (index, result['_id'])
        # process the information from ES whether the document has been
        # successfully indexed
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
        else:
            print('Successfully indexed %s' % doc_id)

if __name__ == '__main__':
    # instantiate es client, connects to localhost:9200 by default
    es = Elasticsearch()

    load_data(es, data)

