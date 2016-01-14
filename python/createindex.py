
from datetime import datetime

from elasticsearch import Elasticsearch


def create_index(client, index):
    # gender is F/M
    author_mapping = {
        'properties': {
            'name': {
                'type': 'multi_field',
                'fields': {
                    'raw': {'type' : 'string', 'index' : 'not_analyzed'},
                    'name': {'type' : 'string'}
                }
            },
            'gender': {'type' : 'string'}
        }
    }

    book_mapping = {       
        'properties': {
            'author': author_mapping,
            'publication': {'type': 'date'},
            'title': {
                'type': 'multi_field',
                'fields': {
                    'raw': {'type' : 'string', 'index' : 'not_analyzed'},
                    'name': {'type' : 'string'}
                }
            }
        }
    }

    # create empty index
    client.indices.create(
        index=index,
        body={
            'settings': {
                # just one shard, no replicas for testing
                'number_of_shards': 1,
                'number_of_replicas': 0
            },
            'mappings': {
                'booklist': {
                    'properties': {
                        'name': {'type' : 'string'},
                        'list' : {
                            'properties': {
                                'book': book_mapping
                            }
                        }
                    }
                }
            }
        },
        # ignore already existing index
        ignore=400
    )


if __name__ == '__main__':
    # instantiate es client, connects to localhost:9200 by default
    es = Elasticsearch()
    indexname = 'superbookie'

    create_index(es, indexname)


