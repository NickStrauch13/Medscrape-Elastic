from abc import ABC, abstractmethod
from http import client
from pydoc import cli
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import newlinejson as nlj
import json


# Abstract class for uploading files to the ElasticSearch cluster
#
# index: Name of the index the data will be ingested into.
# hostname: Public IP of the ElasticSearch cluster with proper port. (i.e. http://elasticsearch.medscrape.com:9200)
class elasticUploader(ABC):
    def __init__(self, index, hostname):
        self.index = index
        try:
            self.client = Elasticsearch(hostname)
        except ValueError:
            print("Invalid hostname. Check public IP and port.")

    # Must return a generator that produces dicts. Each dict will be uploaded to
    # elastic as a single document in the index.
    #
    # file: path to data file
    @abstractmethod
    def generate_actions(self, file):
        pass

    # Uploads processed data to the ElasticSearch cluster
    #
    # file: path to data file
    def upload_to_elastic(self, file):
        if not self.client.indices.exists(index=self.index):
            raise ValueError("Invalid index. Verify the provided index already exists in the ElasticSearch cluster.")
        bulk(client=self.client, index=self.index,
             actions=self.generate_actions(file))

    

# Uploader for new line delimited json files
class ndjsonUploader(elasticUploader):
    def __init__(self, index, hostname):
        super().__init__(index, hostname)

    def generate_actions(self, file):
        with nlj.open(file) as src:
            for line in src:
                yield line


# Uploader for json files
class jsonUploader(elasticUploader):
    def __init__(self, index, hostname):
        super().__init__(index, hostname)

    def generate_actions(self, file):
        with open(file, 'r') as f:
            for data in json.load(f):
                yield data
        
