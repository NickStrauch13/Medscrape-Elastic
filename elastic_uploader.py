from abc import ABC, abstractmethod
from http import client
from pydoc import cli
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import newlinejson as nlj
import json
import xmltodict


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
    # Elastic as a single document in the index.
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


    # Creates an Elasticsearch index based off input data structure.
    # It is recommended to create the indicies directly in Elasticsearch for
    # greater fine-tuning; however, this method serves as a convenient alternative.
    #
    # actions: generator producted by generate_actions
    # index_name: string representing desired name of new index
    def create_index(self, actions, index_name):
        pass

    

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


# Uploader for xml files. NOT READY FOR NON-PUBMED XML FILES YET. STILL TESTING
class xmlUploader(elasticUploader):
    def __init__(self, index, hostname):
        super().__init__(index, hostname)

    def generate_actions(self, file):
        with open(file, 'r') as f:
            xmlData = f.read()
        rawDict = xmltodict.parse(xmlData)
        publications = rawDict[list(rawDict.keys())[0]] #specific to the pubmed xml files right now...
        for d in publications:
            yield d


        
