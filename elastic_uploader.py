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
# hostname: Public IP of the ElasticSearch cluster with proper port. (i.e. http://elasticsearch.medscrape.com:9200)
class elasticUploader(ABC):
    def __init__(self, hostname):
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
    # index: Name of the index the data will be ingested into.
    # file: path to data file
    def upload_to_elastic(self, index, file):
        if not self.client.indices.exists(index=index):
            raise ValueError("Invalid index. Verify the provided index already exists in the ElasticSearch cluster.")
        bulk(client=self.client, index=index,
             actions=self.generate_actions(file))


    # Creates an Elasticsearch index based off input data structure.
    # It is recommended to create the indicies directly in Elasticsearch for
    # greater fine-tuning; however, this method serves as a convenient alternative.
    #
    # file: path to data file
    # index_name: string representing desired name of new index
    def create_index(self, file, index_name):
        doc = next(self.generate_actions(file))

    

# Uploader for new line delimited json files
class ndjsonUploader(elasticUploader):
    def __init__(self, hostname):
        super().__init__(hostname)

    def generate_actions(self, file):
        with nlj.open(file) as src:
            for line in src:
                yield line


# Uploader for json files
class jsonUploader(elasticUploader):
    def __init__(self, hostname):
        super().__init__(hostname)

    def generate_actions(self, file):
        with open(file, 'r') as f:
            for data in json.load(f):
                yield data


# Uploader for xml files. POSSIBLY NOT READY FOR NON-PUBMED XML FILES YET. STILL TESTING
class xmlUploader(elasticUploader):
    def __init__(self, hostname):
        super().__init__(hostname)

    def generate_actions(self, file):
        with open(file, 'r') as f:
            xmlData = f.read()
        rawDict = xmltodict.parse(xmlData) 
        xmlDocumentSet = list(rawDict.values())[0]
        documents = xmlDocumentSet[list(xmlDocumentSet.keys())[0]]
        for d in documents:
            yield d


        
