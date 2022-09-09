from elastic_uploader import ndjsonUploader
from elastic_uploader import jsonUploader
from elastic_uploader import xmlUploader


# Currently being used for testing/manual uploads

HOSTNAME = "http://elasticsearch.medscrape.com:9200"
INDEX = "test_full_abstracts"
FILE = "C:/Users/nickj/OneDrive/Documents/Medscrape/ElasticUploader/pubmed22n0010ascii.xml"


uploader = xmlUploader(INDEX, HOSTNAME)
for d in uploader.generate_actions(FILE):
    print(d)
#uploader.upload_to_elastic(FILE)