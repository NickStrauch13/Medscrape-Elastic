from elastic_uploader import ndjsonUploader
from elastic_uploader import jsonUploader
from elastic_uploader import xmlUploader



HOSTNAME = "http://elasticsearch.medscrape.com:9200"
INDEX = "a_kol"
FILE = "C:/Users/nickj/OneDrive/Documents/Medscrape/Kascii.jsonl"


uploader = xmlUploader(INDEX, HOSTNAME)
uploader.generate_actions("pubmed22n0010ascii.xml")
#uploader.upload_to_elastic(FILE)


