from elastic_uploader import ndjsonUploader
from elastic_uploader import jsonUploader



HOSTNAME = "http://elasticsearch.medscrape.com:9200"
INDEX = "k_kol"
FILE = "C:/Users/nickj/OneDrive/Documents/Medscrape/Kascii.jsonl"


uploader = ndjsonUploader(INDEX, HOSTNAME)
uploader.upload_to_elastic(FILE)


