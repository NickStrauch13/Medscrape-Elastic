from elastic_uploader import ndjsonUploader
from elastic_uploader import jsonUploader
from elastic_uploader import xmlUploader
from elastic_uploader import sqlUploader
import mysql.connector



db = mysql.connector.connect(host = "medscrape-rds.cnlrgyaocyr2.us-east-1.rds.amazonaws.com",
                                        database = "publication_data",
                                        username = "nick", password = "nick123!")
# Import testing data from publication_data
cursor = db.cursor()
cursor.execute("SELECT article_title, article_abstract, keywords FROM publications")
print("SQL query finished")

# Currently being used for testing/manual uploads
HOSTNAME = "http://elasticsearch.medscrape.com:9200"
INDEX = "test_full_abstracts"
FILE = "C:/Users/nickj/OneDrive/Documents/Medscrape/ElasticUploader/pubmed22n0010ascii.xml"



uploader = sqlUploader(HOSTNAME)
uploader.upload_to_elastic(INDEX, cursor)