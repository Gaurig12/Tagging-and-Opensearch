import pandas as pd
from opensearchpy import OpenSearch, helpers
def ensure_index_exists(opensearch_client, index_name):
   if not opensearch_client.indices.exists(index=index_name):
       opensearch_client.indices.create(index=index_name)
       print(f"Index '{index_name}' was created.")
   else:
       print(f"Index '{index_name}' already exists.")
def bulk_index_data(opensearch_client, index_name, data):
   bulk_data = [
       {
           "_index": index_name,
           "_source": {
               column: row[column] for column in data.columns
           }
       }
       for index, row in data.iterrows()
   ]
   helpers.bulk(opensearch_client, bulk_data)
def main():
   df = pd.read_csv('updated_csv_file.csv')
   opensearch = OpenSearch(
       hosts=[{'host': 'localhost', 'port': 9200}],
       http_auth=('admin', 'abc420ABC@'),
       use_ssl=True,
       verify_certs=False,
       ssl_assert_hostname=False,
       ssl_show_warn=False
   )
   index_name = 'csv-index'
   ensure_index_exists(opensearch, index_name)
   bulk_index_data(opensearch, index_name, df)
   print("Data indexed successfully.")
if __name__ == "__main__":
   main()