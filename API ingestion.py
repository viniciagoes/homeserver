from pyairtable import Api
from pyairtable.formulas import match
import os
from dotenv import load_dotenv
import polars as pl
from datetime import datetime
import pathlib

load_dotenv()

api = Api(os.environ['AIRTABLE_API_KEY'])
base_id = os.environ['AIRTABLE_BASE_ID']
table_id = os.environ['AIRTBALE_TABLE_ID']

table = api.table(base_id, table_id)

records = table.all(formula=match({'processed': False}), fields=["id", "link"], max_records=10)

csv_data = []

for record in records:
    csv_data.append(record["fields"])

df = pl.DataFrame(csv_data)

cwd = pathlib.Path(os.getcwd())
csv_path = cwd / "processing" / f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.csv"
df.write_csv(csv_path)