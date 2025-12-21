from pathlib import Path
import sys

# Make src/ importable
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.config import make_paths
from bootcamp_data.io import (
    read_orders_csv,
    read_users_csv,
    write_parquet,
)

def main() -> None:
    paths = make_paths(ROOT)

    # Read raw data
    orders = read_orders_csv(paths.raw / "orders.csv")
    users = read_users_csv(paths.raw / "users.csv")

    # Write processed data
    write_parquet(orders, paths.processed / "orders.parquet")
    write_parquet(users, paths.processed / "users.parquet")

    print("1- Wrote data/processed/orders.parquet")
    print("2- Wrote data/processed/users.parquet")

if __name__ == "__main__":
    main()





#import pandas as pd

#df = pd.read_csv(
#    r"data/raw/orders.csv",
#   dtype={"user_id": str}
#)

#print(df)
#----- old
#df = pd.read_csv(r"data/raw/orders.csv")
#dtypes = { "user_id" : str }
#----- print dtypes
#print(df.dtypes)
