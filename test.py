import sqlite3
import pandas as pd

# Create a test database
conn = sqlite3.connect("test.db")
data = pd.DataFrame({
    "id": [1, 2, 3, 3, 4],
    "revenue": [100, 200, 150, 150, 10000],
    "date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-03", "2025-01-04"]
})
data.to_sql("data", conn, if_exists="replace", index=False)
conn.close()