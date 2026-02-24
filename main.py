import json
import pandas as pd
import sqlite3

# Search params
unit_prefix = 'TFT16'
patch_number = '16.5'

# Match patch number to internal patch code
with open("patch_mapping.json") as f:
    patch_mapping = json.load(f)
internal_patch = patch_mapping[patch_number]

with open("tft-item.json") as f:
    item_data = json.load(f)
item_names = {v["id"]: v["name"] for v in item_data["data"].values() if v.get("name")}

print(internal_patch)
# Search for data of items placed on TFT16 units
con = sqlite3.connect("tft_data.db")
df = pd.read_sql_query(
    """
    SELECT
        item.value      AS item_id,
        pu.character_id AS unit_id,
        p.placement
    FROM participant_units AS pu
    JOIN participants AS p ON p.id = pu.participant_id
    JOIN matches      AS m ON m.match_id = p.match_id,
         json_each(pu.items) AS item
    WHERE unit_id LIKE :unit_prefix
      AND m.game_version LIKE :patch
    """,
    con,
    params={
        "unit_prefix": f"{unit_prefix}%",
        "patch": f"%<Releases/{internal_patch}>",
    },
)
con.close()

df["item_name"] = df["item_id"].map(item_names)
df = df.dropna(subset=["item_name"])

# Aggregate per item
item_stats = (
    df.groupby("item_name")
    .agg(
        games=("placement", "count"),
        avg_placement=("placement", "mean"),
        win_rate=("placement", lambda x: (x <= 4).mean()),
    )
    .reset_index()
    .sort_values("avg_placement")
)

print(item_stats)
