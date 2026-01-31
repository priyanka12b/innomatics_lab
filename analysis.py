import pandas as pd
import sqlite3

# ===============================
# Load datasets
# ===============================
orders = pd.read_csv("orders.csv")
users = pd.read_json("users.json")

print(orders.head())
print(users.head())

# ===============================
# Database setup
# ===============================
with sqlite3.connect("restaurant.db") as conn:
    cursor = conn.cursor()

    with open("restaurants.sql", "r") as f:
        cursor.executescript(f.read())
        conn.commit()

    restaurant = pd.read_sql("SELECT * FROM restaurants", conn)

# Remove duplicate restaurant entries
restaurant = restaurant.drop_duplicates(subset="restaurant_id")
print(restaurant.head())

# ===============================
# Merge datasets (LEFT JOINs)
# ===============================
orders_users = orders.merge(users, on="user_id", how="left")

final_data = orders_users.merge(
    restaurant,
    on="restaurant_id",
    how="left"
)

# ===============================
# Save final dataset
# ===============================
final_data.to_csv("final_food_delivery_dataset.csv", index=False)

# ===============================
# Validation
# ===============================
print("Orders rows:", orders.shape[0])
print("Final rows:", final_data.shape[0])
print("Restaurant columns:", restaurant.columns)

# ===============================
# ANALYSIS
# ===============================

# Gold members data
gold_data = final_data[final_data["membership"] == "Gold"]

# 1. Highest revenue city (Gold)
print(gold_data.groupby("city")["total_amount"].sum().idxmax())

# 2. Highest average order value cuisine
print(final_data.groupby("cuisine")["total_amount"].mean().idxmax())

# 3. Users with total order value > 1000
user_total = final_data.groupby("user_id")["total_amount"].sum()
print((user_total > 1000).sum())

# 4. Rating range with highest revenue
final_data["rating_range"] = pd.cut(
    final_data["rating"],
    bins=[3.0, 3.5, 4.0, 4.5, 5.0],
    labels=["3.0–3.5", "3.6–4.0", "4.1–4.5", "4.6–5.0"],
    include_lowest=True
)

print(final_data.groupby("rating_range")["total_amount"].sum().idxmax())

# 5. Highest avg order value city (Gold)
print(gold_data.groupby("city")["total_amount"].mean().idxmax())

# 6. Cuisine with few restaurants but high revenue
cuisine_summary = final_data.groupby("cuisine").agg(
    restaurant_count=("restaurant_id", "nunique"),
    total_revenue=("total_amount", "sum")
)

threshold = cuisine_summary["total_revenue"].quantile(0.75)
print(cuisine_summary[cuisine_summary["total_revenue"] >= threshold]
      .sort_values("restaurant_count")
      .index[0])

# 7. % orders by Gold members
print(round((len(gold_data) / len(final_data)) * 100))

# 8. Restaurant with highest AOV but < 20 orders
restaurant_stats = final_data.groupby("restaurant_id").agg(
    order_count=("order_id", "count"),
    avg_order_value=("total_amount", "mean")
)

print(restaurant_stats[restaurant_stats["order_count"] < 20]
      .sort_values("avg_order_value", ascending=False)
      .index[0])

# 9. Highest revenue membership + cuisine combo
print(
    final_data.groupby(["membership", "cuisine"])["total_amount"]
    .sum()
    .reset_index()
    .sort_values("total_amount", ascending=False)
)

# 10. Highest revenue quarter
final_data["order_date"] = pd.to_datetime(final_data["order_date"])
final_data["quarter"] = final_data["order_date"].dt.quarter
print(final_data.groupby("quarter")["total_amount"].sum())

# 11. Total orders by Gold members
print(len(gold_data))

# 12. Total revenue from Hyderabad
print(round(final_data[final_data["city"] == "Hyderabad"]["total_amount"].sum()))

# 13. Distinct users
print(final_data["user_id"].nunique())

# 14. Average order value (Gold)
print(round(gold_data["total_amount"].mean(), 2))

# 15. Orders with rating ≥ 4.5
print((final_data["rating"] >= 4.5).sum())

# 16. Orders in top revenue city among Gold members
top_city = gold_data.groupby("city")["total_amount"].sum().idxmax()
print(len(gold_data[gold_data["city"] == top_city]))