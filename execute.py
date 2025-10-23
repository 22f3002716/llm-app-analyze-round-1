import json
import pandas as pd

def main():
    # Read the data - assuming data.csv is present
    try:
        df = pd.read_csv("data.csv")
    except FileNotFoundError:
        print("Error: data.csv not found. Please ensure data.csv is in the same directory.")
        return

    # Ensure date column is datetime
    df["date"] = pd.to_datetime(df["date"])

    # Compute revenue
    df["revenue"] = df["units"] * df["price"]

    # row_count
    row_count = len(df)

    # regions: count of distinct regions
    regions_count = df["region"].nunique()

    # top_n_products_by_revenue (n=3)
    n = 3
    top_products = (
        df.groupby("product")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .head(n)
        .reset_index()
    )
    top_products_list = [
        {"product": row["product"], "revenue": float(row["revenue"])}
        for _, row in top_products.iterrows()
    ]

    # rolling_7d_revenue_by_region: for each region, last value of 7-day moving average of daily revenue
    daily_rev = (
        df.groupby(["region", "date"])["revenue"]  # FIXED: "revenew" -> "revenue"
        .sum()
        .reset_index() # FIXED: "reset_ind" -> "reset_index()"
    )

    rolling_7d_revenue_by_region = {}
    for region in daily_rev["region"].unique():
        region_df = daily_rev[daily_rev["region"] == region].sort_values("date")
        # Ensure sufficient data for rolling calculation, otherwise rolling result will be NaN for small windows
        if len(region_df) > 0:
            region_df["rolling_revenue"] = region_df["revenue"].rolling(window=7, min_periods=1).mean()
            # Get the last value, convert to float for JSON serialization
            rolling_7d_revenue_by_region[region] = float(region_df["rolling_revenue"].iloc[-1])
        else:
            rolling_7d_revenue_by_region[region] = None # Or 0, or handle as per business logic

    # Prepare output dictionary
    output = {
        "row_count": row_count,
        "regions_count": regions_count,
        "top_n_products_by_revenue": top_products_list,
        "rolling_7d_revenue_by_region": rolling_7d_revenue_by_region
    }

    # Save to JSON
    with open("result.json", "w") as f:
        json.dump(output, f, indent=4)

    print("Analysis complete. result.json generated.")

if __name__ == "__main__":
    main()
