import json
import pandas as pd

def main():
    # Read the data from data.csv
    df = pd.read_csv("data.csv")

    # Ensure 'date' column is datetime
    df["date"] = pd.to_datetime(df["date"])

    # Compute revenue
    df["revenue"] = df["units"] * df["price"]

    # row_count: Total number of rows in the dataset
    row_count = len(df)

    # regions_count: Count of distinct regions
    regions_count = df["region"].nunique()

    # top_n_products_by_revenue (n=3): Top N products by total revenue
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

    # rolling_7d_revenue_by_region: For each region, the last value of a 7-day moving average of daily revenue
    # Group by region and date to get daily revenue
    daily_revenue_by_region = (
        df.groupby(["region", "date"])["revenue"] # Fixed typo: 'revenew' -> 'revenue'
        .sum()
        .reset_index()
    )

    rolling_7d_revenue_by_region_output = {}
    for region, group in daily_revenue_by_region.groupby("region"):
        # Set date as index and ensure it's sorted for proper rolling calculation
        group = group.set_index("date").sort_index()

        # Reindex to ensure all dates within the min/max range for the region are present.
        # Fill missing dates' revenue with 0, which is crucial for a true calendar 7-day rolling average.
        idx = pd.date_range(group.index.min(), group.index.max(), freq='D')
        group = group.reindex(idx, fill_value=0)

        # Calculate 7-day rolling mean using a time-based window
        group["rolling_revenue"] = group["revenue"].rolling(window='7D', min_periods=1).mean()

        if not group.empty:
            # The 'last value' refers to the rolling average for the most recent date in the reindexed group.
            rolling_7d_revenue_by_region_output[region] = float(group["rolling_revenue"].iloc[-1])
        else:
            rolling_7d_revenue_by_region_output[region] = None # Handle cases with no data if necessary

    # Prepare the final output dictionary
    output_data = {
        "row_count": row_count,
        "regions_count": regions_count,
        "top_n_products_by_revenue": top_products_list,
        "rolling_7d_revenue_by_region": rolling_7d_revenue_by_region_output,
    }

    # Output the result as JSON to stdout
    print(json.dumps(output_data, indent=2))

if __name__ == "__main__":
    main()
