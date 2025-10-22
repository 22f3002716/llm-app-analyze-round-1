import json
import pandas as pd

def main():
    # Read the data from data.csv
    df = pd.read_csv("data.csv")

    # Compute revenue
    df["revenue"] = df["units"] * df["price"]

    # row_count: total number of rows in the dataset
    row_count = len(df)

    # regions_count: count of distinct regions
    regions_count = df["region"].nunique()

    # top_n_products_by_revenue (n=3): top 3 products by total revenue
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
    df["date"] = pd.to_datetime(df["date"])  # ensure date column is datetime objects
    
    # Calculate daily revenue per region
    daily_rev = (
        df.groupby(["region", "date"])["revenue"] # FIX: 'revenew' -> 'revenue' typo fixed
        .sum()
        .reset_index() # FIX: 'reset_ind' -> 'reset_index()' typo fixed
    )
    
    # Sort by date within each region for correct rolling calculation
    daily_rev = daily_rev.sort_values(by=["region", "date"])

    # Set date as index and group by region to apply rolling window
    # 'closed="right"' includes the current observation in the window
    rolling_rev_df = daily_rev.set_index('date').groupby('region')['revenue'] \
                                .rolling(window='7D', closed='right') \
                                .mean() \
                                .reset_index() # Resulting DF has columns: region, date, revenue (rolling average)

    # Get the last computed rolling average for each region
    rolling_7d_revenue_by_region = {}
    for region_name in rolling_rev_df['region'].unique():
        region_data = rolling_rev_df[rolling_rev_df['region'] == region_name].sort_values('date', ascending=False)
        if not region_data.empty:
            # The latest rolling average for this region
            last_avg = region_data['revenue'].iloc[0]
            rolling_7d_revenue_by_region[region_name] = float(last_avg) if pd.notna(last_avg) else None
        else:
            rolling_7d_revenue_by_region[region_name] = None # No data for this region to compute rolling average

    # Prepare final output dictionary
    output = {
        "row_count": row_count,
        "regions_count": regions_count,
        "top_n_products_by_revenue": top_products_list,
        "rolling_7d_revenue_by_region": rolling_7d_revenue_by_region,
    }

    # Print the output as a JSON string
    print(json.dumps(output, indent=4))

if __name__ == "__main__":
    main()
