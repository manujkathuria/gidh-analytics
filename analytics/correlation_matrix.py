# analytics/correlation_matrix.py

import seaborn as sns
import matplotlib.pyplot as plt
import asyncio
import pandas as pd
from db_connector import fetch_features_data


async def create_correlation_matrices(stock_name: str, interval: str, start_date: str, end_date: str):
    """
    Fetches data and generates multiple, categorized correlation matrix heatmaps.
    The heatmaps are saved as separate PNG files.
    """
    # Fetch all the data once using our modular connector
    df = await fetch_features_data(stock_name, interval, start_date, end_date)

    if df.empty:
        print("No data found. Exiting.")
        return

    # --- 1. Price Correlation Matrix ---
    print("Generating Price Correlation Matrix...")
    price_features = df[['close', 'volume', 'cvd_30m', 'obv', 'mfi', 'rsi', 'lvc_delta']]
    price_features = price_features.rename(columns={'close': 'price'})

    plt.figure(figsize=(10, 8))
    sns.heatmap(
        price_features.corr(),
        cmap="vlag", annot=True, fmt=".2f", linewidths=.5
    )
    plt.title(f'Price vs. Core Features Correlation\n{stock_name} ({interval})', fontsize=16)
    output_filename_price = f'corr_price_vs_features_{stock_name}_{interval}.png'
    plt.savefig(output_filename_price)
    plt.show()
    print(f"Success! Heatmap saved as '{output_filename_price}'")

    # --- 2. Tier 1 Divergence Correlation Matrix ---
    print("\nGenerating Tier 1 Divergence Correlation Matrix...")
    tier1_features = df[['div_price_lvc', 'div_price_cvd', 'div_price_obv', 'div_price_rsi', 'div_price_mfi']]
    tier1_features = tier1_features.rename(columns={
        'div_price_lvc': 'p_vs_lvc', 'div_price_cvd': 'p_vs_cvd',
        'div_price_obv': 'p_vs_obv', 'div_price_rsi': 'p_vs_rsi', 'div_price_mfi': 'p_vs_mfi'
    })

    plt.figure(figsize=(10, 8))
    sns.heatmap(
        tier1_features.corr(),
        cmap="vlag", annot=True, fmt=".2f", linewidths=.5
    )
    plt.title(f'Tier 1 Divergence Correlation (Price vs. Features)\n{stock_name} ({interval})', fontsize=16)
    output_filename_tier1 = f'corr_tier1_divergence_{stock_name}_{interval}.png'
    plt.savefig(output_filename_tier1)
    plt.show()
    print(f"Success! Heatmap saved as '{output_filename_tier1}'")

    # --- 3. Tier 2 Divergence Correlation Matrix ---
    print("\nGenerating Tier 2 Divergence Correlation Matrix...")
    tier2_features = df[['div_lvc_cvd', 'div_lvc_obv', 'div_lvc_rsi', 'div_lvc_mfi']]
    tier2_features = tier2_features.rename(columns={
        'div_lvc_cvd': 'lvc_vs_cvd', 'div_lvc_obv': 'lvc_vs_obv',
        'div_lvc_rsi': 'lvc_vs_rsi', 'div_lvc_mfi': 'lvc_vs_mfi'
    })

    plt.figure(figsize=(10, 8))
    sns.heatmap(
        tier2_features.corr(),
        cmap="vlag", annot=True, fmt=".2f", linewidths=.5
    )
    plt.title(f'Tier 2 Divergence Correlation (LVC vs. Features)\n{stock_name} ({interval})', fontsize=16)
    output_filename_tier2 = f'corr_tier2_divergence_{stock_name}_{interval}.png'
    plt.savefig(output_filename_tier2)
    plt.show()
    print(f"Success! Heatmap saved as '{output_filename_tier2}'")


async def main():
    """Main function to run the correlation analysis."""
    # --- Configuration ---
    STOCK = 'BOSCHLTD'
    INTERVAL = '1m'
    START_DATE = '2025-08-01'
    END_DATE = '2025-08-01'

    await create_correlation_matrices(STOCK, INTERVAL, START_DATE, END_DATE)


if __name__ == '__main__':
    # Run the main asynchronous function
    asyncio.run(main())
