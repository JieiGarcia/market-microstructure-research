# Intra-Candle Tick Analysis & Time-Domain Sampling Research

## Overview
This project explores the limitations of standard time-based bars (e.g., 1-minute, 5-minute candles) in financial markets. By parsing high-frequency tick data, I aim to resolve chronological inconsistencies (High vs. Low timing) and demonstrate the superior dimensionality of event-based sampling (Volume Clocks).

## Objectives
- Construct a deterministic "True Zigzag" indicator using tick-level granularity.
- Analyze the impact of variable tick density on standard OHLC charts.
- Compare time-domain sampling vs. event-domain sampling.

## Tech Stack
- **Language:** Python 3.10+
- **Libraries:** Pandas, NumPy, Matplotlib/Plotly (for visualization)

## Key Findings
My analysis suggests that time-based sampling fails to capture market activity consistent due to volatility clustering. Event-based sampling (e.g., volume bars) provides a more robust structure for algorithmic trading strategies.