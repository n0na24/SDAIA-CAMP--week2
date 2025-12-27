# Week 2 – Analysis Summary

## Key Findings
- **Revenue concentration**: A small number of countries generate the majority of total revenue.
- **Average Order Value (AOV)**: AOV differs meaningfully across countries.
- **Refunds**: Refund rates are low overall and vary by country.

## Definitions
- **Revenue**: Sum of `amount` across all orders.
- **Average Order Value (AOV)**: Mean of `amount` for paid orders.
- **Refund rate**: `number of refunded orders / total orders`, where  
  `refund = status_clean == "refund"`.

## Data Quality Caveats
- Some missing values remain in `amount` after cleaning.
- Not all orders successfully matched to users during joins.
- Extreme values were winsorized using **1.5× IQR** to reduce outlier impact.

## Next Questions
- How do revenue and refunds change over time?
- Which user segments contribute most to revenue?
