import os
import pandas as pd
import matplotlib.pyplot as plt

# BASE PATH
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT_DIR = os.path.join(BASE, "outputs", "csv")

# LOAD ETL OUTPUTS
monthly_tb = pd.read_csv(os.path.join(OUTPUT_DIR, "monthly_tb.csv"))
pl_summary = pd.read_csv(os.path.join(OUTPUT_DIR, "pl_summary.csv"))
monthly_by_class = pd.read_csv(os.path.join(OUTPUT_DIR, "monthly_by_class.csv"))

print("Comparison datasets loaded successfully!")


# ==========================================
# 1. Top 10 Accounts by Total Amount (Bar Chart)
# ==========================================
top_accounts = (
    monthly_tb.groupby("Account_Name")["Total_Amount"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

plt.figure(figsize=(10, 5))
plt.bar(top_accounts.index, top_accounts.values)
plt.title("Top 10 Accounts by Total Amount")
plt.xticks(rotation=45, ha="right")
plt.ylabel("Total Amount")
plt.tight_layout()
plt.show()


# ==================================================
# 2. P&L Comparison – Revenue vs Expense
# ==================================================
plt.figure(figsize=(8, 5))
plt.bar(pl_summary["Account_Class"], pl_summary["Total"])
plt.title("Revenue vs Expense (P&L Overview)")
plt.xlabel("Account Class")
plt.ylabel("Total Amount")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# ==================================================
# 3. Contribution % by Account Class
# ==================================================
pl_summary["Percent"] = (
    pl_summary["Total"] / pl_summary["Total"].sum() * 100
)

plt.figure(figsize=(8, 5))
plt.pie(pl_summary["Percent"], labels=pl_summary["Account_Class"], autopct="%1.1f%%")
plt.title("Contribution by Account Class (%)")
plt.tight_layout()
plt.show()


# ==================================================
# 4. Top 5 Revenue vs Top 5 Expense Accounts
# ==================================================
agg_accounts = monthly_tb.groupby(["Account_Name", "Account_Class"])["Total_Amount"].sum().reset_index()

# Separate revenue & expense
revenue = agg_accounts[agg_accounts["Account_Class"] == "Revenue"].nlargest(5, "Total_Amount")
expense = agg_accounts[agg_accounts["Account_Class"] == "Expense"].nlargest(5, "Total_Amount")

plt.figure(figsize=(12, 5))

# Subplot 1 — Revenue
plt.subplot(1, 2, 1)
plt.bar(revenue["Account_Name"], revenue["Total_Amount"], color="green")
plt.title("Top 5 Revenue Accounts")
plt.xticks(rotation=45, ha="right")

# Subplot 2 — Expense
plt.subplot(1, 2, 2)
plt.bar(expense["Account_Name"], expense["Total_Amount"], color="red")
plt.title("Top 5 Expense Accounts")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()

# ==================================================
# 5. Month-over-Month Trend Comparison
# ==================================================
monthly_by_class["Period"] = (
    monthly_by_class["Year"].astype(str) + "-" +
    monthly_by_class["Month"].astype(str)
)

pivot = monthly_by_class.pivot_table(
    index="Period", columns="Account_Class", values="Total"
)

plt.figure(figsize=(12, 6))
for col in pivot.columns:
    plt.plot(pivot.index, pivot[col], marker="o", label=col)

plt.title("Month-over-Month Trend by Account Class")
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.show()
