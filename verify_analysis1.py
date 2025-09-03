import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# ---------- Database Config ----------
DB_CONFIG = {
    "dbname": "ntp_database",
    "user": "postgres",
    "password": "npl@123",  # <-- your password (special characters handled)
    "host": "localhost",
    "port": "5432"
}

# ---------- Encode password for special characters ----------
password_encoded = quote_plus(DB_CONFIG['password'])

# ---------- SQLAlchemy Engine ----------
engine = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{password_encoded}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

# ---------- Queries ----------
queries = {
    "daily_country": """
        SELECT date, srccountry, COUNT(*) AS hits
        FROM ntp_logs
        GROUP BY date, srccountry
        ORDER BY date, hits DESC;
    """,
    "monthly_country": """
        SELECT to_char(to_date(date, 'YYYY-MM-DD'), 'YYYY-MM') AS month,
               srccountry, COUNT(*) AS hits
        FROM ntp_logs
        GROUP BY month, srccountry
        ORDER BY month, hits DESC;
    """,
    "yearly_country": """
        SELECT to_char(to_date(date, 'YYYY-MM-DD'), 'YYYY') AS year,
               srccountry, COUNT(*) AS hits
        FROM ntp_logs
        GROUP BY year, srccountry
        ORDER BY year, hits DESC;
    """,
    "top_users": """
        WITH total AS (SELECT COUNT(*)::numeric AS total_hits FROM ntp_logs)
        SELECT srcip, COUNT(*) AS hits,
               ROUND(100.0 * COUNT(*) / (SELECT total_hits FROM total), 2) AS percentage
        FROM ntp_logs
        GROUP BY srcip
        ORDER BY hits DESC
        LIMIT 10;
    """,
    "unique_ips_per_day": """
        SELECT date, COUNT(DISTINCT srcip) AS unique_ips
        FROM ntp_logs
        GROUP BY date
        ORDER BY date;
    """,
    "ip_hit_count": """
        SELECT srcip, COUNT(*) AS hit_count
        FROM ntp_logs
        GROUP BY srcip
        ORDER BY hit_count DESC
        LIMIT 20;
    """,
    "top_policies": """
        SELECT policyid, COUNT(*) AS count
        FROM ntp_logs
        GROUP BY policyid
        ORDER BY count DESC
        LIMIT 10;
    """,
    "top_services": """
        SELECT service, COUNT(*) AS count
        FROM ntp_logs
        GROUP BY service
        ORDER BY count DESC
        LIMIT 10;
    """,
    "actions": """
        SELECT action, COUNT(*) AS count
        FROM ntp_logs
        GROUP BY action;
    """
}

# ---------- Fetch Data ----------
def fetch_df(query):
    return pd.read_sql(query, engine)

# ---------- Run Analytics ----------
print("\n📊 Running verification & analytics...\n")

# Fetch all dataframes
df_top_users = fetch_df(queries["top_users"])
df_unique = fetch_df(queries["unique_ips_per_day"])
df_actions = fetch_df(queries["actions"])
df_top_policies = fetch_df(queries["top_policies"])
df_top_services = fetch_df(queries["top_services"])
df_ip_hit_count = fetch_df(queries["ip_hit_count"])
df_daily_country = fetch_df(queries["daily_country"])
df_monthly_country = fetch_df(queries["monthly_country"])
df_yearly_country = fetch_df(queries["yearly_country"])

# ---------- Print Summaries ----------
print("=== Top 10 Users (Source IPs) ===")
print(df_top_users)

print("\n=== Unique IPs per Day ===")
print(df_unique)

print("\n=== Action Counts ===")
print(df_actions)

print("\n=== Top Policies ===")
print(df_top_policies)

print("\n=== Top Services ===")
print(df_top_services)

print("\n=== Top IPs by Hit Count ===")
print(df_ip_hit_count)

print("\n=== Daily Country Hits ===")
print(df_daily_country.head(10))

print("\n=== Monthly Country Hits ===")
print(df_monthly_country.head(10))

print("\n=== Yearly Country Hits ===")
print(df_yearly_country.head(10))

# ---------- Charts ----------

# Pie Chart: Top 10 Source IPs by Hits
plt.figure(figsize=(7,7))
plt.pie(df_top_users['hits'], labels=df_top_users['srcip'], autopct='%1.1f%%')
plt.title("Top 10 Source IPs by Hits")
plt.show()

# Line Chart: Unique IPs per Day
plt.figure(figsize=(10,5))
plt.plot(df_unique['date'], df_unique['unique_ips'], marker='o', color='green')
plt.xticks(rotation=45)
plt.title("Unique IPs per Day")
plt.xlabel("Date")
plt.ylabel("Unique IPs")
plt.tight_layout()
plt.show()

# Bar Chart: Action counts
plt.figure(figsize=(6,4))
plt.bar(df_actions['action'], df_actions['count'], color='skyblue')
plt.title("Action Distribution")
plt.xlabel("Action")
plt.ylabel("Count")
plt.tight_layout()
plt.show()

# Bar Chart: Top Policies
plt.figure(figsize=(8,4))
plt.bar(df_top_policies['policyid'].astype(str), df_top_policies['count'], color='orange')
plt.title("Top 10 Policies by Count")
plt.xlabel("Policy ID")
plt.ylabel("Count")
plt.tight_layout()
plt.show()

# Bar Chart: Top Services
plt.figure(figsize=(8,4))
plt.bar(df_top_services['service'], df_top_services['count'], color='purple')
plt.title("Top 10 Services by Count")
plt.xlabel("Service")
plt.ylabel("Count")
plt.tight_layout()
plt.show()

# Bar Chart: IP Hit Count (Top 20)
plt.figure(figsize=(12,5))
plt.bar(df_ip_hit_count['srcip'], df_ip_hit_count['hit_count'], color='teal')
plt.title("Top 20 IPs by Hit Count")
plt.xlabel("Source IP")
plt.ylabel("Hit Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# ---------- Country-wise Visualizations ----------

# Daily Country Hits (Stacked)
plt.figure(figsize=(12,6))
daily_pivot = df_daily_country.pivot(index='date', columns='srccountry', values='hits').fillna(0)
daily_pivot.plot(kind='bar', stacked=True, figsize=(12,6))
plt.title("Daily Hits per Country")
plt.xlabel("Date")
plt.ylabel("Hits")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Monthly Country Hits (Stacked)
plt.figure(figsize=(12,6))
monthly_pivot = df_monthly_country.pivot(index='month', columns='srccountry', values='hits').fillna(0)
monthly_pivot.plot(kind='bar', stacked=True, figsize=(12,6))
plt.title("Monthly Hits per Country")
plt.xlabel("Month")
plt.ylabel("Hits")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Yearly Country Hits (Stacked)
plt.figure(figsize=(8,5))
yearly_pivot = df_yearly_country.pivot(index='year', columns='srccountry', values='hits').fillna(0)
yearly_pivot.plot(kind='bar', stacked=True, figsize=(8,5))
plt.title("Yearly Hits per Country")
plt.xlabel("Year")
plt.ylabel("Hits")
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()

print("\n✅ Verification & complete dashboard generated successfully.")
