import psycopg2

conn_str = "postgresql://postgres.zjtwtcnlrdkbtibuwlfd:zwr5h4UJDpN08AYj@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres?sslmode=require"

try:
    conn = psycopg2.connect(conn_str)
    print("✅ Connection successful!")
    conn.close()
except Exception as e:
    print("❌ Connection failed:", e)