"""Lab: Introduction to Spark's RDD API (ECE Big Data 2026)."""
import re
import time
from operator import add
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

def title(t):
    print("\n" + "=" * 70 + "\n" + t + "\n" + "=" * 70)

title("1. RDD creation and partitions")
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print("defaultParallelism:", sc.defaultParallelism, "| defaultMinPartitions:", sc.defaultMinPartitions)
print("users partitions:", users_lines.getNumPartitions())
print("orders partitions:", orders_lines.getNumPartitions())
orders_repartitioned = orders_lines.repartition(8)
print("after repartition(8):", orders_repartitioned.getNumPartitions())
print("after coalesce(1):", orders_repartitioned.coalesce(1).getNumPartitions())
print("coalesce(8) on 2 partitions (no shuffle, cannot grow):", orders_lines.coalesce(8).getNumPartitions())

title("2. Lazy evaluation and lineage")
header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())
print("count #1:", orders_data.count())
print("count #2:", orders_data.count())

title("3. Multiline address")
for line in users_lines.take(6):
    print(repr(line))
print("users_lines.count():", users_lines.count())
UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print("users_records.count():", users_records.count())
df_users = spark.read.csv("users.csv", header=True, multiLine=True)
print("DataFrame multiLine=True count:", df_users.count())

title("4. Manual parsing of orders")
def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {"order_id": order_id, "user_id": user_id, "date": date,
            "quantity": int(quantity), "product": product.strip().lower()}

orders_rdd = orders_data.map(parse_order).cache()
for o in orders_rdd.take(3):
    print(o)
bad = sc.parallelize(["id,u,2020-01-01,3,soft,drink"]).map(parse_order)
try:
    bad.collect()
except Exception as e:
    print("Extra comma ->", type(e).__name__, ":", str(e).strip().splitlines()[-1][:120])

title("5. reduceByKey vs groupByKey")
pairs = orders_rdd.map(lambda o: (o["user_id"], o["quantity"]))
user_quantity = pairs.reduceByKey(add)
print("reduceByKey:", sorted(user_quantity.collect())[:5])
user_quantity_grouped = pairs.groupByKey().mapValues(sum)
print("groupByKey :", sorted(user_quantity_grouped.collect())[:5])
print("same result:", sorted(user_quantity.collect()) == sorted(user_quantity_grouped.collect()))
print("distinct users with orders:", user_quantity.count())

title("6. Manual deduplication")
def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b

orders_dedup = orders_rdd.map(lambda o: (o["order_id"], o)).reduceByKey(most_recent).values()
print("orders_rdd:", orders_rdd.count(), "| orders_dedup:", orders_dedup.count())

title("7. Join orders with users")
def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]

users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))
enriched = orders_kv.join(users_kv)
for e in enriched.take(3):
    print(e)
print("join count:", enriched.count())
left = orders_kv.leftOuterJoin(users_kv)
print("leftOuterJoin count:", left.count(), "| no match:", left.filter(lambda kv: kv[1][1] is None).count())
print("join lineage:")
print(enriched.toDebugString().decode())
p = 4
users_p = users_kv.partitionBy(p).cache()
orders_p = orders_kv.partitionBy(p).cache()
print("co-partitioned join lineage (no extra shuffle):")
print(orders_p.join(users_p).toDebugString().decode())

title("Exercise 1. aggregateByKey -> (order_count, total_quantity)")
stats = pairs.aggregateByKey(
    (0, 0),
    lambda acc, q: (acc[0] + 1, acc[1] + q),
    lambda a, b: (a[0] + b[0], a[1] + b[1]),
)
avg_qty = stats.mapValues(lambda c: (c[0], c[1], round(c[1] / c[0], 2)))
for row in sorted(avg_qty.collect())[:5]:
    print(row)

title("Exercise 2. fake user_id + leftOuterJoin")
fake = sc.parallelize([{"order_id": "fake-order", "user_id": "00000000-0000-0000-0000-000000000000",
                        "date": "2026-10-07 00:00:00+00:00", "quantity": 1, "product": "ghost"}])
orders_with_fake = orders_rdd.union(fake)
left_fake = orders_with_fake.map(lambda o: (o["user_id"], o)).leftOuterJoin(users_kv)
orphans = left_fake.filter(lambda kv: kv[1][1] is None)
print("orphan orders:", orphans.count())
print(orphans.take(1))

title("Exercise 3. DataFrame API")
df_orders = spark.read.csv("orders.csv", header=True, inferSchema=True)
df_q = df_orders.groupBy("user_uuid").sum("quantity")
df_q.show(5, truncate=False)
df_q.explain()
print(user_quantity.toDebugString().decode())

title("Exercise 4. cache vs no cache")
def bench(rdd_parsed, label):
    uq = rdd_parsed.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
    for i in range(2):
        t0 = time.perf_counter(); rdd_parsed.count(); t1 = time.perf_counter()
        uq.count(); t2 = time.perf_counter()
        print(f"{label} run {i+1}: orders.count={t1-t0:.3f}s user_quantity.count={t2-t1:.3f}s")

nocache = orders_data.map(parse_order)
bench(nocache, "no cache")
cached = orders_data.map(parse_order).cache()
bench(cached, "cache   ")
print("is_cached:", cached.is_cached, "| storage level:", cached.getStorageLevel())

input("\nSpark UI on http://localhost:4041 (or 4040). Press Enter to stop...") if False else None
spark.stop()
