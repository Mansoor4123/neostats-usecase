# Databricks notebook source
# MAGIC %md
# MAGIC # ASG Airlines - Data Pipeline
# MAGIC
# MAGIC This notebook builds an end-to-end pipeline for the ASG Airlines use case: ingest the raw data,
# MAGIC clean it, protect sensitive passenger information, and produce a data model ready for Power BI.
# MAGIC
# MAGIC The written problem statement only describes the flights table's columns, but the actual file
# MAGIC provided has 4 related sheets - flights, bookings, payments, and passengers - connected through
# MAGIC flight_id, booking_id, and passenger_id. Since all 4 were part of the given data, all 4 are used
# MAGIC here, not just flights.
# MAGIC
# MAGIC Overall structure, using a standard bronze/silver/gold layering:
# MAGIC - **Bronze**: raw data, exactly as given, no changes
# MAGIC - **Silver**: cleaned data, one issue fixed at a time, each one diagnosed first and then fixed
# MAGIC - **Gold**: a proper star schema (fact_bookings + dim_flight + dim_passenger) plus one flattened
# MAGIC   table for building the Power BI dashboard easily
# MAGIC
# MAGIC A note on approach: instead of assuming what's wrong with the data and fixing it blindly, every
# MAGIC step below first checks what the actual problem is, shows the evidence, and then applies a fix
# MAGIC with the reasoning written out. Assumptions that can't be verified from the data are stated
# MAGIC explicitly rather than guessed at silently.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Ingestion (Bronze layer)
# MAGIC
# MAGIC Reading the 4 raw CSV files from a Unity Catalog Volume. Storage-key based access to ADLS
# MAGIC directly wasn't available on serverless compute (a governance restriction, not something
# MAGIC broken), so the files were uploaded into a Volume instead, which is Databricks' own
# MAGIC recommended way to bring in files under Unity Catalog.

# COMMAND ----------

base_path = "/Volumes/asg_airlines_databricks/default/raw_data/raw_csv_for_upload"

flights = spark.read.csv(base_path + "/flights.csv", header=True, inferSchema=True)
bookings = spark.read.csv(base_path + "/bookings.csv", header=True, inferSchema=True)
payments = spark.read.csv(base_path + "/payments.csv", header=True, inferSchema=True)
passengers = spark.read.csv(base_path + "/passengers.csv", header=True, inferSchema=True)

flights.count(), bookings.count(), payments.count(), passengers.count()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Cleaning the flights table (Silver layer)
# MAGIC
# MAGIC The problem statement mentions the flights data has issues like corrupted identifiers,
# MAGIC missing values and time inconsistencies. Instead of assuming what's wrong, checking the
# MAGIC actual data first, then fixing each issue one at a time.
# MAGIC
# MAGIC ### 2a. Exact duplicate rows

# COMMAND ----------

total_rows = flights.count()
distinct_rows = flights.distinct().count()

print("total rows:", total_rows)
print("distinct rows:", distinct_rows)
print("exact duplicate rows:", total_rows - distinct_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC Found 15 rows that are exact duplicates - every single column value matches another row.
# MAGIC This is likely the same flight event getting logged twice somewhere upstream (booking
# MAGIC platform + scheduling system both reporting it, maybe). Since they carry zero new
# MAGIC information and would inflate route-wise traffic counts if left in, dropping them.

# COMMAND ----------

flights_clean = flights.dropDuplicates()
flights_clean.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Same flight_id used for two different flights
# MAGIC
# MAGIC A flight_id should be unique to one flight. Checking if any flight_id shows up more than
# MAGIC once, even after removing exact duplicates.

# COMMAND ----------

dup_ids = flights_clean.groupBy("flight_id").count().filter("count > 1")
dup_ids.show()

# COMMAND ----------

flights_clean.filter(flights_clean.flight_id == "6F250").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Both 6F250 rows are real, different flights - different route, different time. They just
# MAGIC happen to share the same flight_id, most likely a system error where the same ID got
# MAGIC assigned twice.
# MAGIC
# MAGIC Not deleting either one since both are real flights with real data. Instead, adding a new
# MAGIC column that gives every row its own unique number, so flights can always be told apart even
# MAGIC when flight_id repeats. Keeping the original flight_id column too, since it's still useful
# MAGIC for reporting - just noting this as a known data issue in the source system.

# COMMAND ----------

flights_clean = flights_clean.withColumn("flight_row_id", F.monotonically_increasing_id())
flights_clean.select("flight_row_id", "flight_id", "source", "destination").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. Impossible flight duration
# MAGIC
# MAGIC A flight can't arrive before it leaves - even for overnight flights that land the next day,
# MAGIC time is still moving forward. Checking for any flight where arrival minus departure comes
# MAGIC out negative.

# COMMAND ----------

flights_clean = flights_clean.withColumn(
    "duration_minutes",
    (F.unix_timestamp("arrival_time") - F.unix_timestamp("departure_time")) / 60
)

flights_clean.filter(flights_clean.duration_minutes < 0).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Found one flight, SJ192, where arrival time is about 19 hours before departure time. This
# MAGIC isn't a normal overnight flight (those still move forward in time, just cross midnight) -
# MAGIC this is a bad record, maybe departure and arrival got swapped or logged wrong at the source.
# MAGIC Since the times on this one can't be trusted, removing it from the clean dataset rather than
# MAGIC guessing at a fix, but keeping a note of it separately so it's not silently lost.

# COMMAND ----------

invalid_flights = flights_clean.filter(flights_clean.duration_minutes < 0)
flights_clean = flights_clean.filter(flights_clean.duration_minutes >= 0)

print("invalid rows removed:", invalid_flights.count())
print("remaining rows:", flights_clean.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2d. Two different ways of saying "airline unknown"

# COMMAND ----------

flights_clean.groupBy("airline").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Blank/null airline and the literal text "UNKNOWN" are being counted separately, even though
# MAGIC they mean the same thing. Having two different labels for the same "missing" state would
# MAGIC mess up the airline distribution numbers later, so combining them into one.

# COMMAND ----------

flights_clean = flights_clean.fillna({"airline": "UNKNOWN"})
flights_clean.groupBy("airline").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2e. Flagging overnight flights
# MAGIC
# MAGIC The problem statement specifically calls out overnight (cross-day) flights as something
# MAGIC that needs to be handled properly. Adding this as its own column rather than leaving it
# MAGIC buried inside the duration number, so it can be used directly on the dashboard.
# MAGIC
# MAGIC Note: the duration itself (calculated above using full timestamps, not just time-of-day) is
# MAGIC already correct for these flights, since it accounts for the date change automatically. This
# MAGIC flag is just for visibility, not fixing a calculation error.

# COMMAND ----------

flights_clean = flights_clean.withColumn(
    "is_overnight",
    F.to_date("arrival_time") > F.to_date("departure_time")
)

flights_clean.groupBy("is_overnight").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Cleaning the bookings table
# MAGIC
# MAGIC Checking the status column - a booking should always have a clear status.

# COMMAND ----------

bookings.groupBy("status").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Two problems here: some bookings have no status at all (blank), and separately there's a
# MAGIC status called "INVALID" which isn't one of the normal booking states. Treating blank/null
# MAGIC status as "UNKNOWN" (same approach as the airline column), but keeping "INVALID" as its own
# MAGIC separate category rather than merging it in - it likely means the source system itself
# MAGIC flagged that booking as broken, which is different from the status simply not being recorded.

# COMMAND ----------

bookings_clean = bookings.fillna({"status": "UNKNOWN"})
bookings_clean.groupBy("status").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Cleaning the payments table
# MAGIC
# MAGIC ### 4a. Missing amounts

# COMMAND ----------

payments.filter(payments.amount.isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC Found 48 payments with no amount recorded. Not filling these in with 0 or a guessed value,
# MAGIC since that would make it look like the payment was actually zero or an average amount, which
# MAGIC isn't true - it's just not known. Keeping these rows as they are, and only summing the
# MAGIC payments that actually have an amount when calculating revenue later.
# MAGIC
# MAGIC ### 4b. Non-numeric junk in the amount column

# COMMAND ----------

payments.filter(payments.amount == "INVALID").count()

# COMMAND ----------

# MAGIC %md
# MAGIC Found 30 rows where amount is the literal text "INVALID" rather than a number - the same
# MAGIC underlying problem as the blank amounts, just showing up as text instead of null. Converting
# MAGIC these to proper nulls using try_cast, which turns unparseable values into null instead of
# MAGIC crashing the calculation.

# COMMAND ----------

payments = payments.withColumn("amount", F.expr("try_cast(amount as double)"))
payments.filter(payments.amount.isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c. Some bookings have more than one payment record
# MAGIC
# MAGIC Assumed each booking would have at most one payment - checking that assumption before
# MAGIC joining payments in.

# COMMAND ----------

payment_counts = payments.groupBy("booking_id").count()
payment_counts.groupBy("count").count().orderBy("count").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Found that some bookings have multiple payment records - up to 6 for a single booking, with
# MAGIC different amounts and different payment methods. There's no status or timestamp field to
# MAGIC tell whether these are separate legitimate charges (like a split payment) or failed payment
# MAGIC attempts that got logged anyway.
# MAGIC
# MAGIC This is an assumption worth stating explicitly: treating all payment records for a booking
# MAGIC as real amounts and summing them into a total, rather than guessing which one to keep and
# MAGIC discarding the rest. Also adding a flag for bookings with more than one payment record, so
# MAGIC this stays visible on the dashboard as something worth investigating operationally, instead
# MAGIC of disappearing during cleaning.

# COMMAND ----------

payments_agg = payments.groupBy("booking_id").agg(
    F.sum("amount").alias("total_amount"),
    F.count("payment_id").alias("payment_count")
).withColumn("multiple_payment_attempts", F.col("payment_count") > 1)

payments_agg.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Cleaning the passengers table
# MAGIC
# MAGIC ### 5a. Missing last names

# COMMAND ----------

passengers.filter(passengers.last_name.isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC Found 10 passengers with no last name recorded. This doesn't affect any of the KPIs, so
# MAGIC instead of guessing a name, filling it with "Unknown" so the field isn't blank, and it's
# MAGIC clearly visible the name wasn't available rather than silently missing.

# COMMAND ----------

passengers_clean = passengers.fillna({"last_name": "Unknown"})
passengers_clean.filter(passengers_clean.last_name == "Unknown").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. passenger_id not actually unique
# MAGIC
# MAGIC Bookings link to passengers by passenger_id, so checking that this field is actually unique
# MAGIC before relying on it.

# COMMAND ----------

dup_passenger_ids = passengers_clean.groupBy("passenger_id").count().filter("count > 1")
dup_passenger_ids.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Found 36 passenger_ids that each have 2-3 different rows - not exact duplicates, but
# MAGIC conflicting versions of what looks like the same person (same name, age, and gender, but
# MAGIC different email, phone, and aadhaar number). One case even has a corrupted name entry
# MAGIC (first and last name merged together with a blank last name field).
# MAGIC
# MAGIC The name, age, and gender are consistent across each person's duplicate rows, so those facts
# MAGIC aren't actually in question - only contact details differ, and none of the dashboard KPIs
# MAGIC use email, phone, or aadhaar directly. So keeping one clean row per passenger_id, preferring
# MAGIC whichever row has a complete last name, so the corrupted entry doesn't get kept by accident.
# MAGIC This has to happen before masking last_name below, since after masking every value looks
# MAGIC like "X***" and the real null information would be lost.

# COMMAND ----------

w = Window.partitionBy("passenger_id").orderBy(F.col("last_name").isNull().asc())

passengers_deduped = (
    passengers_clean
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)

passengers_deduped.count(), passengers_deduped.select("passenger_id").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC If both numbers above match, passenger_id is now a real unique key - no more conflicting rows.
# MAGIC
# MAGIC ## Step 6: Protecting sensitive passenger data (PII masking)
# MAGIC
# MAGIC Not all sensitive fields need the same treatment - using three different techniques
# MAGIC depending on how the field is used and how sensitive it is:
# MAGIC
# MAGIC - Aadhaar ID and passport number are government ID numbers - never needed in plain text for
# MAGIC   any analysis, so these get one-way hashed. Even the person running this notebook can't
# MAGIC   reverse it back to the original value.
# MAGIC - Email and phone are useful in a limited way (like checking two records are the same
# MAGIC   customer), so instead of hashing, partially masking them - keeping a small visible part,
# MAGIC   hiding the rest.
# MAGIC - Date of birth and emergency contact details aren't needed for any KPI at all, so rather
# MAGIC   than mask them, dropping them from the analysis-ready data entirely. No reason to carry
# MAGIC   data forward that nothing actually uses.
# MAGIC
# MAGIC This is on top of the storage containers already being set to private access, so only people
# MAGIC with proper Azure permissions can even reach this data in the first place.

# COMMAND ----------

passengers_secure = (
    passengers_deduped
    .withColumn("aadhaar_id", F.sha2(F.col("aadhaar_id").cast("string"), 256))
    .withColumn("email", F.concat(F.substring("email", 1, 2), F.lit("***@"), F.substring_index("email", "@", -1)))
    .withColumn("phone", F.concat(F.lit("XXXXXX"), F.substring("phone", -4, 4)))
    .withColumn("last_name", F.concat(F.substring("last_name", 1, 1), F.lit("***")))
    .drop("date_of_birth")
)

passengers_secure.show(5, truncate=False)

# COMMAND ----------

bookings_secure = (
    bookings_clean
    .withColumn("passport_number", F.sha2(F.col("passport_number").cast("string"), 256))
    .drop("emergency_contact_name", "emergency_contact_phone")
)

bookings_secure.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Joining bookings to flights
# MAGIC
# MAGIC Before joining, checking for a problem the 6F250 issue could cause: if a booking references
# MAGIC flight_id 6F250, a normal join would match it to both of the two different flights that
# MAGIC share that ID, since there's nothing else to tell them apart. That would silently double
# MAGIC that booking and throw off the numbers.

# COMMAND ----------

fact_check = bookings_secure.join(flights_clean, on="flight_id", how="left")

ambiguous = fact_check.groupBy("booking_id").count().filter("count > 1")
ambiguous.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Confirmed - 2 bookings can't be reliably matched to one specific flight, since the source
# MAGIC data gives no way to tell which of the two 6F250 flights they actually belong to. Rather than
# MAGIC guess (which could silently attach a booking to the wrong flight, wrong route, wrong
# MAGIC duration), keeping these 2 bookings aside separately as "unresolved" - still valid bookings
# MAGIC for booking-level numbers like revenue, just excluded from anything that needs a specific
# MAGIC flight (like route or duration analysis).

# COMMAND ----------

ambiguous_ids = [row.booking_id for row in ambiguous.collect()]
fact_bookings = fact_check.filter(~fact_check.booking_id.isin(ambiguous_ids))
fact_unresolved = fact_check.filter(fact_check.booking_id.isin(ambiguous_ids))

print("clean joined rows:", fact_bookings.count())
print("unresolved bookings set aside:", fact_unresolved.count())

# COMMAND ----------

# MAGIC %md
# MAGIC One more check: when the invalid SJ192 flight was removed earlier, any booking that
# MAGIC referenced that flight_id would now have no match in the flights table at all. This wouldn't
# MAGIC get caught by the check above, since it only matches 0 flights, not more than 1 - it needs
# MAGIC its own check.

# COMMAND ----------

dangling = fact_bookings.filter(fact_bookings.flight_row_id.isNull())
dangling.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Found 1 booking (B1636) referencing the flight that was removed earlier for having an
# MAGIC invalid negative duration. Since that flight's data can't be trusted, there's no valid route
# MAGIC or duration to attach to this booking either. Same approach as the ambiguous bookings above -
# MAGIC keeping this one aside rather than leaving it silently sitting in the main table with blank
# MAGIC flight data.

# COMMAND ----------

fact_unresolved = fact_unresolved.unionByName(dangling, allowMissingColumns=True)
fact_bookings = fact_bookings.filter(fact_bookings.flight_row_id.isNotNull())

print("fact_bookings count:", fact_bookings.count())
print("total unresolved bookings:", fact_unresolved.count())

# COMMAND ----------

# MAGIC %md
# MAGIC That's the full set of unresolved bookings now - the 2 from the 6F250 ambiguity (4 rows,
# MAGIC since each matched twice) plus this 1 dangling one. Keeping them together in one place
# MAGIC instead of losing track of them.
# MAGIC
# MAGIC ## Step 8: Joining in payment totals
# MAGIC
# MAGIC Using the aggregated payments table from Step 4c (one row per booking), so this join can't
# MAGIC cause the same kind of duplication problem.

# COMMAND ----------

fact_bookings = fact_bookings.join(payments_agg, on="booking_id", how="left")
fact_bookings.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Building the star schema
# MAGIC
# MAGIC Organizing everything into a proper star schema instead of one wide table - a central
# MAGIC fact_bookings table holding the keys and booking-level numbers, with separate dimension
# MAGIC tables for flight details and passenger details. This avoids repeating the same flight
# MAGIC information across every booking row, and matches standard data modeling practice.
# MAGIC
# MAGIC dim_flight also gets one more column here: a flag for flights whose duration is unusually
# MAGIC far off from the average duration on that same route (more than 2 standard deviations away).
# MAGIC There's no "scheduled time" field in this data to calculate a traditional delay, so this is
# MAGIC used as a stand-in anomaly signal instead - a flight taking much longer or shorter than others
# MAGIC on the same route is worth a second look operationally.

# COMMAND ----------

dim_flight = flights_clean.select(
    "flight_row_id", "flight_id", "airline", "source", "destination",
    "departure_time", "arrival_time", "duration_minutes", "is_overnight"
)

route_stats = dim_flight.groupBy("source", "destination").agg(
    F.avg("duration_minutes").alias("route_avg"),
    F.stddev("duration_minutes").alias("route_stddev")
)

dim_flight = dim_flight.join(route_stats, on=["source", "destination"]).withColumn(
    "is_duration_anomaly",
    F.abs(F.col("duration_minutes") - F.col("route_avg")) > 2 * F.col("route_stddev")
).drop("route_avg", "route_stddev")

dim_flight.filter("is_duration_anomaly = true").count()

# COMMAND ----------

dim_passenger = passengers_secure.select(
    "passenger_id", "first_name", "last_name", "age", "gender",
    "email", "phone", "aadhaar_id"
)

fact_bookings = fact_bookings.select(
    "booking_id", "passenger_id", "flight_row_id", "booking_date", "status",
    "seat_number", "passport_number", "total_amount", "payment_count", "multiple_payment_attempts"
)

dim_flight.count(), dim_passenger.count(), fact_bookings.count()

# COMMAND ----------

# MAGIC %md
# MAGIC This is now a real star schema - fact_bookings joins to dim_flight through flight_row_id,
# MAGIC and to dim_passenger through passenger_id, and both keys are guaranteed unique on the
# MAGIC dimension side, so no relationship built on top of this in Power BI can silently fan out.
# MAGIC
# MAGIC ## Step 10: One flattened table for Power BI
# MAGIC
# MAGIC The star schema above is the correct way to model this data, and it's what's documented in
# MAGIC the architecture/data model writeup. For actually building the dashboard, joining everything
# MAGIC into one flat table as well - simpler to work with in Power BI than managing relationships
# MAGIC between three tables, especially without much prior Power BI experience. Also adding one
# MAGIC more useful column here: how many days in advance the booking was made before the flight.

# COMMAND ----------

gold = (
    fact_bookings
    .join(dim_flight, on="flight_row_id", how="left")
    .join(dim_passenger.select("passenger_id", "first_name", "last_name", "age", "gender"), on="passenger_id", how="left")
)

gold = gold.withColumn(
    "booking_lead_time_days",
    (F.unix_timestamp("departure_time") - F.unix_timestamp("booking_date")) / 86400
)

gold.count()

# COMMAND ----------

gold.filter(gold.booking_lead_time_days < 0).count()

# COMMAND ----------

# MAGIC %md
# MAGIC A negative value here would mean a booking got recorded as happening after its own flight
# MAGIC already departed, which wouldn't make sense - a good sanity check to run before trusting
# MAGIC this column on the dashboard.
# MAGIC
# MAGIC ## Step 11: Exporting the Gold layer
# MAGIC
# MAGIC Writing out the star schema tables plus the flat table, ready to be downloaded and loaded
# MAGIC into Power BI.

# COMMAND ----------

gold_path = "/Volumes/asg_airlines_databricks/default/raw_data/gold_output"
dbutils.fs.mkdirs(gold_path)

fact_bookings.toPandas().to_csv(f"{gold_path}/fact_bookings.csv", index=False)
dim_flight.toPandas().to_csv(f"{gold_path}/dim_flight.csv", index=False)
dim_passenger.toPandas().to_csv(f"{gold_path}/dim_passenger.csv", index=False)
gold.toPandas().to_csv(f"{gold_path}/gold_flat_table.csv", index=False)

print("exported 4 files to", gold_path)
