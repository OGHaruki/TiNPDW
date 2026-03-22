import time
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
import config
from typing import Any, Dict

def get_spark_session():
    return SparkSession.builder \
        .appName("TiNPDW_Task2_SQL") \
        .master("local[*]") \
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY) \
        .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY) \
        .getOrCreate()

def load_data(spark):
    start_time = time.time()
    print(f"Loading data from {config.DATA_PATH} using Spark SQL...")
    df = spark.read.csv(config.DATA_PATH, header=True, inferSchema=True)
    df = df.select("OFNS_DESC", "BORO_NM", "JURIS_DESC", "PREM_TYP_DESC", "VIC_AGE_GROUP")
    end_time = time.time()
    print("Data loaded in {:.4f} seconds.".format(end_time - start_time))
    return df

def query_1(df):
    print("\nExecuting Query 1: 10 most frequently reported offenses...")
    start_time = time.time()
    results = df.groupBy("OFNS_DESC") \
                .count() \
                .orderBy(desc("count")) \
                .limit(10)

    print(f"\n--- Results: Query 1 ---")
    results.show(truncate=False)
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")

def query_2(df):
    print("\nExecuting Query 2: 3 most frequently reported complaints in each borough...")
    start_time = time.time()
    df_clean = df.filter(
        col("BORO_NM").isNotNull()
        & (col("BORO_NM") != "")
        & (col("BORO_NM") != "(null)")
        & col("OFNS_DESC").isNotNull()
        & (col("OFNS_DESC") != "")
        & (col("OFNS_DESC") != "(null)")
    )

    boroughs = (
        df_clean.select("BORO_NM")
        .distinct()
        .orderBy("BORO_NM")
        .collect()
    )
    
    complaints_by_borough: Dict[str, Any] = {}
    for borough in boroughs:
        boro_name = borough["BORO_NM"]
        top_complaints = df_clean.filter(col("BORO_NM") == boro_name) \
                        .groupBy("OFNS_DESC") \
                        .count() \
                        .orderBy(desc("count")) \
                        .limit(3)
        complaints_by_borough[boro_name] = top_complaints

    print(f"\n--- Results: Query 2 ---")
    for boro, complaints in complaints_by_borough.items():
        print(f"\nBorough: {boro}")
        complaints.show(truncate=False)
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")


def query_3(df):
    print("\nExecuting Query 3: 3 agencies with most complaints (and their top 3 complaints)...")
    start_time = time.time()
    results = df.groupBy("JURIS_DESC") \
                .count() \
                .orderBy(desc("count")) \
                .limit(3)
    
    complaints_by_agency: Dict[str, Any] = {}
    for row in results.collect():
        agency = row["JURIS_DESC"]
        top_complaints = df.filter(col("JURIS_DESC") == agency) \
                        .groupBy("OFNS_DESC") \
                        .count() \
                        .orderBy(desc("count")) \
                        .limit(3)
        complaints_by_agency[agency] = top_complaints

    
    print(f"\n--- Results: Query 3 ---")
    for agency, complaints in complaints_by_agency.items():
        print(f"\nAgency: {agency}")
        complaints.show(truncate=False)
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")

def query_4(df):
    print("\nExecuting Query 4: 4 locations with most complaints (and their top 3 complaints)...")
    start_time = time.time()
    locations = df.groupBy("PREM_TYP_DESC") \
                .count() \
                .orderBy(desc("count")) \
                .limit(4)
    
    complaints_by_location: Dict[str, Any] = {}
    for location in locations.collect():
        loc_name = location["PREM_TYP_DESC"]
        top_complaints = df.filter(col("PREM_TYP_DESC") == loc_name) \
                        .groupBy("OFNS_DESC") \
                        .count() \
                        .orderBy(desc("count")) \
                        .limit(3)
        complaints_by_location[loc_name] = top_complaints

    print(f"\n--- Results: Query 4 ---")
    for loc, complaints in complaints_by_location.items():
        print(f"\nLocation: {loc}")
        complaints.show(truncate=False)
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")

def query_5(df):
    print("\nExecuting Query 5: Percentage of crime victims by age group...")
    start_time = time.time()
    total_victims = df.filter(col("VIC_AGE_GROUP").isNotNull() & (col("VIC_AGE_GROUP") != "") & (col("VIC_AGE_GROUP") != "(null)")) \
                        .groupBy("VIC_AGE_GROUP") \
                        .count() \
                        .collect()
    total_count = sum(row["count"] for row in total_victims)
    results = sorted([(row["VIC_AGE_GROUP"], (row["count"] / total_count) * 100) for row in total_victims], key=lambda x: -x[1])
    print(f"\n--- Results: Query 5 ---")
    for age_group, percentage in results:
        print(f"{age_group}: {percentage:.2f}%")
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")

    # Generate a plot
    labels = [r[0] for r in results]
    sizes = [r[1] for r in results]
    plt.figure(figsize=(10, 7))
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title('Percentage of Crime Victims by Age Group')
    plt.axis('equal')
    plt.savefig("output/victims_by_age_group_spark_sql.png")
    plt.show()

def run_all_queries(df):
    query_1(df)
    query_2(df)
    query_3(df)
    query_4(df)
    query_5(df)

def main():
    spark = get_spark_session()
    df = load_data(spark)

    while True:
        print("1. Query 1: 10 most frequently reported offenses")
        print("2. Query 2: Top 3 complaints per borough")
        print("3. Query 3: Top 3 agencies with most complaints")
        print("4. Query 4: Top 4 locations with most complaints")
        print("5. Query 5: Percentage of crime victims by age group")
        print("0. Run all queries")
        print("q. Exit")
        
        choice = input("\nSelect query number: ").strip().lower()
        
        if choice == '1':
            query_1(df)
        elif choice == '2':
            query_2(df)
        elif choice == '3':
            query_3(df)
        elif choice == '4':
            query_4(df)
        elif choice == '5':
            query_5(df)
        elif choice == '0':
            run_all_queries(df)
        elif choice == 'q':
            break
        else:
            print("Invalid choice. Try again.")

    spark.stop()

if __name__ == "__main__":
    main()
