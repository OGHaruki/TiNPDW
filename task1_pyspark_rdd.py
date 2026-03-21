import time
import csv
from pyspark import SparkConf, SparkContext
from typing import Any, Dict
import matplotlib.pyplot as plt
import config

def parse_csv_line(line):
    return next(csv.reader([line]))

def get_spark_context():
    conf = SparkConf() \
        .setAppName("TiNPDW_Task1_RDD") \
        .setMaster("local[*]") \
        .set("spark.driver.memory", config.SPARK_DRIVER_MEMORY) \
        .set("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY) \
        .set("spark.worker.memory", config.SPARK_WORKER_MEMORY)
    
    return SparkContext(conf=conf)

def get_data_rdd(sc):
    print(f"Loading data from {config.DATA_PATH}...")
    rdd = sc.textFile(config.DATA_PATH)
    header = rdd.first()
    data_rdd = rdd.filter(lambda line: line != header) \
                  .map(parse_csv_line)
    
    print("Data loaded.")
    return data_rdd

def query_1(data_rdd):
    print("\nExecuting Query 1: 10 most frequently reported offenses...")
    start_time = time.time()
    
    results = data_rdd.map(lambda x: (x[config.COL_OFNS], 1)) \
                      .reduceByKey(lambda a, b: a + b) \
                      .takeOrdered(10, key=lambda x: -x[1])
    
    end_time = time.time()
    
    print("\n--- Results: Query 1 ---")
    for i, (name, count) in enumerate(results, 1):
        print(f"{i}. {name}: {count}")
    print(f"Query 1 execution time: {end_time - start_time:.4f} seconds")

def query_2(data_rdd):
    print("\nExecuting Query 2: Top 3 complaints per borough...")
    start_time = time.time()
    boroughs = data_rdd.map(lambda x: x[config.COL_BORO]) \
                        .distinct() \
                        .filter(lambda x: x != "" and x != "(null)") \
                        .collect()
    
    complaints_by_borough: Dict[str, Any] = {}
    for boro in boroughs:
        top_complaints = data_rdd.filter(lambda x: x[config.COL_BORO] == boro) \
                        .map(lambda x: (x[config.COL_OFNS], 1)) \
                        .reduceByKey(lambda a, b: a + b) \
                        .takeOrdered(3, key=lambda x: -x[1])
        complaints_by_borough[boro] = top_complaints

    end_time = time.time()
    print("\n--- Results: Query 2 ---")
    for boro, complaints in complaints_by_borough.items():
        print(f"\nBorough: {boro}")
        for i, (name, count) in enumerate(complaints, 1):
            print(f"  {i}. {name}: {count}")
    print(f"Query 2 execution time: {end_time - start_time:.4f} seconds")

def query_3(data_rdd):
    print("\nExecuting Query 3: Top 3 agencies with most complaints...")
    start_time = time.time()
    top_agencies = data_rdd.map(lambda x: (x[config.COL_AGENCY], 1)) \
                        .reduceByKey(lambda a, b: a + b) \
                        .takeOrdered(3, key=lambda x: -x[1])
    agency_list = [agency for agency, _ in top_agencies]
    complaints_by_agency: Dict[str, int] = {}
    for agency in agency_list:
        count = data_rdd.filter(lambda x: x[config.COL_AGENCY] == agency) \
                .map(lambda x: (x[config.COL_OFNS], 1)) \
                .reduceByKey(lambda a, b: a + b) \
                .takeOrdered(3, key=lambda x: -x[1])
        complaints_by_agency[agency] = count

    end_time = time.time()
    print("\n--- Results: Query 3 ---")
    for agency, complaints in complaints_by_agency.items():
        print(f"\nAgency: {agency}")
        for i, (name, count) in enumerate(complaints, 1):
            print(f"  {i}. {name}: {count}")
    print(f"Query 3 execution time: {end_time - start_time:.4f} seconds")

def query_4(data_rdd):
    print("\nExecuting Query 4: Top 4 locations with most complaints...")
    start_time = time.time()
    top_agencies = data_rdd.map(lambda x: (x[config.COL_LOC], 1)) \
                        .reduceByKey(lambda a, b: a + b) \
                        .takeOrdered(4, key=lambda x: -x[1])
    location_list = [location for location, _ in top_agencies]
    complaints_by_location: Dict[str, int] = {}
    for location in location_list:
        count = data_rdd.filter(lambda x: x[config.COL_LOC] == location) \
                .map(lambda x: (x[config.COL_OFNS], 1)) \
                .reduceByKey(lambda a, b: a + b) \
                .takeOrdered(3, key=lambda x: -x[1])
        complaints_by_location[location] = count

    end_time = time.time()
    print("\n--- Results: Query 4 ---")
    for location, complaints in complaints_by_location.items():
        print(f"\nLocation: {location}")
        for i, (name, count) in enumerate(complaints, 1):
            print(f"  {i}. {name}: {count}")
    print(f"Query 4 execution time: {end_time - start_time:.4f} seconds")

def query_5(data_rdd):
    print("\nExecuting Query 5: Percentage of crime victims by age group...")
    start_time = time.time()
    total_victims = data_rdd.map(lambda x: (x[config.COL_VIC_AGE_GROUP], 1)) \
                        .filter(lambda x: x[0] and x[0] not in ['', '(null)']) \
                        .reduceByKey(lambda a, b: a + b) \
                        .collect()
    total_count = sum(c for _, c in total_victims)
    results = sorted([(group, (count / total_count) * 100) for group, count in total_victims], key=lambda x: -x[1])

    print("\n--- Results: Query 5 ---")
    for age_group, percentage in results:
        print(f"{age_group}: {percentage:.2f}%")
    end_time = time.time()
    print(f"Query 5 execution time: {end_time - start_time:.4f} seconds")
    
    # Generate a plot
    labels = [r[0] for r in results]
    sizes = [r[1] for r in results]
    plt.figure(figsize=(10, 7))
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title("Percentage of Crime Victims by Age Group")
    plt.axis('equal')
    plt.savefig("output/victims_by_age_group_rdd.png")
    plt.show()

def run_all_queries(data_rdd):
    query_1(data_rdd)
    query_2(data_rdd)
    query_3(data_rdd)
    query_4(data_rdd)
    query_5(data_rdd)

def main():
    sc = get_spark_context()
    data_rdd = get_data_rdd(sc)
    
    if data_rdd is None:
        sc.stop()
        return

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
            query_1(data_rdd)
        elif choice == '2':
            query_2(data_rdd)
        elif choice == '3':
            query_3(data_rdd)
        elif choice == '4':
            query_4(data_rdd)
        elif choice == '5':
            query_5(data_rdd)
        elif choice == '0':
            run_all_queries(data_rdd)
        elif choice == 'q':
            break
        else:
            print("Invalid choice. Try again.")

    sc.stop()

if __name__ == "__main__":
    main()
