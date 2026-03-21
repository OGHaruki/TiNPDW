import sqlite3
import csv
import os
import time
import config
from typing import Any, Dict
import matplotlib.pyplot as plt

DB_PATH = os.path.expanduser("~/home/jajes/studia/magisterka/I_sem/TiNPDW/complaints.db")

def init_database():
    if os.path.exists(DB_PATH):
        print(f"Database already exists at {DB_PATH}. Skipping import.")
        return

    print("Creating database and importing data...")
    start_time = time.time()
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS complaints (
            OFNS_DESC TEXT,
            BORO_NM TEXT,
            JURIS_DESC TEXT,
            PREM_TYP_DESC TEXT,
            VIC_AGE_GROUP TEXT
        )
    ''')

    try:
        with open(config.DATA_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            to_db = ((r['OFNS_DESC'], r['BORO_NM'], r['JURIS_DESC'], r['PREM_TYP_DESC'], r['VIC_AGE_GROUP']) for r in reader)
            
            cursor.executemany(
                "INSERT INTO complaints (OFNS_DESC, BORO_NM, JURIS_DESC, PREM_TYP_DESC, VIC_AGE_GROUP) VALUES (?, ?, ?, ?, ?);", 
                to_db
            )
        conn.commit()
        
        print("Creating indexes...")
        cursor.execute("CREATE INDEX idx_boro ON complaints(BORO_NM)")
        cursor.execute("CREATE INDEX idx_ofns ON complaints(OFNS_DESC)")
        cursor.execute("CREATE INDEX idx_juris ON complaints(JURIS_DESC)")
        conn.commit()
        
    except Exception as e:
        print(f"Error during import: {e}")
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        return
    finally:
        conn.close()
        
    print(f"Import finished in {time.time() - start_time:.2f} seconds.")

def query_1(conn):
    print("\nExecuting Query 1: 10 most frequently reported offenses...")
    start_time = time.time()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT OFNS_DESC, COUNT(*) as count 
        FROM complaints 
        GROUP BY OFNS_DESC 
        ORDER BY count DESC 
        LIMIT 10
    ''')
    
    print("\n--- Results: Query 1 ---")
    for i, (name, count) in enumerate(cursor.fetchall(), 1):
        print(f"{i}. {name}: {count}")
    
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.4f} seconds")

def query_2(conn):
    print("\nExecuting Query 2: Top 3 complaints per borough...")
    start_time = time.time()
    cursor = conn.cursor()
    cursor.execute('''
                   SELECT DISTINCT BORO_NM FROM complaints
                   WHERE BORO_NM IS NOT NULL AND BORO_NM != '' AND BORO_NM != '(null)'
    ''')
    boroughs = [row[0] for row in cursor.fetchall()]
    complaints_by_borough: Dict[str, Any] = {}
    for boro in boroughs:
        cursor.execute('''
            SELECT OFNS_DESC, COUNT(*) as count 
            FROM complaints 
            WHERE BORO_NM = ? 
            GROUP BY OFNS_DESC 
            ORDER BY count DESC 
            LIMIT 3
        ''', (boro,))
        complaints_by_borough[boro] = cursor.fetchall()
    end_time = time.time()
    print("\n--- Results: Query 2 ---")
    for boro, complaints in complaints_by_borough.items():
        print(f"\nBorough: {boro}")
        for i, (name, count) in enumerate(complaints, 1):
            print(f"  {i}. {name}: {count}")
    print(f"Execution time: {end_time - start_time:.4f} seconds")
    cursor.close()


def query_3(conn):
    print("\nExecuting Query 3: Top 3 agencies with most complaints...")
    start_time = time.time()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT JURIS_DESC, COUNT(*) as count 
        FROM complaints 
        GROUP BY JURIS_DESC 
        ORDER BY count DESC 
        LIMIT 3
    ''')
    top_agencies = cursor.fetchall()
    complaints_by_agency: Dict[str, int] = {}
    for agency, _ in top_agencies:
        cursor.execute('''
            SELECT OFNS_DESC, COUNT(*) as count 
            FROM complaints 
            WHERE JURIS_DESC = ? 
            GROUP BY OFNS_DESC 
            ORDER BY count DESC 
            LIMIT 3
        ''', (agency,))
        complaints_by_agency[agency] = cursor.fetchall()
    end_time = time.time()
    print("\n--- Results: Query 3 ---")
    for agency, complaints in complaints_by_agency.items():
        print(f"\nAgency: {agency}")
        for i, (name, count) in enumerate(complaints, 1):
            print(f"  {i}. {name}: {count}")
    print(f"Execution time: {end_time - start_time:.4f} seconds")

def query_4(conn):
    print("\nExecuting Query 4: Top 4 locations with most complaints...")
    start_time = time.time()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT PREM_TYP_DESC, COUNT(*) as count 
        FROM complaints 
        GROUP BY PREM_TYP_DESC 
        ORDER BY count DESC 
        LIMIT 4
    ''')
    top_locations = cursor.fetchall()
    complaints_by_location: Dict[str, int] = {}
    for location, _ in top_locations:
        cursor.execute('''
            SELECT OFNS_DESC, COUNT(*) as count 
            FROM complaints 
            WHERE PREM_TYP_DESC = ? 
            GROUP BY OFNS_DESC 
            ORDER BY count DESC 
            LIMIT 3
        ''', (location,))
        complaints_by_location[location] = cursor.fetchall()
    end_time = time.time()
    print("\n--- Results: Query 4 ---")
    for location, complaints in complaints_by_location.items():
        print(f"\nLocation: {location}")
        for i, (name, count) in enumerate(complaints, 1):
            print(f"  {i}. {name}: {count}")
    print(f"Execution time: {end_time - start_time:.4f} seconds")
    cursor.close()

def query_5(conn):
    print("\nExecuting Query 5: Percentage of complaints by victim age group...")
    start_time = time.time()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT VIC_AGE_GROUP, COUNT(*) as count 
        FROM complaints 
        WHERE VIC_AGE_GROUP IS NOT NULL
            AND VIC_AGE_GROUP NOT IN ('', '(null)')
        GROUP BY VIC_AGE_GROUP 
        ORDER BY count DESC
    ''')
    
    results = cursor.fetchall()
    total_complaints = sum(count for _, count in results)

    labels = []
    sizes = []
    
    print("\n--- Results: Query 5 ---")
    for age_group, count in results:
        percentage = (count / total_complaints) * 100 if total_complaints > 0 else 0
        labels.append(age_group)
        sizes.append(percentage)
        print(f"Victim Age Group: {age_group}, Complaints: {count}, Percentage: {percentage:.2f}%")
    
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.4f} seconds")
    cursor.close()

    plt.figure(figsize=(10, 7))
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, shadow=True)
    plt.title("Percentage of Crime Victims by Age Group")
    plt.axis('equal')
    plt.savefig("output/victims_by_age_group_sqlite.png")
    plt.show()


def run_all_queries(conn):
    query_1(conn)
    query_2(conn)
    query_3(conn)
    query_4(conn)
    query_5(conn)

def main():
    init_database()
    
    if not os.path.exists(DB_PATH):
        print("Database could not be initialized.")
        return

    conn = sqlite3.connect(DB_PATH)
    
    while True:
        print("1. Query 1: 10 most frequently reported offenses")
        print("2. Query 2: Top 3 complaints per borough")
        print("3. Query 3: Top 3 agencies with most complaints")
        print("4. Query 4: Top 4 locations with most complaints")
        print("5. Query 5: Percentage of complaints by victim age group")
        print("0. Run all queries")
        print("q. Exit")
        
        choice = input("\nSelect query number: ").strip().lower()
        
        if choice == '1':
            query_1(conn)
        elif choice == '2':
            query_2(conn)
        elif choice == '3':
            query_3(conn)
        elif choice == '4':
            query_4(conn)
        elif choice == '5':
            query_5(conn)
        elif choice == '0':
            run_all_queries(conn)
        elif choice == 'q':
            break
        else:
            print("Invalid choice. Try again.")

    conn.close()

if __name__ == "__main__":
    main()
