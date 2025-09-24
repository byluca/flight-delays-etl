# ==============================================================================
# Flight Delays ETL Pipeline
#
# Author: Lascaro Gianluca, Morbidelli Filippo,Trincia Elio
# Course: Master in Artificial Intelligence - Data Engineering
#
# Description:
# This script performs a full ETL (Extract, Transform, Load) process on flight
# delay data. It reads raw data from CSV files, transforms it into a clean
# Star Schema (dimensional model), and loads it into a MySQL database.
# Finally, it runs verification queries to demonstrate the model's utility.
# ==============================================================================

# --- Library Imports ---
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy.exc
import matplotlib.pyplot as plt
import seaborn as sns

# ==============================================================================
# --- 1. CONFIGURATION PARAMETERS ---
# Please edit these settings to match your local environment.
# ==============================================================================

# Database Connection Settings
DB_USER = "root"
DB_PASSWORD = "Pirati2206#"  # IMPORTANT: Change this to your MySQL password
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "flight_delays_db"

# Input Data File Paths (relative to this script's location)
# Assumes a 'data' subfolder contains the CSV files.
PATH_FLIGHTS = 'data/flights.csv'
PATH_AIRPORTS = 'data/airports.csv'
PATH_AIRLINES = 'data/airlines.csv'


def main():
    """Main function to run the entire ETL pipeline."""

    # ==============================================================================
    # --- 2. EXTRACT: Load data from CSV files ---
    # ==============================================================================
    print("--- Step 1: Extracting Data from CSV Files ---")
    try:
        flights_df = pd.read_csv(PATH_FLIGHTS, low_memory=False)
        airports_df = pd.read_csv(PATH_AIRPORTS)
        airlines_df = pd.read_csv(PATH_AIRLINES)
        print("✅ Data extracted successfully.")
    except FileNotFoundError:
        print(f"❌ ERROR: A source file was not found. Please ensure the 'data' folder exists.")
        return  # Stop execution if files are missing

    # ==============================================================================
    # --- (Optional) Initial Exploratory Data Analysis (EDA) ---
    # ==============================================================================
    print("\n--- Performing Initial Exploratory Data Analysis ---")
    sns.set_style("whitegrid")

    # Plot 1: Show the number of flights per airline
    plt.figure(figsize=(12, 8))
    sns.countplot(y="AIRLINE", data=flights_df, order=flights_df['AIRLINE'].value_counts().index, hue="AIRLINE", legend=False, palette="viridis")
    plt.title('Total Number of Flights per Airline in 2015', fontsize=16)
    plt.xlabel('Number of Flights', fontsize=12)
    plt.ylabel('Airline IATA Code', fontsize=12)
    plt.tight_layout()
    plt.show()

    # Plot 2: Show the distribution of arrival delays
    plt.figure(figsize=(12, 6))
    sns.histplot(flights_df['ARRIVAL_DELAY'].dropna(), bins=120, kde=False, binrange=(-60, 180))
    plt.title('Distribution of Arrival Delays', fontsize=16)
    plt.xlabel('Arrival Delay (minutes)', fontsize=12)
    plt.ylabel('Number of Flights', fontsize=12)
    plt.axvline(0, color='red', linestyle='--')
    plt.show()

    # ==============================================================================
    # --- 3. TRANSFORM: Build the Star Schema ---
    # ==============================================================================
    print("\n--- Step 2: Transforming Data into a Star Schema ---")

    # --- 3.1 Create Dimension Tables ---
    print("  - Creating dimension tables...")
    # Dimension 1: Airlines
    dim_airlines = airlines_df.copy()
    dim_airlines['airline_id'] = dim_airlines.index + 1

    # Dimension 2: Airports
    dim_airports = airports_df.copy()
    dim_airports['airport_id'] = dim_airports.index + 1

    # Dimension 3: Time
    flights_df['date'] = pd.to_datetime(flights_df[['YEAR', 'MONTH', 'DAY']])
    unique_dates = flights_df['date'].unique()
    dim_time = pd.DataFrame({'date': unique_dates})
    dim_time['year'] = dim_time['date'].dt.year
    dim_time['month'] = dim_time['date'].dt.month
    dim_time['day'] = dim_time['date'].dt.day
    dim_time['day_of_week'] = dim_time['date'].dt.dayofweek
    dim_time['is_weekend'] = dim_time['day_of_week'].isin([5, 6])
    dim_time = dim_time.sort_values(by='date').reset_index(drop=True)
    dim_time['time_id'] = dim_time.index + 1
    print("✅ Dimension tables created.")

    # --- 3.2 Prepare the Fact Table ---
    print("  - Preparing the fact table...")
    columns_to_keep = [
        'date', 'AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT',
        'DEPARTURE_DELAY', 'ARRIVAL_DELAY', 'AIR_TIME', 'DISTANCE', 'CANCELLED', 'DIVERTED'
    ]
    fact_flights = flights_df[columns_to_keep].copy()
    fact_flights.dropna(subset=['ARRIVAL_DELAY', 'DEPARTURE_DELAY', 'AIR_TIME'], inplace=True)

    # Merge with dimension tables to get foreign keys
    fact_flights = pd.merge(fact_flights, dim_time[['date', 'time_id']], on='date', how='left')
    fact_flights = pd.merge(fact_flights, dim_airlines[['IATA_CODE', 'airline_id']], left_on='AIRLINE', right_on='IATA_CODE', how='left')
    fact_flights = pd.merge(fact_flights, dim_airports[['IATA_CODE', 'airport_id']], left_on='ORIGIN_AIRPORT', right_on='IATA_CODE', how='left')
    fact_flights.rename(columns={'airport_id': 'origin_airport_id'}, inplace=True)
    fact_flights = pd.merge(fact_flights, dim_airports[['IATA_CODE', 'airport_id']], left_on='DESTINATION_AIRPORT', right_on='IATA_CODE', how='left')
    fact_flights.rename(columns={'airport_id': 'destination_airport_id'}, inplace=True)

    final_columns = [
        'time_id', 'airline_id', 'origin_airport_id', 'destination_airport_id',
        'ARRIVAL_DELAY', 'DEPARTURE_DELAY', 'AIR_TIME', 'DISTANCE', 'CANCELLED', 'DIVERTED'
    ]
    fact_flights = fact_flights[final_columns]

    # CRITICAL STEP: Remove rows with missing foreign keys to ensure data integrity
    initial_rows = len(fact_flights)
    fact_flights.dropna(inplace=True)
    print(f"  - Cleaned data: Removed {initial_rows - len(fact_flights)} rows with inconsistent foreign keys.")

    # CRITICAL STEP: Correct data types to match the SQL schema
    id_cols = ['time_id', 'airline_id', 'origin_airport_id', 'destination_airport_id']
    fact_flights[id_cols] = fact_flights[id_cols].astype(int)
    fact_flights['ARRIVAL_DELAY'] = fact_flights['ARRIVAL_DELAY'].astype(int)
    fact_flights['DEPARTURE_DELAY'] = fact_flights['DEPARTURE_DELAY'].astype(int)
    print("  - Data types corrected to match SQL schema.")
    print("✅ Fact table is now clean and ready.")

    # ==============================================================================
    # --- 4. LOAD: Populate the Database ---
    # ==============================================================================
    print("\n--- Step 3: Loading Data into the Database ---")
    
    db_connection_str = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    try:
        engine = create_engine(db_connection_str)
        print("  - Connection to database successful.")

        print("  - Loading dimension tables...")
        dim_airlines.to_sql('d_airlines', con=engine, if_exists='append', index=False)
        dim_airports.to_sql('d_airports', con=engine, if_exists='append', index=False)
        dim_time.to_sql('d_time', con=engine, if_exists='append', index=False)
        
        print("  - Loading fact table (this may take a few minutes)...")
        fact_flights.to_sql(
            'f_flights', 
            con=engine, 
            if_exists='append', 
            index=False, 
            chunksize=50000
        )
        print("✅ All data has been successfully loaded into the database!")
    except Exception as e:
        print(f"❌ An error occurred during database operations. Details: {e}")
        return

    # ==============================================================================
    # --- 5. VERIFY: Run analytical queries ---
    # ==============================================================================
    print("\n--- Step 4: Running Verification Queries ---")
    
    # Query 1: Top 10 Airlines by Average Delay
    query_1 = """
    SELECT da.AIRLINE AS airline_name, AVG(ff.ARRIVAL_DELAY) AS average_delay
    FROM f_flights ff JOIN d_airlines da ON ff.airline_id = da.airline_id
    GROUP BY da.AIRLINE ORDER BY average_delay DESC LIMIT 10;
    """
    try:
        print("\n  - Executing Query 1: Top 10 Airlines by Delay...")
        df1 = pd.read_sql(query_1, engine)
        print("    Query Result:")
        print(df1)
        plt.figure(figsize=(12, 8))
        sns.barplot(x='average_delay', y='airline_name', data=df1, palette="plasma", hue='airline_name', legend=False)
        plt.title('Business Question 1: Top 10 Airlines by Average Arrival Delay', fontsize=16)
        plt.xlabel('Average Arrival Delay (minutes)', fontsize=12)
        plt.ylabel('Airline', fontsize=12)
        plt.tight_layout()
        plt.show()
    except Exception as e:
        print(f"❌ Query 1 failed. Details: {e}")

    # Query 2: Top 10 Major Airports by Departure Delay
    query_2 = """
    SELECT da.AIRPORT AS airport_name, AVG(ff.DEPARTURE_DELAY) AS average_departure_delay
    FROM f_flights ff JOIN d_airports da ON ff.origin_airport_id = da.airport_id
    GROUP BY da.AIRPORT HAVING COUNT(ff.flight_id) > 50000
    ORDER BY average_departure_delay DESC LIMIT 10;
    """
    try:
        print("\n  - Executing Query 2: Top 10 Major Airports by Delay...")
        df2 = pd.read_sql(query_2, engine)
        print("    Query Result:")
        print(df2)
        plt.figure(figsize=(12, 8))
        sns.barplot(x='average_departure_delay', y='airport_name', data=df2, palette="magma", hue='airport_name', legend=False)
        plt.title('Business Question 2: Top 10 Major Airports by Departure Delay', fontsize=16)
        plt.xlabel('Average Departure Delay (minutes)', fontsize=12)
        plt.ylabel('Airport', fontsize=12)
        plt.tight_layout()
        plt.show()
    except Exception as e:
        print(f"❌ Query 2 failed. Details: {e}")

    # Query 3: Average Delay on Weekdays vs. Weekends
    query_3 = """
    SELECT dt.is_weekend, AVG(ff.ARRIVAL_DELAY) AS average_arrival_delay
    FROM f_flights ff JOIN d_time dt ON ff.time_id = dt.time_id
    GROUP BY dt.is_weekend ORDER BY dt.is_weekend;
    """
    try:
        print("\n  - Executing Query 3: Weekday vs. Weekend Delay...")
        df3 = pd.read_sql(query_3, engine)
        df3['day_type'] = df3['is_weekend'].apply(lambda x: 'Weekend' if x else 'Weekday')
        print("    Query Result:")
        print(df3[['day_type', 'average_arrival_delay']])
        plt.figure(figsize=(8, 6))
        sns.barplot(x='day_type', y='average_arrival_delay', data=df3, palette="cividis", hue='day_type', legend=False)
        plt.title('Business Question 3: Average Arrival Delay - Weekday vs. Weekend', fontsize=16)
        plt.xlabel('Day Type', fontsize=12)
        plt.ylabel('Average Arrival Delay (minutes)', fontsize=12)
        plt.tight_layout()
        plt.show()
    except Exception as e:
        print(f"❌ Query 3 failed. Details: {e}")

# ==============================================================================
# --- Script Entry Point ---
# This line ensures that the main() function is called only when the script
# is executed directly.
# ==============================================================================
if __name__ == "__main__":
    main()
