
# Flight Delays ETL Pipeline

**Author(s):** Lascaro Gianluca, Morbidelli Filippo, Trincia Elio
**Course:** Master in Artificial Intelligence - Data Engineering

---

## 1. Project Overview

This repository contains a complete, end-to-end Data Engineering pipeline built to process a large dataset of flight information from 2015. The primary goal of this project was not to perform data analysis, but to design and implement the foundational data infrastructure required for any future analytical tasks.

The core of the project is a **Dimensional Model (Star Schema)** designed to be efficient for analytical queries and easy to understand. The pipeline performs a full **ETL (Extract, Transform, Load)** process:

-   **Extract:** Reads raw data from three source CSV files (`flights.csv`, `airports.csv`, `airlines.csv`).
-   **Transform:** Cleans and reshapes the data into a Star Schema, consisting of one central fact table (`f_flights`) and three dimension tables (`d_airlines`, `d_airports`, `d_time`). A critical part of this phase was handling a major data quality issue: **486,165 flight records** were found to be inconsistent and were removed to ensure the referential integrity of the final database.
-   **Load:** Populates a MySQL database with the clean, transformed data. The loading process is optimized for large volumes using data chunking.

The project concludes by running several verification queries to demonstrate the value and functionality of the populated database.

## 2. Technology Stack

-   **Language:** Python 3
-   **Core Libraries:** Pandas, SQLAlchemy, Matplotlib, Seaborn
-   **Database:** MySQL 8.0
-   **DB Driver:** mysql-connector-python

## 3. How to Run the Pipeline

Follow these steps to set up the environment and execute the entire ETL process.

### Step 3.1: Prerequisites

-   A working installation of Python (3.8 or newer).
-   A running MySQL Server instance and a client like MySQL Workbench.
-   The source CSV files, which should be placed in the `data/` folder.

**Note on Data Files:** The CSV files for this project are large and are not included in this repository. Please download them from the official Kaggle source and place them in a `data` subfolder:
[Kaggle Dataset Link](https://www.kaggle.com/code/fabiendaniel/predicting-flight-delays-tutorial/input)

### Step 3.2: Environment Setup

1.  **Clone the Repository (Optional):**
    ```bash
    git clone [URL_DEL_TUO_REPO]
    cd [NOME_CARTELLA_REPO]
    ```

2.  **Install Python Dependencies:**
    It is recommended to use a virtual environment. Install all required libraries using the `requirements.txt` file:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Database Setup:**
    -   Using MySQL Workbench or another client, connect to your MySQL server.
    -   Create a new, empty schema (database). The default name expected by the script is `flight_delays_db`.
    -   Execute the entire `create_tables.sql` script provided in this repository. This will create the necessary table structure.

### Step 3.3: Configuration

Before running the pipeline, you must configure the database connection:
1.  Open the `pipeline.py` file in a text editor.
2.  Locate the `CONFIGURATION PARAMETERS` section at the top of the file.
3.  Change the value of the `DB_PASSWORD` variable to match your MySQL root password.
    ```python
    # Example
    DB_PASSWORD = "YOUR_PASSWORD_HERE"
    ```

### Step 3.4: Execution

With the environment set up and configured, run the pipeline from your terminal in the project's root directory:
```bash
python pipeline.py
```
The script will print its progress to the console. The loading process for the fact table is intensive and may take several minutes to complete. Upon successful completion, it will display several plots showing the results of the verification queries.

## 4. Project Structure

```
.
├── pipeline.py                # The main ETL script
├── create_tables.sql          # SQL script to create the database schema
├── requirements.txt           # Python library dependencies
├── audit_data_integrity.py    # Standalone script to verify source data issues
├── README.md                  # This file
└── data/
    ├── flights.csv            # (Must be downloaded from Kaggle)
    ├── airports.csv           # (Must be downloaded from Kaggle)
    └── airlines.csv           # (Must be downloaded from Kaggle)
```

## 5. Data Integrity Audit

This project includes a separate script, `audit_data_integrity.py`, to independently prove the referential integrity issues found in the source data. Running this script will confirm that **486,165 flight records** reference non-existent airport codes, justifying the data cleaning step implemented in the main pipeline. To run it, use:
```bash
python audit_data_integrity.py
```
```

