# Hotel, Gift Shop, and Payroll Data Warehousing Project - Ongoing

## Project Overview
This project builds a full data pipeline and analytics solution using hotel operations data, gift shop transaction data, and employee/payroll-related data. The project includes data warehouse design, ETL development, incremental loading, data governance tracking, and a final dashboard and presentation.

The work is organized across multiple sprints, with the final sprint focused on the dashboard, open dataset, data product, and presentation.

## Project Goals
The main goals of this project are to:

- design and implement three data warehouses
- build optimized ETL pipelines for loading and refreshing the warehouses
- support business questions related to customer spending, revenue by geography, and employee/payroll insights
- track ETL runs and validation checks using a Data Governance Database (DGDB)
- create a final dashboard and presentation for decision making

## Data Warehouses
This project includes three main warehouses:

### 1. CustomerSpendDW
This warehouse supports customer-focused business questions, including hotel spending and gift shop spending behavior.

### 2. RevenueGeographyDW
This warehouse supports revenue analysis by geography, including hotel revenue and gift shop revenue.

### 3. EmployeePayrollDW
This warehouse supports staffing, revenue, and employee-related analysis.

## ETL Pipeline
The ETL pipeline is written in Python and uses:

- `pyodbc` for SQL Server connections
- `pymongo` for MongoDB connections
- bulk inserts and batching for performance
- incremental loading logic to avoid duplicate reloads
- DGDB ORM logging for ETL runs and validation results

## Incremental Loading
The ETL pipeline is incremental. It does not reload everything each time.

- For dimension tables, it checks whether a value already exists before inserting it.
- For fact tables, it uses a unique combination of important fields to detect whether a fact row already exists.
- This allows the ETL to continue safely even if an earlier run partially loaded data.

## Data Governance Database (DGDB)
The DGDB tracks ETL activity and validation results.

It includes:

- `ETL_Runs`
- `Validation_Results`

The DGDB is managed using an ORM.

## Final Sprint Deliverables
Sprint 6 includes:

- dashboard file
- dashboard documentation
- gift shop open dataset
- data card
- ETL code for open dataset
- PowerPoint presentation
- individual retrospectives
- sprint documentation

## Repository Structure
- `etl/` contains ETL source code
- `sql/` contains warehouse DDL and SQL scripts
- `dashboard/` contains the dashboard file and documentation
- `presentation/` contains the final presentation
- `docs/` contains sprint and project documentation
- `data_card/` contains the open dataset README

## Technologies Used
- Python
- SQL Server
- MongoDB
- Power BI
- SQLAlchemy ORM
- GitHub

## Notes
Sensitive credentials are not included in this repository. Use a local `.env` file for database connections.
