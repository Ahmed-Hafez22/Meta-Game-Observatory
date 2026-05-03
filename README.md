# 🎮 Meta Game Observatory

![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue?logo=postgresql)
![ETL Pipeline](https://img.shields.io/badge/ETL-Pipeline-orange) ![Data
Engineering](https://img.shields.io/badge/Data-Engineering-green)
![Status](https://img.shields.io/badge/Status-Completed-brightgreen)
![License](https://img.shields.io/badge/License-Educational-lightgrey)

------------------------------------------------------------------------

## 📌 Overview

**Meta Game Observatory** is a full-stack data engineering project
designed to collect, process, and visualize video game industry data. It
integrates a robust data pipeline with an interactive analytics
dashboard, enabling users to explore both real-time and historical
insights from major gaming platforms.

------------------------------------------------------------------------

## ⚙️ Architecture

### Data Sources

-   Steam API
-   IGDB API

### ETL Pipeline

-   Extract → Transform → Load pipeline implemented in Python\
-   Handles real-time and historical data

### Data Warehouse

-   Built using **PostgreSQL**
-   Designed with a **Snowflake Schema**:
    -   Normalized dimension tables
    -   Fact tables for game metrics

### Analytics Dashboard

-   Desktop GUI using PySide6
-   Interactive visualizations
-   Role-Based Access Control (RBAC)

------------------------------------------------------------------------

## 🚀 Features

-   Real-time and historical analytics
-   Automated ETL pipeline
-   Snowflake schema design
-   Desktop dashboard
-   Secure access control

------------------------------------------------------------------------

## 🛠️ Tech Stack

-   **Python**
-   **PostgreSQL**
-   **PySide6 (GUI)**
-   **APScheduler**
-   **NumPy**
-   **Requests**

------------------------------------------------------------------------

## 📂 Project Structure

``` text
project/
│── connect.py
│── extract.py
│── transform.py
│── load.py
│── pipeline.py
│── main.py
│── GUI.py
│── schema.sql
```

------------------------------------------------------------------------

## ▶️ Getting Started

``` bash
git clone https://github.com/your-username/meta-game-observatory.git
cd meta-game-observatory
```

Run pipeline:

``` bash
python main.py
```

------------------------------------------------------------------------

## 👨‍🎓 Academic Credits

This project was developed and submitted as part of the **Database
Management Systems (DBMS)** course at **Innovation University**.

-   **Professor:** Dr. Marwa Mostafa
-   **Student:** Ahmed Hafez

------------------------------------------------------------------------

## 📄 License

This project is for educational purposes only.
