# OpenLibrary Data Analysis (1900–2025)

This repository contains Python scripts for analyzing OpenLibrary data, focusing on works, authors, and editions published between 1900 and 2025. The analysis involves partitioning large datasets for efficient processing and performing a comprehensive 10-step Exploratory Data Analysis (EDA) using PySpark. The project generates insights into publication trends, geographic distributions, subjects, and author contributions over time.

## Project Overview

The goal is to explore OpenLibrary data to understand:
- Temporal trends in publications (by decade).
- Geographic contributions (top countries).
- Dominant subjects and their evolution.
- Key authors and their prominence across decades and countries.

The analysis is split into two main phases:
1. **Partitioning**: Saving DataFrames as partitioned Parquet files to enable parallelized processing.
2. **EDA**: A 10-step analysis plan covering timelines, top countries, subjects, authors, and their intersections.

## Dataset Description

The analysis uses three main DataFrames:
- **`df_works_final`**:
  - Columns: `work_key`, `first_publish_date`, `subjects` (array), `author_keys` (array), `decade` (e.g., "1900s", "1910s", ..., "2020s", "Unknown").
  - Source: OpenLibrary works data, filtered for 1900–2025.
  - Partitioned by `partition_decade` (~12–14 partitions).
- **`df_authors_final`**:
  - Columns: `author_key`, `name`.
  - Source: OpenLibrary authors data.
  - Saved with default partitions.
- **`df_eds_final`**:
  - Columns: `work_keys` (array), `publish_country`.
  - Source: OpenLibrary editions data, linking works to publication countries.
  - Assumed partitioned (e.g., by hashed `work_key` or default).

The DataFrames are stored as Parquet files in the `output/` directory after partitioning.

## Analysis Steps

### 1. Partitioning
The partitioning step prepares the DataFrames for efficient parallel processing:
- **Notebook**: `openlib_partitions.ipynb`
- **Process**:
  - **df_works_final**: Partitioned by `decade` (derived from `first_publish_date`, e.g., "1900", "1910", ..., "Unknown"). Saved to `partitioned_data/works_partitioned/`.
  - **df_authors_final**: Saved with default partitions to `partitioned_data/authors_partitioned/`.
  - **df_eds_final**: Saved with default partitions to `partitioned_data/eds_partitioned/`.
- **Output**:
  - Parquet files in `partitioned_data/works_partitioned/` (e.g., `partition_decade=1900/`).
  - Parquet files in `partitioned_data/authors_partitioned/` (e.g., `part-00000.parquet`).
  - Console output with partition counts for verification.

### 2. Exploratory Data Analysis (EDA)
The EDA steps:
- **Notebook**: `openlib_eda.ipynb` (for uploading)
- **Steps**:
- Timeline of Editions (1900–2025, by Decade):
  - Approach: Count editions per decade using publish_date.
  - Fields: publish_date, derived decade.
  - Output: Bar chart.
  - Data: Editions only.
- Top 100 Countries by Edition Count:
  - Approach: Group by publish_country, count editions.
  - Fields: publish_country (map to MARC 21 country codes)
  - Output: Table or bar chart (top 20 visualized for brevity).
  - Data: Editions only.
- Top 50 Global Subjects (Editions):
  - Approach: Use subjects from editions.
  - Fields: subjects.
  - Output: Bar chart of top 50 subjects.
  - Data: Editions only.
- Top 50 Authors Globally (Editions):
  - Approach: Use author_keys from editions, join with df_authors_final to get author_name.
  - Fields: author_keys, author_name (via join).
  - Output: Bar chart of top 50 authors by edition count.
  - Data: Editions + authors join.
- Top 20 Subjects per Decade (Editions):
  - Approach: Use edition subjects grouped by decade.
  - Fields: subjects, decade.
  - Output: Bar charts for the last 5 decades.
  - Data: Editions only.
- Top 20 Subjects per Country per Decade (Editions):
  - Approach: Use edition subjects, publish_country, decade.
  - Fields: subjects, publish_country, decade.
  - Output: Bar charts for top 5 countries, last 5 decades.
  - Data: Editions only.
- Top 10 Authors per Decade (Editions):
  - Approach: Use author_keys by decade, join with df_authors_final for author_name.
  - Fields: author_keys, decade, author_name (via join).
  - Output: Table or bar chart.
  - Data: Editions + authors join.
- Top 10 Authors per Country per Decade (Editions):
  - Approach: Use author_keys, publish_country, decade, join with df_authors_final for author_name.
  - Fields: author_keys, publish_country, decade, author_name (via join).
  - Output: Table or bar chart.
  - Data: Editions + authors join.
- Top 10 Countries for Top 10 Global Subjects (Editions):
  - Approach: Use edition subjects, publish_country.
  - Fields: subjects, publish_country.
  - Output: Heatmap?
  - Data: Editions only.
- Top 10 Countries for Top Subject per Decade (Editions):
  - Approach: Use edition subjects, publish_country, decade.
  - Fields: subjects, publish_country, decade.
  - Output: Table or bar chart.
  - Data: Editions only.