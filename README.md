# Open Library Subject Analysis

## Overview

This project analyzes the Open Library dataset, comprising 89,677,650 bibliographic records, to uncover global publishing trends and subject distributions from the 1900s to the 2020s. Using Apache Spark and Apache Parquet, the pipeline processes a large-scale, semi-structured dataset to reveal temporal and geographic trends, subject co-occurrences, and thematic evolution. Key findings include a publishing peak in the 2000s (26.8M editions), dominance by the USA and UK (27M and 7M editions), an average of 5.1 subjects per publication, and universal subjects like history across 249 countries.

The pipeline is split across three Jupyter notebooks:
1. **`openlib_partitions.ipynb`**: Reads the raw Open Library text file and partitions it into initial DataFrames (editions, works, authors).
2. **`openlib_working_df_partitions.ipynb`**: Preprocesses the editions DataFrame to create two working DataFrames (`df_eds`, `df_exp`) for analysis.
3. **`openlib_final.ipynb`**: Performs analyses on publication trends, subject diversity, and co-occurrences.

The project addresses questions such as:
- How have publication volumes evolved, and which countries drive global publishing?
- How do subjects co-occur, and how do they vary by region and decade?

## Features

- **Data Ingestion**: Processes a 41.89 GB compressed text file into structured DataFrames.
- **Scalable Pipeline**: Leverages PySpark for transformations, Parquet for storage, and partitioning by decade/country for efficiency.
- **Analyses**:
  - Publication trends by decade and country.
  - Subject diversity and co-occurrence patterns.
  - Temporal emergence of new subjects.
  - Geographic subject distribution.
- **Outputs**: Parquet files and statistical summaries for further exploration.

## Usage

1. **Run the Pipeline**:
   - Open each notebook in Jupyter Notebook or JupyterLab:
     ```bash
     jupyter notebook
     ```
   - Execute notebooks in order:
     - **1. `openlib_partitions.ipynb`**: Reads `ol_cdump_latest.txt.gz`, parses JSON, and saves partitioned Parquet files for editions, works, and authors at `/partitioned_data/eds_partitioned/`.
     - **2. `openlib_working_df_partitions.ipynb`**: Loads the editions Parquet, preprocesses data (year extraction, country standardization, subject cleaning), and creates `df_eds` (decade, country, subject_clean) and `df_exp` (decade, country, subject) at `/partitioned_data/df_eds_partitioned/` and `/partitioned_data/df_exp_partitioned/`.
     - **3. `openlib_final.ipynb`**: Loads `df_eds` and `df_exp`, performs analyses (e.g., publication counts, top subjects, subject pairings), and prints results.

2. **Key Outputs**:
   - **Parquet Files**:
     - `/partitioned_data/eds_partitioned/` (initial editions, 12.96 GB).
     - `/partitioned_data/df_eds_partitioned/` (`df_eds`: decade, country, subject_clean).
     - `/partitioned_data/df_exp_partitioned/` (`df_exp`: decade, country, subject).
   - **Analyses**: Printed summaries in `openlib_final.ipynb` (e.g., top countries, subject co-occurrences).
   - **Visualizations**: (Commented out) Pandas-based plots for trends and distributions.

3. **Customize Analyses**:
   - Modify Spark queries in `openlib_final.ipynb` for custom metrics (e.g., specific subjects, regions).
   - Uncomment visualization code and adjust plotting parameters for graphical outputs.