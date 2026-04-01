[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)
# ETL Pipeline — Amman Digital Market

## Overview

This project implements a complete ETL (Extract, Transform, Load) Pipeline** designed to process retail data from a PostgreSQL database. The pipeline extracts raw sales data, cleans and transforms it into actionable customer analytics, and loads the final results back into the database and a CSV file for reporting.

## Setup

Follow these steps to set up the environment and the database:

1. Start PostgreSQL container:
   ```bash
   docker run -d --name postgres-m3-int \
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=amman_market \
     -p 5432:5432 -v pgdata_m3_int:/var/lib/postgresql/data \
     postgres:15-alpine
   ```
2. Load schema and data:
   ```bash
   psql -h localhost -U postgres -d amman_market -f schema.sql
   psql -h localhost -U postgres -d amman_market -f seed_data.sql
   ```
3. Install dependencies: `pip install -r requirements.txt`

## How to Run

```bash
Ensure you have the required libraries installed (pandas, sqlalchemy, psycopg2-binary), then run the pipeline:

python etl_pipeline.py
```

## Output

customer_id,customer_name,total_orders,total_revenue,avg_order_value
1,Ahmad Al-Masri,9,1003.5,111.5
2,Fatima Al-Husseini,8,827.0,103.375
3,Omar Khalifeh,6,751.5,125.25
4,Layla Abdallah,6,730.0,121.66666666666667
5,Khaled Nasser,8,930.5,116.3125
6,Rania Al-Khatib,6,666.5,111.08333333333333
7,Yousef Haddad,6,721.5,120.25
8,Noor Al-Jabari,6,801.0,133.5
9,Tariq Obeidat,7,806.5,115.21428571428571
10,Hana Suleiman,6,750.0,125.0
11,Sami Al-Rawashdeh,6,523.0,87.16666666666667
12,Dina Qasem,5,363.5,72.7
13,Mazen Tawalbeh,3,499.0,166.33333333333334
14,Rana Abu-Ghazaleh,3,371.0,123.66666666666667
15,Fadi Al-Zoubi,2,228.0,114.0
16,Mona Batayneh,5,526.5,105.3
17,Waleed Shraideh,5,403.0,80.6
18,Lina Al-Omari,6,447.5,74.58333333333333
19,Ibrahim Jaradat,6,803.0,133.83333333333334
20,Samar Ababneh,7,1100.0,157.14285714285714
21,Hassan Smadi,4,530.5,132.625
22,Yasmin Al-Sharif,7,974.5,139.21428571428572
23,Basil Malkawi,7,685.5,97.92857142857143
24,Reem Gharaibeh,7,660.5,94.35714285714286
25,Nidal Fraihat,6,544.0,90.66666666666667
26,Asma Qudah,5,751.0,150.2
27,Zaid Al-Momani,7,932.0,133.14285714285714
28,Jumana Tarawneh,6,719.5,119.91666666666667
29,Rashid Al-Ajlouni,6,569.0,94.83333333333333
30,Nisreen Bakri,6,511.0,85.16666666666667
31,Ayman Daradkeh,6,745.0,124.16666666666667
32,Ghada Nsour,6,910.0,151.66666666666666
33,Mahmoud Al-Azza,8,908.0,113.5
34,Suha Masarweh,7,569.5,81.35714285714286
35,Raed Habashneh,5,436.0,87.2
36,Abeer Al-Tamimi,4,423.0,105.75
37,Muhannad Awamleh,2,127.0,63.5
38,Sawsan Majali,4,579.5,144.875
39,Tamer Zu'bi,8,873.0,109.125
40,Huda Al-Karaki,6,676.0,112.66666666666667
41,Osama Louzi,7,642.5,91.78571428571429
42,Rula Shobaki,7,715.0,102.14285714285714
43,Bilal Faouri,7,1003.5,143.35714285714286
44,Maysoon Al-Qadi,7,603.0,86.14285714285714
45,Wael Bani-Hani,7,750.5,107.21428571428571
46,Shireen Khasawneh,6,671.0,111.83333333333333
47,Adnan Rousan,6,514.0,85.66666666666667
48,Lubna Al-Hawari,6,630.5,105.08333333333333
49,Firas Dmour,6,699.0,116.5
50,Eman Shdeifat,7,789.5,112.78571428571429
51,Suhail Qtaishat,7,609.0,87.0
52,Dalal Abu-Rumman,4,629.0,157.25
53,Hazem Maayta,6,713.0,118.83333333333333
54,Najwa Al-Bdour,6,713.0,118.83333333333333
55,Amjad Halasa,5,490.0,98.0
56,Siham Majdalawi,3,145.5,48.5
57,Rafiq Ghawanmeh,5,499.5,99.9
58,Iman Al-Masalmeh,4,525.5,131.375
59,Kamal Sarhan,4,372.0,93.0
60,Tahani Toubasi,6,600.5,100.08333333333333
61,Nasr Rabadi,6,662.5,110.41666666666667
62,Wafa Al-Harahsheh,5,524.0,104.8
63,Jalal Theibat,5,763.0,152.6
64,Kholoud Bani-Ata,3,202.0,67.33333333333333
65,Nayef Zu'mot,5,501.0,100.2
66,Ahlam Bataineh,3,314.0,104.66666666666667
67,Ragheb Al-Dabbas,3,308.0,102.66666666666667
68,Fidaa Obaidat,3,408.0,136.0
69,Munther Bsoul,3,323.5,107.83333333333333
70,Haneen Khreisat,2,169.0,84.5
71,Saleem Tahat,1,73.0,73.0
72,Rawia Al-Fugaha,2,258.0,129.0
73,Imad Dmaiseh,2,140.5,70.25
74,Zeinab Shnikat,2,131.5,65.75
75,Amer Saraireh,3,279.0,93.0
76,Buthayna Quraan,1,107.0,107.0
77,Hamzeh Al-Btoush,2,192.0,96.0
78,Mayada Tal,2,226.0,113.0
79,Shaker Hijazeen,2,159.0,79.5
80,Lamis Rababah,2,316.0,158.0
81,Bashar Al-Adwan,3,295.5,98.5
82,Tamam Khreisha,2,369.0,184.5
83,Ismail Hmoud,2,198.0,99.0
84,Sabah Nsairat,1,64.0,64.0
85,Qais Al-Fayez,2,130.0,65.0
86,Mais Qawasmeh,2,122.5,61.25
87,Baraa Otoum,1,51.0,51.0
88,Thuraya Al-Madi,2,254.0,127.0
89,Haitham Krishan,2,204.0,102.0
90,Nawal Fayyad,2,260.0,130.0
91,Mutaz Al-Shawabkeh,1,165.0,165.0
92,Rim Bani-Khaled,2,264.0,132.0
93,Saif Majeed,2,140.5,70.25
94,Arwa Hajjaj,2,160.0,80.0
95,Nasser Al-Dhoon,2,132.5,66.25
96,Duha Atiyat,2,444.0,222.0
97,Samer Jaber,2,191.0,95.5
98,Laith Al-Khalidi,2,195.0,97.5
99,Farah Hamaydeh,2,231.0,115.5
100,Yazan Bani-Mustafa,1,81.0,81.0

## Quality Checks

To ensure the reliability of the generated analytics, the pipeline performs the following automated data quality validations:

* Null Value Detection: Verifies that `customer_id` and `customer_name` are fully populated. This prevents "anonymous" records from appearing in the final report.
* Positive Revenue Check: Confirms that `total_revenue > 0` for all summarized customers. This ensures we are only reporting on active, revenue-generating accounts and flags potential calculation errors.
* Uniqueness Validation: Checks that each `customer_id` is unique in the final table. This guarantees that no customer's data is double-counted or split into multiple rows.
* Active Order Verification: Ensures `total_orders > 0`. This filters out accounts that might exist in the system but haven't actually completed a transaction.

Why these checks? In a real-world production environment, "dirty data" can lead to incorrect business decisions. By implementing these checks, the pipeline will stop (Raise `ValueError`) if it detects data that doesn't meet our standards, preventing bad data from reaching the final CSV or Database.

---

## Challenge Extensions


I have successfully extended this pipeline with the following professional features:

1. Incremental Data Loading: 
   - Implemented a `Metadata` tracking system in PostgreSQL.
   - The pipeline now remembers the last successful run and only extracts new orders, significantly improving performance.
   
2. Automated Quality Reporting:
   - Each run generates a structured `quality_report.json` in the `output/` directory.
   - This report includes timestamps, data validation results, and outlier detection.

3. Statistical Outlier Detection:
   - Added logic to flag customers whose revenue exceeds 3 standard deviations from the mean.
   - Outliers are marked in the final CSV and listed in the JSON report for auditing.

## License

This repository is provided for educational use only. See [LICENSE](LICENSE) for terms.

You may clone and modify this repository for personal learning and practice, and reference code you wrote here in your professional portfolio. Redistribution outside this course is not permitted.
