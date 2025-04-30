from flask import Flask, render_template, request, redirect, url_for
import sqlite3


app = Flask(__name__)

DATABASE_PATH = "data.db"


@app.route('/')
def index():
    categories = [
        "Transport", "Communication", "Energy", "Buildings",
         "Food", "Health", "Finance"
    ]
    return render_template("index.html", categories=categories)


@app.route('/category/<category>')
def category_page(category):
    return render_template('category_select.html', category=category)


CATEGORY_TABLES = {
    "Buildings": [
        "dwelling_ages_gm2015_full", "dwelling_ages_gm2021_full", "fire_gm_2024",
        "flooding2016", "flooding2018", "flooding2020",
        "gm_accomm2001", "gm_accomm2011", "gm_accomm2021",
        "hospitals_gm_2017", "schools_gm_2025"
    ],
    "Communication": ["fixed_coverage_finalv2", "fixed_performance_finalv2"],
    "Energy": [
        "co2_emmissions_2023", "co2_emmissions_2024",
        "eff_scores_2023", "eff_scores_2024",
        "epc_2024", "fuel_type_2023", "fuel_type_2024", "ps_2024"
    ],
    "Finance": [
        "company_financial_records_final", "economic_activity_status_finalv2",
        "employment_history_finalv2", "households_in_poverty_estimates_ahc_final",
        "households_in_poverty_estimates_bhc_final"
    ],
    "Food": ["priority_places_for_food_index_finalv2"],
    "Health": ["gp_practices_finalv2"],
    "Transport": [
        "car_or_van_availability_finalv2", "cycle_hubs_finalv2",
        "distance_travelled_to_work_finalv2", "gmal_finalv2",
        "metrolink_stops_and_rail_stations_finalv2",
        "traffic_signal_locations_finalv2"
    ]
}

ALL_DATASETS = [
    {"name": "car_or_van_availability_finalv2", "year": 2021, "levels": ["LSOA", "MSOA"]},
    {"name": "cycle_hubs_finalv2", "year": None, "levels": ["LSOA", "MSOA"]},
    {"name": "distance_travelled_to_work_finalv2", "year": 2021, "levels": ["LSOA", "MSOA"]},
    {"name": "gmal_finalv2", "year": None, "levels": ["LSOA", "MSOA"]},
    {"name": "metrolink_stops_and_rail_stations_finalv2", "year": None, "levels": ["LSOA", "MSOA"]},
    {"name": "traffic_signal_locations_finalv2", "year": None, "levels": ["LSOA", "MSOA"]},
    {"name": "gp_practices_finalv2", "year": 2024, "levels": ["LSOA", "MSOA"]},
    {"name": "priority_places_for_food_index_finalv2", "year": 2022, "levels": ["LSOA", "MSOA"]},
    {"name": "company_financial_records_final", "year": 2018, "levels": ["MSOA"]},
    {"name": "economic_activity_status_finalv2", "year": 2025, "levels": ["LSOA", "MSOA"]},
    {"name": "employment_history_finalv2", "year": 2025, "levels": ["LSOA", "MSOA"]},
    {"name": "households_in_poverty_estimates_ahc_final", "year": 2013, "levels": ["MSOA"]},
    {"name": "households_in_poverty_estimates_bhc_final", "year": None, "levels": ["MSOA"]},
    {"name": "ps_2024", "year": 2024, "levels": ["LSOA"]},
    {"name": "fuel_type_2024", "year": 2024, "levels": ["MSOA"]},
    {"name": "fuel_type_2023", "year": 2023, "levels": ["MSOA"]},
    {"name": "epc_2024", "year": 2024, "levels": ["MSOA"]},
    {"name": "eff_scores_2024", "year": 2024, "levels": ["MSOA"]},
    {"name": "eff_scores_2023", "year": 2023, "levels": ["MSOA"]},
    {"name": "co2_emmissions_2024", "year": 2024, "levels": ["MSOA"]},
    {"name": "co2_emmissions_2023", "year": 2023, "levels": ["MSOA"]},
    {"name": "fixed_coverage_finalv2", "year": 2023, "levels": ["LSOA", "MSOA"]},
    {"name": "fixed_performance_finalv2", "year": 2023, "levels": ["LSOA", "MSOA"]},
    {"name": "schools_gm_2025", "year": 2025, "levels": ["LSOA"]},
    {"name": "hospitals_gm_2017", "year": 2023, "levels": ["LSOA"]},
    {"name": "gm_accomm2021", "year": 2021, "levels": ["LSOA"]},
    {"name": "gm_accomm2011", "year": 2011, "levels": ["LSOA"]},
    {"name": "gm_accomm2001", "year": 2001, "levels": ["LSOA"]},
    {"name": "flooding2020", "year": 2020, "levels": ["LSOA"]},
    {"name": "flooding2018", "year": 2018, "levels": ["LSOA"]},
    {"name": "flooding2016", "year": 2016, "levels": ["LSOA"]},
    {"name": "fire_gm_2024", "year": 2024, "levels": ["LSOA"]},
    {"name": "dwelling_ages_gm2021_full", "year": 2021, "levels": ["LSOA"]},
    {"name": "dwelling_ages_gm2015_full", "year": 2015, "levels": ["LSOA"]}
]


DATASET_ALIAS = {
    "indices_of_deprivation_final": "Indices of Deprivation",
    "car_or_van_availability_final": "Car or Van Availability",
    "cycle_hubs_final": "Cycle Hubs",
    "distance_travelled_to_work_final": "Distance Travelled to Work",
    "gmal_final": "GMAL",
    "metrolink_stops_and_rail_stations_final": "Metrolink Stops and Rail Stations",
    "traffic_signal_locations_final": "Traffic Signal Locations",
    "gp_practices_finalv2": "GP Practices",
    "priority_places_for_food_index_finalv2": "Priority Places for Food Index",
    "company_financial_records_final": "Company Financial Records",
    "economic_activity_status_finalv2": "Economic Activity Status",
    "employment_history_finalv2": "Employment History",
    "households_in_poverty_estimates_ahc_final": "Households in Poverty Estimates (AHC)",
    "households_in_poverty_estimates_bhc_final": "Households in Poverty Estimates (BHC)",
    "ps_2024": "PS 2024",
    "fuel_type_2023": "Fuel Type",
    "fuel_type_2024": "Fuel Type",
    "epc_2024": "EPC",
    "eff_scores_2023": "Efficiency Scores",
    "eff_scores_2024": "Efficiency Scores",
    "co2_emmissions_2023": "CO2 Emissions",
    "co2_emmissions_2024": "CO2 Emissions",
    "fixed_coverage_finalv2": "Fixed Coverage",
    "fixed_performance_finalv2": "Fixed Performance",
    "schools_gm_2025": "Schools GM",
    "hospitals_gm_2017": "Hospitals",
    "gm_accomm2001": "GM Accommodation",
    "gm_accomm2011": "GM Accommodation",
    "gm_accomm2021": "GM Accommodation",
    "flooding2016": "Flooding",
    "flooding2018": "Flooding",
    "flooding2020": "Flooding",
    "fire_gm_2024": "Fire GM",
    "dwelling_ages_gm2015_full": "GM Dwelling Ages",
    "dwelling_ages_gm2021_full": "GM Dwelling Ages"
}

from collections import defaultdict

GROUPED_DATASETS = defaultdict(list)
for dataset in ALL_DATASETS:
    name = dataset["name"]
    year = dataset["year"]
    levels = dataset["levels"]
    alias = DATASET_ALIAS.get(name, name)

    GROUPED_DATASETS[alias].append({
        "table_name": name,
        "year": year,
        "levels": levels,
        "category": dataset.get("category") 
    })



@app.route('/third', methods=['POST'])
def third_page():
    category   = request.form['category']
    geo_level  = request.form['geo_level']


    names_in_category = CATEGORY_TABLES.get(category, [])


    matching_datasets = {}

    for alias, versions in GROUPED_DATASETS.items():
        filtered = [
            v for v in versions
            if v["table_name"] in names_in_category and geo_level in v["levels"]
        ]
        if filtered:
            matching_datasets[alias] = filtered



    return render_template(
        "third_page.html",
        category=category,
        geo_level=geo_level,
        datasets=matching_datasets

    )


@app.route('/columns_page', methods=['POST'])
def columns_page():
    selected_table = request.form.get('selected_dataset')
    geo_level = request.form.get('geo_level')
    category = request.form.get('category')

    selected_table_with_year = request.form.get(f"selected_years_{selected_table}")


    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute(f"PRAGMA table_info({selected_table_with_year})")
    columns_info = cursor.fetchall()
    column_names = [col[1] for col in columns_info]

    conn.close()

    return render_template(
        "columns_page.html",
        table_name=selected_table_with_year,
        alias=selected_table,
        geo_level=geo_level,
        category=category,
        column_names=column_names
    )



@app.route('/finalquery', methods=['POST'])
def finalquery():

    table_name       = request.form.get('table_name')             
    selected_columns = request.form.getlist('columns')            

    if not table_name or not selected_columns:
        return "please choose table and column", 400


    table_name = table_name.strip()
    selected_columns = [f'"{col.strip()}"' for col in selected_columns]


    cols_sql = ", ".join(selected_columns)
    query    = f"SELECT {cols_sql} FROM {table_name} LIMIT 100"    

    try:
        
        conn   = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
    except sqlite3.OperationalError as e:
        
        return f"error：{e}", 400

   
    clean_cols = [col.strip('"') for col in selected_columns]
    return render_template(
        'finalquery.html',
        table_name=table_name,
        columns=clean_cols,
        rows=rows
    )




if __name__ == "__main__":
    app.run(debug=True)
