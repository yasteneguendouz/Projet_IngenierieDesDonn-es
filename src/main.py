from data_agregation import (
    create_agregate_tables,
    agregate_dim_city,
    agregate_dim_station,
    agregate_fact_station_statements
)
from data_consolidation import (
    create_consolidate_tables,
    consolidate_city_data,
    consolidate_station_data,
    consolidate_station_statement_data
)
from data_ingestion import (
    get_paris_realtime_bicycle_data,
    get_nantes_realtime_bicycle_data,
    get_toulouse_realtime_bicycle_data,
    get_communes_data
)

def main():
    print("Process start.")

    # Data ingestion
    print("Data ingestion started.")
    get_paris_realtime_bicycle_data()
    get_nantes_realtime_bicycle_data()
    get_toulouse_realtime_bicycle_data()
    get_communes_data()
    print("Data ingestion ended.")

    # Data consolidation
    print("Data consolidation started.")
    create_consolidate_tables()
    consolidate_city_data()
    consolidate_station_data()
    consolidate_station_statement_data()
    print("Data consolidation ended.")

    # Data aggregation
    print("Data aggregation started.")
    create_agregate_tables()
    agregate_dim_city()
    agregate_dim_station()
    agregate_fact_station_statements()
    print("Data aggregation ended.")

    print("Process completed successfully.")

if __name__ == "__main__":
    main()
