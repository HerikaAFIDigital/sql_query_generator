## To run a file

python ui.py

## To check for static one table in postgres

python test_postgres.py

# To run ui

streamlit run ui.py


# To activate venv
source .venv/bin/activate

# To migrate csv to postgres
python load_csvs_to_postgres.py


# To inspect the tables
python inspect_tables.py
python inspect_tables.py onboarding_events