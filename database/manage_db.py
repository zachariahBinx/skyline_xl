import sqlite3
import os, sys
import ctypes
from sqlalchemy import create_engine, Table, MetaData
import pandas


class DB:
    def __init__(self, db_file):

        # Check if db already exists
        if os.path.exists(db_file):
            ctypes.windll.user32.MessageBoxW(0,
                                             f"Database path already exists",
                                             "Database Error",
                                             0
                                             )
            sys.exit(1)

        # Set up connections to database
        self.db_file = db_file
        self.connection = sqlite3.connect(self.db_file)
        self.cursor = self.connection.cursor()
        self.engine = create_engine(f"sqlite:///{db_file}")

        self.create_english_yarders()
        self.create_metric_yarders()
        self.create_english_carriages()
        self.create_metric_carriages()
        self.create_english_wire_rope()
        self.create_metric_wire_rope()


    def create_english_yarders(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS english_yarders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(64) NOT NULL,
                tower_height_ft FLOAT NOT NULL,
                skyline_diameter_in FLOAT NOT NULL,
                skyline_len_ft FLOAT NOT NULL,
                skyline_type VARCHAR(6),
                skyline_weight_lbs_per_ft FLOAT NOT NULL,
                skyline_tension_lbs FLOAT NOT NULL,
                mainline_diameter_in FLOAT NOT NULL,
                mainline_len_ft FLOAT NOT NULL,
                mainline_type VARCHAR(6),
                mainline_weight_lbs_per_ft FLOAT NOT NULL,
                mainline_tension_lbs FLOAT NOT NULL,
                hallback_diameter_in FLOAT NOT NULL,
                hallback_len_ft FLOAT NOT NULL,
                hallback_type VARCHAR(6),
                hallback_weight_lbs_per_ft FLOAT NOT NULL,
                hallback_tension_lbs FLOAT NOT NULL,
                slack_diameter_in FLOAT NOT NULL,
                slack_len_ft FLOAT NOT NULL,
                slack_type VARCHAR(6),
                slack_weight_lbs_per_ft FLOAT NOT NULL,
                slack_tension_lbs FLOAT NOT NULL,
                horsepower FLOAT NOT NULL,
                mainline_drum_width_in FLOAT,
                mainline_drum_diameter_in FLOAT,
                mainline_break_torque_ft_lbs FLOAT,
                hallback_drum_width_in FLOAT,
                hallback_drum_diameter_in FLOAT,
                hallback_break_torque_ft_lbs FLOAT,
                size VARCHAR(2) NOT NULL
            )
        ''')
        self.connection.commit()


    def create_metric_yarders(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS metric_yarders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(64) NOT NULL,
                tower_height_m FLOAT NOT NULL,
                skyline_diameter_mm FLOAT NOT NULL,
                skyline_len_m FLOAT NOT NULL,
                skyline_type VARCHAR(6),
                skyline_weight_kg_per_m FLOAT NOT NULL,
                skyline_tension_kg FLOAT NOT NULL,
                mainline_diameter_mm FLOAT NOT NULL,
                mainline_len_m FLOAT NOT NULL,
                mainline_type VARCHAR(6),
                mainline_weight_kg_per_m FLOAT NOT NULL,
                mainline_tension_kg FLOAT NOT NULL,
                hallback_diameter_mm FLOAT NOT NULL,
                hallback_len_m FLOAT NOT NULL,
                hallback_type VARCHAR(6),
                hallback_weight_kg_per_m FLOAT NOT NULL,
                hallback_tension_kg FLOAT NOT NULL,
                slack_diameter_mm FLOAT NOT NULL,
                slack_len_m FLOAT NOT NULL,
                slack_type VARCHAR(6),
                slack_weight_kg_per_m FLOAT NOT NULL,
                slack_tension_kg FLOAT NOT NULL,
                horsepower FLOAT NOT NULL,
                mainline_drum_width_mm FLOAT,
                mainline_drum_diameter_mm FLOAT,
                mainline_break_torque_m_kg FLOAT,
                hallback_drum_width_mm FLOAT,
                hallback_drum_diameter_mm FLOAT,
                hallback_break_torque_m_kg FLOAT,
                size VARCHAR(2) NOT NULL
            )
        ''')
        self.connection.commit()


    def create_english_carriages(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS english_carriages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(64) NOT NULL,
                weight_lbs FLOAT NOT NULL,
                horsepower FLOAT NOT NULL,
                skyline_clamp BOOL NOT NULL,
                slackpulling_method VARCHAR(8),
                intermediate_jacks BOOL NOT NULL,
                num_drums INT NOT NULL,
                skyline_diameter_min_in FLOAT NOT NULL,
                skyline_diameter_max_in FLOAT NOT NULL,
                mainline_diameter_min_in FLOAT NOT NULL,
                mainline_diameter_max_in FLOAT NOT NULL,
                torque FLOAT,
                dropline_len_ft FLOAT,
                dropline_diameter_in FLOAT   
            )
        ''')
        self.connection.commit()


    def create_metric_carriages(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS metric_carriages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(64) NOT NULL,
                weight_kg FLOAT NOT NULL,
                horsepower FLOAT NOT NULL,
                skyline_clamp BOOL NOT NULL,
                slackpulling_method VARCHAR(8),
                intermediate_jacks BOOL NOT NULL,
                num_drums INT NOT NULL,
                skyline_diameter_min_mm FLOAT NOT NULL,
                skyline_diameter_max_mm FLOAT NOT NULL,
                mainline_diameter_min_mm FLOAT NOT NULL,
                mainline_diameter_max_mm FLOAT NOT NULL,
                torque FLOAT,
                dropline_len_m FLOAT,
                dropline_diameter_mm FLOAT
            )
        ''')
        self.connection.commit()


    # TODO does "fraction" need to be converted to string???? ex. 1/4 = 0.25???
    def create_english_wire_rope(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS english_wire_rope (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bs_eips FLOAT NOT NULL,
                bs_swaged FLOAT NOT NULL,
                fraction FLOAT NOT NULL,
                diameter FLOAT NOT NULL,
                swaged_swl_fac3 FLOAT NOT NULL,
                eips_swl_fac3 FLOAT NOT NULL,
                swaged_wt FLOAT NOT NULL,
                eips_wt FLOAT NOT NULL
            )
        ''')
        self.connection.commit()


    def create_metric_wire_rope(self):
        self.cursor.execute('''
               CREATE TABLE IF NOT EXISTS metric_wire_rope (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   bs_eips FLOAT NOT NULL,
                   bs_swaged FLOAT NOT NULL,
                   fraction FLOAT NOT NULL,
                   diameter FLOAT NOT NULL,
                   swaged_swl_fac3 FLOAT NOT NULL,
                   eips_swl_fac3 FLOAT NOT NULL,
                   swaged_wt FLOAT NOT NULL,
                   eips_wt FLOAT NOT NULL
               )
           ''')
        self.connection.commit()


    def insert_default_english_yarder_data(self, yarder_path: str = "./default_english_yarders.parquet"):
        # Load data
        df = pandas.read_parquet(yarder_path)

        df.to_sql(
            name="english_yarders",
            con=self.engine,
            if_exists="replace",  # Options: 'fail', 'replace', 'append'
            index=False,  # Set to True if the DataFrame index should be included
        )


    def insert_default_metric_yarder_data(self, yarder_path: str = "./default_metric_yarders.parquet"):
        # Load data
        df = pandas.read_parquet(yarder_path)

        df.to_sql(
            name="metric_yarders",
            con=self.engine,
            if_exists="replace",  # Options: 'fail', 'replace', 'append'
            index=False,  # Set to True if the DataFrame index should be included
        )


    def insert_default_english_carriage_data(self, carriage_path: str = "./default_metric_carriages.parquet"):
        # Load data
        df = pandas.read_parquet(carriage_path)

        df.to_sql(
            name="english_carriages",
            con=self.engine,
            if_exists="replace",  # Options: 'fail', 'replace', 'append'
            index=False,  # Set to True if the DataFrame index should be included
        )


    def insert_default_metric_carriage_data(self, carriage_path: str = "./default_metric_carriages.parquet"):
        # Load data
        df = pandas.read_parquet(carriage_path)

        df.to_sql(
            name="metric_carriages",
            con=self.engine,
            if_exists="replace",  # Options: 'fail', 'replace', 'append'
            index=False,  # Set to True if the DataFrame index should be included
        )


    def insert_new_english_yarder(self):
        pass


    def insert_new_metric_yarder(self):
        pass


    def insert_new_english_carriage(self):
        pass


    def insert_new_metric_carriage(self):
        pass


    def clear_all_data(self):
        self.cursor.execute('''
            SELECT name FROM sqlite_master WHERE type='table'
        ''')
        data = self.cursor.fetchall()
        tables = [table[0] for table in data]
        for t in tables:
            self.cursor.execute(f'''
                DROP TABLE IF EXISTS {t}
            ''')
        self.connection.commit()

        self.create_english_yarders()
        self.create_metric_yarders()
        self.create_english_carriages()
        self.create_metric_carriages()
        self.create_english_wire_rope()
        self.create_metric_wire_rope()



def main():
    db = DB("./test_outputs/test.db")
    db.insert_default_english_yarder_data()
    db.insert_default_metric_yarder_data()
    db.insert_default_english_carriage_data()
    db.insert_default_metric_carriage_data()

if __name__ == "__main__":
    main()
