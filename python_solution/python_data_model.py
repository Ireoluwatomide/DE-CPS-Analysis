# Path: python_solution/python_data_model.py

# Import the required libraries
from .python_logger import logs
from psycopg2 import sql
from .python_db_connect import Database


class DataModel:
    
    def __init__(self):
        
        self.db = Database()
        self.db.connect()
        self.logger = logs(__name__)
        
        # Schema name
        self.cps_schema_name = 'basic_monthly_cps'

        # Table names
        self.dec_monthly_cps = 'fct_dec_monthly_cps'
        self.final_outcome_table = 'dim_final_outcome'
        self.housing_unit_table = 'dim_housing_unit_type'
        self.household_type_table = 'dim_household_type'
        self.telephone_in_house_table = 'dim_telephone_in_house'
        self.access_to_telephone_elsewhere_table = 'dim_access_to_telephone_elsewhere'
        self.telephone_interview_accepted_table = 'dim_telephone_interview_accepted'
        self.type_of_interview_table = 'dim_type_of_interview'
        self.family_income_table = 'dim_family_income'
        self.geographic_division_table = 'dim_geographic_division'
        self.race_table = 'dim_race'
    
    def create_schema(self):
        
        create_schema_query = sql.SQL('''
            CREATE SCHEMA IF NOT EXISTS {};
        ''').format(sql.Identifier(self.cps_schema_name))

        self.db.execute(create_schema_query)
        self.db.commit()
        self.logger.info(f'Schema {self.cps_schema_name} created successfully')

    def create_table(self, table_name, columns_query):
        
        create_table_query = sql.SQL('''
            CREATE TABLE IF NOT EXISTS {}.{} (
                {}
            );
        ''').format(
            sql.Identifier(self.cps_schema_name),
            sql.Identifier(table_name),
            columns_query
        )

        self.db.execute(create_table_query)
        self.db.commit()
        self.logger.info(f'Table {table_name} created successfully')

    def create_dec_monthly_cps_table(self):
        
        columns_query = sql.SQL('''
            id                              text not null
                                            constraint fct_dec_monthly_cps_pk
                                            primary key,
            household_identifier            text not null,
            interview_time                  date not null,
            final_outcome                   integer not null,
            housing_unit                    integer null,
            household_type                  integer null,
            telephone_in_house              integer null,
            access_to_telephone_elsewhere   integer null,
            telephone_interview_accepted    integer null,
            type_of_interview               integer null,
            family_income_range             integer null,
            geographic_division             integer null,
            race                            integer null
        ''')

        self.create_table(self.dec_monthly_cps, columns_query)

    def create_final_outcome_table(self):
        
        columns_query = sql.SQL('''
            code        integer not null 
                        constraint final_outcome_pk 
                        primary key,
            description text  not null
        ''')

        self.create_table(self.final_outcome_table, columns_query)

    def create_housing_unit_table(self):
        
        columns_query = sql.SQL('''
            id          integer not null 
                        constraint housing_unit_type_pk 
                        primary key,
            description text not null
        ''')

        self.create_table(self.housing_unit_table, columns_query)

    def create_household_type_table(self):
        
        columns_query = sql.SQL('''
            id          integer not null 
                        constraint household_type_pk 
                        primary key,
            description text not null
        ''')

        self.create_table(self.household_type_table, columns_query)

    def create_telephone_in_house_table(self):
        
        columns_query = sql.SQL('''
            id                  integer not null 
                                constraint telephone_in_house_pk 
                                primary key,
            telephone_in_house  text not null
        ''')

        self.create_table(self.telephone_in_house_table, columns_query)

    def create_access_to_telephone_elsewhere_table(self):
        
        columns_query = sql.SQL('''
            id                                      integer not null 
                                                    constraint access_to_telephone_elsewhere_pk 
                                                    primary key,
            access_to_telephone_elsewhere    text not null
        ''')

        self.create_table(self.access_to_telephone_elsewhere_table, columns_query)

    def create_telephone_interview_accepted_table(self):
        
        columns_query = sql.SQL('''
            id                              integer not null 
                                            constraint telephone_interview_accepted_pk 
                                            primary key,
            telephone_interview_accepted  text not null
        ''')

        self.create_table(self.telephone_interview_accepted_table, columns_query)

    def create_type_of_interview_table(self):
        
        columns_query = sql.SQL('''
            id          integer not null 
                        constraint type_of_interview_pk 
                        primary key,
            description text not null
        ''')

        self.create_table(self.type_of_interview_table, columns_query)

    def create_family_income_table(self):
        
        columns_query = sql.SQL('''
            id          integer not null 
                        constraint family_income_pk 
                        primary key,
            description text not null
        ''')

        self.create_table(self.family_income_table, columns_query)

    def create_geographic_division_table(self):
        
        columns_query = sql.SQL('''
            id          integer not null 
                        constraint geographic_division_pk 
                        primary key,
            description text not null
        ''')

        self.create_table(self.geographic_division_table, columns_query)
        
    def create_race_table(self):
        
        columns_query = sql.SQL('''
            id          integer not null 
                        constraint race_pk 
                        primary key,
            description text not null
        ''')
        
        self.create_table(self.race_table, columns_query)
        
    def create_all_tables(self):
        
        self.create_dec_monthly_cps_table()
        self.create_final_outcome_table()
        self.create_housing_unit_table()
        self.create_household_type_table()
        self.create_telephone_in_house_table()
        self.create_access_to_telephone_elsewhere_table()
        self.create_telephone_interview_accepted_table()
        self.create_type_of_interview_table()
        self.create_family_income_table()
        self.create_geographic_division_table()
        self.create_race_table()
        self.logger.info('All tables created successfully')
