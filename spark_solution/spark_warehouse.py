from .spark_logger import logs
from .spark_session import SparkContextManager

from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class DataWarehouse:

    def __init__(self):
        self.spark = SparkContextManager().get_spark_session()
        self.logger = logs(__name__)

    def create_table(self, table_name, schema_fields, data):
        schema = StructType([StructField(field[0], field[1], False) for field in schema_fields])
        df = self.spark.createDataFrame(data, schema=schema)
        df.write.mode('overwrite').saveAsTable(table_name)

    def create_dec_monthly_cps_table(self, dataframe):

        dataframe.write.mode('overwrite').saveAsTable('fct_dec_monthly_cps')
        self.logger.info('fct_dec_monthly_cps table created successfully')

    def create_final_outcome_table(self):

        schema_fields = [('code', IntegerType()), ('description', StringType())]

        # Define data
        data = [
            (1, 'FULLY COMPLETE CATI INTERVIEW'),
            (2, 'PARTIALLY COMPLETED CATI INTERVIEW'),
            (3, 'COMPLETE BUT PERSONAL VISIT REQUESTED NEXT MONTH'),
            (4, 'PARTIAL, NOT COMPLETE AT CLOSEOUT'),
            (5, 'LABOR FORCE COMPLETE, SUPPLEMENT INCOMPLETE - CATI'),
            (6, 'LF COMPLETE, SUPPLEMENT DK ITEMS INCOMPLETE AT CLOSEOUT'),
            (20, 'HH OCCUPIED ENTIRELY BY ARMED FORCES MEMBERS OR ALL UNDER 15 YEARS OF AGE'),
            (201, 'CAPI COMPLETE'),
            (202, 'CALLBACK NEEDED'),
            (203, 'SUFFICIENT PARTIAL - PRECLOSEOUT'),
            (204, 'SUFFICIENT PARTIAL - AT CLOSEOUT'),
            (205, 'LABOR FORCE COMPLETE, - SUPPL. INCOMPLETE - CAPI'),
            (213, 'LANGUAGE BARRIER'),
            (214, 'UNABLE TO LOCATE'),
            (216, 'NO ONE HOME'),
            (217, 'TEMPORARILY ABSENT'),
            (218, 'REFUSED'),
            (219, 'OTHER OCCUPIED - SPECIFY'),
            (223, 'ENTIRE HOUSEHOLD ARMED FORCES'),
            (224, 'ENTIRE HOUSEHOLD UNDER 15'),
            (225, 'TEMP. OCCUPIED W/PERSONS WITH URE'),
            (226, 'VACANT REGULAR'),
            (227, 'VACANT - STORAGE OF HHLD FURNITURE'),
            (228, 'UNFIT, TO BE DEMOLISHED'),
            (229, 'UNDER CONSTRUCTION, NOT READY'),
            (230, 'CONVERTED TO TEMP BUSINESS OR STORAGE'),
            (231, 'UNOCCUPIED TENT OR TRAILER SITE'),
            (232, 'PERMIT GRANTED - CONSTRUCTION NOT STARTED'),
            (233, 'OTHER - SPECIFY'),
            (240, 'DEMOLISHED'),
            (241, 'HOUSE OR TRAILER MOVED'),
            (242, 'OUTSIDE SEGMENT'),
            (243, 'CONVERTED TO PERM. BUSINESS OR STORAGE'),
            (244, 'MERGED'),
            (245, 'CONDEMNED'),
            (246, 'BUILT AFTER APRIL 1, 2000'),
            (247, 'UNUSED SERIAL NO./LISTING SHEET LINE'),
            (248, 'OTHER - SPECIFY'),
            (256, 'REMOVED DURING SUB-SAMPLING'),
            (257, 'UNIT ALREADY HAD A CHANCE OF SELECTION')
        ]

        self.create_table('dim_final_outcome', schema_fields, data)
        self.logger.info('dim_final_outcome table created successfully')

    def create_housing_unit_table(self):

        schema_fields = [('id', IntegerType()), ('description', StringType())]

        data = [
            (0, 'OTHER UNIT'),
            (1, 'HOUSE, APARTMENT, FLAT'),
            (2, 'HU IN NONTRANSIENT HOTEL, MOTEL, ETC.'),
            (3, 'HU PERMANENT IN TRANSIENT HOTEL, MOTEL'),
            (4, 'HU IN ROOMING HOUSE'),
            (5, 'MOBILE HOME OR TRAILER W/NO PERM. ROOM ADDED'),
            (6, 'MOBILE HOME OR TRAILER W/1 OR MORE PERM. ROOMS ADDED'),
            (7, 'HU NOT SPECIFIED ABOVE'),
            (8, 'QUARTERS NOT HU IN ROOMING OR BRDING HS'),
            (9, 'UNIT NOT PERM. IN TRANSIENT HOTL, MOTL'),
            (10, 'UNOCCUPIED TENT SITE OR TRLR SITE'),
            (11, 'STUDENT QUARTERS IN COLLEGE DORM'),
            (12, 'OTHER UNIT NOT SPECIFIED ABOVE')
        ]

        self.create_table('dim_housing_unit_type', schema_fields, data)
        self.logger.info('dim_housing_unit_type table created successfully')

    def create_household_type_table(self):

        schema_fields = [('id', IntegerType()), ('description', StringType())]

        data = [
            (0, 'NON-INTERVIEW HOUSEHOLD'),
            (1, 'HUSBAND/WIFE PRIMARY FAMILY (NEITHER AF)'),
            (2, 'HUSB/WIFE PRIM. FAMILY (EITHER/BOTH AF)'),
            (3, 'UNMARRIED CIVILIAN MALE-PRIM. FAM HHLDER'),
            (4, 'UNMARRIED CIV. FEMALE-PRIM FAM HHLDER'),
            (5, 'PRIMARY FAMILY HHLDER-RP IN AF, UNMAR.'),
            (6, 'CIVILIAN MALE PRIMARY INDIVIDUAL'),
            (7, 'CIVILIAN FEMALE PRIMARY INDIVIDUAL'),
            (8, 'PRIMARY INDIVIDUAL HHLD-RP IN AF'),
            (9, 'GROUP QUARTERS WITH FAMILY'),
            (10, 'GROUP QUARTERS WITHOUT FAMILY')
        ]

        self.create_table('dim_household_type', schema_fields, data)
        self.logger.info('dim_household_type table created successfully')

    def create_telephone_in_house_table(self):

        schema_fields = [('id', IntegerType()), ('telephone_in_house', StringType())]

        data = [
            (1, 'YES'),
            (2, 'NO')
        ]

        self.create_table('dim_telephone_in_house', schema_fields, data)
        self.logger.info('dim_telephone_in_house table created successfully')

    def create_access_to_telephone_elsewhere_table(self):

        schema_fields = [('id', IntegerType()), ('access_to_telephone_elsewhere', StringType())]

        data = [
            (1, 'YES'),
            (2, 'NO')
        ]

        self.create_table('dim_access_to_telephone_elsewhere', schema_fields, data)
        self.logger.info('dim_access_to_telephone_elsewhere table created successfully')

    def create_telephone_interview_accepted_table(self):

        schema_fields = [('id', IntegerType()), ('telephone_interview_accepted', StringType())]

        data = [
            (1, 'YES'),
            (2, 'NO')
        ]

        self.create_table('dim_telephone_interview_accepted', schema_fields, data)
        self.logger.info('dim_telephone_interview_accepted table created successfully')

    def create_type_of_interview_table(self):

        schema_fields = [('id', IntegerType()), ('description', StringType())]

        data = [
            (0, 'NONINTERVIEW/INDETERMINATE'),
            (1, 'PERSONAL'),
            (2, 'TELEPHONE')
        ]

        self.create_table('dim_type_of_interview', schema_fields, data)
        self.logger.info('dim_type_of_interview table created successfully')

    def create_family_income_table(self):

        schema_fields = [('id', IntegerType()), ('description', StringType())]

        data = [
            (1, 'LESS THAN $5,000'),
            (2, '5,000 TO 7,499'),
            (3, '7,500 TO 9,999'),
            (4, '10,000 TO 12,499'),
            (5, '12,500 TO 14,999'),
            (6, '15,000 TO 19,999'),
            (7, '20,000 TO 24,999'),
            (8, '25,000 TO 29,999'),
            (9, '30,000 TO 34,999'),
            (10, '35,000 TO 39,999'),
            (11, '40,000 TO 49,999'),
            (12, '50,000 TO 59,999'),
            (13, '60,000 TO 74,999'),
            (14, '75,000 TO 99,999'),
            (15, '100,000 TO 124,999'),
            (16, '125,000 TO 149,999'),
            (17, '150,000 TO 199,999'),
            (18, '200,000 OR MORE'),
            (19, 'NO REPORTED INCOME')
        ]

        self.create_table('dim_family_income', schema_fields, data)
        self.logger.info('dim_family_income table created successfully')

    def create_geographic_division_table(self):

        schema_fields = [('id', IntegerType()), ('description', StringType())]

        data = [
            (1, 'NEW ENGLAND'),
            (2, 'MIDDLE ATLANTIC'),
            (3, 'EAST NORTH CENTRAL'),
            (4, 'WEST NORTH CENTRAL'),
            (5, 'SOUTH ATLANTIC'),
            (6, 'EAST SOUTH CENTRAL'),
            (7, 'WEST SOUTH CENTRAL'),
            (8, 'MOUNTAIN'),
            (9, 'PACIFIC')
        ]

        self.create_table('dim_geographic_division', schema_fields, data)
        self.logger.info('dim_geographic_division table created successfully')

    def create_race_table(self):

        schema_fields = [('id', IntegerType()), ('description', StringType())]

        data = [
            (1, 'White Only'),
            (2, 'Black Only'),
            (3, 'American Indian and Alaska Native Only'),
            (4, 'Asian Only'),
            (5, 'Hawaiian/Pacific Islander Only'),
            (6, 'White-Black'),
            (7, 'White-AI'),
            (8, 'White-Asian'),
            (9, 'White-HP'),
            (10, 'Black-AI'),
            (11, 'Black-Asian'),
            (12, 'Black-HP'),
            (13, 'AI-Asian'),
            (14, 'AI-HP'),
            (15, 'Asian-HP'),
            (16, 'W-B-AI'),
            (17, 'W-B-A'),
            (18, 'W-B-HP'),
            (19, 'W-AI-A'),
            (20, 'W-AI-HP'),
            (21, 'W-A-HP'),
            (22, 'B-AI-A'),
            (23, 'W-B-AI-A'),
            (24, 'W-AI-A-HP'),
            (25, 'Other 3 Race Combinations'),
            (26, 'Other 4 and 5 Race Combinations')
        ]

        self.create_table('dim_race', schema_fields, data)
        self.logger.info('dim_race table created successfully')

    def create_all_tables(self):

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
