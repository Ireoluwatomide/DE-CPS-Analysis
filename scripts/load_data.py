# Path: scripts/load_data.py

# Import the required libraries
from .logger import logs
from psycopg2 import sql
from .db_connect import Database


class DataLoader:
    def __init__(self):
        
        self.db = Database()
        self.db.connect()
        self.logger = logs(__name__)
        
        # Schema name
        self.cps_schema_name = 'basic_monthly_cps'

    def load_data(self, table_name, data):
        
        try:
            insert_query = sql.SQL('INSERT INTO {}.{} VALUES {} ON CONFLICT DO NOTHING').format(
                sql.Identifier(self.cps_schema_name),
                sql.Identifier(table_name),
                sql.SQL(',').join(map(sql.Literal, data))
            )
            self.db.execute(insert_query)
            self.db.commit()
            self.logger.info(f'Data loaded into {table_name} successfully')
            
        except Exception as e:
            self.logger.error(f'Error loading data into {table_name}: {str(e)}')
        
    def load_dec_monthly_cps(self, dataframe):
        data = [tuple(row) for row in dataframe.itertuples(index=False, name=None)]
        
        self.load_data('fct_dec_monthly_cps', data)

    def load_final_outcome_table(self):
        
        data = [('001', 'FULLY COMPLETE CATI INTERVIEW'),
                ('002', 'PARTIALLY COMPLETED CATI INTERVIEW'),
                ('003', 'COMPLETE BUT PERSONAL VISIT REQUESTED NEXT MONTH'),
                ('004', 'PARTIAL, NOT COMPLETE AT CLOSEOUT'),
                ('005', 'LABOR FORCE COMPLETE, SUPPLEMENT INCOMPLETE - CATI'),
                ('006', 'LF COMPLETE, SUPPLEMENT DK ITEMS INCOMPLETE AT CLOSEOUT—ASEC ONLY'),
                ('020', 'HH OCCUPIED ENTIRELY BY ARMED FORCES MEMBERS OR ALL UNDER 15 YEARS OF AGE'),
                ('201', 'CAPI COMPLETE'),
                ('202', 'CALLBACK NEEDED'),
                ('203', 'SUFFICIENT PARTIAL - PRECLOSEOUT'),
                ('204', 'SUFFICIENT PARTIAL - AT CLOSEOUT'),
                ('205', 'LABOR FORCE COMPLETE, - SUPPL. INCOMPLETE - CAPI'),
                ('213', 'LANGUAGE BARRIER'),
                ('214', 'UNABLE TO LOCATE'),
                ('216', 'NO ONE HOME'),
                ('217', 'TEMPORARILY ABSENT'),
                ('218', 'REFUSED'),
                ('219', 'OTHER OCCUPIED - SPECIFY'),
                ('223', 'ENTIRE HOUSEHOLD ARMED FORCES'),
                ('224', 'ENTIRE HOUSEHOLD UNDER 15'),
                ('225', 'TEMP. OCCUPIED W/PERSONS WITH URE'),
                ('226', 'VACANT REGULAR'),
                ('227', 'VACANT - STORAGE OF HHLD FURNITURE'),
                ('228', 'UNFIT, TO BE DEMOLISHED'),
                ('229', 'UNDER CONSTRUCTION, NOT READY'),
                ('230', 'CONVERTED TO TEMP BUSINESS OR STORAGE'),
                ('231', 'UNOCCUPIED TENT OR TRAILER SITE'),
                ('232', 'PERMIT GRANTED - CONSTRUCTION NOT STARTED'),
                ('233', 'OTHER - SPECIFY'),
                ('240', 'DEMOLISHED'),
                ('241', 'HOUSE OR TRAILER MOVED'),
                ('242', 'OUTSIDE SEGMENT'),
                ('243', 'CONVERTED TO PERM. BUSINESS OR STORAGE'),
                ('244', 'MERGED'),
                ('245', 'CONDEMNED'),
                ('246', 'BUILT AFTER APRIL 1, 2000'),
                ('247', 'UNUSED SERIAL NO./LISTING SHEET LINE'),
                ('248', 'OTHER - SPECIFY'),
                ('256', 'REMOVED DURING SUB-SAMPLING'),
                ('257', 'UNIT ALREADY HAD A CHANCE OF SELECTION')]
        
        self.load_data('dim_final_outcome', data)

    def load_housing_unit_table(self):
        
        data = [('0', 'OTHER UNIT'),
                ('1', 'HOUSE, APARTMENT, FLAT'),
                ('2', 'HU IN NONTRANSIENT HOTEL, MOTEL, ETC.'),
                ('3', 'HU PERMANENT IN TRANSIENT HOTEL, MOTEL'),
                ('4', 'HU IN ROOMING HOUSE'),
                ('5', 'MOBILE HOME OR TRAILER W/NO PERM. ROOM ADDED'),
                ('6', 'MOBILE HOME OR TRAILER W/1 OR MORE PERM. ROOMS ADDED'),
                ('7', 'HU NOT SPECIFIED ABOVE'),
                ('8', 'QUARTERS NOT HU IN ROOMING OR BRDING HS'),
                ('9', 'UNIT NOT PERM. IN TRANSIENT HOTL, MOTL'),
                ('10', 'UNOCCUPIED TENT SITE OR TRLR SITE'),
                ('11', 'STUDENT QUARTERS IN COLLEGE DORM'),
                ('12', 'OTHER UNIT NOT SPECIFIED ABOVE')]
        
        self.load_data('dim_housing_unit_type', data)

    def load_household_type_table(self):
        
        data = [('0', 'NON-INTERVIEW HOUSEHOLD'),
                ('1', 'HUSBAND/WIFE PRIMARY FAMILY (NEITHER AF)'),
                ('2', 'HUSB/WIFE PRIM. FAMILY (EITHER/BOTH AF)'),
                ('3', 'UNMARRIED CIVILIAN MALE-PRIM. FAM HHLDER'),
                ('4', 'UNMARRIED CIV. FEMALE-PRIM FAM HHLDER'),
                ('5', 'PRIMARY FAMILY HHLDER-RP IN AF, UNMAR.'),
                ('6', 'CIVILIAN MALE PRIMARY INDIVIDUAL'),
                ('7', 'CIVILIAN FEMALE PRIMARY INDIVIDUAL'),
                ('8', 'PRIMARY INDIVIDUAL HHLD-RP IN AF'),
                ('9', 'GROUP QUARTERS WITH FAMILY'),
                ('10', 'GROUP QUARTERS WITHOUT FAMILY')]
        
        self.load_data('dim_household_type', data)
        
    def load_telephone_in_house_table(self):
            
        data = [('1', 'YES'),
                ('2', 'NO')]
        
        self.load_data('dim_telephone_in_house', data)
        
    def load_access_to_telephone_elsewhere_table(self):
            
        data = [('1', 'YES'),
                ('2', 'NO')]
        
        self.load_data('dim_access_to_telephone_elsewhere', data)
        
    def load_telephone_interview_accepted_table(self):
            
        data = [('1', 'YES'),
                ('2', 'NO')]
        
        self.load_data('dim_telephone_interview_accepted', data)
        
    def load_type_of_interview_table(self):
            
        data = [('0', 'NONINTERVIEW/INDETERMINATE'),
                ('1', 'PERSONAL'),
                ('2', 'TELEPHONE')]
        
        self.load_data('dim_type_of_interview', data)
        
    def load_family_income_table(self):
            
        data = [('1', 'LESS THAN $5,000'),
                ('2', '5,000 TO 7,499'),
                ('3', '7,500 TO 9,999'),
                ('4', '10,000 TO 12,499'),
                ('5', '12,500 TO 14,999'),
                ('6', '15,000 TO 19,999'),
                ('7', '20,000 TO 24,999'),
                ('8', '25,000 TO 29,999'),
                ('9', '30,000 TO 34,999'),
                ('10', '35,000 TO 39,999'),
                ('11', '40,000 TO 49,999'),
                ('12', '50,000 TO 59,999'),
                ('13', '60,000 TO 74,999'),
                ('14', '75,000 TO 99,999'),
                ('15', '100,000 TO  149,999'),
                ('16', '150,000 OR MORE')]
        
        self.load_data('dim_family_income', data)
        
    def load_geographic_division_table(self):
        
        data = [('1', 'NEW ENGLAND'),
                ('2', 'MIDDLE ATLANTIC'),
                ('3', 'EAST NORTH CENTRAL'),
                ('4', 'WEST NORTH CENTRAL'),
                ('5', 'SOUTH ATLANTIC'),
                ('6', 'EAST SOUTH CENTRAL'),
                ('7', 'WEST SOUTH CENTRAL'),
                ('8', 'MOUNTAIN'),
                ('9', 'PACIFIC'),]
        
        self.load_data('dim_geographic_division', data)
        
    
    def load_race_table(self):
        
        data = [('01', 'White Only'),
                ('02', 'Black Only'),
                ('03', 'American Indian, Alaskan Native Only'),
                ('04', 'Asian Only'),
                ('05', 'Hawaiian/Pacific Islander Only'),
                ('06', 'White-Black'),
                ('07', 'White-AI'),
                ('08', 'White-Asian'),
                ('09', 'White-HP'),
                ('10', 'Black-AI'),
                ('11', 'Black-Asian'),
                ('12', 'Black-HP'),
                ('13', 'AI-Asian'),
                ('14', 'AI-HP'),
                ('15', 'Asian-HP'),
                ('16', 'W-B-AI'),
                ('17', 'W-B-A'),
                ('18', 'W-B-HP'),
                ('19', 'W-AI-A'),
                ('20', 'W-AI-HP'),
                ('21', 'W-A-HP'),
                ('22', 'B-AI-A'),
                ('23', 'W-B-AI-A'),
                ('24', 'W-AI-A-HP'),
                ('25', 'Other 3 Race Combinations'),
                ('26', 'Other 4 and 5 Race Combinations')]
        
        self.load_data('dim_race', data)

    def load_all_tables(self):
        
        self.load_final_outcome_table()
        self.load_housing_unit_table()
        self.load_household_type_table()
        self.load_telephone_in_house_table()
        self.load_access_to_telephone_elsewhere_table()
        self.load_telephone_interview_accepted_table()
        self.load_type_of_interview_table()
        self.load_family_income_table()
        self.load_geographic_division_table()
        self.load_race_table()
        self.logger.info('All tables loaded successfully')
