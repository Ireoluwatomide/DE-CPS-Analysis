# Path: spark_solution/spark_analysis.py

from .spark_logger import logs
from .spark_session import SparkContextManager


class SparkQueryExecutor:

    def __init__(self):

        self.logger = logs(__name__)
        self.spark = SparkContextManager().get_spark_session()

    def query_fct_table(self):

        result = self.spark.sql('''
            SELECT
                a.household_identifier,
                DATE_FORMAT(a.interview_time, 'yyyy-MM') as interview_time,
                b.description as survey_outcome,
                c.description as housing_unit_type,
                d.description as household_type,
                e.telephone_in_house,
                f.access_to_telephone_elsewhere,
                g.telephone_interview_accepted,
                h.description as type_of_interview,
                j.description as family_income_range,
                k.description as geographic_division,
                l.description as race
            FROM
                fct_dec_monthly_cps a
            LEFT JOIN
                dim_final_outcome b ON a.final_outcome = b.code
            LEFT JOIN
                dim_housing_unit_type c ON a.housing_unit = c.id
            LEFT JOIN
                dim_household_type d ON a.household_type = d.id
            LEFT JOIN
                dim_telephone_in_house e ON a.telephone_in_house = e.id
            LEFT JOIN
                dim_access_to_telephone_elsewhere f ON a.access_to_telephone_elsewhere = f.id
            LEFT JOIN
                dim_telephone_interview_accepted g ON a.telephone_interview_accepted = g.id
            LEFT JOIN
                dim_type_of_interview h ON a.type_of_interview = h.id
            LEFT JOIN
                dim_family_income j ON a.family_income_range = j.id
            LEFT JOIN
                dim_geographic_division k ON a.geographic_division = k.id
            LEFT JOIN
                dim_race l ON a.race = l.id
            LIMIT 10
            ''')

        print('Table showing the first 10 rows of the fact table - fct_dec_monthly_cps')
        return result.show()

    def query_count_of_resp_per_income(self):

        result = self.spark.sql('''
            SELECT
                j.description as family_income_range,
                count(a.household_identifier) as count_of_respondents
            FROM
                fct_dec_monthly_cps a
            LEFT JOIN
                dim_family_income j ON a.family_income_range = j.id
            GROUP BY
                j.description
            ''')

        print('Table showing the count of respondents per family income range')
        return result.show()

    def query_count_resp_per_geographical_division_and_race(self):

        result = self.spark.sql('''
            SELECT
                b.description as geographic_division,
                c.description as race,
                COUNT(a.household_identifier) as responder_count
            FROM
                fct_dec_monthly_cps a
            LEFT JOIN
                dim_geographic_division b ON a.geographic_division = b.id
            LEFT JOIN
                dim_race c ON a.race = c.id
            GROUP BY
                b.description, c.description
            ORDER BY
                responder_count DESC
            LIMIT 10
            ''')

        print('\nTable showing the count of respondents per geographical division/location and race')
        return result.show()

    def query_count_resp_no_phone_can_access_elsewhere(self):

        result = self.spark.sql('''
            SELECT
                COUNT(a.household_identifier) AS responders_count
            FROM
                fct_dec_monthly_cps a
            WHERE
                telephone_in_house = 2
            AND access_to_telephone_elsewhere = 1
            AND telephone_interview_accepted = 1;
        ''')

        print('Table showing the count of respondents with no phone but can access elsewhere and telephone interview '
              'is accepted')
        return result.show()

    def query_count_resp_can_access_elsewhere_no_interview(self):

        result = self.spark.sql('''
            SELECT
                COUNT(a.household_identifier) AS responders_count
            FROM
                fct_dec_monthly_cps a
            WHERE (access_to_telephone_elsewhere = 1 OR telephone_in_house = 1)
            AND telephone_interview_accepted = 2;
        ''')

        print('Table showing the count of respondents that can access a phone elsewhere but telephone interview '
              'is not accepted')
        return result.show()

    def query_executor(self):
        self.query_fct_table()
        self.query_count_of_resp_per_income()
        self.query_count_resp_per_geographical_division_and_race()
        self.query_count_resp_no_phone_can_access_elsewhere()
        self.query_count_resp_can_access_elsewhere_no_interview()
        self.logger.info('Queries executed successfully')
