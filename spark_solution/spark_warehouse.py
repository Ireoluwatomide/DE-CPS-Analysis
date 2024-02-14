# from spark_logger import logs
from spark_session import SparkContextManager

from pyspark.sql.types import StructType, StructField, StringType

# Intialise the spark session
spark = SparkContextManager().get_spark_session()


def create_final_outcome_table():
    # Define schema for the DataFrame
    schema = StructType([
        StructField("code", StringType(), False),
        StructField("description", StringType(), False)
    ])

    # Define data
    data = [
        ("001", "FULLY COMPLETE CATI INTERVIEW"),
        ("002", "PARTIALLY COMPLETED CATI INTERVIEW"),
        ("003", "COMPLETE BUT PERSONAL VISIT REQUESTED NEXT MONTH"),
        ("004", "PARTIAL, NOT COMPLETE AT CLOSEOUT"),
        ("005", "LABOR FORCE COMPLETE, SUPPLEMENT INCOMPLETE - CATI"),
        ("006", "LF COMPLETE, SUPPLEMENT DK ITEMS INCOMPLETE AT CLOSEOUT"),
        ("020", "HH OCCUPIED ENTIRELY BY ARMED FORCES MEMBERS OR ALL UNDER 15 YEARS OF AGE"),
        ("201", "CAPI COMPLETE"),
        ("202", "CALLBACK NEEDED"),
        ("203", "SUFFICIENT PARTIAL - PRECLOSEOUT"),
        ("204", "SUFFICIENT PARTIAL - AT CLOSEOUT"),
        ("205", "LABOR FORCE COMPLETE, - SUPPL. INCOMPLETE - CAPI"),
        ("213", "LANGUAGE BARRIER"),
        ("214", "UNABLE TO LOCATE"),
        ("216", "NO ONE HOME"),
        ("217", "TEMPORARILY ABSENT"),
        ("218", "REFUSED"),
        ("219", "OTHER OCCUPIED - SPECIFY"),
        ("223", "ENTIRE HOUSEHOLD ARMED FORCES"),
        ("224", "ENTIRE HOUSEHOLD UNDER 15"),
        ("225", "TEMP. OCCUPIED W/PERSONS WITH URE"),
        ("226", "VACANT REGULAR"),
        ("227", "VACANT - STORAGE OF HHLD FURNITURE"),
        ("228", "UNFIT, TO BE DEMOLISHED"),
        ("229", "UNDER CONSTRUCTION, NOT READY"),
        ("230", "CONVERTED TO TEMP BUSINESS OR STORAGE"),
        ("231", "UNOCCUPIED TENT OR TRAILER SITE"),
        ("232", "PERMIT GRANTED - CONSTRUCTION NOT STARTED"),
        ("233", "OTHER - SPECIFY"),
        ("240", "DEMOLISHED"),
        ("241", "HOUSE OR TRAILER MOVED"),
        ("242", "OUTSIDE SEGMENT"),
        ("243", "CONVERTED TO PERM. BUSINESS OR STORAGE"),
        ("244", "MERGED"),
        ("245", "CONDEMNED"),
        ("246", "BUILT AFTER APRIL 1, 2000"),
        ("247", "UNUSED SERIAL NO./LISTING SHEET LINE"),
        ("248", "OTHER - SPECIFY"),
        ("256", "REMOVED DURING SUB-SAMPLING"),
        ("257", "UNIT ALREADY HAD A CHANCE OF SELECTION")
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, schema=schema)

    # Save DataFrame to a table
    table_name = "final_outcome"
    df.write.mode("overwrite").saveAsTable(table_name)

