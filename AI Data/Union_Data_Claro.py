import os
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, lit, concat, when

def rename_columns(df, column_map):
    """Renames columns based on a provided mapping."""
    for old_name, new_name in column_map.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df

def Save_Data_Frame (Data_Frame, Directory_to_Save, Partitions, year_data, month_data):

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d")
    Type_File = f"Lectura Data Claro {Time_File}"

    output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
    Data_Frame.repartition(Partitions).write.mode("overwrite").option("header", "true").option("delimiter",";").csv(output_path)
    
    for root, dirs, files in os.walk(output_path):
        for file in files:
            if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                os.remove(os.path.join(root, file))
    
    for i, file in enumerate(os.listdir(output_path), start=1):
        if file.endswith(".csv"):
            old_file_path = os.path.join(output_path, file)
            new_file_path = os.path.join(output_path, f'Compilado Data Claro {year_data} - {month_data}.csv')
            os.rename(old_file_path, new_file_path)
            
def UnionDataframes(input_folder, output_path, num_partitions, month_data, year_data):
    spark = SparkSession.builder.appName("BD_MONTH") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.driver.memory", '16g')\
        .config("spark.executor.memory", '16g')\
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs", "false")
    
    # File paths
    Root_Data = os.path.join(input_folder, f"Asignacion Estructurada/Asignacion Multimarca {year_data}-{month_data}.csv")
    Root_Base = os.path.join(input_folder, f"Base General Mensual/BaseGeneral {year_data}-{month_data}.csv")
    Root_Touch = os.path.join(input_folder, f"Toques por Telemática/Toques {year_data}-{month_data}.csv")
    Root_Demo = os.path.join(input_folder, f"Demograficos Estructurados/Demograficos {year_data}-{month_data}.csv")
    
    try:
        # Read DataFrames
        File_Data = spark.read.csv(Root_Data, header=True, sep=";")
        File_Base = spark.read.csv(Root_Base, header=True, sep=";")
        File_Touch = spark.read.csv(Root_Touch, header=True, sep=";")
        File_Demo = spark.read.csv(Root_Demo, header=True, sep=";")
        
        # Rename columns
        column_renames = {
            "CUENTA_NEXT": "CUENTA NEXT",
            "Cuenta_Sin_Punto": "CUENTA NEXT",
            "Cuenta_Real": "CUENTA NEXT",
            "marca": None  # We will handle this in the loop below
        }
        
        # Create a dictionary for DataFrames
        dataframes = {
            'Data': File_Data,
            'Touch': File_Touch,
            'Demo': File_Demo,
            'Base': File_Base
        }
        
        # List of columns to exclude
        days_columns = []
        for i in range(1, 32):
            days_columns.append(f"Dia_{i}")

        phone_columns = []
        for i in range(1, 21):
            phone_columns.append(f"phone{i}")
        
        email_columns = []
        for i in range(1, 6):
            email_columns.append(f"email{i}")
            
        columns_custom = ['MES DE ASIGNACION', 'PERIODO DE ASIGNACION']
        column_touch = ['Cuenta_Sin_Punto', 'marca']
        
        columns_to_exclude_data = columns_custom + days_columns
        columns_to_exclude_demo = columns_custom + phone_columns + email_columns
        columns_to_exclude_touch = columns_custom + column_touch
        columns_to_exclude_base = ['COLUMNA 2', ' COLUMNA 3', 'COLUMNA 4', 'SEGMENTO55', 'SEGMENTO']
        
        # Assuming columns_to_exclude_data, columns_to_exclude_demo, and columns_to_exclude_touch are lists of column names
        dataframes['Data'] = dataframes['Data'].drop(*columns_to_exclude_data)  # Use * to unpack the list
        dataframes['Demo'] = dataframes['Demo'].drop(*columns_to_exclude_demo)  # Use * to unpack the list
        dataframes['Touch'] = dataframes['Touch'].drop(*columns_to_exclude_touch)  # Use * to unpack the list
        dataframes['Base'] = dataframes['Base'].drop(*columns_to_exclude_base)  # Use * to unpack the list
        
        dataframes['Touch'] = dataframes['Touch'].withColumn("Cuenta_Real", regexp_replace(col("Cuenta_Real"), "\\.", ""))
        dataframes['Demo'] = dataframes['Demo'].withColumnRenamed("cuenta", "CUENTA NEXT")
        
        dataframes['Data'] = dataframes['Data'].withColumnRenamed("MARCA", "MARCA DATA")
        dataframes['Data'] = dataframes['Data'].withColumnRenamed("FECHA INGRESO", "FECHA INGRESO DATA")
        dataframes['Data'] = dataframes['Data'].withColumnRenamed("FECHA RETIRO", "FECHA RETIRO DATA")
        
        # Rename columns in each DataFrame and rename 'CUENTA NEXT' to avoid ambiguity
        for base in dataframes.keys():
            dataframes[base] = rename_columns(dataframes[base], {k: v for k, v in column_renames.items() if v is not None})
            #dataframes[base] = dataframes[base].withColumnRenamed("CUENTA NEXT", f"CUENTA_NEXT_{base}")
        
        # Initialize Data_Frame with File_Base
        dataframes['Touch'] = dataframes['Touch'].withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        Data_Frame = dataframes['Base']
        Data_Frame = Data_Frame.withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        
        Data_Frame = Data_Frame.join(dataframes['Demo'], on='CUENTA NEXT', how='outer')
        Data_Frame = Data_Frame.join(dataframes['Touch'], on='CUENTA NEXT', how='outer')
        Data_Frame = Data_Frame.join(dataframes['Data'], on='CUENTA NEXT', how='outer') 
        
        
        Data_Frame = Data_Frame.withColumn(
            "FILTRO DESCUENTO",
            when(col("PORCENTAJE") == 0, lit("No Aplica")).otherwise(lit("Aplica"))
        )
        
        columns_tofilter = [
            # Main customer information
            'CUENTA NEXT', 'TIPO DE DOCUMENTO', 'CRM Origen', 'Marca', 'MARCA DATA', 'Plan', 'SEGMENTO', 'Ciudad', 'LIQUIDACION',

            # Ranges and segmentation
            'RANGO', 'RANGO DEUDA', 'GRUPO RANGO DE DIAS', 'RANGO DE DIAS', 'RANGO CON DESCUENTO',
            
            # Contact information
            'NumMovil', 'NumFijo', 'NumEmail',

            # Financial information
            'SALDO', 'VALOR DEUDA', 'TIPO DE PAGO', 'FECHA ULT PAGO',

            # Assignment and management data
            'FECHA INGRESO', 'FECHA INGRESO DATA', 'FECHA RETIRO DATA', 'FECHA VENCIMIENTO',
            'DIAS ASIGNADA', 'DIAS RETIRADA',

            # Automated contact attempts
            'Toques por SMS', 'Toques por EMAIL', 'Toques por BOT', 'Toques por IVR', 'PORCENTAJE',

            # Additional filters or references
            'MULTICUENTA', 'MULTIPRODUCTO', 'MULTIMORA',
            'Filtro Demografico', 'FILTRO REFERENCIA', 'ESTADO CUENTA', 'FILTRO DESCUENTO'
        ]

        Data_Frame = Data_Frame.select(columns_tofilter)
        
        # Filter and write the DataFrame
        if Data_Frame.count() > 0:
            
            print(Data_Frame.columns)
            Data_Frame = Data_Frame.filter(~col("FECHA INGRESO").contains("Manual"))
            Data_Frame = Data_Frame.filter(col("Marca") == "0")
            
            Save_Data_Frame(Data_Frame, output_path, num_partitions, year_data, month_data)
            
        else:
            print("No data was merged.")

        
        return Data_Frame
    except Exception as e:
        
        print(f"Error in UnionDataframes: {e}")
        return None
    
# Input parameters
input_folder = r"D:\Cloud\OneDrive - Recupera SAS\Data Claro\Claro_Data_Lake"
output_folder = r"C:/Users/juan_/Downloads/"
num_partitions = 1
month_data = "12"
year_data = "2024"
# Execute the function
UnionDataframes(input_folder, output_folder, num_partitions, month_data, year_data)

## Compilado Data Claro 2025 - 02