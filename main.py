import pandas as pd
import cx_Oracle
import os

# Configurația bazei de date
dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='orcl')
connection = cx_Oracle.connect(user='C##Andrei', password='1234', dsn=dsn_tns)


def clear_table():
    cursor = connection.cursor()
    try:
        cursor.execute("TRUNCATE TABLE EVENTS")
        connection.commit()
        print("Tabela EVENTS a fost golită cu succes.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print(f"Eroare la ștergerea datelor: {error.message}")
    finally:
        cursor.close()


def import_csv_to_oracle(csv_file):
    # Specifică tipurile de date pentru a păstra numerele de telefon ca șiruri de caractere
    dtype_spec = {
        'CALLING': str,
        'CALLED': str
    }

    # Încarcă fișierul CSV și transformă coloanele de timestamp
    df = pd.read_csv(csv_file, dtype=dtype_spec)

    # Verifică coloanele disponibile
    print("Coloane disponibile:", df.columns)

    # Converteste timestamp-urile folosind pd.to_datetime() care gestionează nanosecunde
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
    df['ANSWER_DATETIME'] = pd.to_datetime(df['ANSWER_DATETIME'], errors='coerce')
    df['RELEASE_DATETIME'] = pd.to_datetime(df['RELEASE_DATETIME'], errors='coerce')

    cursor = connection.cursor()
    for index, row in df.iterrows():
        try:
            # Transformă valorile NaN în None pentru a permite inserarea ca NULL în baza de date
            row = row.where(pd.notna(row), None)
            cursor.execute("""
                INSERT INTO EVENTS (timestamp, call_id, status, calling, called, 
                                    answer_datetime, release_datetime, bearer, 
                                    itr_traffic_type_name, data_subscriber_type, 
                                    reason, total_call_time) 
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)""",
                           (row['TIMESTAMP'], row['CALL_ID'], row['STATUS'], row['CALLING'], row['CALLED'],
                            row['ANSWER_DATETIME'], row['RELEASE_DATETIME'], row['BEARER'],
                            row['ITR_TRAFFIC_TYPE_NAME'], row['DATA_SUBSCRIBER_TYPE'], row['REASON'],
                            row['TOTAL_CALL_TIME']))
        except KeyError as e:
            print(f"Coloana lipsă: {e}")
        except Exception as e:
            print(f"Eroare la inserare: {e}")

    connection.commit()
    cursor.close()

    print("Tabela EVENTS a fost actualizată cu succes.")


# Șterge toate datele din tabela EVENTS
clear_table()

# Găsește ultimul fișier CSV creat într-un folder specificat
folder_path = 'C:/Users/Andrei/Desktop/'
list_of_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.csv')]
latest_file = max(list_of_files, key=os.path.getctime)

# Importă datele din ultimul fișier CSV
import_csv_to_oracle(latest_file)

# Închide conexiunea la baza de date
connection.close()
