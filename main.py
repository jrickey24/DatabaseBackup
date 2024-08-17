import os
import pyodbc
import pandas as pd
from datetime import datetime


def backup_database(db_server, db_name):
    backup_folder = 'C:/DbBackup'
    backup_file = db_name + '_' + str(datetime.now().strftime('%Y%m%d_%H%M%S'))
    backup_path = os.path.join(backup_folder, backup_file) + '.bak'
    backup_details_file = 'backup_details.csv'
    conn_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_server};DATABASE={db_name};Trusted_Connection=yes"

    try:
        conn = pyodbc.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"BACKUP DATABASE {db_name} TO DISK='{backup_path}' WITH NOFORMAT, NOINIT, NAME = '{backup_file}',"
                       f"SKIP, REWIND, NOUNLOAD, STATS = 10")
        while cursor.nextset():
            pass
        conn.close()
        print("Backup completed successfully.")
    except pyodbc.Error as e:
        print("Error:", e)

    backup_details = pd.DataFrame({
        'database': db_name,
        'backup_file': backup_file,
        'backup_time': datetime.now()}, index=[0])
    backup_df = backup_details
    backup_df.to_csv(os.path.join(backup_folder, backup_details_file), index=False)


def main() -> None:
    backup_database()  # pass in db_server & db_name values


if __name__ == '__main__':
    main()
