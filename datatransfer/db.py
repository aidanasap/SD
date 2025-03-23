import psycopg2
from psycopg2 import sql
from datetime import datetime

source_db_config = {
    'dbname': 'source_db',
    'user': 'your_user',
    'password': 'your_password',
    'host': 'localhost',
    'port': 5432
}

dest_db_config = {
    'dbname': 'dest_db',
    'user': 'your_user',
    'password': 'your_password',
    'host': 'localhost',
    'port': 5432
}

users_num = 500

def transfer_user_batch(source_cursor, dest_cursor, dest_conn, user_batch, transfer_date):
    try:
        source_cursor.execute(
            sql.SQL("SELECT * FROM recommendation WHERE user_id = ANY(%s) AND date = %s"),
            (user_batch, transfer_date)
        )
        rows = source_cursor.fetchall()

        dest_cursor.executemany(
            sql.SQL("INSERT INTO recommendation VALUES (%s, %s, %s)"),
            rows
        )
        dest_conn.commit()

        dest_cursor.execute(
            sql.SQL("""
                INSERT INTO transfer_log (status, user_ids, rows_transferred, transfer_date)
                VALUES (%s, %s, %s, %s)
            """),
            ('success', user_batch, len(rows), transfer_date)
        )
        dest_conn.commit()

    except Exception as e:
        dest_cursor.execute(
            sql.SQL("""
                INSERT INTO transfer_log (status, user_ids, rows_transferred, error_message, transfer_date)
                VALUES (%s, %s, %s, %s, %s)
            """),
            ('error', user_batch, 0, str(e), transfer_date))
        dest_conn.commit()
        raise e  # Re-raise the exception to trigger retry logic

def retry_failed_batchs(dest_cursor, source_cursor, dest_conn, transfer_date):
    dest_cursor.execute(
        sql.SQL("SELECT user_ids FROM transfer_log WHERE status = 'error' AND transfer_date = %s"),
        (transfer_date,)
    )
    failed_batchs = dest_cursor.fetchall()

    for batch in failed_batchs:
        user_batch = batch[0]
        print(f"Retrying batch with user_ids: {user_batch}")
        transfer_user_batch(source_cursor, dest_cursor, dest_conn, user_batch, transfer_date)

def validate_transfer(source_cursor, dest_cursor, user_ids, transfer_date):
    source_cursor.execute(
        sql.SQL("SELECT COUNT(*) FROM recommendation WHERE user_id = ANY(%s) AND date = %s"),
        (user_ids, transfer_date)
    )
    source_count = source_cursor.fetchone()[0]

    dest_cursor.execute(
        sql.SQL("SELECT COUNT(*) FROM recommendation WHERE user_id = ANY(%s) AND date = %s"),
        (user_ids, transfer_date)
    )
    dest_count = dest_cursor.fetchone()[0]

    if source_count != dest_count:
        print(f"Row count mismatch: Source={source_count}, Destination={dest_count}")
    else:
        print("Row counts match.")

    source_cursor.execute(
        sql.SQL("""
            SELECT * FROM recommendation
            WHERE user_id = ANY(%s) AND date = %s
            AND (user_id, item_id) NOT IN (
                SELECT user_id, item_id FROM recommendation
                WHERE user_id = ANY(%s) AND date = %s
            )
        """),
        (user_ids, transfer_date, user_ids, transfer_date)
    )
    missing_rows = source_cursor.fetchall()
    if missing_rows:
        print(f"Missing rows: {missing_rows}")
    else:
        print("No missing rows.")

def main():
    source_conn = psycopg2.connect(**source_db_config)
    dest_conn = psycopg2.connect(**dest_db_config)

    source_cursor = source_conn.cursor()
    dest_cursor = dest_conn.cursor()

    transfer_date = datetime.today().date()

    try:
        source_cursor.execute(
            sql.SQL("SELECT DISTINCT user_id FROM recommendation WHERE date = %s"),
            (transfer_date,)
        )
        user_ids = [row[0] for row in source_cursor.fetchall()]

        for i in range(0, len(user_ids), users_num):
            user_batch = user_ids[i:i + users_num]
            try:
                transfer_user_batch(source_cursor, dest_cursor, dest_conn, user_batch, transfer_date)
            except Exception as e:
                print(f"batch failed: {e}. It will be retried later.")

        retry_failed_batchs(dest_cursor, source_cursor, dest_conn, transfer_date)
        validate_transfer(source_cursor, dest_cursor, user_ids, transfer_date)
    finally:
        source_cursor.close()
        dest_cursor.close()
        source_conn.close()
        dest_conn.close()

if __name__ == "__main__":
    main()