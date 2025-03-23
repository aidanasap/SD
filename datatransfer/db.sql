CREATE TABLE transfer_log (
    id SERIAL PRIMARY KEY,
    sys_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT,                      
    user_ids TEXT[],
    rows_transferred INT,
    error_message TEXT,
    transfer_date DATE
);