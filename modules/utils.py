from sqlalchemy import create_engine, event

def postgres_connection():
    
    connection_url = "postgresql://{user}:{passw}@{host}:{port}/{db}".format(
        user=getenv,
        passw=getenv,
        host=getenv,
        db=getenv,
        port=getenv
        )

    db_conn = create_engine(
        connection_url,
        paramstyle="format",
        )

    # Codigo para aumentar la velocidad de los inserts
    @event.listens_for(db_conn, 'before_cursor_execute')
    def receive_before_cursor_execute(
        conn, 
        cursor, 
        statement, 
        params, 
        context, 
        executemany
        ):
        if executemany:
            cursor.fast_executemany = True
            cursor.commit()


    connection = db_conn.connect()

    return connection