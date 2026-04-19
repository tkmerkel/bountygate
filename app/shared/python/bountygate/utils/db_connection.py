import os
import pandas as pd
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DateTime,
    Float,
    Boolean,
    Text,
    inspect,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base
import psycopg2
import warnings
from datetime import datetime
import uuid

warnings.filterwarnings('ignore')


def _load_env_file_if_present() -> None:
    """Populate os.environ from every .env walking up from this file.

    Local .env (closer to this module) takes precedence — we populate via
    ``setdefault`` so an already-set key is never overwritten.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    for _ in range(6):
        env_path = os.path.join(here, ".env")
        if os.path.isfile(env_path):
            with open(env_path, encoding="utf-8") as f:
                for raw in f:
                    line = raw.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, _, value = line.partition("=")
                    os.environ.setdefault(key.strip(), value.strip())
        parent = os.path.dirname(here)
        if parent == here:
            return
        here = parent


_load_env_file_if_present()
DATABASE_URL = os.environ["DATABASE_URL"]

def fetch_data(query):
    engine = create_engine(DATABASE_URL)
    try:
        from sqlalchemy import text as _text
        with engine.connect() as conn:
            res = conn.execute(_text(query))
            rows = res.fetchall()
            cols = list(res.keys())
        df = pd.DataFrame(rows, columns=cols)
        if df.empty:
            print("No data returned from the query.")
        else:
            print(f"Fetched {len(df)} rows from the database.")
        return df
    finally:
        engine.dispose()


def delete_from_table(table_name, column_name, value):
    # Create a SQLAlchemy engine
    engine = create_engine(DATABASE_URL)
    
    # Create a sessionmaker
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Reflect the table from the database
        meta = MetaData()
        meta.reflect(bind=engine)
        table = meta.tables[table_name]
        
        # Construct the delete query
        delete_query = table.delete().where(table.c[column_name] == value)
        
        # Execute the delete query
        result = session.execute(delete_query)
        
        # Commit the transaction
        session.commit()
        
        # Get the number of rows deleted
        rows_deleted = result.rowcount
        print(f"{rows_deleted} rows deleted.")
        
    except Exception as e:
        print(f"Error: {e}")
        session.rollback()
    finally:
        # Close the session
        session.close()


def _sa_type_for_dtype(dtype) -> object:
    try:
        import numpy as np  # type: ignore
    except Exception:
        np = None  # pragma: no cover
    if np is not None:
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return DateTime(timezone=True)
        if pd.api.types.is_float_dtype(dtype):
            return Float()
        if pd.api.types.is_integer_dtype(dtype):
            return Integer()
        if pd.api.types.is_bool_dtype(dtype):
            return Boolean()
    # default
    return Text()


def _ensure_table_exists(engine, table_name: str, df: pd.DataFrame) -> None:
    meta = MetaData()
    inspector = inspect(engine)
    if table_name in inspector.get_table_names():
        return
    # Create a simple table based on dataframe columns
    cols = []
    for col in df.columns:
        cols.append(Column(col, _sa_type_for_dtype(df[col].dtype)))
    Table(table_name, meta, *cols)
    meta.create_all(engine)


def create_table_from_dataframe(df, table_name, if_exists='fail'):
    """
    Create a table from a pandas DataFrame
    
    Args:
        df: pandas DataFrame
        table_name: Name of the table to create
        if_exists: What to do if table exists ('fail', 'replace', 'append')
    """
    engine = create_engine(DATABASE_URL)
    try:
        with engine.begin() as conn:
            inspector = inspect(engine)
            if if_exists == 'replace' and table_name in inspector.get_table_names():
                conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            _ensure_table_exists(engine, table_name, df.head(1))
            # Reflect and bulk insert
            meta = MetaData()
            table = Table(table_name, meta, autoload_with=engine)
            records = df.to_dict(orient='records')
            if records:
                conn.execute(table.insert(), records)
        print(f"Table '{table_name}' created successfully with {len(df)} rows.")
    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")
    finally:
        engine.dispose()


def insert_data(df, table_name, if_exists='append'):
    """
    Insert data from DataFrame into existing table
    
    Args:
        df: pandas DataFrame containing data to insert
        table_name: Name of the target table
        if_exists: What to do if table exists ('fail', 'replace', 'append')
    """
    engine = create_engine(DATABASE_URL)
    try:
        with engine.begin() as conn:
            inspector = inspect(engine)
            if if_exists == 'replace' and table_name in inspector.get_table_names():
                conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            _ensure_table_exists(engine, table_name, df.head(1))
            meta = MetaData()
            table = Table(table_name, meta, autoload_with=engine)
            records = df.to_dict(orient='records')
            if records:
                conn.execute(table.insert(), records)
        print(f"Inserted {len(df)} rows into table '{table_name}'.")
    except Exception as e:
        print(f"Error inserting data into '{table_name}': {e}")
    finally:
        engine.dispose()


def update_table_data(table_name, update_values, where_condition):
    """
    Update records in a table
    
    Args:
        table_name: Name of the table
        update_values: Dictionary of column:value pairs to update
        where_condition: Dictionary of column:value pairs for WHERE clause
    """
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Reflect the table from the database
        meta = MetaData()
        meta.reflect(bind=engine)
        table = meta.tables[table_name]
        
        # Build WHERE clause
        where_clause = None
        for column, value in where_condition.items():
            condition = table.c[column] == value
            where_clause = condition if where_clause is None else where_clause & condition
        
        # Construct the update query
        update_query = table.update().where(where_clause).values(**update_values)
        
        # Execute the update query
        result = session.execute(update_query)
        session.commit()
        
        rows_updated = result.rowcount
        print(f"{rows_updated} rows updated in table '{table_name}'.")
        
    except Exception as e:
        print(f"Error updating table '{table_name}': {e}")
        session.rollback()
    finally:
        session.close()


def drop_table(table_name, confirm=False):
    """
    Drop a table from the database
    
    Args:
        table_name: Name of the table to drop
        confirm: Safety confirmation (must be True to execute)
    """
    if not confirm:
        print(f"Operation cancelled. Set confirm=True to drop table '{table_name}'.")
        return
    
    engine = create_engine(DATABASE_URL)
    
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.commit()
        print(f"Table '{table_name}' dropped successfully.")
    except Exception as e:
        print(f"Error dropping table '{table_name}': {e}")
    finally:
        engine.dispose()


def truncate_table(table_name, confirm=False):
    """
    Remove all data from a table (but keep the table structure)
    
    Args:
        table_name: Name of the table to truncate
        confirm: Safety confirmation (must be True to execute)
    """
    if not confirm:
        print(f"Operation cancelled. Set confirm=True to truncate table '{table_name}'.")
        return
    
    engine = create_engine(DATABASE_URL)
    
    try:
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            conn.commit()
        print(f"Table '{table_name}' truncated successfully.")
    except Exception as e:
        print(f"Error truncating table '{table_name}': {e}")
    finally:
        engine.dispose()


def table_exists(table_name):
    """
    Check if a table exists in the database
    
    Args:
        table_name: Name of the table to check
        
    Returns:
        bool: True if table exists, False otherwise
    """
    engine = create_engine(DATABASE_URL)
    
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        exists = table_name in tables
        print(f"Table '{table_name}' {'exists' if exists else 'does not exist'}.")
        return exists
    except Exception as e:
        print(f"Error checking if table '{table_name}' exists: {e}")
        return False
    finally:
        engine.dispose()


def get_table_info(table_name):
    """
    Get information about a table (columns, types, etc.)
    
    Args:
        table_name: Name of the table
        
    Returns:
        dict: Table information
    """
    engine = create_engine(DATABASE_URL)
    
    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        indexes = inspector.get_indexes(table_name)
        primary_keys = inspector.get_primary_keys(table_name)
        foreign_keys = inspector.get_foreign_keys(table_name)
        
        info = {
            'columns': columns,
            'indexes': indexes,
            'primary_keys': primary_keys,
            'foreign_keys': foreign_keys
        }
        
        print(f"Table '{table_name}' information retrieved successfully.")
        return info
    except Exception as e:
        print(f"Error getting table info for '{table_name}': {e}")
        return None
    finally:
        engine.dispose()


def list_all_tables():
    """
    List all tables in the database
    
    Returns:
        list: List of table names
    """
    engine = create_engine(DATABASE_URL)
    
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print(f"Found {len(tables)} tables in the database:")
        for table in tables:
            print(f"  - {table}")
        return tables
    except Exception as e:
        print(f"Error listing tables: {e}")
        return []
    finally:
        engine.dispose()


def get_row_count(table_name):
    """
    Get the number of rows in a table
    
    Args:
        table_name: Name of the table
        
    Returns:
        int: Number of rows
    """
    engine = create_engine(DATABASE_URL)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
            print(f"Table '{table_name}' has {count} rows.")
            return count
    except Exception as e:
        print(f"Error getting row count for '{table_name}': {e}")
        return None
    finally:
        engine.dispose()


def execute_raw_sql(sql_query, fetch_results=True):
    """
    Execute raw SQL query
    
    Args:
        sql_query: The SQL query to execute
        fetch_results: Whether to fetch and return results
        
    Returns:
        pandas.DataFrame or None: Query results if fetch_results=True
    """
    engine = create_engine(DATABASE_URL)
    
    try:
        if fetch_results:
            # For SELECT queries
            with engine.connect() as conn:
                result = pd.read_sql(sql_query, conn)
            print(f"Query executed successfully. Returned {len(result)} rows.")
            return result
        else:
            # For DDL/DML queries (CREATE, INSERT, UPDATE, DELETE)
            with engine.connect() as conn:
                result = conn.execute(text(sql_query))
                conn.commit()
                print(f"Query executed successfully. Affected rows: {result.rowcount}")
                return None
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        return None
    finally:
        engine.dispose()


def backup_table(table_name, backup_table_name=None):
    """
    Create a backup copy of a table
    
    Args:
        table_name: Name of the source table
        backup_table_name: Name of the backup table (optional)
        
    Returns:
        str: Name of the backup table created
    """
    if backup_table_name is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_table_name = f"{table_name}_backup_{timestamp}"
    
    engine = create_engine(DATABASE_URL)
    
    try:
        with engine.connect() as conn:
            # Create backup table as a copy of the original
            sql = f"CREATE TABLE {backup_table_name} AS SELECT * FROM {table_name}"
            conn.execute(text(sql))
            conn.commit()
        
        print(f"Backup table '{backup_table_name}' created successfully.")
        return backup_table_name
    except Exception as e:
        print(f"Error creating backup of table '{table_name}': {e}")
        return None
    finally:
        engine.dispose()


def create_index(table_name, column_names, index_name=None, unique=False):
    """
    Create an index on one or more columns
    
    Args:
        table_name: Name of the table
        column_names: List of column names or single column name
        index_name: Name of the index (optional)
        unique: Whether to create a unique index
    """
    if isinstance(column_names, str):
        column_names = [column_names]
    
    if index_name is None:
        index_name = f"idx_{table_name}_{'_'.join(column_names)}"
    
    engine = create_engine(DATABASE_URL)
    
    try:
        with engine.connect() as conn:
            columns_str = ', '.join(column_names)
            unique_str = 'UNIQUE ' if unique else ''
            sql = f"CREATE {unique_str}INDEX {index_name} ON {table_name} ({columns_str})"
            conn.execute(text(sql))
            conn.commit()
        
        print(f"Index '{index_name}' created successfully on table '{table_name}'.")
    except Exception as e:
        print(f"Error creating index on table '{table_name}': {e}")
    finally:
        engine.dispose()



def get_or_create_sportsbook_id(sportsbook_key):
    """
    Get the UUID for a sportsbook by its key, creating it if it doesn't exist.
    
    Args:
        sportsbook_key: The key identifier for the sportsbook
        
    Returns:
        str: UUID of the sportsbook
    """
    engine = create_engine(DATABASE_URL)
    
    try:
        with engine.connect() as conn:
            # First, try to find existing sportsbook
            select_sql = """
            SELECT id FROM sportsbooks WHERE key = :sportsbook_key
            """
            result = conn.execute(text(select_sql), {'sportsbook_key': sportsbook_key})
            row = result.fetchone()
            
            if row:
                return str(row[0])
            
            # If not found, create new sportsbook
            insert_sql = """
            INSERT INTO sportsbooks (name, key, created_at_utc)
            VALUES (:name, :key, NOW())
            RETURNING id
            """
            result = conn.execute(text(insert_sql), {
                'name': sportsbook_key.replace('_', ' ').title(),
                'key': sportsbook_key
            })
            conn.commit()
            new_id = result.fetchone()[0]
            print(f"Created new sportsbook: {sportsbook_key} with ID: {new_id}")
            return str(new_id)
            
    except Exception as e:
        print(f"Error getting/creating sportsbook ID for '{sportsbook_key}': {e}")
        # Fallback to UUID generation
        
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"sportsbook_{sportsbook_key}"))
    finally:
        engine.dispose()


def get_or_create_betting_market_id(event_id, market_key, description, point_value, ignore_check_existing=False):
    """
    Get the UUID for a betting market, creating it if it doesn't exist.
    
    Args:
        event_id: The game event ID
        market_key: The market type key
        description: Player description
        point_value: The point/line value
        
    Returns:
        str: UUID of the betting market
    """
    engine = create_engine(DATABASE_URL)
    
    try:
        with engine.connect() as conn:
            # Create a unique identifier for the betting market
            market_identifier = f"{event_id}_{market_key}_{description}_{point_value}"
            if not ignore_check_existing:
                

                # First, try to find existing betting market
                select_sql = """
                SELECT id FROM betting_markets 
                WHERE game_event_id::text = :event_id 
                AND market_category_id::text LIKE :market_key
                AND specifiers->>'description' = :description
                AND specifiers->>'point' = :point_value
                """
                result = conn.execute(text(select_sql), {
                    'event_id': event_id,
                    'market_key': f"%{market_key}%",
                    'description': description,
                    'point_value': str(point_value)
                })
                row = result.fetchone()
                
                if row:
                    return str(row[0])
            
                # If not found, generate a deterministic UUID
                import uuid
                betting_market_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, market_identifier))
                print(f"Generated betting market ID: {betting_market_id} for {market_identifier}")

            else:
                # If not found, generate a deterministic UUID
                import uuid
                betting_market_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, market_identifier))
                print(f"Generated betting market ID: {betting_market_id} for {market_identifier}")
            return betting_market_id
            
    except Exception as e:
        print(f"Error getting/creating betting market ID: {e}")
        # Fallback to UUID generation
        import uuid
        market_identifier = f"{event_id}_{market_key}_{description}_{point_value}"
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, market_identifier))
    finally:
        engine.dispose()


def upsert_market_lines(df, table_name='market_lines'):
    """
    Upsert data into the market_lines table using PostgreSQL's ON CONFLICT clause.
    
    For market_lines table, we'll use a simpler conflict resolution based on:
    - betting_market_id, sportsbook_id, outcome_name
    And we'll handle the time-based logic in the UPDATE clause
    
    Args:
        df: pandas DataFrame containing market lines data
        table_name: Name of the target table (default: 'market_lines')
    """
    if df.empty:
        print("No data to upsert into market_lines table.")
        return
        
    engine = create_engine(DATABASE_URL)
    
    try:
        # Convert DataFrame to records for bulk insert
        records = df.to_dict('records')
        
        # Create the upsert SQL statement with simpler conflict resolution
        upsert_sql = f"""
        INSERT INTO {table_name} (
            fetched_at_utc, betting_market_id, sportsbook_id, line_value, 
            price_decimal, price_american, outcome_name
        ) VALUES (
            :fetched_at_utc, :betting_market_id, :sportsbook_id, :line_value,
            :price_decimal, :price_american, :outcome_name
        )
        ON CONFLICT (betting_market_id, sportsbook_id, outcome_name)
        DO UPDATE SET
            line_value = EXCLUDED.line_value,
            price_decimal = EXCLUDED.price_decimal,
            price_american = EXCLUDED.price_american,
            fetched_at_utc = EXCLUDED.fetched_at_utc
        WHERE EXCLUDED.fetched_at_utc > {table_name}.fetched_at_utc
        """
        
        with engine.connect() as conn:
            # First, ensure the simpler unique constraint exists
            create_constraint_sql = f"""
            ALTER TABLE {table_name} 
            DROP CONSTRAINT IF EXISTS unique_market_line_hourly;
            
            ALTER TABLE {table_name} 
            DROP CONSTRAINT IF EXISTS unique_market_line_simple;
            
            ALTER TABLE {table_name} 
            ADD CONSTRAINT unique_market_line_simple 
            UNIQUE (betting_market_id, sportsbook_id, outcome_name);
            """
            
            try:
                conn.execute(text(create_constraint_sql))
                conn.commit()
                print("Created unique constraint for market lines")
            except Exception as constraint_error:
                print(f"Note: Constraint creation skipped: {constraint_error}")
            
            # Execute the upsert for each record
            successful_upserts = 0
            for record in records:
                try:
                    conn.execute(text(upsert_sql), record)
                    successful_upserts += 1
                except Exception as record_error:
                    print(f"Error upserting record: {record_error}")
                    print(f"Problematic record: {record}")
            
            conn.commit()
            print(f"Successfully upserted {successful_upserts} out of {len(records)} records into {table_name} table.")
            
    except Exception as e:
        print(f"Error upserting data into '{table_name}': {e}")
    finally:
        engine.dispose()


def create_schema(db_uri=DATABASE_URL):
    """
    Connects to a PostgreSQL database and creates the BountyGate schema.

    This script is idempotent and can be run multiple times safely.
    It creates necessary extensions, custom types, and all tables with their
    respective constraints and relationships.

    Args:
        db_uri (str): The database connection URI.
                      Example: "postgresql://user:password@host:port/dbname"
    """
    conn = None
    try:
        print("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(db_uri)
        cur = conn.cursor()

        print("Creating schema for BountyGate...")

        # --- Extensions and Custom Types ---
        print("Step 1: Creating extensions and custom types...")
        cur.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")

        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'wager_status_enum') THEN
                    CREATE TYPE wager_status_enum AS ENUM ('pending', 'won', 'lost', 'push', 'void');
                END IF;
            END$$;
        """)

        # --- Core Sports Entities Layer ---
        print("Step 2: Creating Core Sports Entities Layer...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sports (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                name VARCHAR(50) UNIQUE NOT NULL,
                key VARCHAR(50) UNIQUE NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS leagues (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                sport_id UUID NOT NULL REFERENCES sports(id) ON DELETE CASCADE,
                name VARCHAR(100) UNIQUE NOT NULL,
                abbreviation VARCHAR(10) UNIQUE NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS teams (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                league_id UUID NOT NULL REFERENCES leagues(id) ON DELETE RESTRICT,
                name VARCHAR(100) NOT NULL,
                abbreviation VARCHAR(10) NOT NULL,
                location VARCHAR(100),
                logo_url TEXT,
                created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(league_id, name)
            );

            CREATE TABLE IF NOT EXISTS players (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                full_name VARCHAR(150) NOT NULL,
                first_name VARCHAR(75),
                last_name VARCHAR(75),
                birth_date DATE,
                height_inches SMALLINT,
                weight_lbs SMALLINT,
                primary_position VARCHAR(10),
                headshot_url TEXT,
                created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS player_team_contracts (
                player_id UUID NOT NULL REFERENCES players(id) ON DELETE CASCADE,
                team_id UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
                start_date DATE NOT NULL,
                end_date DATE,
                PRIMARY KEY (player_id, team_id, start_date)
            );

            CREATE TABLE IF NOT EXISTS game_events (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                league_id UUID NOT NULL REFERENCES leagues(id) ON DELETE RESTRICT,
                home_team_id UUID NOT NULL REFERENCES teams(id) ON DELETE RESTRICT,
                away_team_id UUID NOT NULL REFERENCES teams(id) ON DELETE RESTRICT,
                event_datetime_utc TIMESTAMPTZ NOT NULL,
                location VARCHAR(255),
                status VARCHAR(20) NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        # --- Market Data Ingestion Layer ---
        print("Step 3: Creating Market Data Ingestion Layer...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sportsbooks (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                name VARCHAR(100) UNIQUE NOT NULL,
                key VARCHAR(50) UNIQUE NOT NULL,
                website_url TEXT,
                created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS betting_market_categories (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                sport_id UUID NOT NULL REFERENCES sports(id) ON DELETE CASCADE,
                name VARCHAR(100) NOT NULL,
                key VARCHAR(100) NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(sport_id, key)
            );

            CREATE TABLE IF NOT EXISTS betting_markets (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                game_event_id UUID NOT NULL REFERENCES game_events(id) ON DELETE CASCADE,
                market_category_id UUID NOT NULL REFERENCES betting_market_categories(id) ON DELETE RESTRICT,
                player_id UUID REFERENCES players(id) ON DELETE SET NULL,
                specifiers JSONB,
                created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS market_lines (
                id BIGSERIAL PRIMARY KEY,
                betting_market_id UUID NOT NULL REFERENCES betting_markets(id) ON DELETE CASCADE,
                sportsbook_id UUID NOT NULL REFERENCES sportsbooks(id) ON DELETE CASCADE,
                line_value DECIMAL(8, 2),
                price_decimal DECIMAL(8, 4),
                price_american SMALLINT,
                outcome_name VARCHAR(50) NOT NULL,
                fetched_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        
        # --- Entity Mapping Layer ---
        print("Step 4: Creating Entity Mapping Layer...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS entity_source_mappings (
                entity_id UUID NOT NULL,
                source_id UUID NOT NULL REFERENCES sportsbooks(id) ON DELETE CASCADE,
                source_entity_id VARCHAR(255) NOT NULL,
                entity_type VARCHAR(50) NOT NULL, -- e.g., 'player', 'team', 'game_event'
                created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (source_id, source_entity_id, entity_type)
            );
        """)

        # --- User Application Layer ---
        print("Step 5: Creating User Application Layer...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                email VARCHAR(255) UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_login_utc TIMESTAMPTZ
            );

            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                display_name VARCHAR(50) UNIQUE,
                avatar_url TEXT
            );

            CREATE TABLE IF NOT EXISTS betting_strategies (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                name VARCHAR(100) NOT NULL,
                description TEXT,
                created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS wagers (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                betting_strategy_id UUID REFERENCES betting_strategies(id) ON DELETE SET NULL,
                market_line_id BIGINT NOT NULL REFERENCES market_lines(id) ON DELETE RESTRICT,
                wagered_amount DECIMAL(10, 2) NOT NULL,
                wager_status wager_status_enum NOT NULL DEFAULT 'pending',
                payout_amount DECIMAL(10, 2),
                placed_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        conn.commit()
        print("Schema creation process completed successfully.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while creating PostgreSQL schema: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")


