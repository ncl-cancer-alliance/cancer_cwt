import pandas as pd

from snowflake.snowpark.session import Session
from snowflake import connector as sfc
from snowflake.ml.feature_store import FeatureStore, CreationMode

def snowpark_session_create(connection_params, query_tag=False):
    """
    Create a Snowpark session using the provided configuration.
    connection_params: Dictionary containing connection parameters:
        - account: Snowflake account name
        - user: Snowflake username
        - authenticator: Snowflake authenticator (e.g., 'externalbrowser', 'snowflake')
        - warehouse: Snowflake warehouse
        - role: Snowflake role
        - database: Snowflake database
    query_tag: Tag for logging and monitoring purposes
    Returns:
        - session: Snowpark session object
   """

    session = Session.builder.configs(connection_params).create()

    if query_tag:
        session.query_tag = query_tag

    return session

def snowflake_connection_create(connection_params, query_tag=False):
    """
    Create a Snowflake connection using the provided configuration.
    connection_params: Dictionary containing connection parameters:
        - account: Snowflake account name
        - user: Snowflake username
        - authenticator: Snowflake authenticator (e.g., 'externalbrowser', 'snowflake')
        - warehouse: Snowflake warehouse
        - role: Snowflake role
        - database: Snowflake database
    query_tag: Tag for logging and monitoring purposes
    Returns:
        - session: Snowflake connection object
    """
    
    session_parameters = {}
    if query_tag:
        session_parameters["QUERY_TAG"] = query_tag

    ctx = sfc.connect(
    account="ATKJNCU-NCL",
    user= "JAKE.KEALEY@NHS.NET",
    authenticator= "externalbrowser",
    role= "NCL-USERGROUP-STAFF-SNOWFLAKE-BI-ANALYST",
    warehouse= "NCL_ANALYTICS_XS",
    database= "DATA_LAB_NCL_TRAINING_TEMP",
    schema= "CANCER_CWT",
    session_parameters = session_parameters
    )

    return ctx

def pull_data_from_query(query, 
                         connection=False, connection_params={},
                         query_tag=False):
    """
    Runs a SELECT query and returns the result as a pandas dataframe.
    query: String containing the SELECT query
    
    EITHER PASS CONNECTION_PARAMS OR A SNOWFLAKE CONNECTION OBJECT. 
    IF BOTH ARE PASSED THIS WILL USE THE CONNECTION BY DEFAULT.
    connection: Connection object from snowflake.connector.connect()
    connection_params: Dictionary containing connection parameters:
        - account: Snowflake account name
        - user: Snowflake username
        - authenticator: Snowflake authenticator (e.g., 'externalbrowser', 'snowflake')
        - warehouse: Snowflake warehouse
        - role: Snowflake role
        (Optional)
        - database: Snowflake database
        - schema: Snowflake schema
        query_tag: Tag for logging and monitoring purposes (This is only used with connection_params)

    Returns:
        - df: Pandas dataframe containing the query results
    """

    #Use passed connection method
    if connection == False:
        ctx = snowflake_connection_create(connection_params, query_tag)
    else:
        ctx = connection

    #Execute the query
    cur = ctx.cursor()
    cur.execute(query)

    #Return the result
    return cur.fetch_pandas_all()


def load_feature_store(session, database, name, warehouse="NCL_ANALYTICS_XS"):
    
    """
    Load an existing Snowflake Feature Store by name.
    session: Object for the Snowflake connection
    database: Name of the database containing the feature store
    name: Name of the feature store
    warehouse: Name of the default warehouse to use for processing.
    Returns:
        - fs: Object representing the Feature Store
    """
    
    fs = FeatureStore(
        session=session,
        database=database,
        name=name,
        default_warehouse=warehouse,
        creation_mode=CreationMode.FAIL_IF_NOT_EXIST
    )

    return fs

def load_entity(feature_store, entity_name):

    """
    Load an existing Snowflake Entity by name.
    feature_store: Object for the relevant feature store
    entity_name: The name of the target entity
    Returns:
        - entity: Object representing the entity
   """

    try:
        entity = feature_store.get_entity(name=entity_name)
    except ValueError:
        raise Exception(f"{entity_name} entity not found.")
    except Exception as e:
        raise e

    return entity

def create_dynamic_features(transformation_func, params):
    connection_params = {
        "account": params["account"],
        "user": params["user"],
        "authenticator": params["authenticator"],
        "role": params["role"],
        "warehouse": params["warehouse"],
        "database": params["session_database"],
        "schema": params["session_schema"]
    }

    session = snowpark_session_create(
        connection_params, params["query_tag"])
    
    #Load the base data
    df_base = session.table(params["base_table"])

    #Create the dynamic table
    destination_full = ".".join([
        params["destination_database"],
        params["destination_schema"],
        params["destination_table"],
    ])
    
    if "fdt_lag" not in params.keys():
        params["fdt_lag"] = "24 hours"

    if "fdt_mode" not in params.keys():
        params["fdt_mode"] = "overwrite"

    if "fdt_refresh_mode" not in params.keys():
        params["fdt_refresh_mode"] = "FULL"

    if "fdt_initialize" not in params.keys():
        params["fdt_initialize"] = "ON_CREATE"

    
    transformation_func(df_base).create_or_replace_dynamic_table(
        name=destination_full,
        warehouse=params["warehouse"],
        lag=params["fdt_lag"],
        comment=params["fdt_comment"],
        mode=params["fdt_mode"],
        refresh_mode=params["fdt_refresh_mode"],
        initialize=params["fdt_initialize"] 
    )