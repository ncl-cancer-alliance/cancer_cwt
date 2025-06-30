from snowflake.snowpark.session import Session
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