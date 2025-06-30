from snowflake.snowpark.session import Session

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

def load_entity(feature_store, entity_name):

    """
    Load a Snowflake Entity by name.
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