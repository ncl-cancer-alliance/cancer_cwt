from snowflake.snowpark.session import Session
from snowflake.snowpark.context import get_active_session

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

    return session