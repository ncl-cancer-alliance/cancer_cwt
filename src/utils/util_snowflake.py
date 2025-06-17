from snowflake.snowpark.session import Session

def snowpark_session_create(connection_params):
    """
    Create a Snowpark session using the provided configuration.
    param config: Dictionary containing connection parameters:
        - account: Snowflake account name
        - user: Snowflake username
        - authenticator: Snowflake authenticator (e.g., 'externalbrowser', 'snowflake')
        - warehouse: Snowflake warehouse
        - role: Snowflake role
        - database: Snowflake database
    Returns:
        - session: Snowpark session object
   """

    session = Session.builder.configs(connection_params).create()

    return session