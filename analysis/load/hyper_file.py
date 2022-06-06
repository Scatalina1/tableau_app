from tableau_api_lib.utils.querying import get_projects_dataframe
from tableauhyperapi import NOT_NULLABLE, NULLABLE, HyperProcess, Connection, Nullability, TableDefinition, SqlType, Telemetry, Inserter, CreateMode, TableName
from tableau_api_lib.utils.querying import get_projects_dataframe

## this script creates the datasource for the dashboard that will be published

def create_hyper_file(conn, my_df, project_name):
    """
    create_hyper_file is a function of the tableau server connection (conn), the data retrieved from the server (my_df), ...
    and the project where the data source and the dashboard will be stored
    the function returns the project id that will be used to publish the workbook
    """
    PATH_TO_HYPER = 'metadata_extract.hyper'

    project_name = project_name
    my_df = my_df

    my_df.fillna(" ", inplace=True)

    # Step 1: Start a new private local Hyper instance
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU, user_agent='MetaApp' ) as hyper:

    # Step 2:  Create the the .hyper file, replace it if it already exists
        with Connection(endpoint=hyper.endpoint, 
                        create_mode=CreateMode.CREATE_AND_REPLACE,
                        database=PATH_TO_HYPER) as connection:

    # Step 3: Create the schema
            connection.catalog.create_schema('Extract')

    # Step 4: Create the table definition
            schema = TableDefinition(table_name=TableName('Extract','Extract'),
                columns=[
                TableDefinition.Column('COLUMN_ID', SqlType.varchar(max_length= 500), NOT_NULLABLE),
                TableDefinition.Column('COLUMN_NAME', SqlType.varchar(max_length= 500), NOT_NULLABLE),
                TableDefinition.Column('DATABASE.UPSTR_DB_ID', SqlType.varchar(max_length= 500)),
                TableDefinition.Column('database.upstr_db_isEmbedded', SqlType.bool()),
                TableDefinition.Column('database.upstr_db_name', SqlType.varchar(max_length= 500)),
                TableDefinition.Column('database.upstr_db_type', SqlType.varchar(max_length= 500)),
                TableDefinition.Column('database_hostname', SqlType.varchar(max_length= 500)),
 #               TableDefinition.Column('database_port', SqlType.varchar(max_length=500)),
                TableDefinition.Column('EMB_DS_NAME', SqlType.varchar(max_length= 500)),
                TableDefinition.Column('EMB_DS_REFRESH_TIME', SqlType.varchar(max_length=500)),
                TableDefinition.Column('FIELD_ID', SqlType.varchar(max_length= 500), NOT_NULLABLE),
                TableDefinition.Column('FIELD_NAME',SqlType.varchar(max_length= 500), NOT_NULLABLE),
                TableDefinition.Column('FORMULA', SqlType.text()),
                TableDefinition.Column('OWNER.NAME',SqlType.varchar(max_length= 500), NOT_NULLABLE),
                TableDefinition.Column('site.name', SqlType.varchar(max_length= 500), NOT_NULLABLE),
                TableDefinition.Column('UPSTR_SCHEMA', SqlType.varchar(max_length= 500), NOT_NULLABLE),
 #               TableDefinition.Column('TABLE_NAME', SqlType.varchar(max_length= 500), NOT_NULLABLE),
                TableDefinition.Column('UPSTR_TABLE_ID', SqlType.varchar(max_length= 500), NOT_NULLABLE),
                TableDefinition.Column('UPSTR_TABLE_NAME', SqlType.varchar(max_length= 500), NOT_NULLABLE),
                TableDefinition.Column('VIEW_ID', SqlType.varchar(max_length= 500), NOT_NULLABLE),
                TableDefinition.Column('VIEW_NAME', SqlType.varchar(max_length= 500), NOT_NULLABLE),
                TableDefinition.Column('VIEW_TYPE', SqlType.varchar(max_length= 500), NOT_NULLABLE),
                TableDefinition.Column('WORKBOOK_ID', SqlType.varchar(max_length= 500), NOT_NULLABLE),
                TableDefinition.Column('WORKBOOK_NAME', SqlType.varchar(max_length= 500), NOT_NULLABLE),
                TableDefinition.Column('WORKBOOK_PROJECT', SqlType.varchar(max_length= 500), NOT_NULLABLE),
                TableDefinition.Column('WORKBOOK_URI', SqlType.varchar(max_length= 500), NOT_NULLABLE)           
             ])


    # Step 5: Create the table in the connection catalog
            connection.catalog.create_table(schema)

            with Inserter(connection, schema) as inserter:
                for index, row in my_df.iterrows():
                    inserter.add_row(row)
                inserter.execute()

        print("The connection to the Hyper file is closed.")

    conn.sign_in()
    projects_df = get_projects_dataframe(conn)
    projects_df = (projects_df[['name', 'id']])

    # Extract project ID
    projectId = projects_df[projects_df['name']==project_name].pop('id')
    projectId = (projectId.values[0])

    response = conn.publish_data_source(datasource_file_path=PATH_TO_HYPER,
                                       datasource_name='MetaData',
                                      project_id=projectId)
    
    response = (response.json())
    print(response)
    conn.sign_out()
    return projectId

if __name__ == "__main__":
        create_hyper_file(conn, my_df, project_name)
