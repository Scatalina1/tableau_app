import pandas as pd
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_projects_dataframe, get_workbooks_dataframe

# publishes the dashboard from workbook/ImpactAnalysis.twb to the selected project in the active Tableau server connection

def publish_workbook(conn, project_id):
    """
    to publish the workbook we need a Tableau server connection (conn)
    and a project id to store the workbook (project_id)
    """
    conn.sign_in()
    projectId = project_id
    ## .twb file without data ##
    
    # workbook_name will be replaced with a user input to select the name of the dashboard
    # connection_username, connection_password and server_address is related to the datasource of the workbook.
    # since the original workbook is from tableau.bi-concepts.ch, we need to pass this url as server address
    publish_response = conn.publish_workbook(
    workbook_file_path='workbook/ImpactAnalysis.twb', workbook_name='ImpactAnalysisApp', show_tabs_flag='false',
    project_id=projectId, port_number='443' ,connection_username='', connection_password='', server_address='tableau.bi-concepts.ch')
    #, parameter_dict= {'workbook-file-type' : 'twb','overwrite': 'overwrite=true', 'skip-connection-check-flag' : 'skip-connection-check-flag=true'})

    print(publish_response.json())
    # extract the workbook url from the api response to provide a direkt link to the dashboard
    try:
            data = pd.json_normalize(publish_response.json()['workbook'])
            tableau_workbook_url = (data['webpageUrl'].values[0])
    except KeyError:
            print('Workbook could not be published')
    finally:
            conn.sign_out()
    return tableau_workbook_url


if __name__ == '__main__':
    publish_workbook(conn, project_id)
