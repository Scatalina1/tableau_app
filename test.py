import pandas as pd
from tableau_api_lib import TableauServerConnection
import streamlit as st

from analysis.integrate.impact_analysis import graphql_queries
from analysis.load.hyper_file import create_hyper_file
from tableau_api_lib.utils.querying import get_projects_dataframe, get_workbooks_dataframe


## this script serves as test for the analysis (retrieving different metadata and joining data, create and publish hyper file and publish workbook)
server_config = {
        'my_env': {
                'server': 'http://tableau-dev.bi-concepts.ch',
                'api_version': '3.15',
                'username': os.environ.get('TABLEAU_USER'),
                'password': os.environ.get('TABLEAU_PASSWORD'),
                'site_name': '""',
                'site_url': ''
                }
            }

conn = TableauServerConnection(config_json=server_config,env='my_env')

df = graphql_queries(conn=conn)


response = create_hyper_file(conn = conn, my_df = df)

#print(response)
conn.sign_in()

projectId = create_hyper_file(conn=conn, my_df=combined_df, project_name=firstlevel)
webpageurl = publish_workbook(conn, projectId)

conn.sign_out()
