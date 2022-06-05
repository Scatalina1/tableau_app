# -*- coding: utf-8 -*-

import pandas as pd
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import flatten_dict_list_column
import streamlit as st


#  define the GraphQL queries to run against the Metadata API

def graphql_queries(conn):

  conn.sign_in()

  query_sheets_fields = """
  {
  sheets {  
        view_name: name
        view_id: id
        upstreamFields
            {
          field_id: id
          field_name: name
                upstreamColumns {
            column_id: luid	
            column_name: name
          }
        }
      }
  }
  """
  query_dashboards_fields = """
  {
  dashboards {
        view_name: name
        view_id: id
        upstreamFields
            {
          field_id: id
          field_name: name
                upstreamColumns {
            column_id: luid	
            column_name: name
          }
        }
      }
  }
  """
  query_columns = """
  {
  columns {
          column_id : luid
           table {
              upstr_table_id: id
              table_name: name
            }
          }
  } 
  """
  query_workbooks = """
  {
    workbooks {
       site
      {
        name 
      }
      workbook_name: name
      workbook_id: luid
      workbook_project: projectName
      views {
        view_type: __typename
        view_id: id

      }
      upstreamTables {
        upstr_table_name: name
        upstr_table_id: id
        database {
          upstr_db_name: name
          upstr_db_type: connectionType
          upstr_db_id: luid
          upstr_db_isEmbedded: isEmbedded
        }
      }
      upstreamDatasources {
        upstr_ds_name: name
        upstr_ds_id: luid
        upstr_ds_project: projectName
      }
      embeddedDatasources {
        emb_ds_name: name
        emb_ds_refresh_time: extractLastRefreshTime
      }
    }
  }
  """
  query_databases = """
  {
    databaseServers {
      database_hostname: hostName
  		database_port: port
      upstr_db_id: luid
    }
  }
  """


  # query_project and link with workbook

  #  query metadata from our Tableau environment
  # databases
  db_query_results = conn.metadata_graphql_query(query_databases)
  db_query_results_json = db_query_results.json()['data']['databaseServers']
  db_df = pd.DataFrame(db_query_results_json)
  
  # workbooks
  wb_query_results = conn.metadata_graphql_query(query_workbooks)

  #views
  wb_query_results_json = wb_query_results.json()['data']['workbooks']
  wb_views_df = pd.json_normalize(data=wb_query_results_json, record_path='views', meta='workbook_id')

  #tables
  wb_tables_dbs_df = pd.json_normalize(data=wb_query_results_json, record_path='upstreamTables', meta='workbook_name')
  #wb_tables_dbs_df = flatten_dict_list_column(df=wb_tables_df, col_name='Database')
  wb_df = pd.json_normalize(wb_query_results.json()['data']['workbooks'])
  print(wb_df['workbook_name'].drop_duplicates)
  wb_df.drop(columns=['workbook_name','views', 'upstreamTables', 'upstreamDatasources', 'embeddedDatasources'], inplace=True)
  
  # datasources
  wb_uds_df = pd.json_normalize(data=wb_query_results_json, record_path='upstreamDatasources', meta='workbook_id')
  wb_eds_df = pd.json_normalize(data=wb_query_results_json, record_path='embeddedDatasources', meta='workbook_id')

  #columns
  co_query_results = conn.metadata_graphql_query(query_columns)
  co_query_results_json = co_query_results.json()['data']['columns']
  wb_columns_df = pd.json_normalize(data=co_query_results_json)
  
  #fields dashboards
  fi_query_results = conn.metadata_graphql_query(query_dashboards_fields)
  fi_query_results_json = fi_query_results.json()['data']['dashboards']
  wb_fields_df = pd.json_normalize(data=fi_query_results_json, record_path= 'upstreamFields', meta=['view_name', 'view_id'])
  wb_fields_all = flatten_dict_list_column(df=wb_fields_df, col_name='upstreamColumns')
  #wb_fields_col = pd.json_normalize(data=fi_query_results_json, record_path= 'upstreamColumns',meta=['view_id'])
  wb_fields = pd.json_normalize(data=fi_query_results_json)

  #fields sheets
  fis_query_results = conn.metadata_graphql_query(query_sheets_fields)
  fis_query_results_json = fis_query_results.json()['data']['sheets']
  wb_fields_sheet = pd.json_normalize(data=fis_query_results_json, record_path= 'upstreamFields', meta=['view_id','view_name'])
  wb_fields_sheet_all = flatten_dict_list_column(df=wb_fields_sheet, col_name='upstreamColumns')

  # union fields for sheets and dashboards
  fields_union = pd.concat([wb_fields_all, wb_fields_sheet_all])

  conn.sign_out()

  # merge data from all queries
  combined_df = fields_union.merge(wb_views_df, how='left', on='view_id')
  combined_df = combined_df.merge(wb_columns_df, how='left', on='column_id')
  combined_df = combined_df.merge(wb_tables_dbs_df, how='left', right_on='upstr_table_id', left_on='table.upstr_table_id')
  combined_df = combined_df.merge(wb_uds_df, how='left', on='workbook_id')
  combined_df = combined_df.merge(wb_eds_df, how='left', on='workbook_id')
  combined_df = combined_df.merge(wb_df, how='left', on='workbook_id')
  combined_df = combined_df.merge(db_df, how='left', left_on='database.upstr_db_id', right_on='upstr_db_id')
  
  # dropped some columns to make the process work if not all fields are available on the server
  ## this is just a workaround! 
  combined_df = combined_df.sort_index(axis=1)
  try:
    combined_df.drop(columns=['database_port','upstr_db_id','upstr_table_id','upstr_ds_id','upstr_ds_name', 'upstr_ds_project'], inplace = True)
  except KeyError:
    combined_df.drop(columns=['database_port','upstr_db_id','upstr_table_id'], inplace = True)
  finally:
    print(combined_df.columns)
  return combined_df


if __name__ == '__main__':
  graphql_queries(conn)
