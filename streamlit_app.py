# streamlit imports
import streamlit as st
import streamlit_authenticator as stauth

# general python imports
import pandas as pd
import time
import requests
from requests.exceptions import ConnectionError
import os

# tableau libraries
import tableauserverclient as TSC
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_projects_dataframe

# customer functions import
from analysis.integrate.impact_analysis import graphql_queries
from analysis.load.hyper_file import create_hyper_file
from analysis.load.publish_workbook import publish_workbook

# page configuration with logo and menu items
st.set_page_config(
     page_icon=('Logo_single_transparent.png'),
     initial_sidebar_state="expanded",
     menu_items={
         'Get Help': 'https://www.bi-concepts.ch/contact.html',
         'Report a bug': "https://www.bi-concepts.ch/contact.html",
         'About': "This application helps you keep up with changes that are linked to Tableau dashboards. This is an *extremely* cool app!"
     }
 )

# structure for title and logo
col1, mid, col2 = st.columns([15,2,2])
with col1:
    st.title("Welcome to BICLUID")
with col2:
    st.image('Logo_single_transparent.png', width=60)
st.text('This app is powered by BI Concepts GmbH')

# define user names and passwords for login
names = ['Benjamin','Selma','Dev']
usernames = ['Benny','Selma','Dev']
passwords = ['Bicli0987.','hello123','test000!']
hashed_passwords = stauth.Hasher(passwords).generate()

authenticator = stauth.Authenticate(names,usernames,hashed_passwords,
    'some_cookie_name','some_signature_key',cookie_expiry_days=30)

name, authentication_status, username = authenticator.login('Login','main')

if authentication_status:
    authenticator.logout('Logout', 'main')
    st.write('Welcome *%s*' % (name))
    st.title('Set up your Tableau Server Connection')
elif authentication_status == False:
    st.error('Username/password is incorrect')
elif authentication_status == None:
    st.warning('Please enter your username and password')

# retrieve Tableau Server API version, this is needed to set up server connection
def get_version(url):
    server = TSC.Server(url, use_server_version=True)
    api_version = (server.version)
    return api_version

# define Tableau server configuration
def tableau_config(server_url, user ,password, api_version):
    tableau_server_config = {
        'my_env': {
                'server': server_url,
                'api_version': api_version,
                'username': user,
                'password': password,
                'site_name': '""',
                'site_url': ''
                }
            }
    return tableau_server_config

# set up connection based on tableau configuration. TableauServerConnection is provided by the python library tableau_api_lib 
@st.cache(allow_output_mutation=True)
def tableau_conn(url, user, password, api_version):
    server_config = tableau_config(url_input, user_input, password_input, api_version)
    conn = TableauServerConnection(config_json=server_config,env='my_env')
    return conn


# function to retrieve the 'parent' projects from the tableau server. 
# it serves as dropdown selection for the user
def get_projects():
    connection = conn
    connection.sign_in()
    projects_df = get_projects_dataframe(connection)
    projects_df = (projects_df[['name', 'id', 'topLevelProject','parentProjectId']])
    toplevel_project_df = projects_df[projects_df['parentProjectId'].isna()]
    toplevel_projects = toplevel_project_df['name']
    connection.sign_out()
    return toplevel_projects

# with this function I tried to imitate the tree structure for the user input for projects on the tableau server
# structure starts with parent project, and can be extended to as many child projects as prefered
## THIS FUNCTION IS NOT USED ##
def next_projects(toplevel):
    connection = conn
    connection.sign_in()
    projects_df = get_projects_dataframe(connection)
    projects_df = (projects_df[['name', 'id', 'topLevelProject','parentProjectId']])
    toplevelproject = projects_df[projects_df['name']==toplevel]

    secondlevel_project_df = toplevelproject[toplevelproject['parentProjectId']]
    secondlevel_projects = secondlevel_project_df['name']
    connection.sign_out()
    return secondlevel_projects



# --- Initialising SessionState ---
if "load_state" not in st.session_state:
     st.session_state.load_state = False
    
if authentication_status:
        st.write('Welcome', name, '! Set up your Tableau server connection')
        url_input = st.text_input('URL (https://<YOUR_SERVER_NAME>.com)')
        user_input =st.text_input('User')
        password_input = st.text_input('Password', type = 'password')
        col1, col2 = st.columns(2)

        # set up tableau server connection with the configuration set by the user
        if  col1.button('Test') or st.session_state.load_state:
            if url_input and user_input and password_input:
                version = get_version(url=url_input)
                conn = tableau_conn(url=url_input, user=user_input, password = password_input, api_version = version)
                st.session_state.load_state = True
                try: 
                    conn.server_info()
                    conn.sign_in()
                    st.success('**Connection suceeded!** :rocket:')
                    response = conn.query_sites()
                    site_list = [(site['name']) for site in response.json()['sites']['site']]
                    list_df = pd.DataFrame(site_list)
                    options = st.multiselect('Select the relevant Tableau sites', list_df)
                    conn.sign_out()
                except KeyError as k: 
                    st.error("Invalid user name or password")
                except ConnectionError as e: 
                    st.error("Oops!  This URL does not exist.  Try again...")
#                if conn.sign_in().status_code == 200:
#                    st.write('**Connection suceeded!** :rocket:')
##                    conn.sign_in()
#                    response = conn.query_sites()
#                    site_list = [(site['name']) for site in response.json()['sites']['site']]
#                    list_df = pd.DataFrame(site_list)
#                    options = st.multiselect('Select the relevant Tableau sites', list_df)
#                else:
#                    st.error('Username/password is incorrect')
                
        if st.button('Confirm selection') or st.session_state.load_state:
             # the process only works for one site per connection for now
            st.write('You selected the following site(s):',(','.join(options)) ,'!')
            st.header('Destination for your Analysis Dashboard')
            projects = get_projects()
            firstlevel = st.selectbox("Select an existing project to store the dashboard and the data source",projects)
            if firstlevel:
                    st.write('You selected the following project: ',firstlevel)
            st.session_state.load_state = True

            # initiate the analysis, run impact_analysis.py, hyper_file.py and publish_workbook.py
            if st.button('Run analysis'):
                with st.spinner('Processing and uploading data...'):
                        conn = tableau_conn(url=url_input, user=user_input, password = password_input, api_version = version)
                        combined_df = graphql_queries(conn)
                        projectId = create_hyper_file(conn=conn, my_df=combined_df, project_name=firstlevel)
                        webpageurl = publish_workbook(conn, projectId)
                       # if percent_complete == 9:
                        st.success('Analysis complete!')
                        url = 'https://tableau.bi-concepts.ch/#/views/ImpactAnalysis/ImpactAnalysis'
                        st.markdown("### Click this [link](%s) to access your dashboard" % webpageurl)
                        st.balloons()
                        st.session_state.load_state = True
