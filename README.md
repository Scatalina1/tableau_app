# Streamlit app

Mock up application for Tableau Impact Analysis.
BICLUID is a Tableau server application to get all metadata from a selected server and site in one dashboard.

# Libraries
This project relies heavily on the Tableau API libary 'tableau_api_lib': https://github.com/divinorum-webb/tableau-api-lib
The library facilitates authenticating to the server, transforming data and publishing resources to the server.

# Content

### app
streamlit_app.py is the main script that contains the workflow for the application.

### analysis
- integrate:
     - impact_analysis: retrieves metadata from server with graphql queries and joins the data in one dataframe
- load:
     - hyper_file: uses the Tableau hyper API (for JS, REST API needs to be used) to create a hyper file with the data from impact_analysis
     - publish_workbook: Uses the Tableau REST API to first publish the hyper file and then the workbook (impactanalysis.twb) to the Tableau Server
     
### workbook
contains the tableau workbook (.twb file) that will be published to the selected Tableau server.

# Useful links

  - Tableau Server Authentication: 
     - https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_authentication.htm#sign_in
     - postman example: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_get_started_tutorial_part_1.htm
  
  - Metadata API:   https://www.tableau.com/developer/learning/metadata-api
  
  - Publish data source (hyper file): https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_publishing.htm#publish_data_source
  
  - Publish workbook: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_publishing.htm#publish_workbook
  


## Slack channel

 Tableau dev Slack channel, if documentation is not useful:
  https://join.slack.com/t/tableau-datadev/shared_invite/zt-139w33thz-cuhb9cV5lJ~vMBIhamLM1Q
