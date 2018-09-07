# Python 2.7.6
# RestfulClient.py
import requests, re
from requests.auth import HTTPDigestAuth
import json
import datetime
import pyodbc
import requests
import os
import socket
import pandas as pd
from pandas.io.json import json_normalize
import datetime

#sql commit batch size
sql_commit_batch_size = 500

serverhostname = socket.gethostname()
print(serverhostname)
if "HDC" in serverhostname:
    os.environ["https_proxy"] = "https://proxy.hdc.mdsol.com:3128"
    db_connect_string = "Driver={SQL Server};Server=HDC405PRDBSV007;Database=buzzard;uid=buzzard_user;pwd=aGXnFducpqdAgY86dugYVyZ7KxfPk8"
else:
    db_connect_string = "Driver={SQL Server};Server=localhost,2017;Database=buzzard;uid=buzzard_user;pwd=aGXnFducpqdAgY86dugYVyZ7KxfPk8"


#create sql connection
connection = pyodbc.connect(db_connect_string)
cursor = connection.cursor()
#truncate the table
sqltruncate = ("truncate table falcon_sites")
cursor.execute(sqltruncate)

baseurl = "https://premnmapi.mdsol.com/falcon/api"
url = baseurl + "/token"
payload = "grant_type=password&username=svc_sddba&password=wa2QfEc3UxBLN75AS946RG8bqDXjT!!"
headers = {'content-type': "application/x-www-form-urlencoded"}
# execute a PUT in order to get back the Authorization token
myResponse = requests.request("GET", url, data=payload, headers=headers)
print(myResponse.text)
if (myResponse.ok):
    jData = json.loads(myResponse.content)
    print("The response contains {0} properties".format(len(jData)))
    print("\n")
    # Need token_type and access_token for future submissions
    token_type = str(jData['token_type'])
    access_token = str(jData['access_token'])
    url = baseurl + "/v1/rave/sites"
    headers = {'authorization': token_type + ' ' + access_token}
    response = requests.request("GET", url, headers=headers)
    print(url)

    if (response.ok):
        jData1 = json.loads(response.content)
        with open('data.json', 'w') as outfile:
            json.dump(jData1, outfile)
        df = pd.DataFrame.from_dict(json_normalize(jData1), orient='columns')
        #clean pandas dataframe N/A and None values
        df['service_level'] = df['service_level'].str.replace('N/A', '')
        df['vpn'] = df['vpn'].str.replace('N/A', '')
        df['reporting_server_server'] = df['reporting_server_server'].str.replace('None', '')
        df['rave_reporting_db'] = df['rave_reporting_db'].str.replace('None', '')
        df['external_ftp'] = df['external_ftp'].str.replace('N/A', '')
        df['internal_ftp'] = df['internal_ftp'].str.replace('N/A', '')
        df['pm'] = df['pm'].str.replace('N/A', '')
        df['pd'] = df['pd'].str.replace('N/A', '')
        df['csp'] = df['csp'].str.replace('N/A', '')
        df['page_turn_sla'] = df['page_turn_sla'].str.replace('N/A', '')
        df['decommission_dt'] = df['decommission_dt'].str.replace('N/A', '')
        df['deployment_type'] = df['deployment_type'].str.replace('N/A', '')
        # add is_duplicated column which marks all but the first occurrence of the url as a duplicate
        df['is_duplicated'] = df.duplicated(subset='url')
        # remove rows where is_duplicated = True
        df = df.drop(df.query('is_duplicated==True').index)
        #dataframe row count
        df_row_count = df.shape[0]

        datalist = []
        loop_counter = 0
        for index, row in df.iterrows():
            loop_counter = loop_counter + 1
            r_url = row['url']
            r_sponsor = row['sponsor']
            r_partner = row['partner']
            r_data_center = row['data_center']
            r_rave_version = row['rave_version']
            r_release = row['release']
            r_is_replicated = row['is_replicated']
            r_is_ravex_enabled = row['is_ravex_enabled']
            r_is_cdc_enabled = row['is_cdc_enabled']
            r_team = row['team']
            r_service_level = row['service_level']
            r_environment = row['environment']
            r_region = row['region']
            r_vip = row['vip']
            r_vpn = row['vpn']
            r_backend_server = row['backend_server']
            r_backend_db_name = row['backend_db_name']
            r_sql_server = row['sql_server']
            r_rave_db = row['rave_db']
            r_database_size_mb = row['database_size_mb']
            r_reporting_server = row['reporting_server_server']
            r_rave_reporting_db = row['rave_reporting_db']
            r_external_ftp = row['external_ftp']
            r_internal_ftp = row['internal_ftp']
            r_pm = row['pm']
            r_pd = row['pd']
            r_csp = row['csp']
            r_sla = row['sla']
            r_page_turn_sla = row['page_turn_sla']
            r_decommission_dt = row['decommission_dt']
            r_page_views = row['page_views']
            r_validated = row['validated']
            r_verification_status = str(row['verification_status'])
            r_last_verification_date_time = row['last_verification_date_time']
            r_deployment_type = row['deployment_type']
            r_generic_url = row['generic_url']
            r_alias_url = row['alias_url']
            r_alias_rave_db_server = row['alias_rave_db_server']
            r_alias_reporting_db_server = row['alias_reporting_db_server']
            r_last_completion_date_time = row['last_completion_date_time']
            datalist.append((r_url, r_sponsor, r_partner, r_data_center, r_rave_version, r_release, r_is_replicated,r_is_ravex_enabled, r_is_cdc_enabled, r_team, r_service_level, r_environment, r_region,r_vip, r_vpn, r_backend_server, r_backend_db_name, r_sql_server, r_rave_db, r_database_size_mb, r_reporting_server, r_rave_reporting_db, r_external_ftp, r_internal_ftp, r_pm, r_pd, r_csp, r_sla, r_page_turn_sla, r_decommission_dt, r_page_views, r_validated, r_verification_status, r_last_verification_date_time, r_deployment_type, r_generic_url, r_alias_url, r_alias_rave_db_server, r_alias_reporting_db_server, r_last_completion_date_time))
            if loop_counter % sql_commit_batch_size == 0 or loop_counter == df_row_count:
                print("Committing {0} rows.".format(len(datalist)))
                # load the table from datalist
                cursor.fast_executemany = True
                cursor = connection.cursor()
                sqlcommand = ("INSERT INTO [dbo].[falcon_sites] ([url],[sponsor],[partner],[data_center],[rave_version],[release],[is_replicated],[is_ravex_enabled],[is_cdc_enabled],[team],[service_level],[environment],[region],[vip],[vpn],[backend_server],[backend_db_name],[sql_server],[rave_db],[database_size_mb],[reporting_server],[rave_reporting_db],[external_ftp],[internal_ftp],[pm],[pd],[csp],[sla],[page_turn_sla],[decomission_dt],[page_views],[validated],[verification_status],[last_verification_date_time],[deployment_type],[generic_url],[alias_url],[alias_rave_db_server],[alias_reporting_db_server],[last_completion_date_time]) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                cursor.executemany(sqlcommand, datalist)
                connection.commit()
                datalist = []
    else:
        # If response code is not ok (200), print the resulting http error code with description
        response.raise_for_status()
else:
    # If response code is not ok (200), print the resulting http error code with description
    myResponse.raise_for_status()