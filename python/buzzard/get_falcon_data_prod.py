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

def extractURL(tpc):
    tpc = tpc.lower()

serverhostname = socket.gethostname()
print(serverhostname)
if "HDC" in serverhostname:
    os.environ["https_proxy"] = "https://proxy.hdc.mdsol.com:3128"
    db_connect_string = "Driver={SQL Server};Server=HDC405PRDBSV007;Database=buzzard;uid=buzzard_user;pwd=aGXnFducpqdAgY86dugYVyZ7KxfPk8"
else:
    db_connect_string = "Driver={SQL Server};Server=localhost,2017;Database=buzzard;uid=buzzard_user;pwd=aGXnFducpqdAgY86dugYVyZ7KxfPk8"


baseurl = "https://mnmapi.mdsol.com/falcon/api"
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
    print (url)

    if (response.ok):
        jData1 = json.loads(response.content)
        with open('data.json', 'w') as outfile:
            json.dump(jData1, outfile)
        df = pd.DataFrame.from_dict(json_normalize(jData1), orient='columns')
        #clean N/A and None values
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

        datalist = []
        for index, row in df.iterrows():
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
            #r_verification_status = row['verification_status']
            #r_last_verification_date_time = row['last_verification_date_time']
            r_verification_status = 'False'
            r_last_verification_date_time = datetime.datetime.now()
            r_deployment_type = row['deployment_type']
            r_generic_url = 'Hello'
            r_alias_url = 'Hello'
            r_alias_rave_db_server = 'Hello'
            r_alias_reporting_db_server = 'Hello'
            r_last_completion_date_time = datetime.datetime.now()
            datalist.append((r_url, r_sponsor, r_partner, r_data_center, r_rave_version, r_release, r_is_replicated,r_is_ravex_enabled, r_is_cdc_enabled, r_team, r_service_level, r_environment, r_region,r_vip, r_vpn, r_backend_server, r_backend_db_name, r_sql_server, r_rave_db, r_database_size_mb, r_reporting_server, r_rave_reporting_db, r_external_ftp, r_internal_ftp, r_pm, r_pd, r_csp, r_sla, r_page_turn_sla, r_decommission_dt, r_page_views, r_validated, r_verification_status, r_last_verification_date_time, r_deployment_type, r_generic_url, r_alias_url, r_alias_rave_db_server, r_alias_reporting_db_server, r_last_completion_date_time))
            #datalist.append((row['url'],row['sponsor'],row['partner'],row['data_center'],row['rave_version'],row['release'],row['is_replicated'],row['is_ravex_enabled'],row['is_cdc_enabled'],row['team'],row['service_level'],row['environment'],row['region'],row['vip'],row['vpn'],row['backend_server'],row['backend_db_name'],row['sql_server'],row['rave_db'],row['database_size_mb'],row['reporting_server'],row['rave_reporting_db'],row['external_ftp'],row['internal_ftp'],row['pm'],row['pd'],row['csp'],row['sla'],row['page_turn_sla'],row['decommission_dt'],row['page_views'],row['validated'],row['verification_status'],row['last_verification_date_time'],row['deployment_type'],row['generic_url'],row['alias_url'],row['alias_rave_db_server'],row['alias_reporting_db_server'],row['last_completion_date_time))

        # print("The response contains {0} properties".format(len(jData1)))
        # now build out the basic URL summary line for CS support information
        #print(jData1)
        #print(type(jData1))
        #print(type(df))
        #print(len(jData1))
        #for i in range(len(jData1)):
            #r_url = jData1[i]['url']
            #r_sponsor = jData1[i]['sponsor']
            #r_partner = jData1[i]['partner']
            #r_data_center = jData1[i]['data_center']
            #r_rave_version = jData1[i]['rave_version']
            #r_release = jData1[i]['release']
            #r_is_replicated = jData1[i]['is_replicated']
            #r_is_ravex_enabled = jData1[i]['is_ravex_enabled']
            #r_is_cdc_enabled = jData1[i]['is_cdc_enabled']
            #r_team = jData1[i]['team']
            #r_service_level = jData1[i]['service_level']
            #r_environment = jData1[i]['environment']
            #r_region = jData1[i]['region']
            #r_vip = jData1[i]['vip']
            #r_vpn = jData1[i]['vpn']
            #r_backend_server = jData1[i]['backend_server']
            #r_backend_db_name = jData1[i]['backend_db_name']
            #r_sql_server = jData1[i]['sql_server']
            #r_rave_db = jData1[i]['rave_db']
            #r_database_size_mb = jData1[i]['database_size_mb']
            #r_reporting_server = jData1[i]['reporting_server_server']
            #r_rave_reporting_db = jData1[i]['rave_reporting_db']
            #r_external_ftp = jData1[i]['external_ftp']
            #r_internal_ftp = jData1[i]['internal_ftp']
            #r_pm = jData1[i]['pm']
            #r_pd = jData1[i]['pd']
            #r_csp = jData1[i]['csp']
            #r_sla = jData1[i]['sla']
            #r_page_turn_sla = jData1[i]['page_turn_sla']
            #r_decommission_dt = jData1[i]['decommission_dt']
            #r_page_views = jData1[i]['page_views']
            #r_validated = jData1[i]['validated']
            #r_verification_status = jData1[i]['verification_status']
            #r_last_verification_date_time = jData1[i]['last_verification_date_time']
            #r_deployment_type = jData1[i]['deployment_type']
            #r_generic_url = jData1[i]['generic_url']
            #r_alias_url = jData1[i]['alias_url']
            #r_alias_rave_db_server = jData1[i]['alias_rave_db_server']
            #r_alias_reporting_db_server = jData1[i]['alias_reporting_db_server']
            #r_last_completion_date_time = jData1[i]['last_completion_date_time']
            #print(r_url)
            #print(r_sponsor)
            #print(r_partner)
            #print(r_data_center)
            #print(r_rave_version)
            #print(r_release)
            #print(r_is_replicated)
            #print(r_is_ravex_enabled)
            #print(r_is_cdc_enabled)
            #print(r_team)
            #print(r_service_level)
            #print(r_environment)
            #print(r_region)
            #print(r_vip)
            #print(r_vpn)
            #print(r_backend_server)
            #print(r_backend_db_name)
            #print(r_sql_server)
            #print(r_rave_db)
            #print(r_database_size_mb)
            #print(r_reporting_server)
            #print(r_rave_reporting_db)
            #print(r_external_ftp)
            #print(r_internal_ftp)
            #print(r_pm)
            #print(r_pd)
            #print(r_csp)
            #print(r_sla)
            #print(r_page_turn_sla)
            #print(r_decommission_dt)
            #print(r_page_views)
            #print(r_deployment_type)
            #print(r_validated)
            #print(r_verification_status)
            #print(r_last_verification_date_time)
            #print(r_deployment_type)
            #print(r_generic_url)
            #print(r_alias_url)
            #print(r_alias_rave_db_server)
            #print(r_alias_reporting_db_server)
            #print(r_last_completion_date_time)

            #datalist.append((r_url,r_sponsor,r_partner,r_data_center,r_rave_version,r_release,r_is_replicated,r_is_ravex_enabled,r_is_cdc_enabled,r_team,r_service_level,r_environment,r_region,r_vip,r_vpn,r_backend_server,r_backend_db_name,r_sql_server,r_rave_db,r_database_size_mb,r_reporting_server,r_rave_reporting_db,r_external_ftp,r_internal_ftp,r_pm,r_pd,r_csp,r_sla,r_page_turn_sla,r_decommission_dt,r_page_views,r_validated,r_verification_status,r_last_verification_date_time,r_deployment_type,r_generic_url,r_alias_url,r_alias_rave_db_server,r_alias_reporting_db_server,r_last_completion_date_time))
            #cursor = connection.cursor()
            #sqlcommand = ("INSERT INTO [dbo].[falcon_sites] ([url],[sponsor],[partner],[data_center],[rave_version],[release],[is_replicated],[is_ravex_enabled],[is_cdc_enabled],[team],[service_level],[environment],[region],[vip],[vpn],[backend_server],[backend_db_name],[sql_server],[rave_db],[database_size_mb],[reporting_server],[rave_reporting_db],[external_ftp],[internal_ftp],[pm],[pd],[csp],[sla],[page_turn_sla],[decomission_dt],[page_views],[validated],[verification_status],[last_verification_date_time],[deployment_type],[generic_url],[alias_url],[alias_rave_db_server],[alias_reporting_db_server],[last_completion_date_time]) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            #sqlvalues = [r_url,r_sponsor,r_partner,r_data_center,r_rave_version,r_release,r_is_replicated,r_is_ravex_enabled,r_is_cdc_enabled,r_team,r_service_level,r_environment,r_region,r_vip,r_vpn,r_backend_server,r_backend_db_name,r_sql_server,r_rave_db,r_database_size_mb,r_reporting_server,r_rave_reporting_db,r_external_ftp,r_internal_ftp,r_pm,r_pd,r_csp,r_sla,r_page_turn_sla,r_decommission_dt,r_page_views,r_validated,r_verification_status,r_last_verification_date_time,r_deployment_type,r_generic_url,r_alias_url,r_alias_rave_db_server,r_alias_reporting_db_server,r_last_completion_date_time]
            #cursor.execute(sqlcommand, sqlvalues)
    else:
        # If response code is not ok (200), print the resulting http error code with description
        response.raise_for_status()
else:
    # If response code is not ok (200), print the resulting http error code with description
    myResponse.raise_for_status()

#create sql connection
connection = pyodbc.connect(db_connect_string)
cursor = connection.cursor()
#truncate the table
sqltruncate = ("truncate table falcon_sites")
cursor.execute(sqltruncate)
#load the table from datalist
cursor.fast_executemany = True
cursor = connection.cursor()
sqlcommand = ("INSERT INTO [dbo].[falcon_sites] ([url],[sponsor],[partner],[data_center],[rave_version],[release],[is_replicated],[is_ravex_enabled],[is_cdc_enabled],[team],[service_level],[environment],[region],[vip],[vpn],[backend_server],[backend_db_name],[sql_server],[rave_db],[database_size_mb],[reporting_server],[rave_reporting_db],[external_ftp],[internal_ftp],[pm],[pd],[csp],[sla],[page_turn_sla],[decomission_dt],[page_views],[validated],[verification_status],[last_verification_date_time],[deployment_type],[generic_url],[alias_url],[alias_rave_db_server],[alias_reporting_db_server],[last_completion_date_time]) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
cursor.executemany(sqlcommand, datalist)
connection.commit()
