import pandas as pd
import numpy as np

# Small subset
filea = "C:\\Users\\jong\\Documents\\GitHub\\data-projects\\python\\get_recent_10_logins\\data\\individual_files\\abbottdev.mdsol.com.csv"
fileb = "C:\\Users\\jong\\Documents\\GitHub\\data-projects\\python\\get_recent_10_logins\\data\\individual_files\\abbottvascular.mdsol.com.csv"
fileall = "C:\\Users\\jong\\Documents\\GitHub\\data-projects\\python\\get_recent_10_logins\\data\\all_data.csv"

pd1 = pd.read_csv(filea)
pd2 = pd.read_csv(fileb)
pd3 = pd1.append(pd2)
pd4 = pd3[["url", "AttemptID", "LoginName", "Attempted"]]
pd5 = pd4.sort_values(by=["url","AttemptID"], ascending=[True, False])
pd6 = pd5.groupby("url").head(10)

#Working example one to load the full data but only able to select 4 columns
pdchunk3 = pd.DataFrame({"AttemptID": [0,1], "LoginName": ['jared','jared2'], "url": ['naurl.mdsol.com','naurl.mdsol.com'], "Attempted": ['12/12/2020 3:27:04 PM','12/12/2020 3:27:04 PM']})
chunksize = 100000
#for chunk in pd.read_csv(fileb, chunksize=chunksize, dtype={"AttemptID": int64, "LoginName": object, "LoginID": int64, "Attempted": datetime64, "NetworkAddress object, "Success": int64, "Created": datetime64, "Updated": datetime64, "ServerSyncDate": datetime64, "Guid": object, "userid": int64, "firstname": object, "middlename": object, "lastname": object, "title": object, "email": object, "Telephone": object, "PasswordExpires": datetime64, "enabled": bool, "LockedOut": bool, "InstitutionName": object, "UserActive": bool, "ExternalURL": object, "ExternalUserID": int64, "UUID": object, "Created1": object, "Updated1": object, "url": object, "urltype": object, "dbname": object}):
for chunk in pd.read_csv(fileall, chunksize=chunksize, low_memory=False):
    pdchunk3 = pdchunk3.append(chunk[["AttemptID","LoginName","url","Attempted"]])

pdchunk3 = pdchunk3.sort_values(by=["url", "AttemptID"], ascending=[True, False])
pdchunk3 = pdchunk3.groupby(by="url").head(15)

#Load all data at once
pdchunk4 = pd.read_csv(fileall, low_memory=False)
pdchunk4 = pdchunk4.sort_values(by=["url","Attempted"], ascending=[True,False])
pdchunk4 = pdchunk4.groupby(by="url").head(15)

pdchunk4.to_csv("C:\\temp\\logins.csv")