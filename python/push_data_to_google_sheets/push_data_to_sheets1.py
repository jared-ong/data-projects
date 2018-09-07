import gspread
from oauth2client.service_account import ServiceAccountCredentials

# use creds to create a client to interact with the Google Drive API
scope = ['https://www.googleapis.com/auth/spreadsheets']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
gc = gspread.authorize(creds)

# Find a workbook by name and open the first sheet
# Make sure you use the right name here.
spreadsheet = gc.open_by_key('1M-5lJcVO9eUrxJpp7YM46gW_NKtTGUsm4EKGQ6TSVew')
#spreadsheet.add_worksheet(title="Sheet3", rows="100", cols="20")
sheet = spreadsheet.sheet1
sheet2 = spreadsheet.worksheet("Sheet2")

# Extract and print all of the values
list_of_hashes = sheet.get_all_records()
print(list_of_hashes)

# Select a range
cell_list = sheet2.range('A1:C7')

for cell in cell_list:
    cell.value = 'O_o'

# Update in batch
sheet2.update_cells(cell_list)