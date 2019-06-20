from __future__ import print_function
import pandas as pd
from collections import OrderedDict
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# should this eventually connect to the google spreadsheet instead? probably.
path = "C:/Users/ajames.FACS_ORG/Documents/CDSA/"
file = "Chicago thru 6-2-19.xlsx"
# file = "PhoneTest.xlsx"
compare = "ANDatabase.xlsx"
landlines = ""

inputFile = path + file
compareFile = path + compare

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1dWdi87PCZVRaf8nVaDd6fszNDE3zo7G9-EF1zHwLTHE'
SAMPLE_RANGE_NAME = 'PhoneCorrections!A1:D'
REMOVE_RANGE_NAME = 'Remove!A1:C'


def main():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    result_remove = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=REMOVE_RANGE_NAME).execute()
    # df_google = pd.DataFrame()

    header = result.get('values', [])[0]   # Assumes first line is header!
    values = result.get('values', [])[1:]  # Everything else is data.

    removeheader = result_remove.get('values', [])[0]   # Assumes first line is header!
    removevalues = result_remove.get('values', [])[1:]  # Everything else is data.


    df_google = pd.DataFrame(values, columns=header)
    df_remove = pd.DataFrame(removevalues, columns=removeheader)

    df_cleansed = member_file_import(df_google)

    df_output = remove_contact(df_remove, df_cleansed)

    writer = pd.ExcelWriter('output.xlsx')
    df_output.to_excel(writer, 'Sheet1')
    writer.save()


def column_expander(df, column, prefix, foo):

    df2 = df[column].apply(foo)
    a = len(df2.columns)
    i = 1
    x = ""
    list = []
    while i <= a:
        x = prefix + str(i)
        list.append(x)
        i += 1
    return list


def remove_contact(dfg, df):
    dfg = dfg.set_index('AK_ID')
    df = pd.merge(df, dfg, left_index=True, right_index=True, indicator=True
                  , suffixes=('_left', '_right'), how='outer').query('_merge=="left_only"').drop(['_merge', 'Why', 'Notes'], axis=1)
    # df = df.join(dfg.set_index('AK_ID'), on='AK_ID', how="left")
    # df = pd.merge(df, dfg, how='left', on='AK_ID')
    return df

def member_file_import(dfg):
    df = pd.read_excel(inputFile)
    df2 = pd.read_excel(compareFile)

    df['email_DQ'] = df['Email']
    df.loc[df['email_DQ'].isnull(), 'email_DQ'] = 'CDSA' + df['AK_ID'].astype(str) + '@fakeDomain.com'

    df = df.astype(str)
    df2 = df2.astype(str)

    foo = lambda x: pd.Series([i for i in reversed(x.split(','))])

    home = column_expander(df, "Home_Phone", "Home_", foo)

    df[home] = df['Home_Phone'].apply(foo)

    mobile = column_expander(df, "Mobile_Phone", "Mobile_", foo)
    df[mobile] = df['Mobile_Phone'].apply(foo)

    work = column_expander(df, "Work_Phone", "Work_", foo)
    df[work] = df['Work_Phone'].apply(foo)

    cleanse = []
    cleanse = home + mobile + work

    df[cleanse] = df[cleanse].astype(str).replace('\.0', '', regex=True)
    df2 = df2.astype(str).replace('\.0', '', regex=True)

    df2.rename(columns={"Phone1": "anPhone1", "Phone2": "anPhone2"
            , "Phone3": "anPhone3", "Phone4": "anPhone4", "National ID": "AK_ID"}, inplace=True)

    df2 = df2[['AK_ID', 'Phone', 'phoneNumber']]

    cleanse_phone(df, cleanse)
    cleanse_phone(df2, ['Phone', 'phoneNumber'])

    df = df.replace('nan', '', regex=True)
    df2 = df2.replace('nan', '', regex=True)
    df.fillna(value='', inplace=True)
    df2.fillna(value='', inplace=True)

    df = df.set_index('AK_ID').join(df2.set_index('AK_ID'), on='AK_ID', how="left")

    anNumbers = ['Phone', 'phoneNumber']
    cleanse.extend(anNumbers)

    df['Test'] = df[cleanse].apply(lambda x: x.str.cat(sep=','), axis=1)
    df['Test'] = df['Test'].map(lambda x: x.lstrip(',').rstrip(', ')).str.replace('-', '')
    df['Test'] = df['Test'].str.replace('\W+', ' ')

    # this deduplicates the values from the test column
    df['Desired'] = df['Test'].str.split().apply(lambda x: OrderedDict.fromkeys(x).keys()).str.join(' ')

    foo = lambda x: pd.Series([i for i in reversed(x.split(' '))])

    cols = column_expander(df, "Desired", "Phone", foo)
    df[cols] = df['Desired'].apply(foo)

    fincols = ['first_name', 'last_name', 'middle_name', 'Address_Line_1', 'Address_Line_2', 'City', 'State'
            , 'Zip', 'Country', 'Mail_preference', 'Do_Not_Call', 'Join_Date', 'Xdate', 'Memb_status', 'email_DQ'
            , 'membership_type', 'monthly_status']
    fincols.extend(cols)

    df = df[fincols]

    # If IN/DC/WN - delete the phone number outright
    # If CH - replace the phone number value
    # If LL - Validate through the API and lookup list

    # Google columns: NationalID Result PhoneNumber ChangeNumber
    for index, row in dfg.iterrows():
        if row['NationalID'] in df.index:
            for i in cols:
                try:
                    if row['Result'] == 'CH' and df.at[row['NationalID'], i] == row['PhoneNumber']:
                        df.at[row['NationalID'], i] = row['ChangeNumber']
                    elif row['Result'] in ['IN', 'WN', 'DC'] and df.at[row['NationalID'], i] == row['PhoneNumber']:
                        df.at[row['NationalID'], i] = ''
                except:
                    continue
                # if row['Result'] == 'CH':
                #     try:
                #         for i in cols:
                #             if df.at[row['NationalID'], i] == row['PhoneNumber'] and row['ChangeNumber'] != '':
                #                 print(df.at[row['NationalID'], i])
                #                 df.at[row['NationalID'], i] = row['ChangeNumber']
                #     except:
                #         continue
                # elif row['Result'] in ['IN', 'WN', 'DC']:
                #     # print(row['Result'])
                #     try:
                #         for i in cols:
                #             if df.at[row['NationalID'], i] == row['PhoneNumber']:
                #                 df.at[row['NationalID'], i] = ''
                    # except:
                    #     continue
            else:
                continue
        else:
            continue

    return df


def cleanse_phone(df, series):
    for column in series:
        # remove special characters
        df[column] = df[column].str.replace(' ', '')
        df[column] = df[column].str.replace('-', '')
        df[column] = df[column].str.replace('.', '')
        df[column] = df[column].str.replace('(', '')
        df[column] = df[column].str.replace(')', '')
        df[column] = df[column].str.replace(' ', '')
        df[column] = df[column].str.replace('+', '')
        df[column] = df[column].str.replace('*', '')
        df[column] = df[column].apply(lambda x: x[1:] if x.startswith("1") and len(x) > 10 else x)
        df[column] = df[column].apply(lambda x: x[2:] if x.startswith(",1") else x)


if __name__ == '__main__':
    main()
