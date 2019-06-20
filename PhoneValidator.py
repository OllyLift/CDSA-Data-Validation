from twilio.rest import Client
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np

file = 'C:/Users/ajames.FACS_ORG/Documents/CDSA/PhoneNumbersForTwilio.xlsx'


def df_empty(columns, dtypes, index=None):
    assert len(columns) == len(dtypes)
    df = pd.DataFrame(index=index)
    for c, d in zip(columns, dtypes):
        df[c] = pd.Series(dtype=d)
    return df


def main():
    # Your Account Sid and Auth Token from twilio.com/console
    # DANGER! This is insecure. See http://twil.io/secure
    account_sid = ''
    auth_token = ''
    client = Client(account_sid, auth_token)

    df = pd.read_excel(file)

    dfvals = df_empty(['mobile_country_code', 'mobile_network_code', 'name', 'type', 'error_code', 'phone']
                   , dtypes=['str', 'str', 'str', 'str', 'str', 'str'])

    for i in df.index:
        val = df.at[i, 'Phone1']
        try:
            phone_number = client.lookups.phone_numbers(val).fetch(country_code='US', type='carrier')
        # phone_number = {'mobile_country_code': '310', 'mobile_network_code': '160', 'name': 'Metro PCS, Inc.', 'type': 'mobile', 'error_code': None}

            result = phone_number.carrier
            result['phone'] = val
            dfvals = dfvals.append(result, ignore_index=True)
        except:
            continue


    writer = pd.ExcelWriter('validatedPhones.xlsx')
    dfvals.to_excel(writer, 'Sheet1')
    writer.save()


if __name__ == '__main__':
    main()

