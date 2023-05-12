## Project      No direct Project - Adhoc Request from Sean White
## Purpose      Used to gather information on CustomProperty4 through Control API
## Input        Input is an excel file that has instanceid, accountid, and webhost name
##                      -- ANALYTICS.DBO.CORE__RPT_BILLINGS
## Output     - Output is an excel file with number of sessions and if the parnter
##              changed the CustomProperty4 Setting
#####################################################################################
import asyncio
import ssl
from datetime import datetime

import aiohttp
import pandas as pd

# from examples.config import CONFIG


CERTIFICATE = "C:/Users/Justin.Nahas/Downloads/jnahas_control.pem" #CONFIG['cloudcontrol']['certpath']
CC_PATH = 'https://cloudcontrol.screenconnect.com/Service'
HEADER = {'X-Client-Certificate-Authentication': 'true',
              'Content-Type': 'application/json'}
CLOUD_PATH = 'https://cloud.screenconnect.com/Service'


print('Starting routine')
start = datetime.now().isoformat()

ssl_context = ssl.create_default_context()
ssl_context.load_cert_chain(CERTIFICATE)


async def check_cp4(instance):

    result = []

    try:

        conn = aiohttp.TCPConnector(ssl=ssl_context)
        session = aiohttp.ClientSession(connector=conn, headers=HEADER)

        impersonate = await session.post(f"{CC_PATH}/SetLoginImpersonationForInstance",
                                         json=[instance.get('AccountID'),
                                               'Analytics -- Access session count',
                                               instance.get('InstanceID')])

        if impersonate.status != 200:
            await session.close()
      ##      return [instance['InstanceID'], -2]
            return [instance['InstanceID'], None, None]


        try:
            session_resp = await session.get(
                f"https://{instance.get('WebHostName')}.screenconnect.com/Report.json?ReportType=Session",  timeout=180)
        except Exception as e:
            return [instance['InstanceID'], None, None]

        session_data = await session_resp.json()

        try:
            ##global session_count
            session_count = len(session_data.get('Items'))
            results = []
            ##global cnt
            cnt = 0
            for item in session_data.get('Items'): 
                cp4 = item[9]
                is_changed = (len(cp4) > 0)
                cnt += is_changed
            # act_sess = list(filter(lambda x: len(x['ActiveConnections']) > 0, sessions))
            # result = []
            # for sess in act_sess:
            #     result.append([instance['InstanceID'], sess.get('SessionID'), sess.get('GuestOperatingSystemName')])
        except Exception as e:
            print(e)
            conn_count = -1
            result = [[instance['InstanceID'], None, None]]


        result =  [instance.get('InstanceID'), session_count, cnt]
    except Exception as e:
        print(e)
        result = [instance['InstanceID'], -1]
        # result = [[instance['InstanceID'], None, None]]
        await session.close()
    finally:
        await session.close()
        return result


# check_cp4({'AccountID': 'qpvfw3', 'InstanceID': 'j7rukz', 'WebHostName': 'ctullos'})


input_df = pd.read_csv('C:/Users/Justin.Nahas/Downloads/PG_543_instances.csv').head()

def custom_property_4(acct_df):

    chunk = 250
    result_set = []
    iter_inst = acct_df.to_dict(orient='records')

    for i in range(0, len(iter_inst), chunk):
        futures = [check_cp4(inst) for inst in iter_inst[i:i+chunk]]

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        done, _ = loop.run_until_complete(asyncio.wait(futures))
        print([_.result() for _ in done])
        result_set.extend([_.result() for _ in done])
        # loop.close()
        print('Completed loop', i)

    test = pd.DataFrame(result_set, columns=['InstanceID', 'SessionCount', 'customProperty4'])
    merge_df = acct_df.merge(test, on='InstanceID')
    return merge_df


##agent_counts(input_df)

# iter_inst = input_df.to_dict(orient='records')
# result_df = pd.DataFrame(columns = ['InstanceID','SessionCount', 'customProperty4'])

# results_list = []

# for i, inst in enumerate(iter_inst): 
        
#     results_list.append(check_cp4(inst))
    

# result_df = pd.DataFrame(results_list, columns = ['InstanceID','SessionCount', 'customProperty4'])


# result_df.to_csv('C:/Users/Justin.Nahas/Downloads/Output_Instances.csv')



if __name__ == '__main__':
    df  = pd.read_csv('C:/Users/Justin.Nahas/Downloads/PG_543_instances.csv')
    ##agent_counts(df)
    output_df = custom_property_4(df)
    output_df['query_date'] = pd.to_datetime('today').strftime("%Y-%m-%d")
    print(output_df)
    output_df.to_csv('./Test_Instances_out.csv')