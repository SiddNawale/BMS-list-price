
import asyncio
import ssl
from datetime import datetime
import requests as req
import aiohttp
import pandas as pd
import json

# from examples.config import CONFIG


CERTIFICATE = "alireza_control.pem" #CONFIG['cloudcontrol']['certpath']
CC_PATH = 'https://cloudcontrol.screenconnect.com/Service'
HEADER = {'X-Client-Certificate-Authentication': 'true',
              'Content-Type': 'application/json'}
PAYLOAD = ['2', ['All Machines'], '', None, None, None]
CLOUD_PATH = 'https://cloud.screenconnect.com/Service'


# print('Starting routine')
# start = datetime.now().isoformat()

# ssl_context = ssl.create_default_context()
# ssl_context.load_cert_chain(CERTIFICATE)


def check_for_access_instances(instance):

    result = []

    try:

        # conn = aiohttp.TCPConnector(ssl=ssl_context)
        # session = aiohttp.ClientSession(connector=conn, headers=HEADER)

        # impersonate = await session.post(f"{CC_PATH}/SetLoginImpersonationForInstance",
        #                                  json=[instance.get('AccountID'),
        #                                        'Analytics -- Access session count',
        #                                        instance.get('InstanceID')])
        session = req.Session()
        impersonate = session.post('https://cloudcontrol.screenconnect.com/Service/SetLoginImpersonationForInstance', data=json.dumps([instance.get('AccountID'), 'Analytics -- Access session count', instance.get('InstanceID')]),
                headers=HEADER, cert=CERTIFICATE)
        if impersonate.status_code != 200:
            session.close()
            return [instance['InstanceID'], -1]
            # return [[instance['InstanceID'], None, None]]

        try:
            access_resp = session.post(f"https://{instance.get('WebHostName')}.screenconnect.com/Services/PageService.ashx/GetHostSessionInfo", data=json.dumps(PAYLOAD), headers=HEADER)
        except Exception as e:
            print(e)
            return [instance['InstanceID'], -1]
            # return [[instance['InstanceID'], None, None]]

        access_data = access_resp.json()

        try:
            sessions = access_data['Sessions']
            sess_count = len(sessions)
            conn_count = len(list(filter(lambda x: len(x['ActiveConnections']) > 0, sessions)))
            # act_sess = list(filter(lambda x: len(x['ActiveConnections']) > 0, sessions))
            # result = []
            # for sess in act_sess:
            #     result.append([instance['InstanceID'], sess.get('SessionID'), sess.get('GuestOperatingSystemName')])
        except Exception as e:
            print(e)
            conn_count = -1
            # result = [[instance['InstanceID'], None, None]]

        result = [instance['InstanceID'], conn_count, sess_count]

    except Exception as e:
        print(e)
        result = [instance['InstanceID'], -1]
        # result = [[instance['InstanceID'], None, None]]
        session.close()
    finally:
        session.close()
        return result


def agent_counts(acct_df):

    chunk = 250
    result_df =pd.DataFrame()
    iter_inst = acct_df.to_dict(orient='records')

    for i, inst in enumerate(iter_inst): 
        # futures = [check_for_access_instances(inst)
        #            for inst in iter_inst[i:i+chunk]]
        # loop = asyncio.new_event_loop()
        # asyncio.set_event_loop(loop)
        # done, _ = loop.run_until_complete(asyncio.wait(futures))
        # result_set.extend([_.result() for _ in done])
        # loop.close()
        tmp_results =[check_for_access_instances(inst)]
        tmp_df = pd.DataFrame(tmp_results, columns=['InstanceID', 'AccessGuestCount', 'AccessSessionCount'])

        result_df=pd.concat([result_df,tmp_df])
        print('Completed loop', i)
    
    return result_df

if __name__ == '__main__':
    input_df = pd.read_csv('./TestInstances.csv')
    output_df = agent_counts(input_df) 
    merge_df = input_df.merge(output_df, on='InstanceID')
    print(merge_df)
    merge_df.to_csv('./Output_Instances.csv')

