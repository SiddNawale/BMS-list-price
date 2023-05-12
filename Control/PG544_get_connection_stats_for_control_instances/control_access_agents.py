
import asyncio
import ssl
from datetime import datetime

import aiohttp
import pandas as pd

# from examples.config import CONFIG


CERTIFICATE = "alireza_control.pem" #CONFIG['cloudcontrol']['certpath']
CC_PATH = 'https://cloudcontrol.screenconnect.com/Service'
HEADER = {'X-Client-Certificate-Authentication': 'true',
              'Content-Type': 'application/json'}
CLOUD_PATH = 'https://cloud.screenconnect.com/Service'


print('Starting routine')
start = datetime.now().isoformat()

ssl_context = ssl.create_default_context()
ssl_context.load_cert_chain(CERTIFICATE)


async def check_for_access_instances(instance):

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
            return [instance['InstanceID'], -1]
            # return [[instance['InstanceID'], None, None]]

        access_payload = ["2", ["All Machines"], "", None, None, None]

        try:
            access_resp = await session.post(
                f"https://{instance.get('WebHostName')}.screenconnect.com/Services/Page"
                f"Service.ashx/GetHostSessionInfo", json=access_payload, timeout=180)
        except Exception as e:
            print(e)
            return [instance['InstanceID'], -1]
            # return [[instance['InstanceID'], None, None]]

        access_data = await access_resp.json()

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
        await session.close()
    finally:
        await session.close()
        return result


def agent_counts(acct_df):

    chunk = 250
    result_set = []
    iter_inst = acct_df.to_dict(orient='records')

    for i in range(0, len(iter_inst), chunk):
        futures = [check_for_access_instances(inst)
                   for inst in iter_inst[i:i+chunk]]

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        done, _ = loop.run_until_complete(asyncio.wait(futures))
        result_set.extend([_.result() for _ in done])
        # loop.close()
        print('Completed loop', i)

    test = pd.DataFrame(result_set, columns=['InstanceID', 'AccessGuestCount', 'AccessSessionCount'])
    merge_df = acct_df.merge(test, on='InstanceID')
    return merge_df


if __name__ == '__main__':
    df = agent_counts(pd.read_csv('./Test_Instances.csv'))
    df['query_date'] = pd.to_datetime('today').strftime("%Y-%m-%d")
    print(df)
    df.to_csv('./Test_Instances_out.csv')