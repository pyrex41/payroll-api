import functools
import aiohttp
import asyncio
import requests
import json
import os

# Helper Utils
foldl = lambda func, acc, xs: functools.reduce(func, xs, acc)

def safemap(func, arr):
    out = []
    err = []
    for x in arr:
        try:
            out.append(func(x))
        except:
            err.append(x)
    return out, err


# Hubspot
def hs_get(path, params={}):
    return requests.get(hs_path(path), headers=hs_header(), params=params)

def hs_path(path):
    return 'https://api.hubapi.com/crm/v3/' + path

def hs4_path(path):
    return 'https://api.hubapi.com/crm/vr/'+path

def hs_header():
    return {
        'Authorization': "Bearer " + os.environ['HUBSPOT_API_TOKEN'],
        'Content-Type': 'application/json'
    }



# Agency bloc
def ab_header():
    return {'Content-Type': 'application/x-www-form-urlencoded'}

def ab_body(dic):
    dic['key'] = os.environ['AB_API_KEY']
    dic['sid'] = os.environ['AB_API_SID']
    return dic

def ab_path(path):
    return "https://app.agencybloc.com/api/v1/" + path

def ab_post(path, **params):
    dic = ab_body(params)
    dic["limit"] = 0
    path = ab_path(path)
    return requests.post(path, data=dic, headers=ab_header())




# Hubspot async paging functions
async def post_batch_after(path, data, session, s=0, **kargs):
    k = kargs.get('k', .3)
    if s > 0:
        await asyncio.sleep(s * k) # rate limiter
    async with session.post(path, headers=hs_header(), data=json.dumps(data)) as r:
        res = False
        if r.status == 200:
            res = True
        else:
            print(r.status)

        out = await r.json()
        return (res, r.status, out)


async def get_batch_after(path, params, session, s=0, **kargs):
    k = kargs.get('k', .3)
    await asyncio.sleep(s * k) # rate limiter
    async with session.get(path, headers=hs_header(), params=params) as r:
        res = False
        if r.status == 200:
            res = True
        out = await r.json()
        return (res, r.status, out)

async def asyncio_wrap_(func, *args, **kargs):
    limit_per_host = kargs.get('limit_per_host', 100)
    async with aiohttp.TCPConnector(limit_per_host=limit_per_host) as conn:
        async with aiohttp.ClientSession(connector=conn) as session:
            return await func(session, *args, **kargs)

def asyncio_wrap(func, *args, **kargs):
    return asyncio.run(asyncio_wrap_(func, *args, **kargs))
