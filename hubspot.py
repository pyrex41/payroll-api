import asyncio
import aiohttp
import json
from pprint import pprint
from hubspot_config import *
from helpers import post_batch_after, get_batch_after, hs_path, hs_header, foldl, asyncio_wrap

# for async script, import:
# show_all_record_policies
# batch_contacts
# async_get_agents

# show all records -- call w/in async script
async def show_all_record_policies(session, object_id, **kargs):
    k = kargs.get('k', .3)
    n = kargs.get('n', 10_000)
    atasks = []
    # first run:
    suc = []
    fail_i = []
    path = hs_path("objects/"+object_id)
    ii = 0
    data = {
        "limit": min(n, 100),
        "properties": ["agent_originating", "agent_submitting", "policy_number", "carrier", "status", "plan_name", "plan__status"],
        "associations": ["contacts"]
    }
    success, stat, out = await get_batch_after(path, data, session, 0, k=k)
    if success:
        suc.append(out['results'])
    else:
        print("Fail 2")
        fail_i.append(i)
    page_next = out.get('paging', {}).get('next', {}).get('after', None)
    while page_next:
        ii += 100
        data['after'] = page_next
        data['limit'] = min(n-ii, 100)
        if ii >= n:
            break
        s = ii / 100
        success, stat, out = await get_batch_after(path, data, session, 1, k=k)
        if success:
            suc.append(out['results'])
        else:
            fail_i.append(i)
        page_next = out.get('paging', {}).get('next', {}).get('after', None)
        print(page_next)

    res = foldl(lambda a,b: a + list(b), [], suc)
    return res, fail_i

# records paging inner function
async def show_records_after(object_id, i, session, s, k):
    path = hs_path("objects/"+object_id)
    data = {
        "limit": min(z-i, 100),
        'after': i,
        "properties": ["agent_originating", "agent_submitting", "policy_number", "carrier"],
        "associations": ["contacts"]
    }
    pprint(data)
    return await get_batch_after(path, data, session, s, k=k)

# records paging function wrappers for repl interaction
def show_record_policies(object_id, **kargs):
    return asyncio_wrap(show_all_record_policies, object_id, **kargs)


# Batch pull of contacts -- call w/in async script
async def async_batch_contacts(session, arr, k, **kargs):
    atasks = []
    suc = []
    fail_i = []
    n = min(100, len(arr))
    print(n)
    print(len(arr))
    success_1, rstatus, out = await batch_contacts_after(arr[0:n], session, 0, n, k)
    if success_1:
        suc.append(out['results'])
    else:
        fail_i.append(0)

    if n < len(arr):
        for i in range(n,len(arr),100):
            s = i / 100 # rate limiter
            z = min(i+100, len(arr))
            a = arr[i:z]
            print({i: len(a)})
            t = asyncio.create_task(batch_contacts_after(a, session, s, len(a),k=k))
            atasks.append(t)
        at = await asyncio.gather(*atasks)
        for tup in at:
            success, stat, out = tup
            if success:
                suc.append(out['results'])
            else:
                fail_i.append(i)

    res = foldl(lambda a,b: a + list(b), [], suc)
    return res, fail_i


# inner function for batching contact pages
async def batch_contacts_after(arr, session, s, lim, k, **kargs):
    path = hs_path("objects/contacts/batch/read")
    data = {
        "inputs": [{'id': x} for x in arr],
        "limit": lim,
        "properties": ["ss_number", "firstname", "lastname", "email", "date_of_birth"]
    }
    return await post_batch_after(path, data, session, s, k=k, **kargs)

# debug functions for repl interaction
def batch_contacts(arr, **kargs):
    k = .2
    if 'k' in kargs:
        k = kargs.pop('k')
    return asyncio_wrap(async_batch_contacts, arr, k=k, **kargs)


# function to get agent names -- call in script
async def async_get_agents(session):
    async with session.get(hs_path("owners"), headers=hs_header()) as r:
        if not r.ok:
            raise Exception("can't pull owners?!?")
        j = await r.json()
        arr = j["results"]
    dic = {}
    for d in arr:
        dic[d['id']] = {
            'email': d['email'],
            'firstName': d['firstName'],
            'lastName': d['lastName']
        }
    return dic

# developer convenience functions for repl
def get_agents(**kargs):
    return asyncio_wrap(async_get_agents, **kargs)

# side effect!
def status_check_fix(arr):
    for dic in arr:
        is_active, status_code = status_check_inner(dic)
        dic['active'] = is_active
        dic['status'] = status_code

def status_check_inner(dic):
    d = dic['properties']
    if 'status' in d:
        status_code = d['status']
    else:
        status_code = d['plan__status']
    is_active = status_code == '200'
    return is_active, status_code


def batch_search_inner(path, data, **kargs):
    r, t = asyncio_wrap(async_batch_search_inner, path, data, **kargs)
    return r,t

def batch_search(path, data, **kargs):
    n = kargs.get('n', None)
    batch_size = min(kargs.get('batch_size', 1000), 10_000)
    print("Batch size is", batch_size)
    print("Batch 1 begin")
    nmax = batch_size
    if type(n)==int:
        nmax = min(n, batch_size)
    out, total = batch_search_inner(path, data, nmax=nmax, **kargs)
    print("Batch 1 complete:", len(out),"of", total)
    has_all = len(out) == total or len(out) == n
    ii = 1
    while not has_all:
        ii += 1
        print("Batch", ii, "begin")
        try:
            min_id = int(out[-1]['id'])
            nmax = batch_size
            if type(n)==int:
                nmax = min(n-len(out), batch_size)
            r, t = batch_search_inner(path, data, nmax=nmax, min_object_id = min_id, **kargs)
            out = out + r
            has_all = len(r) == t or len(out) == n
            print("Batch", ii,"-- ", len(r),"complete:", len(out), "of", total, "--", t-len(r), "left")
        except Exception as e:
            pprint(e)
            has_all = True # break

    return out, total


async def async_batch_search_inner(session, path, data, **kargs):
    min_object_id = kargs.get('min_object_id', None)
    max_object_id = kargs.get('max_object_id', None)
    nmax = kargs.get('nmax', 10_000)
    assert path.split("/")[-1] == "search"
    k = kargs.get('k', 0)
    if 'after' in data:
        data.pop('after')

    farr = data.get("filters", [])[0:1]
    if min_object_id and max_object_id:
        filt = {
            "propertyName": "hs_object_id",
            "operator": "BETWEEN",
            "value": min_object_id,
            "highValue": max_object_id
        }
        farr.append(filt)
    elif min_object_id:
        filt = {
            "propertyName": "hs_object_id",
            "operator": "GT",
            "value": min_object_id,
        }
        farr.append(filt)
    elif max_object_id:
        filt = {
            "propertyName": "hs_object_id",
            "operator": "LT",
            "value": max_object_id
        }
        farr.append(filt)
    data["filters"] = farr
    data['limit'] = 100
    data['sorts'] = [{
        'propertyName': 'hs_object_id',
        'direction': 'ASCENDING'
    }]
    n = 0

    arr = []
    total = 0
    keep_paging = True
    while keep_paging:
        if n >= nmax:
            break
        res, status, out = await post_batch_after(path, data, session, 1, k=k)
        try:
            arr = arr + out["results"]
            total = out['total']
            keep_paging = out.get('paging', {}).get('next', {}).get('after', None)
            data['after'] = keep_paging
            n = len(arr)
        except Exception as e:
            print("Error:")
            pprint(e)
            pprint(out)
            keep_paging = False

    return arr, total


async def async_get_contacts(session, params, **kargs):
    path = hs_path("objects/contacts")
    ii = 0
    n = kargs.get('n', 10_000)
    k = kargs.get('k', 0)
    params['limit'] = min(n, 100)
    suc = []
    fail_i = []
    async with session.get(path, headers=hs_header(), params=params) as r:
        if r.status != 200:
            print("what")
            return await r.text()
        t0 = await r.text()
        j0 = json.loads(t0)
        suc.append(j0['results'])
    page_next = j0.get('paging', {}).get('next', {}).get('after', None)
    while page_next:
        ii += 100
        params['after'] = page_next
        params['limit'] = min(n-ii, 100)
        if ii >= n:
            break
        success, stat, out = await get_batch_after(path, params, session, 1, k=k)
        if success:
            suc.append(out['results'])
        else:
            fail_i.append(ii)
        page_next = out.get('paging', {}).get('next', {}).get('after', None)
        print(page_next)

    res = foldl(lambda a, b: a + list(b), [], suc)
    return res, fail_i

def get_contacts(params={}, **kargs):
    limit_per_host = 30
    if 'limit_per_host' in kargs:
        limit_per_host = kargs.pop('limit_per_host')
    return asyncio_wrap(async_get_contacts, params, limit_per_host=limit_per_host, **kargs)

def get_object(object_id, params={}, filters=None, **kargs):
    k = .2
    if 'k' in kargs:
        k = kargs.pop('k')
    path = hs_path("objects/"+object_id+"/search")
    data = {
        'limit': 100,
        'sorts': [{'propertyName': 'hs_object_id', 'direction': 'ASCENDING'}],
        'properties': params
    }
    if filters:
        data['filters'] = filters
    return batch_search(path, data, k=k, **kargs)
