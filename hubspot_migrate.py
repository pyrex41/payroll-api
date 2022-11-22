import asyncio
import pickle
import aiohttp
import json
import requests
import csv
import numpy as np
from pprint import pprint
from hubspot_config import *
from hubspot import batch_search
from helpers import post_batch_after, get_batch_after, hs_path, hs_header, foldl, asyncio_wrap, hs_get

def parse_map_csv(filename, skip_header=True):
    dic = {}
    with open(filename, mode='r') as file:
        reader = csv.reader(file)
        if skip_header:
            next(reader, None)
        for row in reader:
            k = row[0]
            v = row[1]
            if v == 'None':
                v = None
            if len(row) > 2:
                if len(row[2]) > 0:
                    print(k, "--")
                    print(row[2])
                    print()
                    print()
            dic[k] = v
    return dic


def pick(arr, size=1):
    out = np.random.choice(arr, size=size, replace=False)
    if size == 1:
        return out[0]
    else:
        return out

def error_check_inner(d, nm, verbose=False):
    try:
        assert d[nm]
        return True
    except Exception as e:
        if verbose:
            print(nm)
            print(e)
        return False

def error_check(d):
    names = ['carrier', 'plan_name', 'policy_number', 'agent_originating']
    errs = []
    if d.get('status') == '200':
        test_res = []
        for nm in names:
            res = error_check_inner(d, nm)
            test_res.append(res)
            if not res:
                errs.append(nm)
        comb = foldl(lambda a,b: a and b, True, test_res)
        if d.get('carrier') == 'TRANSAMERICA':
            comb = True
        return comb, errs
    else:
        return True, []

def process_error_check(arr):
    err_dic = {}
    passed = []
    failed = []
    for d in arr:
        ok, errs = error_check(d)
        if not ok:
            for e in errs:
                a = err_dic.get(e, [])
                a.append(d['originally_associated_contact_id'])
                err_dic[e] = a
            failed.append(d)
        else:
            passed.append(d)

    for k,v in err_dic.items():
        print("Errors for", k, ":", len(v))
    return passed, failed, err_dic


def get_contact_properties():
    return hs_get("properties/contacts").json()["results"]

def prop_names(objid="contacts"):
    r = hs_get("properties/"+objid).json()["results"]
    return sorted([x["name"] for x in r])

def get_prop_options(prop_name, contact_properties=get_contact_properties(), show_labels=True):
    a = list(filter(lambda x: x['name'] == prop_name, contact_properties))[0]
    opt_list = a['options']
    vals = [x['value'] for x in opt_list]
    labels = [x['label'] for x in opt_list]
    if show_labels:
        return list(zip(vals, labels))
    else:
        return vals

async def async_batch(session, path, p_arr, **kargs):
    at = []
    k = kargs['k']
    batch_size = kargs.get('batch_size', 100)
    batch_size = min(max(batch_size, 0), 100)
    print('batch_size:', batch_size)
    p_lists = [p_arr[i:i+batch_size] for i in range(0, len(p_arr), batch_size)]
    for i,plist in enumerate(p_lists):
        sleep_ = k * i
        task = asyncio.create_task(async_batch_inner(session, path, plist, sleep=sleep_, **kargs))
        at.append(task)
    print(len(at), "tasks")
    return await asyncio.gather(*at)

async def async_batch_inner(session, path, plist, **kargs):
    path = path
    if path[0:4] != 'http':
        path = hs_path(path)
    sleep_ = kargs.get('sleep')
    if sleep_:
        await asyncio.sleep(sleep_)
    data = {'inputs': plist}
    if 'properties' in kargs:
        data['properties'] = kargs.get('properties')
    async with session.post(path, headers=hs_header(), data = json.dumps(data)) as r:
        t = await r.text()
        try:
            return json.loads(t)
        except:
            return t

async def async_batch_delete_obj(session, ids, obj_id, **kargs):
    path = 'objects/'+obj_id+'/batch/archive'
    plist = [{"id": id_} for id_ in ids]
    return await async_batch(session, path, plist, **kargs)

async def async_batch_create_obj(session, plist, obj_id, **kargs):
    path = 'objects/'+obj_id+'/batch/create'
    props = [{'properties': x} for x in plist]
    return await async_batch(session, path, props, **kargs)

async def async_batch_read_obj(session, ids, obj_id, props, **kargs):
    path = 'objects/'+obj_id+'/batch/read'
    plist = [{"id": id_} for id_ in ids]
    return await async_batch(session, path, plist, properties=props, **kargs)

async def async_batch_update_obj(session, plist, obj_id, **kargs):
    path = 'objects/'+obj_id+'/batch/update'
    props = [{'properties': y, 'id': x} for x,y in plist]
    return await async_batch(session, path, props, **kargs)

async def async_batch_assoc_read(session, obj1, obj2, idlist, **kargs):
    path = 'associations/'+obj1+'/'+obj2+'/batch/read'
    props = [{'id': x} for x in idlist]
    return await async_batch(session, path, props, **kargs)

def batch_read(ids, obj_id, props={}, **kargs):
    return asyncio_wrap(async_batch_read_obj, ids, obj_id, props, **kargs)

def batch_assoc_read(obj1, obj2, idlist, **kargs):
    k = .2
    if 'k' in kargs:
        k = kargs.pop('k')
    raw_return = asyncio_wrap(async_batch_assoc_read, obj1, obj2, idlist, k=k, **kargs)
    return foldl(lambda a,b: a+b, [], [x.get("results", []) for x in raw_return])

def batch_update_obj(plist, obj_id, **kargs):
    k = .2
    if 'k' in kargs:
        k = kargs.pop('k')
    return asyncio_wrap(async_batch_update_obj, plist, obj_id, k = k, **kargs)

def delete_obj(ids,obj_id,  **kargs):
    k = .2
    if 'k' in kargs:
        k = kargs.pop('k')
    if type(ids) != list:
        ids = [ids]
    return asyncio_wrap(async_batch_delete_obj, ids, obj_id, k=k, **kargs)

def batch_create_obj(arr_props, obj_id, **kargs):
    return asyncio_wrap(async_batch_create_obj, arr_props, obj_id, **kargs)

async def async_create_obj(session, record, obj_id, **kargs):
    data = {'properties': record}
    sleep_ = kargs.get('sleep')
    if sleep_:
        await asyncio.sleep(sleep_)
    async with session.post(hs_path('objects/'+obj_id), headers=hs_header(), data=json.dumps(data)) as r:
        t = await r.text()
        try:
            return json.loads(t)
        except:
            return t

async def async_several_obj(session, record_arr, obj_id, **kargs):
    at = []
    k = kargs.get('k', 0)
    for i, record in enumerate(record_arr):
        sleep_ = k * i
        at.append(asyncio.create_task(async_create_obj(session, record, obj_id, sleep=sleep_, **kargs)))
    return await asyncio.gather(*at)

def create_obj(record, **kargs):
    if type(record) == list:
        return asyncio_wrap(async_several_obj, record, obj_id, **kargs)
    else:
        assert type(record) == dict
        return asyncio_wrap(async_create_cobj, record, obj_id, **kargs)

def bulk_create_inner(arr, obj_id, batch_size, k, **kargs):
    print("inner:", len(arr))
    ok_batch = []
    err_batch = []
    r = batch_create_obj(arr, obj_id, batch_size=batch_size, k=k, **kargs)
    for i,x in enumerate(r):
        if x['status'] == 'COMPLETE':
            ok_batch.append((i*batch_size, x))
        else:
            err_batch.append((i*batch_size, x))
    return ok_batch, err_batch

def bulk_loop(obj_id, eindex, big_arr, group_size, batch_size, **kargs):
    oks = []
    errs = []
    for i in eindex:
        print(i)
        arr = big_arr[i:i+group_size]
        ok, err = bulk_create_inner(arr, obj_id, batch_size=batch_size, k=0, **kargs)
        for i_rel, ok_msg in ok:
            i_new = i + i_rel
            oks.append((i_new, ok_msg))
        for i_rel, e_msg in err:
            i_new = i_rel + i
            errs.append((i_new, e_msg))
    return oks, errs

def bulk_create(prop_arr, obj_id, **kargs):
    k = .2
    if 'k' in kargs:
        k = kargs.pop('k')
    ok1, e1 = bulk_create_inner(prop_arr, obj_id, batch_size=100, k=0, **kargs)
    e1_index = [x[0] for x in e1]
    ok2, e2 = bulk_loop(obj_id, e1_index, prop_arr, 100, 10, **kargs)
    e2_index = [x[0] for x in e2]
    ok3, e3 = bulk_loop(obj_id, e2_index, prop_arr, 10, 2, **kargs)
    e3_index = [x[0] for x in e3]
    ok4, e4 = bulk_loop(obj_id, e3_index, prop_arr, 2, 1, **kargs)
    oks = (ok1,ok2,ok3,ok4)
    ok = foldl(lambda a,b: a + b, [], oks)
    out = {}
    for a in ok:
        i0 = a[0]
        for i,x in enumerate(a[1]["results"]):
            out[i+i0] = x
    for k,v in e4:
        out[k] = v
    return out, e4


async def async_batch_associate(session, from_type, to_type, assoc_ype, dic, **kargs):
    path = "associations/"+from_type+"/"+to_type+"/batch/create"
    arr = []
    for k,v in dic.items():
        d = {
            'from': {'id': k},
            'to': {'id': v},
            'type': assoc_type
        }
        arr.append(d)
    return await async_batch(session, path, arr, **kargs)

def batch_associate(*args, **kargs):
    return asyncio_wrap(async_batch_associate,*args,**kargs)

def fetch_obj(obj_id, *prop_names, **kargs):
    path = hs_path("objects/"+obj_id+"/search")
    data = {}
    if prop_names:
        data['properties'] = prop_names
    if kargs.get('filters'):
        data['filters'] = kargs.get('filters')
    k = .2
    if 'k' in kargs:
        k = kargs.pop('k')
    r, _ = batch_search(path, data, k=k, **kargs)
    return r
