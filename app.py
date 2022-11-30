import agency_bloc as ab
from helpers import asyncio_wrap_ as wrap
from helpers import foldl
import production_funcs as pf
import hubspot as hs
import hubspot_migrate as hm
from report_funcs import *

from datetime import datetime, timedelta, date
from enum import Enum
import csv
import json
from os.path import exists
import pickle
import zipfile
import pathlib
from copy import copy
import os
import string
import itertools
import xlsxwriter
import time
import operator as op
import pandas as pd
import secrets
import gc
from pympler import asizeof
import asyncio
from asgiref.sync import sync_to_async
from deta import Deta

from fastapi import Depends, FastAPI, HTTPException, status, Request, Form, BackgroundTasks
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import FileResponse, RedirectResponse


deta = Deta("b0hdifmg_JFEgLc7cqC3cJbsYBVbfYY5NNMA645VN")
drive = deta.Drive("pickles")
status = deta.Base("status")
reports = deta.Drive("reports")



app = FastAPI()

security = HTTPBasic()

users = {
    b"josh": b"negroni"
}

def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    current_username_bytes = credentials.username.encode("utf8")
    current_password_bytes = credentials.password.encode("utf8")

    is_correct_credentials = False
    for k,v in users.items():
        is_correct_user = secrets.compare_digest(current_username_bytes, k)
        is_correct_password = secrets.compare_digest(current_password_bytes, v)
        if is_correct_user and is_correct_password:
            return credentials.username

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Incorrect email or password",
        headers={"WWW-Authenticate": "Basic"},
    )

def deta_status_get(key: str):
    resp = status.get(key)
    if resp:
        return resp["value"]
    return False

@app.post("/data/load", tags=["Data"])
async def load_data_call(background_tasks: BackgroundTasks, user: str = Depends(get_current_username)):
    loading_data = deta_status_get("loading_data")
    if not loading_data:
        background_tasks.add_task(load_data)
        return {'msg': 'initiating data_load'}
    else:
        return {'msg': 'data already loading'}

def load_data():
    status.put(True, "loading_data")
    print("data load beginning")
    at = []
    for x in ["msp", "map", "dvh", "copay", "contacts"]:
        fname = x + ".pickle"
        fetch_and_save(fname)
    print("data load complete")
    status.put(False, "loading_data")

def fetch_and_save(fname, drive = drive):
    print("Fetching", fname)
    data = drive.get(fname)
    print(data)
    print(type(data))
    if data:
        with open(fname, "wb+") as file:
            for chunk in data.iter_chunks(4096):
                file.write(chunk)
            data.close()
        return True
    return None


"""
Use this to check on whether data is stale or not
"""
@app.get("/data/refresh", tags=["Data"])
async def check_refresh(usr: str = Depends(get_current_username)):
    refresh_running = deta_status_get("data_refresh_active")
    if refresh_running:
        msg = "Refresh is running. If more than 10min has elapsed since you triggered this, you may need to restart the server"
    else:
        msg = "Refresh process is not running."
    if "msp.pickle" not in os.listdir():
        load_data()
    try:
        ti_c = os.path.getctime("contacts.pickle")
        c_ti = time.ctime(ti_c)
        return {'msg': msg, 'last_refresh': c_ti}
    except Exception as ee:
        return {'error': ee, 'last_refresh': "NA"}

def set_refresh(bool_: bool):
    status.put(bool_, "data_refresh_active")

"""``
Use this end point to trigger a data refresh. May timeout; use GET to check if refresh in progress.
"""
def hs_load_and_save(name):
    data = pf.hb_load_policies(name, False)
    print("size", asizeof.asizeof(data))
    fname = name + ".pickle"
    if fname in os.listdir():
        os.remove(fname)
    with open(fname, "wb") as file:
        pickle.dump(data, file)
    del data
    gc.collect()
    try:
        print(len(data))
    except Exception as ee:
        print(ee)
    drive.put(fname, path="./"+fname)
    #os.remove(fname)
    gc.collect()


def hs_load_and_save_contacts():
    props = hm.prop_names()
    contacts = hm.fetch_obj('contacts', *props, k=.005)
    with open('contacts.pickle', 'wb') as file:
        pickle.dump(contacts, file)
    del contacts
    gc.collect()
    drive.put("contacts.pickle", path = "./contacts.pickle")
    #os.remove('contacts.pickle')
    try:
        print(len(contacts))
    except Exception as ee:
        print(ee)
    gc.collect()


@app.post("/data/refresh/", tags=["Data"])
async def call_fetch(user: str = Depends(get_current_username)):
    is_running = deta_status_get("data_refresh_active")
    msg = "Data refresh is running"
    if not is_running:
        background_tasks.add_task(fetch_hubspot_data)
        msg = "Data refresh is initiated"
    return {'msg': msg}

def fetch_hubspot_data():
    set_refresh(True)

    file_list = ["msp", "map", "dvh", "copay"]
    for f in file_list:
        hs_load_and_save(f)

    hs_load_and_save_contacts()

    set_refresh(False)
    with open('last_run.txt', "w") as file:
        file.write(time.ctime())
    drive.put("last_run.txt", path = "./last_run.txt")
    print("data refresh is complete")
    set_refresh(False)

"""
Run Reports
"""
@app.post("/reports/run", tags=["Reports"])
async def respond_and_process(report_month, report_year, background_tasks: BackgroundTasks, usr: str = Depends(get_current_username)):
    background_tasks.add_task(process_agent_reports, report_month, report_year)
    return {"msg": "Processing reports in background. Use GET /reports/fetch to retrieve"}


def process_agent_reports(report_month, report_year):
    status.put(True, "processing_report")

    agents = hs.get_agents()
    contact_props = ['hubspot_owner_id']

    with open("msp.pickle", "rb") as file:
        supps = pickle.load(file)
    with open("map.pickle", "rb") as file:
        maps = pickle.load(file)
    with open("dvh.pickle", "rb") as file:
        dvh = pickle.load(file)
    with open("copay.pickle", "rb") as file:
        copay = pickle.load(file)
    with open("contacts.pickle", "rb") as file:
        contacts = pickle.load(file)

    cdic = {x['id']: x for x in contacts}

    psup, superrs, supdups = parse_through_data(supps[0], cdic, "msp", agents, report_year, report_month)

    pmap, maperrs, mapdups = parse_through_data(maps[0], cdic, "map", agents, report_year, report_month)
    pdvh, dvherrs, dvhdups = parse_through_data(dvh[0], cdic, "dvh", agents, report_year, report_month)
    pcopay, copayerrs, copaydups = parse_through_data(copay[0], cdic, "copay", agents, report_year, report_month)
    pcore = merge_dic(psup, pmap)

    adcore, aecore = sort_by_agents(pcore, originating=False)
    addvh, aedvh = sort_by_agents(pdvh, originating=True)
    adcopay, aecopay = sort_by_agents(pcopay, originating=True)

    last_month_new_dic = {agent: apply_month_filter(arr, "date_actual_submission", report_year, report_month) for agent, arr in adcore.items()}

    last_month_cancel_dic = {agent: apply_month_filter(arr, "date_cancel_notification", report_year, report_month) for agent, arr in adcore.items()}


    agent_longterm_cancels = cancel_counter(psup) | cancel_counter(pmap)

    agent_dvh_cancels = cancel_counter(pdvh)
    agent_copay_cancels = cancel_counter(pcopay)


    # book of business
    with open('agent_list.pickle', 'rb') as file:
        alist = pickle.load(file)

    agent_bob_full = get_agent_books(adcore, report_year, report_month)
    agent_bob = {k: len(v) for k,v in agent_bob_full.items()}
    abob_new = {k:v for k,v in agent_bob.items() if k in alist}
    with open('abob_core_report.pickle', 'wb') as file:
        pickle.dump(abob_new, file)

    agent_dvh_bob_full = get_agent_books(addvh, report_year, report_month)
    agent_dvh_bob = {k: len(v) for k,v in agent_dvh_bob_full.items()}
    with open('abob_dvh_report.pickle', 'wb') as file:
        pickle.dump(agent_dvh_bob, file)

    agent_copay_bob_full = get_agent_books(adcopay, report_year, report_month)
    agent_copay_bob = {k: len(v) for k,v in agent_copay_bob_full.items()}
    with open('abob_copay_report.pickle', 'wb') as file:
        pickle.dump(agent_copay_bob, file)


    #diff = diff_bob()

    folder_name = "agent_reports"
    if not os.path.exists(folder_name):
        os.mkdir(folder_name)
    old_files = os.listdir(folder_name)
    for filename in old_files:
        os.remove(folder_name+'/'+filename)


    fieldnames =  [
        'id',
        'originally_associated_contact_id',
        'firstname',
        'lastname',
        'agent_originating',
        'agent_firstname',
        'agent_lastname',
        'hubspot_owner_id',
        'hs_owner_firstname',
        'hs_owner_lastname',
        'policy_number',
        'commission_id',
        'carrier',
        'plan_name',
        'status',
        'gi___not_commissionable',
        'plan_type',
        'date_actual_submission',
        'date_requested_submission',
        'date_effective',
        'date_cancel_notification',
        'date_cancel_actual',
        'notes_misc_cancel',
        'first_date_effective',
        'active_policy',
        'residual',
        'grey'
    ]
    csv_outer(adcore, agents, fieldnames, "core")
    csv_outer(addvh, agents, fieldnames, "dvh")
    csv_outer(adcopay, agents, fieldnames, "copay")

    core_errs = error_filter(aecore + [group for group in superrs.values()] + [group for group in maperrs.values()])
    dvh_errs = error_filter(aedvh + [group for group in dvherrs.values()])
    copay_errs = error_filter(aecopay + [group for group in copayerrs.values()])

    error_fieldnames = ['error_type'] + fieldnames
    if len(core_errs) > 0:
        csv_inner(core_errs, error_fieldnames, 'agent_reports/_core_agent_errors.csv',show_errors=True)
    if len(dvh_errs) > 0:
        csv_inner(dvh_errs, error_fieldnames, 'agent_reports/_dvh_agent_errors.csv',show_errors=True)
    if len(copay_errs) > 0:
        csv_inner(copay_errs, error_fieldnames, 'agent_reports/_copay_agent_errors.csv',show_errors=True)

    if len(supdups) > 0:
        csv_inner(supdups, error_fieldnames, 'agent_reports/_supp_possible_duplicates.csv',show_errors=True)
    if len(mapdups) > 0:
        csv_inner(mapdups, error_fieldnames, 'agent_reports/_map_possible_duplicates.csv', show_errors=True)
    if len(dvhdups) > 0:
        csv_inner(dvhdups, error_fieldnames, 'agent_reports/_dvh_possible_duplicates.csv',show_errors=True)
    if len(copaydups) > 0:
        csv_inner(copaydups, error_fieldnames, 'agent_reports/_copay_possible_duplicates.csv',show_errors=True)

    directory = pathlib.Path("agent_reports/")
    with zipfile.ZipFile("error_reports.zip", mode="w") as archive:
        for file_path in directory.iterdir():
            if file_path.name[0] == "_":
                archive.write(file_path, arcname=file_path.name, compress_type = zipfile.ZIP_DEFLATED)

    """
    """
    abob_comp = diff_bob(RType("core"))
    abob_dvh = diff_bob(RType("dvh"))
    abob_copay = diff_bob(RType("copay"))
    groups = group_csv_by_agent('agent_reports')
    debit_adjustments_all = {str(k):v for k,v in show_all_adjustments().items()}
    print(debit_adjustments_all)
    new_debit_adjustments = {}

    summary_sheet_data = {}

    for aname, x in groups.items():
        bob_old, bob_new = abob_getter(abob_comp, aname)
        dvh_old, dvh_new = abob_getter(abob_dvh, aname)
        copay_old, copay_new = abob_getter(abob_copay, aname)

        lt_cancels, _ = agent_longterm_cancels.get(aname, (0,0))
        lt_dvh_cancels, _ = agent_dvh_cancels.get(aname, (0,0))
        lt_copay_cancels,_ = agent_copay_cancels.get(aname, (0,0))
        debit_adjustment = debit_adjustments_all.get(aname, 0)

        new_debit_adjustment = safe_add(bob_new, -1*safe_int(bob_old), lt_cancels,  -30, debit_adjustment)

        if new_debit_adjustment < 0 and str(aname) in alist:
            new_debit_adjustments[aname] = new_debit_adjustment

        recent_month_adds = last_month_new_dic.get(aname, [])
        recent_month_cancel = last_month_cancel_dic.get(aname, [])

        residual_counter = write_workbook(x, recent_month_adds, recent_month_cancel, bob_old, bob_new, lt_cancels, lt_dvh_cancels, lt_copay_cancels, debit_adjustment, dvh_old, dvh_new, copay_old, copay_new, folder='agent_reports', outdir='excel_reports')

        #net_new_active = safe_add(-1*safe_int(bob_old), bob_new, lt_cancels)
        #net_new_commis = safe_add(net_new_active, base_draw, debit_adjustment)
        #net_new_dvh = safe_add(-1*safe_int(dvh_old), dvh_new, lt_dvh_cancels)
        #net_new_copay = safe_add(-1*safe_int(copay_old), copay_new, lt_copay_cancels)
        summary_sheet_info = {
            'Last Month Active': bob_old,
            'Current Month Active': bob_new,
            'Lifetime Cancels': lt_cancels,
            'Net New Active': None, #net_new_active,
            'Base Draw Policies': base_draw,
            'Prior Month Debit': debit_adjustment,
            'Net New Commissionable': None, # net_new_commis
            'Core - Commission': None, #commission_amount * net_new_commis,
            #########
            'Residual Core Plans': residual_counter,
            'Core - Residual': None, #residual_counter * residual_amount,
            #########
            'Previous Active DVH': dvh_old,
            'Current Active DVH': dvh_new,
            'Lifetime Cancels DVH': lt_dvh_cancels,
            'Net New DVH': None, #net_new_dvh,
            'DVH - Commission': None, #net_new_dvh * dvh_amount,
            #########
            'Previous Active Copay': copay_old,
            'Current Active Copay': copay_new,
            'Lifetime Cancels Copay': lt_copay_cancels,
            'Net New Copay': None, #net_new_copay,
            'Copay - Commission': None, #net_new_copay * copay_amount,
        }
        if aname in alist:
            summary_sheet_data[aname] = summary_sheet_info

    agents_filtered = {k:v for k,v in agents.items() if k in alist}
    write_summary_book(summary_sheet_data, agents_filtered)

    print(new_debit_adjustments)
    with open("debit_adjustments_report.pickle", "wb") as file:
        pickle.dump(new_debit_adjustments, file)

    directory = pathlib.Path("excel_reports/")
    with zipfile.ZipFile("agent_reports.zip", mode="w") as archive:
        for file_path in directory.iterdir():
            archive.write(file_path, arcname=file_path.name, compress_type = zipfile.ZIP_DEFLATED)

    reports.put("agent_reports.zip", path = "./agent_reports.zip")
    status.put(False, "processing_report")



@app.get("/reports/errors", response_class=FileResponse, tags=["Reports"])
def fetch_error_reports(usr: str = Depends(get_current_username)):
    if "error_reports.zip" in os.listdir():
        return "error_reports.zip"
    raise HTTPException(
        status_code=418,
        detail="No error report zip file found; please run report first",
        headers={"WWW-Authenticate": "Basic"},
    )

@app.get("/reports/fetch", response_class=FileResponse, tags=["Reports"])
def fetch_last_reports(usr: str = Depends(get_current_username)):
    is_running = deta_status_get("processing_report")
    detail = "Reports are still being generated; hold your horses."
    if not is_running:
        is_ok = fetch_and_save("agent_reports.zip", reports)
        print("ok?", is_ok)
        if is_ok:
            print("returning")
            return "agent_reports.zip"
        detail = "Report does not exist; please generate reports"
    raise HTTPException(
        status_code=418,
        detail=detail,
        headers={"WWW-Authenticate": "Basic"},
    )


"""
Lock in Report Numbers for BOB and Debit Balances
"""
@app.post("/reports/lock", tags=["Reports", "Lock"])
def lock_abob_and_debit(usr: str = Depends(get_current_username)):
    try:
        bob_lock(RType("core"))
        bob_lock(RType("dvh"))
        bob_lock(RType("copay"))
        lock_debit_report()
        return {'msg': 'success'}
    except Exception as ee:
        return {'msg': 'error', 'details': ee}


"""
Debit Functions
"""
@app.get("/debit/{agent_id}/fetch", tags=["Debit Adjustments"])
def fetch_adjustment(agent_id: int, usr = Depends(get_current_username)):
    agents = show_agents()
    fname = 'debit_adjustments.pickle'
    if fname in os.listdir():
        with open(fname, 'rb') as file:
            fdic = pickle.load(file)
    else:
        fdic = {}
    if str(agent_id) in agents:
        res = fdic.get(agent_id)
        b = agents[str(agent_id)]
        firstname = b.get('firstName')
        lastname = b.get('lastName')
        if res:
            return {'msg': 'record of debit balance found', 'value': res}
        else:
            return {'msg': 'no debit balance found for {} {}'.format(firstname, lastname)}
    raise HTTPException(
        status_code=400,
        detail="must provide a valid agent id",
        headers={"WWW-Authenticate": "Basic"},
    )

@app.post("/debit/{agent_id}/set", tags=["Debit Adjustments"])
def make_adjustment(agent_id: int, value: int, usr = Depends(get_current_username)):
    agents = show_agents()
    if str(agent_id) in agents:
        holder = show_all_adjustments(history=True)
        if len(holder) > 0:
            fdic = copy(holder[-1])
        else:
            fdic = {}
        fdic[str(agent_id)] = value
        fname = "debit_adjustments.pickle"
        holder.append(fdic)
        with open(fname, 'wb') as file:
            pickle.dump(holder, file)
        return holder

    raise HTTPException(
        status_code=400,
        detail="must provide a valid agent id",
        headers={"WWW-Authenticate": "Basic"},
    )

@app.get("/debit/show", tags=["Debit Adjustments"])
def show_all_adjustments(history: bool = False, usr = Depends(get_current_username)):
    fname = 'debit_adjustments.pickle'
    if fname in os.listdir():
        with open(fname, 'rb') as file:
            holder = pickle.load(file)
        if history:
            return holder
        else:
            return holder[-1]
    else:
        holder = [{}]
        with open(fname, 'wb') as file:
            pickle.dump(holder, file)
        print('created empty adjustment file: {}'.format(fname))
        return holder

@app.post("/debit/lock", tags=["Debit Adjustments", "Lock"])
def lock_debit_report(usr: str = Depends(get_current_username)):
    f1 = "debit_adjustments.pickle"
    f2 = "debit_adjusments_report.pickle"
    print("hey")
    return lock_generic(f1, f2)

@app.post("/debit/wipe", tags=["Debit Adjustments"])
def wipe_debit_balances(usr: str = Depends(get_current_username)):
    fname = 'debit_adjustments.pickle'
    return wipe_generic(fname)

def wipe_generic(fname):
    if fname in os.listdir():
        os.remove(fname)
    with open(fname, 'wb') as file:
        pickle.dump({}, file)
    return {'msg': 'created empty file {}'.format(fname)}

"""
Agent Book-Of-Business Functions for Report Baselines

"""
class RType(str, Enum):
    core = "core"
    dvh = "dvh"
    copay = "copay"



@app.get("/bob/{rtype}/report", tags=["BOB Funcs"])
def fetch_abob_report(rtype: RType, username = Depends(get_current_username)):
    return fetch_abob_report_generic(base_report_name_match(rtype))

def base_report_name_match(rtype: RType):
    print(rtype)
    fname = 'abob_' + rtype + '.pickle'
    print(fname)
    return fname

def new_report_name_match(rtype: RType):
    return 'abob_' + rtype.value + '_report.pickle'


def fetch_abob_report_generic(filename):
    if filename not in os.listdir():
        return {'msg': 'please run report first'}
    with open(filename, 'rb') as file:
        out = pickle.load(file)
    return out

@app.post("/bob/{rtype}/lock", tags = ["BOB Funcs", "Lock"])
def bob_lock(rtype: RType, username = Depends(get_current_username)):
    f1 = base_report_name_match(rtype)
    f2 = new_report_name_match(rtype)
    return lock_generic(f1, f2)

def lock_generic(hold_file, new_file):
    if hold_file in os.listdir():
        with open(hold_file, 'rb') as file:
            holder = pickle.load(file)
    else:
        holder = [{}]

    if new_file in os.listdir():
        with open(new_file, 'rb') as file:
            new_dic = pickle.load(file)
        if holder[-1] != new_dic:
            holder.append(new_dic)
        with open(hold_file, 'wb') as file:
            pickle.dump(holder, file)
        os.remove(new_file)
        return holder
    else:
        return {'msg': 'no report found; please run report first'}

@app.delete("/bob/{rtype}/pop", tags = ["BOB Funcs"])
def pop_last_abob(rtype: RType, username: str = Depends(get_current_username)):
    fname = base_report_name_match(rtype)
    with open(fname, 'rb') as file:
        abobs = pickle.load(file)
    if len(abobs) > 0:
        abobs.pop()
    with open(fname, 'wb') as file:
        pickle.dump(abobs, file)
    return abobs

@app.delete("/bob/{rtype}/wipe", tags=["BOB Funcs"])
def wipe_abob(rtype: RType, username: str = Depends(get_current_username)):
    f1 = base_report_name_match(rtype)
    f2 = new_report_name_match(rtype)
    return wipe_abobs_generic(f1, f2)

def wipe_abobs_generic(*fnames):
    out = {}
    for f in fnames:
        try:
            os.remove(f)
            out[f] = "successfully deleted"
        except Exception as ee:
            out[f] = str(ee)
    with open(fnames[0], 'wb') as file:
        pickle.dump([{}], file)
    out['msg'] = 'created empty {} file'.format(fnames[0])
    return out

@app.get("/bob/{rtype}/diff", tags = ["BOB Funcs"])
def diff_bob_fetch(rtype: RType, user=Depends(get_current_username)):
    return diff_bob(rtype)

def diff_bob(rtype: RType):
    f1 = base_report_name_match(rtype)
    f2 = new_report_name_match(rtype)
    return diff_bob_generic(f1, f2)

def diff_bob_generic(hold_file,new_file):
    if hold_file not in os.listdir():
        return {"msg": "missing abob file; please use wipe function to reset"}
    if new_file not in os.listdir():
        return {"msg": "missing abob report; please run report first"}
    with open(new_file, 'rb') as file:
        abob_new = pickle.load(file)
    with open(hold_file, 'rb') as file:
        abobs = pickle.load(file)
        abob_old = abobs[-1]
    agents = hs.get_agents()
    diff = {
        k: {
            'diff': v - abob_old.get(k, 0),
            'old': abob_old.get(k, 0),
            'new': v,
            'firstname': agents.get(k, {}).get('firstName'),
            'lastname': agents.get(k,{}).get('lastName')
        } for k,v in abob_new.items()
    }
    return diff

@app.get('/agents/list', tags=["Agent Functions"])
def show_agents(username: str = Depends(get_current_username)):
    agents = hs.get_agents()
    with open('agent_list.pickle', 'rb') as file:
        alist = pickle.load(file)
    out = {k:v for k,v in agents.items() if k in alist}
    return out

@app.get('/agents/show_all', tags=["Agent Functions"])
def show_all_agents(usr: str = Depends(get_current_username)):
    return hs.get_agents()

@app.post('/agents/add', tags=["Agent Functions"])
def add_agent(agent_id: int, username: str = Depends(get_current_username)):
    s = str(agent_id)
    agents = hs.get_agents()
    with open('agent_list.pickle', 'rb') as file:
        alist = pickle.load(file)
    if s in agents:
        if s in alist:
            msg = {'message': 'agent already active'}
        else:
            alist.append(s)
            with open('agent_list.pickle', 'wb') as file:
                pickle.dump(alist, file)
            msg = {'message': 'agent has been added', 'agent_info': agents.get(s)}
    else:
        msg = {'message': 'agent_id not found in list of active hubspot users'}
    return msg

@app.delete('/agents/remove', tags=["Agent Functions"])
def remove_agent(agent_id: int, username: str = Depends(get_current_username)):
    s = str(agent_id)
    agents = hs.get_agents()
    with open('agent_list.pickle', 'rb') as file:
        alist = pickle.load(file)
    if s not in alist:
        msg = {'message': 'agent_id not found in list of active agents'}
    else:
        new_alist = [a for a in alist if a != s]
        with open('agent_list.pickle', 'wb') as file:
            pickle.dump(new_alist, file)
        msg = {'message': 'agent_id {} has been removed from active agent list'.format(s)}
    return msg


"""
Default Server Funcs
"""
@app.get("/")
async def root():
    return RedirectResponse(url='/docs')

class OLabel(str, Enum):
    copay = 'copay'
    dvh = 'dvh'
    map_ = 'map'
    msp = 'msp'

@app.get("/josh/show")
def funcall(username: str = Depends(get_current_username)):
    return {'msg': 'middle_finger'}

@app.get("/users/me")
def read_current_user(username: str = Depends(get_current_username)):
    return {"username": username}

@app.get("/demo/ellis")
def show_ellis(username: str = Depends(get_current_username)):
    return {"msg": "Hi!!"}

@app.get("/dir")
def gdir():
    globs = list(set(globals()))
    out = {}
    for k in globs:
        try:
            out[k] = asizeof.asizeof(k)
        except Exception as ee:
            print(ee)
    for k in dir():
        try:
            out[k] = asizeof.asizeof(k)
        except Exception as ee:
            print(ee)
    return out

@app.post("/gc")
def clear_garbage():
    gc.collect()
    return True

templates = Jinja2Templates(directory="templates/")

@app.get("/form")
def form_post(request: Request):
    tod = datetime.now()
    year = tod.year
    month = tod.month
    print(year)
    print(month)
    msg = "NA"
    return templates.TemplateResponse('form.html', context={'request': request, 'result': msg, 'month': month, 'year': year})


@app.post("/form")
def form_post(request: Request, year: int = Form(...), month: int = Form(...)):
    if year >= 2022 and month > 0 and month < 13:
        return RedirectResponse(url='/reports/run?report_month={}&report_year={}'.format(month, year))
    return templates.TemplateResponse('form.html', context={'request': request, 'msg': "Invalid Entry"})
