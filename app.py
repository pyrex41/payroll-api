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

deta_token = os.environ["DETA_ACCESS_TOKEN"]
deta = Deta(deta_token)
drive = deta.Drive("pickles")
status = deta.Base("status")
reports = deta.Drive("reports")

# set defaults
status.put(False, "data_refresh_active")
status.put(False, "processing_report")
status.put(False, "loading_data")



#app = FastAPI()
app = FastAPI(swagger_ui_parameters={
        "useUnsafeMarkdown": True
    }
)


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
    status.put(True, "loading_data", expire_in=300)
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
    if bool_:
        status.put(bool_, "data_refresh_active", expire_in=6000)
    else:
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
async def call_fetch(background_tasks: BackgroundTasks, user: str = Depends(get_current_username)):
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
async def respond_and_process(report_month, report_year, background_tasks: BackgroundTasks,request: Request, usr: str = Depends(get_current_username), refresh: bool=True):
    background_tasks.add_task(process_agent_reports, report_month, report_year, refresh)
    msg = "Report is being processed in background. Click link below to download:"
    return templates.TemplateResponse('landing.html', context={'request': request, 'msg': msg})


def process_agent_reports(report_month, report_year, refresh):
    status.put(True, "processing_report", expire_in=1000)
    if refresh:
        load_data()
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

    last_month_cancel_dic = {agent: apply_month_to_date_filter(arr, "date_cancel_notification", report_year, report_month) for agent, arr in adcore.items()}


    agent_map_cancels = cancel_counter(pmap)
    agent_longterm_cancels = cancel_counter(psup)
    for k,tup in agent_map_cancels.items():
        x,y = tup
        a,b = agent_longterm_cancels.get(k, (0,0))
        agent_longterm_cancels[k] = (a+x, y+b)

    print(agent_longterm_cancels)

    agent_dvh_cancels = cancel_counter(pdvh)
    agent_copay_cancels = cancel_counter(pcopay)


    # book of business
    with open('agent_list.pickle', 'rb') as file:
        alist = pickle.load(file)

    agent_pickle_writer(adcore, "abob_core_report.pickle", len, get_agent_books, report_year, report_month)
    agent_pickle_writer(addvh, "abob_dvh_report.pickle", len, get_agent_books, report_year, report_month)
    agent_pickle_writer(adcopay, "abob_copay_report.pickle", len, get_agent_books, report_year, report_month)

    agent_pickle_writer(agent_longterm_cancels, "ltc_core_report.pickle", lambda x: x[0])
    agent_pickle_writer(agent_dvh_cancels, "ltc_dvh_report.pickle", lambda x: x[0])
    agent_pickle_writer(agent_copay_cancels, "ltc_copay_report.pickle", lambda x: x[0])


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
    ltc_comp = diff_bob(RType("core"), "ltc")
    ltc_dvh = diff_bob(RType("dvh"), "ltc")
    ltc_copay = diff_bob(RType("copay"), "ltc")
    groups = group_csv_by_agent('agent_reports')
    debit_adjustments_all = {str(k):v for k,v in show_all_adjustments().items()}
    print(debit_adjustments_all)
    new_debit_adjustments = {}

    summary_sheet_data = {}

    for aname, x in groups.items():
        bob_old, bob_new = abob_getter(abob_comp, aname)
        dvh_old, dvh_new = abob_getter(abob_dvh, aname)
        copay_old, copay_new = abob_getter(abob_copay, aname)

        #lt_cancels, _ = agent_longterm_cancels.get(aname, (0,0))
        #lt_dvh_cancels, _ = agent_dvh_cancels.get(aname, (0,0))
        #lt_copay_cancels,_ = agent_copay_cancels.get(aname, (0,0))
        ltc_diff = abob_getter(ltc_comp, aname)
        ltc_dvh_diff = abob_getter(ltc_dvh, aname)
        ltc_copay_diff= abob_getter(ltc_copay, aname)

        debit_adjustment = debit_adjustments_all.get(aname, 0)

        lt_cancels = tdiff(ltc_diff)

        new_debit_adjustment = safe_add(bob_new, -1*safe_int(bob_old), lt_cancels,  -30, debit_adjustment)

        if new_debit_adjustment < 0 and str(aname) in alist:
            new_debit_adjustments[aname] = new_debit_adjustment

        recent_month_adds = last_month_new_dic.get(aname, [])
        recent_month_cancel = last_month_cancel_dic.get(aname, [])

        residual_counter = write_workbook(x, recent_month_adds, recent_month_cancel, bob_old, bob_new, ltc_diff, ltc_dvh_diff, ltc_copay_diff, debit_adjustment, dvh_old, dvh_new, copay_old, copay_new, folder='agent_reports', outdir='excel_reports')

        #net_new_active = safe_add(-1*safe_int(bob_old), bob_new, lt_cancels)
        #net_new_commis = safe_add(net_new_active, base_draw, debit_adjustment)
        #net_new_dvh = safe_add(-1*safe_int(dvh_old), dvh_new, lt_dvh_cancels)
        #net_new_copay = safe_add(-1*safe_int(copay_old), copay_new, lt_copay_cancels)
        summary_sheet_info = {
            'Last Month Active': bob_old,
            'Current Month Active': bob_new,
            'New Lifetime Cancels': lt_cancels,
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
            'New Lifetime Cancels DVH': tdiff(ltc_dvh_diff),
            'Net New DVH': None, #net_new_dvh,
            'DVH - Commission': None, #net_new_dvh * dvh_amount,
            #########
            'Previous Active Copay': copay_old,
            'Current Active Copay': copay_new,
            'New Lifetime Cancels Copay': tdiff(ltc_copay_diff),
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
    print("Reports Generated")


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
def fetch_last_reports(request: Request, usr: str = Depends(get_current_username)):
    is_running = deta_status_get("processing_report")
    msg = "Reports are still being generated; hold your horses. Please reload this page in a few moments"
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
    else:
        return templates.TemplateResponse("landing.html", context={'request': request, 'msg': 'Report is still running; please try again in a moment'})


"""
Lock in Report Numbers for BOB and Debit Balances
"""
@app.post("/reports/lock", tags=["Reports", "Lock"])
def lock_abob_and_debit(usr: str = Depends(get_current_username)):
    try:
        bob_lock(RType("core"))
        bob_lock(RType("dvh"))
        bob_lock(RType("copay"))
        ltc_lock(RType("core"))
        ltc_lock(RType("dvh"))
        ltc_lock(RType("copay"))
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

def base_report_name_match(rtype: RType, prefix: str = "abob"):
    print(rtype)
    fname = prefix + '_' + rtype + '.pickle'
    print(fname)
    return fname

def new_report_name_match(rtype: RType, prefix: str = "abob"):
    return prefix + '_' + rtype.value + '_report.pickle'


def fetch_abob_report_generic(filename):
    if filename not in os.listdir():
        return {'msg': 'please run report first'}
    with open(filename, 'rb') as file:
        out = pickle.load(file)
    return out

@app.post("/bob/set/{agent_id}/{rtype}/", tags=["BOB Funcs", "Manual Adjustments"])
def manual_agent_bob_set(numb: int, rtype: RType, agent_id: int, usr: str = Depends(get_current_username)):
    return manual_agent_set(numb, rtype, agent_id, "abob")

def manual_agent_set(numb: int, rtype: RType, agent_id: int, prefix: str):
    agents = show_agents()
    fname = base_report_name_match(rtype, prefix = prefix)
    if str(agent_id) in agents:
        with open(fname, 'rb') as file:
            abobs = pickle.load(file)
        dic = abobs[-1]
        dic[str(agent_id)] = numb
        with open(fname, 'wb') as file:
            pickle.dump(abobs, file)
        drive.put(fname, path="./"+fname)
        return dic
    else:
        return {'msg': 'invalid agent_id'}

@app.post("/bob/{rtype}/lock", tags = ["BOB Funcs", "Lock"])
def bob_lock(rtype: RType, username = Depends(get_current_username)):
    return diff_lock(rtype, "abob")

def diff_lock(rtype: RType, prefix: str):
    f1 = base_report_name_match(rtype, prefix)
    f2 = new_report_name_match(rtype, prefix)
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
        drive.put(hold_file, path="./"+hold_file)
        os.remove(new_file)
        return holder
    else:
        return {'msg': 'no report found; please run report first'}

@app.delete("/bob/{rtype}/pop", tags = ["BOB Funcs"])
def pop_last_abob(rtype: RType, username: str = Depends(get_current_username)):
    return pop_last_generic(rtype, "abob")

def pop_last_generic(rtype: RType, prefix: str):
    fname = base_report_name_match(rtype, prefix)
    with open(fname, 'rb') as file:
        abobs = pickle.load(file)
    if len(abobs) > 0:
        abobs.pop()
    with open(fname, 'wb') as file:
        pickle.dump(abobs, file)
    drive.put(fname, path="./"+fname)
    return abobs

@app.delete("/bob/{rtype}/wipe", tags=["BOB Funcs"])
def wipe_abob(rtype: RType, username: str = Depends(get_current_username)):
    return wipe_generic(rtype, "abob")

def wipe_generic(rtype: RType, prefix):
    f1 = base_report_name_match(rtype, prefix)
    f2 = new_report_name_match(rtype, prefix)
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
    return diff_bob(rtype, "abob")

def diff_bob(rtype: RType, prefix: str = "abob"):
    f1 = base_report_name_match(rtype, prefix)
    f2 = new_report_name_match(rtype, prefix)
    return diff_bob_generic(f1, f2, prefix)

def diff_bob_generic(hold_file,new_file, prefix: str):
    if hold_file not in os.listdir():
        return {"msg": "missing {} file; please use wipe function to reset".format(prefix)}
    if new_file not in os.listdir():
        return {"msg": "missing {} report; please run report first".format(prefix)}
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
Save the pickles
"""
@app.post("/pickles/backup")
def save_pickles(background_tasks: BackgroundTasks, usr: str = Depends(get_current_username)):
    for pick in os.listdir():
        if ".pickle" in pick:
            background_tasks.add_task(pickle_put,drive, pick)
    return {'msg': 'saving to backup'}

def pickle_put(drive, fname):
    return drive.put(fname, path="./"+fname)

@app.get("/pickles/load_from_backup")
def load_pickles(background_tasks: BackgroundTasks, usr: str = Depends(get_current_username)):
    pickle_list = drive.list().get('names')
    for pick in pickle_list:
        if ".pickle" in pick:
            background_tasks.add_task(fetch_and_save,pick)
    return {'msg': 'loading from backup'}

"""
Long-term Cancel Lock
"""

@app.get("/ltc/{rtype}/report", tags=["LTC Funcs"])
def fetch_ltc_cancel_report(rtype: RType, usr: str = Depends(get_current_username)):
    typestr = base_report_name_match(rtype, prefix = "ltc")
    return fetch_abob_report_generic(typestr)

@app.post("/ltc/set/{agent_id}/{rtype}", tags=["LTC Funcs", "Manual Adjustments"])
def manual_agent_ltc_set(numb: int, rtype: RType, agent_id: int, usr: str = Depends(get_current_username)):
    return manual_agent_set(numb, rtype, agent_id, "ltc")

@app.post("/ltc/{rtype}/lock", tags=["LTC Funcs", "Lock"])
def ltc_lock(rtype: RType, usr: str = Depends(get_current_username)):
    return diff_lock(rtype, "ltc")

@app.delete("/ltc/{rtype}/pop", tags=["LTC Funcs"])
def pop_last_ltc(rtype: RType, usr: str = Depends(get_current_username)):
    return pop_last_generic(rtype, "ltc")

@app.delete("/ltc/{rtype}/wipe", tags=["LTC Funcs"])
def wipe_ltc(rtype: RType, usr: str = Depends(get_current_username)):
    return wipe_generic(rtype, "ltc")

@app.get("/ltc/{rtype}/diff", tags=["LTC Funcs"])
def diff_ltc_fetch(rtype: RType, user: str = Depends(get_current_username)):
    return diff_bob(rtype, "ltc")



"""
Default Server Funcs
"""

@app.get("/deta/token")
def show_token(usr: str = Depends(get_current_username)):
    return deta_token

@app.get("/")
async def root():
    return RedirectResponse(url='/form')

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
def form_post(request: Request, usr: str = Depends(get_current_username)):
    tod = datetime.now()
    year = tod.year
    month = tod.month - 1
    if month < 1:
        month = 12
        year -= 1
    print(year)
    print(month)
    msg = "NA"
    return templates.TemplateResponse('form.html', context={'request': request, 'result': msg, 'month': month, 'year': year})


@app.post("/form")
def form_post(request: Request, year: int = Form(...), month: int = Form(...), usr: str = Depends(get_current_username)):
    if year >= 2022 and month > 0 and month < 13:
        return RedirectResponse(url='/reports/run?report_month={}&report_year={}'.format(month, year))
    return templates.TemplateResponse('form.html', context={'request': request, 'msg': "Invalid Entry"})
