from datetime import datetime, timedelta, date
from enum import Enum
import csv
import json
from os.path import exists
import pickle
import zipfile
import pathlib
from copy import copy, deepcopy
import os
import string
import itertools
import xlsxwriter
import time
import operator as op
import pandas as pd
import secrets
from helpers import foldl

# configuration params
base_draw = -30
commission_amount = 150
residual_amount = 2.50   # not currently used
copay_amount = 100
dvh_amount = 50


label_dic = {
    '2-7775359': 'MAP',
    '2-8483761': 'MSP',
    '2-8483892': 'COPAY',
    '2-8483915': 'DVH',
    '2-8567541': 'PDP'
}
oid_dic = {
    'COPAY': '2-8483892',
    'DVH': '2-8483915',
    'MAP': '2-7775359',
    'MSP': '2-8483761',
    'PDP': '2-8567541',
    'MAPD': '2-7775359'
}


def abob_getter(dic, aname):
    old = dic.get(aname, {}).get('old')
    new = dic.get(aname, {}).get('new')
    return old, new

# getting book of business
def get_agent_books(agent_core_dic, report_year, report_month):
    agent_strip_dic0 = purge_no_submit_date(agent_core_dic)
    agent_strip_dic1 = agent_strip_dic0 #purge_2020(agent_strip_dic0)
    agent_strip_dic = purge_before_report(agent_strip_dic1, report_year, report_month)
    filtered_groups = {}
    for k,groups in agent_strip_dic.items():
        fgroups = []
        for group in groups:
            if apply_filts(group, report_year, report_month):
                fgroups.append(group)
        filtered_groups[k] = fgroups

    return filtered_groups

def purge_no_submit_date(dic):
    out = {}
    for k,grouplist in dic.items():
        vnew = purge_no_submit_date_arr(grouplist)
        if vnew:
            out[k] = vnew
    return out

def purge_no_submit_date_arr(grouplist):
    out = []
    for group in grouplist:
        a = []
        for d in group:
            date_submit = d.get('date_actual_submission')
            if date_submit:
                a.append(d)
        if a:
            out.append(a)
    return out

def purge_before_report(dic, report_year, report_month):
    return {k: purge_before_report_arr(v, report_year, report_month) for k,v in dic.items()}

def purge_before_report_arr(arr, report_year, report_month):
    out = []
    y_int = int(report_year)
    m_int = int(report_month)
    if m_int == 12:
        y = y_int + 1
        m = 1
    else:
        y = y_int
        m = m_int + 1
    date_thresh = datetime(y,m,1,0,0)
    for group in arr:
        new_group = []
        for d in group:
            date_raw = d['date_actual_submission']
            dt = datetime.strptime(d['date_actual_submission'], "%Y-%m-%d")
            if dt < date_thresh:
                new_group.append(d)
        if new_group:
            out.append(new_group)
    return out

def purge_2020(dic):
    return {k: purge_2020_arr(v) for k,v in dic.items()}

def purge_2020_arr(arr):
    out = []
    for group in arr:
        d = group[0]
        date_raw = d['date_actual_submission']
        dt = datetime.strptime(d['date_actual_submission'], "%Y-%m-%d")
        if dt.year != 2020:
            out.append(group)
    return out


def apply_filts(group, record_year, record_month):
    has_active_and_commissionable = False
    for d in group:
        status = d.get('status', ' ')
        gi = d.get('gi___not_commissionable')
        if status[0] =='2' or status[0] == '1':
            if gi != "Not Commissionable":
                has_active_and_commissionable = True
    date_first = group[0].get('date_actual_submission')
    date_ok = book_date_filter(date_first, record_year, record_month)
    return date_ok and has_active_and_commissionable

def book_date_filter(x, record_year, record_month):
    year_i = int(record_year)
    month_i = int(record_month)
    if month_i == 12:
        lead_year = year_i +1
        lead_month = 1
    else:
        lead_year = year_i
        lead_month = month_i + 1
    date_thresh = datetime(lead_year, lead_month, 1, 0, 0)
    date_x = datetime.strptime(x, '%Y-%m-%d')
    return date_x < date_thresh


def csv_outer(dic, agents, fieldnames, nametype):
    for k,grouplist in dic.items():
        firstname = agents.get(k, {}).get('firstName', 'Inactive')
        lastname = agents.get(k, {}).get('lastName', 'Agent')
        fname = "agent_reports/" + firstname + '_'+ lastname + '_' + k + "_" + nametype + ".csv"
        csv_inner(grouplist, fieldnames, fname)

def csv_inner(grouplist, fieldnames, filename, show_errors=False):
    with open(filename, 'w') as file:
        writer = csv.DictWriter(file, fieldnames)
        writer.writeheader()
        for group in grouplist:
            cgroup = deepcopy(group)
            if not show_errors:
                for d in cgroup:
                    if 'error_type' in d:
                        d.pop('error_type')
            writer.writerows(cgroup)
            writer.writerow({})

def group_csv_by_agent(folder):
    fa = filter(lambda x: x[0] != '_', os.listdir(folder))
    fb = filter(lambda x: x.split('.')[-1] == 'csv', fa)
    fc = filter(lambda x: 'Inactive' not in x, fb)
    keyfunc = lambda x: x.split('_')[-2]
    groups = itertools.groupby(sorted(fc, key=keyfunc), key=keyfunc)
    gsortdic = {'core': 1, 'dvh': 2, 'copay': 3}
    gsort = lambda x: gsortdic.get(x.split('_')[-1].split('.')[0])
    return {k:sorted(list(x), key = gsort) for k,x in groups}


def error_filter(arr):
    out = []
    for a in arr:
        a_filt = []
        for d in a:
            status = d.get('status')
            if status:
                if status[0] in ['2', '4', '5']:
                    a_filt.append(d)
        out.append(a_filt)
    return out

def error_filter2(dd):
    out = {}
    for k,a in dd.items():
        a_filt = []
        for d in a:
            props = d.get('properties', {})
            status = props.get('status')
            if not status:
                status = props.get('plan__status')
            if status:
                if status[0] in ['2', '3', '4', '5']:
                    date_effective_raw = d.get('date_effective')
                    if date_effective_raw:
                        date_effective = datetime.strptime(date_effective_raw, "%Y-%m-%d")
                        today = date.today()
                        three_months = (date_effective.date() - date.today()).days > 90
                        carrier = d.get('carrier')
                        if carrier:
                            is_aarp = 'AARP' in carrier
                        else:
                            is_aarp = False
                        d['properties']['grey'] = three_months and is_aarp
                    else:
                        d['properties']['grey'] = False
                    a_filt.append(d)
        out[k] = a_filt
    return out

def date_filter(date):
    if date:
        return datetime.strptime(date, "%Y-%m-%d")
    else:
        return datetime.fromisoformat("2300-01-01")

def sort_by_agents(dic, originating):
    all_dics = foldl(lambda a,b: a + b, [], list(dic.values()))
    originating_agents = list(set([x['agent_originating'] for x in all_dics if x] + [x['hubspot_owner_id'] for x in all_dics if x]))
    ad = {}
    agent_errors = []

    for group in dic.values():
        possible_agents = get_possible_agents(group)
        if len(possible_agents) > 1 or len(possible_agents) == 0:
            remove_report = False
            for d in group:
                d['error_type'] = 'agent_error' + str(len(possible_agents))
                remove = d.get('remove_from_commission_reporting_error_check_report__mgmt_only_')
                if remove:
                    remove_report = True
            if not remove_report:
                agent_errors.append(group)

        try:
            agent = possible_agents[0]
            group.sort(key=lambda dic: date_filter(dic.get('date_effective')))
            arr = ad.get(agent, [])
            arr.append(group)
            ad[agent] = arr
        except Exception as ee:
            print(ee)

    return ad, agent_errors

def get_possible_agents(group):
    possible_set = set([x['agent_originating'] for x in group if x]).union(set([x['hubspot_owner_id'] for x in group if x]))
    possible_agents = list(filter(lambda x: x != None and x != '', list(possible_set)))
    return possible_agents

def parse_through_data(hdic_full, cdic, olabel, agents, report_year, report_month):
    hdic = {}
    for k,arr in hdic_full.items():
        anew = []
        for d in arr:
            props = d.get('properties')
            if props:
                if olabel == 'map':
                    status = props.get('plan__status')
                else:
                    status = props.get('status')
                if status:
                    if status[0] in ['1', '2', '3', '4', '5']:
                        anew.append(d)
        if anew:
            hdic[k] = anew

    dups = dict(filter(lambda tup: len(tup[1]) != 1, hdic.items()))
    dups = error_filter2(dups)

    ok = hdic

    policy_props = [
        'originally_associated_contact_id',
        'agent_originating',
        'policy_number',
        'commission_id',
        'carrier',
        'plan_name',
        'status',
        'gi___not_commissionable',
        'date_actual_submission',
        'date_requested_submission',
        'date_effective',
        'date_cancel_notification',
        'date_cancel_actual',
        'first_date_effective',
        'active_policy',
        'residual',
        'grey',
        'notes_misc_cancel',
    ]
    contact_props = ['hubspot_owner_id', 'firstname', 'lastname']

    out = parse_data_inner(ok.values(), cdic, agents, policy_props, contact_props, olabel)
    dup_parse = parse_data_inner(dups.values(), cdic, agents, policy_props, contact_props, olabel)
    dup_dic = {}
    for odic in dup_parse:
        if odic['commission_id'] in ['', 'N/A', None]:
            odic['error_type'] = 'missing_commission_id'
        else:
            odic['error_type'] = 'possible_duplicate'
        com_id = odic.get('commission_id')
        arr = dup_dic.get(com_id, [])
        arr.append(odic)
        dup_dic[com_id] = arr

    dd1 = {}
    for odic in out:
        cid = odic.get('originally_associated_contact_id')
        a = dd1.get(cid, [])
        a.append(odic)
        dd1[cid] = a
    dd = {}
    for k,arr in dd1.items():
        anew = sorted(arr, key=date_sort_key)
        dd[k] = anew
    if olabel in ["msp", "map"]:
        for k,arr in dd.items():
            dfirst = arr[0]['date_effective']
            is_active = '200' in [x['status'] for x in arr]
            commissionable = True
            for d in arr:
                d['first_date_effective'] = dfirst
                d['active_policy'] = is_active
                if d['gi___not_commissionable'] == 'Not Commissionable':
                    commissionable = False
            report_date = datetime.strptime(report_year+"-"+report_month+"-01","%Y-%m-%d")
            init_date_raw = d['first_date_effective']
            if init_date_raw:
                init_date = datetime.strptime(init_date_raw, "%Y-%m-%d")
                residual_eligible = (report_date - init_date).days > 364
                arr[-1]['residual'] = residual_eligible and arr[-1]['active_policy'] and commissionable
    ok, err = filter_errors(dd, olabel)
    return ok, err, list(dup_dic.values())

def date_sort_key(x):
    draw = x.get('date_effective')
    if draw:
        return datetime.strptime(draw, "%Y-%m-%d")
    else:
        return datetime.strptime('2300-01-01', "%Y-%m-%d")

def group_date_logic(arr):
    for group in arr:
        edates = [x.get('date_effective') for x in group]


def parse_data_inner(arr_iter, cdic, agents, policy_props, contact_props, olabel):
    out = []
    for arr in arr_iter:
        for dic in arr:
            odic = {x: dic['properties'].get(x) for x in policy_props}
            odic['id'] = dic['id']

            # handling weirdness for MAP
            if olabel == "map":
                odic['date_effective'] = dic['properties'].get('date__effective')
                odic['date_requested_submission'] = dic['properties'].get('date__submitted___requested')
                odic['date_cancel_actual'] = dic['properties'].get('date__cancel___actual')
                odic['date_cancel_notification'] = dic['properties'].get('date_cancelled')
                odic['status'] = dic['properties'].get('plan__status')
                odic['date_actual_submission'] = dic['properties'].get('date_enrollment')
                odic['notes_misc_cancel'] = dic['properties'].get('notes__misc__cancel_notes')
            elif olabel == 'dvh':
                pass
            elif olabel == 'copay':
                pass

            odic['plan_type'] = olabel
            contact_id = odic['originally_associated_contact_id']
            contact_record = cdic.get(contact_id, {}).get('properties', {})

            agent_id = odic.get('agent_originating')
            hs_owner_id = contact_record.get('hubspot_owner_id')

            firstname, lastname = format_agent_name(agents, agent_id)
            odic['agent_firstname'] = firstname
            odic['agent_lastname'] = lastname

            firstname, lastname = format_agent_name(agents, hs_owner_id)
            odic['hs_owner_firstname'] = firstname
            odic['hs_owner_lastname'] = lastname

            for k in contact_props:
                odic[k] = cdic.get(contact_id, {}).get('properties', {}).get(k)
            out.append(odic)
    return out

def filter_errors(dd, olabel):
    ok,err = {},{}
    for k,group in dd.items():
        has_err = False
        for odic in group:
            is_ok, etype = errcheck(odic, olabel)
            if not is_ok:
                has_err = True
                odic['error_type'] = etype
        if has_err:
            for odic in group:
                if not odic.get('error_type'):
                    odic['error_type'] = 'NA'
            err[k] = group
        else:
            ok[k] = group
    out = dict(sorted(list(ok.items()), key=sortkey))
    return out, err

def sortkey(tup):
    val = tup[1][0].get('lastname')
    if val == None:
        return ''
    else:
        return val

def errcheck(odic, olabel):
    submit_date = odic.get('date_actual_submission')
    if not submit_date:
        submit_date = odic.get('date_enrollment')
    if not submit_date:
        status = odic['status']
        if status[0] == '1' or status[0] == '2':
            return False, 'missing_submit_date'

    """
    elif olabel == "dvh":
        if not odic.get("commission_id__if_applicable_") or not odic.get("vision_member_id"):
            return False, "missing_commission_id"
    elif olabel == "copay":
        if not odic.get("commission_id__if_applicable_"):
            return False, "missing_commission_id"
    """
    return True, None

def format_agent_name(agents, agent_id):
    firstname = agents.get(agent_id, {}).get('firstName')
    lastname = agents.get(agent_id, {}).get('lastName')
    if firstname and lastname:
        return firstname, lastname
    else:
        return 'Inactive','Agent'

def merge_dic(*dics):
    assert len(dics) > 1
    dic0 = copy(dics[0])
    rdics = dics[1:]
    for d in rdics:
        for k,v in d.items():
            a0 = dic0.get(k, [])
            dic0[k] = a0 + v
    return dic0

def id_link_map(id_, plan_type):
    if pd.notna(id_) and pd.notna(plan_type):
        oid = oid_dic[plan_type.upper()]
        link = "https://app.hubspot.com/contacts/7879306/record/" + oid + "/" + str(id_)
        return "=HYPERLINK(\"" + link + "\", \"" + str(id_) + "\")"
    else:
        return ""

def write_workbook(group, recent_month_adds, recent_month_cancel, bob_old, bob_new, lt_cancels, lt_dvh_cancels, lt_copay_cancels, debit_adjustment, dvh_old, dvh_new, copay_old, copay_new, folder="", outdir=None):
    ss = group[0].split("_")
    firstname, lastname = ss[0:2]
    fname = firstname+"_"+lastname+".xlsx"
    if not outdir:
        outdir = os.getcwd()
    fname = outdir + "/" + fname
    print(fname)
    workbook = xlsxwriter.Workbook(fname)

    currency_format = workbook.add_format({'num_format': '$#,##0.00'})
    currency_format.set_align('center')
    center_format = workbook.add_format()
    center_format.set_align('center')

    currency_format_bold = workbook.add_format({'num_format': '$#,##0.00', 'align': 'right', 'bold': 1})

    front = workbook.add_worksheet("Summary")

    write_lastmonth_worksheet(workbook, "Last Month NEW", recent_month_adds)
    write_lastmonth_worksheet(workbook, "Last Month CANCEL", recent_month_cancel, preamble=cancel_preamble)

    residual_counter = 0
    for csvs_raw in group:
        csvs=csvs_raw
        if folder:
            csvs = folder+"/"+csvs
        rows = []
        with open(csvs, "r") as file:
            csvFile = csv.reader(file)
            for row in csvFile:
                rows.append(row)
        sheetname = csvs.split("_")[-1].split(".")[0].capitalize()
        headers = {
            'policy_url': 'Policy Record', #id
            'contact_url':'Contact Record', #originally_associated_contact_id
            'firstname':'First',
            'lastname':'Last',
            'agent_firstname':'OA First',
            'agent_lastname':'OA Last',
            'hs_owner_firstname':'Agent First',
            'hs_owner_lastname':'Agent Last',
            'carrier': 'Carrier',
            'plan_name':'Plan',
            'plan_type': 'Type',
            'status':'Status',
            'date_actual_submission': 'Submitted',
            'date_effective': 'Effective',
            'date_cancel_notification': 'Notified of Cancel',
            'date_cancel_actual':'Term Date',
            'active_policy':'Active?',
            'notes_misc_cancel': 'Cancellation Notes'
        }


        full_headers = rows[0]
        data_full = rows[1:]
        ddic1 = [{full_headers[i]:v for i,v in enumerate(row)} for row in data_full]
        ddic = []
        for rdic in ddic1:
            record_id = rdic.get('id')
            olabel = rdic.get('plan_type')
            url1 = ''
            if olabel:
                oid = oid_dic[olabel.upper()]
                url1 = "https://app.hubspot.com/contacts/7879306/record/" + oid + "/" + record_id
            rdic['policy_url'] = url1
            contact_id = rdic.get('originally_associated_contact_id')
            url2 = ''
            if contact_id:
                url2 = "https://app.hubspot.com/contacts/7879306/contact/"+contact_id
            rdic['contact_url'] = url2
            date_fields = filter(lambda x: 'date' in x, rdic.keys())
            for d in date_fields:
                ds = rdic.get(d)
                if ds:
                    rdic[d] = date_format(ds)
            status = rdic.get('status')
            if status:
                rdic['status'] = status_format(status)
            plan_type = rdic.get('plan_type')
            if plan_type:
                rdic['plan_type'] = plan_format(plan_type)
            ddic.append(rdic)

        if "core" in csvs:
            ddic2 = copy(ddic)
            ddic = []
            headers['residual'] = 'Eligible for Residual'
            headers['residual_amount'] = 'Residual'
            headers['gi___not_commissionable'] = 'Commissionable?'
            headers['msp_and_residual'] = 'MSP and Residual'
            headers['map_and_residual'] = 'MAP and Residual'
            for rdic in ddic2:
                res_eligable = rdic.get('residual')
                if res_eligable == 'True':
                    residual_counter += 1
                    rdic['residual_amount'] = 2.5
                    if rdic['plan_type'] == 'msp':
                        rdic['msp_and_residual'] = True
                        rdic['map_and_residual'] = ''
                    else:
                        rdic['msp_and_residual'] = ''
                        rdic['map_and_residual'] = True
                else:
                    rdic['residual_amount'] = ''

                    rdic['msp_and_residual'] = ''
                    rdic['map_and_residual'] = ''
                ddic.append(rdic)
        data = [[x[field] for field in headers.keys()] for x in ddic]

        write_worksheet(workbook, sheetname, list(headers.values()), data, center_format, currency_format)

    left = workbook.add_format({'align': 'left'})
    right = workbook.add_format({'align': 'right'})
    center = workbook.add_format({'align': 'center'})
    centerBold = workbook.add_format({'align': 'center', 'bold': 1})
    rightBold = workbook.add_format({'align': 'right', 'bold': 1})
    leftBold = workbook.add_format({'align': 'left', 'bold': 1})
    leftItalic = workbook.add_format({'align': 'left', 'italic': 1})

    currency_format.set_align("right")
    front.write("A1", "Core Plans Summary", leftBold)
    front.write("A2", "Last Month Active", right)
    front.write("A3", "Current Month Active", right)
    front.write("A4", "Lifetime Cancels (12+ Month Duration)", right)
    front.write("C4", "(These plans are added back into Current Month Total Active)", leftItalic)
    front.write("A5", "Net New Active", rightBold)
    front.write("A6", "Base Draw Policies (Pre-Paid)", right)
    front.write("A7", "Prior Month Debit Adjustment", right)
    front.write("C7", "(These plans are added/subtracted from Last Month Total Active)", leftItalic)
    front.write("A8", "Net New Commissionable", rightBold)
    front.write("C8", "(If negative, this will be carried forward as a debit in the next month)", leftItalic)
    front.write("A9", "Core - Commission", rightBold)

    front.write("A11", "Details", leftBold)
    front.write("A12", "Total Core Plan Submissions Last Month", right)
    front.write("A13", "Core Plan Cancelations Posted Last Month", right)

    front.write("A15", "Residuals", leftBold)
    front.write("A16", "Residual-Eligible Core Plans", right)
    front.write("A17", "Core - Residual", rightBold)

    front.write("A19", "Ancillary Plans", leftBold)
    front.write("A20", "Previous Active DVH Plans:", right)
    front.write("A21", "Current Active DVH Plans:", right)
    front.write("A22", "DVH Lifetime Cancels (12+ Month Duration)", right)
    front.write("A23", "Net New DVH Plans:", right)
    front.write("A24", "DVH - Commission:", rightBold)

    front.write("A26", "Previous Active Copay Plans:", right)
    front.write("A27", "Current Active Copay Plans:", right)
    front.write("A28", "Copay Lifetime Cancels (12+ Month Duration)", right)
    front.write("A29", "Net New Copay Plans:", right)
    front.write("A30", "Copay - Commission:", rightBold)


    front.write("B2", bob_old, right)
    front.write("B3", bob_new, right)
    front.write("B4", lt_cancels, right)
    front.write("B5", '=B3+B4-B2', rightBold)
    front.write("B6", base_draw, right)
    front.write("B7", debit_adjustment, right)
    front.write("B8", '=SUM(B5:B7)', rightBold)
    front.write("B9", '=MAX(0, B8*{})'.format(commission_amount), currency_format_bold)

    front.write("B12", len(recent_month_adds), right)
    front.write("B13", len(recent_month_cancel), right)

    front.write("B16", str(residual_counter), right)
    front.write("B17", '=SUM(Core!T:T)', currency_format_bold)

    front.write("B20", dvh_old)
    front.write("B21", dvh_new)
    front.write("B22", lt_dvh_cancels, right)
    front.write("B23", '=-B20+B21+B22')
    front.write("B24", '=B23*{}'.format(dvh_amount), currency_format_bold)

    front.write("B26", copay_old)
    front.write("B27", copay_new)
    front.write("B28", lt_copay_cancels, right)
    front.write("B29", '=-B26+B27+B28')
    front.write("B30", '=B29*{}'.format(copay_amount), currency_format_bold)



    front.set_column(0, 0, 36)
    front.set_column(1,1,9)
    workbook.close()
    return residual_counter

def write_worksheet(workbook, sheetname, headers, data, cformat, curformat):
    az_upper = string.ascii_uppercase
    worksheet = workbook.add_worksheet(sheetname)
    bold = workbook.add_format({'bold': True})
    dd = [{headers[i]:x for i,x in enumerate(row)} for row in data]

    # formatting
    worksheet.set_column(0,1,13, cformat)
    worksheet.set_column(2,8,9, cformat)
    worksheet.set_column(9,9,8, cformat)
    worksheet.set_column(10,15,11)
    worksheet.set_column(16,18,10, cformat)
    worksheet.set_column('S:S', 10, curformat)
    worksheet.set_column('T:T', 14, cformat)

    for i,n in enumerate(headers):
        worksheet.write(az_upper[i] + '1', n, bold)
    for i,row in enumerate(data):
        if len(row[0]) > 0:
            policy_id = row[0].split('/')[-1]
            worksheet.write_url(i+1, 0, row[0], string=policy_id)
            contact_id = row[1].split('/')[-1]
            worksheet.write_url(i+1, 1, row[1], string=contact_id)
        for j,v in enumerate(row[2:]):
            worksheet.write(i+1, j+2, v)
    if sheetname.lower() == "core":
        worksheet.set_column('U:W', None, None, {'hidden': True})

def write_summary_workbook(folder):
    file_list = os.listdir(folder)
    headers = [
        "Current Month Active",
        "Net New Commissionable",
        "Total Core Commission",
        "Base Draw (Pre-Paid)",
        "Commission Check",
        "Residual-Eligible Core Plans",
        "Total Core Plans",
        "Residual $ Total",
        "New DVH Plans:",
        "DVH Commission:",
        "New Active Copay Plans:",
        "Copay Commission:",
    ]
    workbook = xlsxwriter.Workbook('Summary.xlsx')
    ws = workbook.add_worksheet("Sheet 1")
    for i,h in enumerate(headers):
        ws.write(0,i+1,h)

    row=1
    col=1
    for file in file_list:
        df = pd.read_excel(folder+"/"+file, data_only=True)
        df.drop(df.columns[[2]], axis=1, inplace=True)
        name_raw = file.split(".")[0]
        a,b = name_raw.split("_")
        ws.write(row, 0, a + " " + b)
        col = 1
        for h in headers:
            s = df.loc[(df["Totals"] == h)]
            print(s)
            try:
                v = s.values[0][-1]
                ws.write(row, col, v)
            except Exception as ee:
                print(ee)
            col += 1
        row += 1
    workbook.close()



# utilities and functions to evaluate whether > 1 year cancel

def flatten(arr):
    return foldl(lambda a,b: a+b, [], list(arr.values()))

def get_status(props):
    status = props.get('status')
    if not status:
        status = props.get('plan_status')
    return status

def cancel_filt(status):
    if status:
        return status[0] == '5' or status[0] == '4'
    return False

def filter_cancels(arr):
    return list(filter(lambda x: cancel_filt(get_status(x)), arr))

def greater_oneyear_residual_cancel(dic):
    d0_raw = dic['date_effective']
    d1_raw = dic['date_cancel_actual']
    if d0_raw and d1_raw:
        date_0 = datetime.strptime(d0_raw, "%Y-%m-%d")
        date_1 = datetime.strptime(d1_raw, "%Y-%m-%d")
        return (date_1 - date_0).days > 364
    return False


def map_update(arr, fieldname, ret=False):
    for d in arr:
        val = greater_oneyear_residual_cancel(d)
        d[fieldname] = val
    if ret:
        return arr

def is_active_group(arr):
    for d in arr:
        status = d['status']
        if status:
            if status[0] == '1' or status[0] == '2':
                return True
    return False


def cancel_process_group(arr):
    if len(arr) == 1:
        return greater_oneyear_residual_cancel(arr[0])
    else:
        out = [arr[0]]
        for d in arr[1:]:
            d0_raw = out[-1].get('date_cancel_actual')
            d1_raw = d.get('date_effective')
            if d0_raw and d1_raw:
                date_0 = datetime.strptime(d0_raw, "%Y-%m-%d")
                date_1 = datetime.strptime(d1_raw, "%Y-%m-%d")
                if (date_1-date_0).days > 180:
                    out = [d]
                else:
                    out.append(d)
            else:
                out.append(d)
        d0_raw = out[0].get('date_effective')
        d1_raw = out[-1].get('date_cancel_actual')
        if d0_raw and d1_raw:
            date_0 = datetime.strptime(d0_raw, "%Y-%m-%d")
            date_1 = datetime.strptime(d1_raw, "%Y-%m-%d")
            return (date_1 - date_0).days > 364
        else:
            return False

def cancel_counter(dic):
    adic, _ = sort_by_agents(dic, False)
    out = {}
    for k,v in adic.items():
        i = 0
        s = 0
        cancels = filter(lambda x: not is_active_group(x), v)
        for group in cancels:
            if cancel_process_group(group):
                i += 1
            else:
                s += 1
        out[k] = i, s
    return out

def recent_cancel_counter(dic, report_year, report_month):
    adic, _ = sort_by_agents(dic, False)
    out = {}
    for k,v in adic.items():
        i = 0
        cancels = filter(lambda x: not is_active_group(x), v)
        for group in cancels:
            if recent_cancel_test(group, report_year, report_month):
                i += 1
        out[k] = i

def recent_counter(dic, report_year, report_month):
    adic, _ = sort_by_agents(dic, False)
    out = {}
    for k,v in adic.items():
        i = 0
        cancels = filter(lambda x: not is_active_group(x), v)
        for group in cancels:
            if recent_cancel_test(group, report_year, report_month):
                i += 1
        out[k] = i
    return out

def recent_cancel_test(group, report_year, report_month):
    y = int(report_year)
    m = int(report_month)
    for d in group:
        draw = d.get('date_cancel_notification')
        if not draw:
            draw = d.get('date_cancelled')
        if draw:
            dt = datetime.strptime(draw, "%Y-%m-%d")
            if dt.month == m and dt.year == y:
                return True
    return False


def month_filt(group, field, record_year, record_month):
    is_month = False
    for d in group:
        dtraw = d.get(field)
        if dtraw:
            try:
                dt = datetime.strptime(dtraw, "%Y-%m-%d")
                if (dt.year, dt.month) == (int(record_year), int(record_month)):
                    is_month = True
            except:
                pass
    return is_month

def apply_month_filter(arr, field, report_year, report_month):
    arr = filter(lambda x: month_filt(x, field, report_year, report_month), arr)
    return sorted(arr, key=datesort)

def datesort(arr):
    d = arr[-1]
    dtraw = d.get('date_actual_submission')
    try:
        return datetime.strptime(dtraw, "%Y-%m-%d")
    except:
        return datetime(2300,1,1)


def process_lastmonth_row(dic):
    cid = dic['originally_associated_contact_id']
    pid = dic['id']
    plan_type = dic['plan_type']
    oid = oid_dic[plan_type.upper()]
    policy_headers = {
        'id': 'Policy Record',
        'originally_associated_contact_id': 'Contact Recrod',
        'firstname': "First Name",
        'lastname': "Last Name",
        'carrier': "Carrier",
        'commission_id': "Commission ID",
        'status': 'Status',
        'plan_type': "Plan Type",
        'date_actual_submission': "Date Actual Submission",
        'date_effective': 'Effective Date',
        'date_cancel_actual': 'Date Actual Cancel',
        'date_cancel_notification': 'Date Notified Cancel',
        'notes_misc_cancel': 'Cancellation Notes',
    }
    process_policy = {
        'id': lambda x: 'https://app.hubspot.com/contacts/7879306/record/{}/{}'.format(oid, str(pid)),
        'originally_associated_contact_id': lambda x: 'https://app.hubspot.com/contacts/7879306/contact/{}'.format(cid),
        'date_actual_submission': date_format,
        'date_cancel_actual': date_format,
        'date_cancel_notification': date_format,
        'date_effective': date_format,
        'status': status_format,
        'plan_type': plan_format,
    }
    out = {}
    for k,knew in policy_headers.items():
        func = process_policy.get(k, lambda x: x)
        v = dic.get(k)
        out[knew] = func(v)
    return out

def plan_format(plan_type):
    if plan_type.lower() == 'msp':
        return 'Supplemental'
    elif plan_type.lower() == 'map':
        return 'Advantage'
    else:
        return plan_type


def status_format(code):
    try:
        d = code[0]
        if d == '1':
            word = 'Submitted'
        elif d == '2':
            word = 'Issued'
        elif d == '3':
            word = 'Pending'
        elif d == '4' or d == '5':
            word = 'Cancelled'
        elif d == '0':
            word = 'Proposed'
        return '{} - {}'.format(code, word)
    except:
        return code

def date_format(dtraw):
    try:
        dt = datetime.strptime(dtraw, "%Y-%m-%d")
        oform="%m-%d-%Y"
        return dt.strftime(oform)
    except:
        return dtraw

def write_lastmonth_worksheet(workbook, nm, groups, preamble = ""):
    pgroups = [list(map(process_lastmonth_row, x)) for x in groups]
    hformat = workbook.add_format({'align': 'center', 'bold': 1})
    bformat = workbook.add_format({'align': 'center'})
    if pgroups:
        headers = list(pgroups[0][0].keys())
        ws = workbook.add_worksheet(nm)
        iplus = 0
        if preamble:
            merge_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})
            ws.set_row(0,100)
            for j in range(5):
                ws.write(0,j, '', merge_format)
            ws.merge_range('A1:E1', preamble, merge_format)
            iplus = 1
        for j,h in enumerate(headers):
            ws.write(iplus, j, h, hformat)
        i=1+iplus
        for group in pgroups:
            for d in group:
                for j, value in enumerate(d.values()):
                    if j >= 2:
                        ws.write(i, j, value, bformat)
                    else:
                        ws.write(i, j, value)
                i += 1
            i += 1 # blank line between groups
        ws.set_column(0,6,13)
        ws.set_column(7,12,25)

cancel_preamble = """
Note: These are not all chargebacks. Some likely are, some likely aren't. We DO NOT add up your sales and then subtract the cancelations listed below.  Instead, we calculate your commission report based upon "Net New" Active clients.  This sheet is a reference only so you can see a list of all policies that were updated to a "canceled" status during the last month.
"""

def safe_int(x):
    try:
        return int(x)
    except:
        return 0

def safe_add(*args):
    safe_ints = map(safe_int, args)
    return sum(safe_ints)


def write_summary_book(infodic, alist, name="excel_reports/_Summary_Info.xlsx"):
    headers = list(list(infodic.values())[0].keys())
    workbook = xlsxwriter.Workbook(name)
    ws = workbook.add_worksheet()
    boldCenter = workbook.add_format({'align': 'center', 'bold': True, 'text_wrap': True, 'font_size': 10})
    right = workbook.add_format({'align': 'right'})
    center = workbook.add_format({'align': 'center'})
    left = workbook.add_format({'align': 'left'})
    currency_format = workbook.add_format({'num_format': '$#,##0.00', 'align': 'right'})
    # write headers
    ws.write(0,0,"Name", boldCenter)

    order_list = sorted(list(infodic.keys()), key=lambda x: alist[x].get('firstName', 'zzz'))

    for j,h in enumerate(headers):
        ws.write(0,j+1, h, boldCenter)
    for i,agent in enumerate(order_list):
        firstname = alist[agent].get('firstName')
        lastname = alist[agent].get('lastName')
        dname = '{} {}'.format(firstname, lastname)
        ii = i + 1
        ws.write(ii, 0, dname, left)
        irow = i + 2
        data = infodic[agent]
        data['Net New Active'] = '=-1*B{}+C{}+D{}'.format(irow, irow, irow)
        data['Net New Commissionable'] = '=sum(E{}:G{})'.format(irow, irow)
        data['Core - Commission'] = '=max(0, H{} * {})'.format(irow, commission_amount)
        data['Core - Residual'] = '=J{} * {}'.format(irow, residual_amount)
        data['Net New DVH'] = '=-1*L{}+M{}+ N{}'.format(irow, irow, irow)
        data['DVH - Commission'] = '=O{} * {}'.format(irow, dvh_amount)
        data['Net New Copay'] = '=-1*Q{}+R{}+S{}'.format(irow, irow, irow)
        data['Copay - Commission'] = '=T{} * {}'.format(irow, copay_amount)
        for j,key in enumerate(data.keys()):
            jj = j + 1
            val = data[key]
            if jj in [8, 10, 15, 20]:
                ws.write(ii, jj, val, currency_format)
            else:
                ws.write(ii, jj, val, center)
    ws.set_row(0, 45)
    ws.set_column(0,0, 20)
    ws.set_column(1,7,7)
    ws.set_column(8,8,10)
    ws.set_column(9,9,7)
    ws.set_column(10,10,10)
    ws.set_column(1,14,7)
    ws.set_column(15,15,10)
    ws.set_column(16,19,7)
    ws.set_column(20,20,10)
    workbook.close()
