from app import *
import pandas as pd
from pprint import pprint
from deta import Deta
import asyncio

deta = Deta("b0hdifmg_JFEgLc7cqC3cJbsYBVbfYY5NNMA645VN")

db_contacts = deta.Base('contacts')


async def a_save_contact(rec):
    return await sync_to_async(db_contacts.insert)(rec)

async def a_save_contacts(ls):
    at = []
    for x in ls:
        task = asyncio.create_task(a_save_contact(x))
        at.append(task)
    return await asyncio.gather(*at)


def save_contacts(ls):
    return asyncio.run(a_save_contacts(ls))

report_year = '2022'
report_month = '10'

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

last_month_cancel_dic = {agent: apply_month_filter(arr, "date_cancel_actual", report_year, report_month) for agent, arr in adcore.items()}


agent_longterm_cancels = cancel_counter(psup) | cancel_counter(pmap)

agent_dvh_cancels = cancel_counter(pdvh)
agent_copay_cancels = cancel_counter(pcopay)

fdic = get_agent_books(adcore, report_year, report_month)

df = pd.read_csv("/Users/reuben/Downloads/production-report-new.csv")

alist = show_agents()
agent_dic = {k: v['firstName'] + ' ' + v['lastName'] for k,v in agents.items() if k in alist}

flat_dic = {}
for k,arr in fdic.items():
    out = []
    for a in arr:
        for d in a:
            out.append(d)
    flat_dic[k] = out
