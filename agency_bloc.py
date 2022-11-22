import aiohttp
import asyncio
import json
from pprint import pprint
from helpers import ab_path, ab_header, ab_body, asyncio_wrap

# in script func, import:
# async_check_policies, async_create_policies, async_Check_create_many

# check ab for policies -- call in script

# inner function for ab policies
async def async_check_policy(session, pid, policy_id, k, **kargs):
    path = "policies/search"
    body = {"policyNumber": policy_id}
    await asyncio.sleep(k)
    out = {pid: None}
    async with session.post(ab_path(path), headers=ab_header(), data = ab_body(body)) as r:
        if r.status != 200:
            print("error for ", policy_id)
            pprint(r.status)
            return out
        else:
            try:
                res = await r.text()
                j = json.loads(res)
                if len(j) > 0:
                    try:
                        out[pid] = j[0]
                    except Exception as e:
                        pprint(e)
                        pprint(j)
                return out
            except Exception as ee:
                pprint(ee)
                return out

async def async_check_policies(session, arr, k, **kargs):
    at = []
    for x in arr:
        pid, policy_id = x
        task = asyncio.create_task(async_check_policy(session, pid, policy_id, k, **kargs))
        at.append(task)
    return await asyncio.gather(*at)

# developer convenience functions for repl:
def check_policies(arr, **kargs):
    k = 0
    if 'k' in kargs:
        k = kargs.pop('k')
    return asyncio_wrap(async_check_policies, arr, k, **kargs)

async def async_check_policy2(session, policy_id):
    path = "policies/search"
    body = {"policyNumber": policy_id}
    async with session.post(ab_path(path), headers=ab_header(), data = ab_body(body)) as r:
        if r.status != 200:
            return False, "Error for: "+ policy_id, {}
        try:
            res = await r.text()
            j = json.loads(res)
            if len(j) > 0:
                return True, policy_id + " exists!", j
            else:
                return False, policy_id + " does not exist.", j
        except:
            return False, policy_id + " triggered an error", {}

def get_commission_field_label(olabel):
    o = olabel.lower()
    if o == 'msp':
        return 'commission_id'
    elif o == 'dvh':
        return 'policy_number'
    elif o == 'map' or 'pdp' or 'copay':
        return 'commission_id__if_applicable_'
    else:
        return None

def get_commission_field(props, olabel):
    label = get_commission_field_label(olabel)
    ss = props.get(label)
    if type(ss) == str:
        return ss.strip()
    else:
        return None

def policy_args_to_dic(d, coveragetype, oid):
    cont = d.get('contact')
    if coveragetype.upper() == "MAP":
        status = d['properties']['plan__status']
    else:
        status = d['properties']['status']
    body = {
        "entityID": cont.get('ab_individual_data',{}).get('individualID'),
        "entityTypeID": "Individual",
        "policyCoverageType": coveragetype.upper(),
        "carrier": d['properties']['carrier'],
        "policyNumber": get_commission_field(d['properties'], coveragetype),
        "policyStatus": status,
        "servicingAgentFirstName": d['agent']['firstName'],
        "servicingAgentLastName": d['agent']['lastName'],
        "custom_hubspot_link": "https://app.hubspot.com/contacts/7879306/record/"+oid+"/"+d['id'],
        "custom_hubspot_id": d['id']
    }
    return body

# create policies in agencybloc
async def async_create_policies(session, arr, coverage_type, oid, **kargs):
    at = []
    k = kargs.get('k', 0)
    for i,dic in enumerate(arr):
        sleep_ = i * k
        try:
            body = policy_args_to_dic(dic, coverage_type, oid)
            task = asyncio.create_task(async_create_policy(session, body, sleep=sleep_))
            at.append(task)
        except Exception as ee:
            print("Error:", str(ee))
    return await asyncio.gather(*at)


# inner function to create policies in ab
async def async_create_policy(session, body, **kargs):
    path = "policies/create"
    sleep_ = kargs.get('sleep')
    if sleep_ and sleep_ > 0:
        await asyncio.sleep(sleep_)
    print("starting")
    async with session.post(ab_path(path), headers=ab_header(), data=ab_body(body)) as r:
        out = await r.text()
        print("finishing")
        try:
            out = json.loads(out)
            if out:
                return True, out
            else:
                print("False; what?", out)
                return False, out
        except Exception as ee:
            print("Exception:", str(ee))
            print(out)
            return False, {"error": ee}

def create_policy(dic, coverage_type, oid, **kargs):
    body = policy_args_to_dic(dic, coverage_type, oid)
    return asyncio_wrap(async_create_policy, body, **kargs)

# developer convenience functions for repl
def create_policies(*args, **kargs):
    return asyncio_wrap(async_create_policies, *args, **kargs)


# for a list of individuals: -- for script
# de-duplicate
# check if the exist
# create them if not
def check_create_individuals(arr, **kargs):
    return asyncio_wrap(async_check_create_individuals, arr, **kargs)

async def async_check_create_individuals(session, arr, **kargs):
    data_dict = {x['id']: x for x in arr}
    arg_dic = {}
    for dic in arr:
        d = dic['properties']
        firstname = d.get('firstname', None)
        lastname = d.get('lastname', None)
        email = d.get('email', None)
        dob = d.get('date_of_birth', None)
        ssn = d.get('ss_number', None)
        k = (firstname, lastname, dob, ssn)
        arg_arr = arg_dic.get(k, [])
        arg_arr.append(dic['id'])
        arg_dic[k] = arg_arr
    arg_set = arg_dic.keys()
    at = []
    i = 0
    k = kargs.get('k', 0)
    for firstname, lastname, dob, ssn in arg_set:
        sleep_ = k * i
        task = asyncio.create_task(async_check_create_individual(session, firstname, lastname, dob, ssn, sleep=sleep_))
        at.append(task)
        i += 1
    ls  = await asyncio.gather(*at)
    datas = [x[1] for x in ls]
    for k, data in zip(arg_dic, datas):
        id_array = arg_dic[k]
        for hs_id in id_array:
            data_dict[hs_id]['ab_individual_data'] = data
    return data_dict

def check_create_individual(*args, **kargs):
    return asyncio_wrap(async_check_create_individual, *args, **kargs)

# inner function for creating individuals
async def async_check_create_individual(session, firstname, lastname, dob, ssn, **kargs):
    sleep = kargs.get('sleep')
    if sleep:
        await asyncio.sleep(sleep)
    k = (firstname, lastname, dob, ssn)
    ok, data_if_exists = await ab_customer_data(session, firstname, lastname, dob)
    try:
        if ok and data_if_exists:
            print(firstname, lastname, "already exists!")
            return (True, data_if_exists)
        else:
            print("Does not exist; creating", firstname, lastname)
            await ab_create_individual(session, firstname, lastname, dob, ssn)
            ok, r = await ab_customer_data(session, firstname, lastname, dob)
            return (True, r)
    except:
        return False, {}

async def async_check_check_create_policy(session, firstname, lastname, dob, ssn, coveragetype, carrier, policynumber, policystatus, agentfirstname, agentlastname):
    indiv_success, indiv_data = await async_check_create_individual(session, firstname, lastname, dob, ssn)
    if not indiv_success:
        return False, "Error getting/creating individual: "+policynumber, {}
    eid = indiv_data.get("individualID")
    policy_exists, msg, data = await async_check_policy2(session, policynumber)
    if policy_exists:
        return True, msg, data
    policy_success, policy_data = await async_create_policy(session, eid, coveragetype, carrier, policynumber, policystatus, agentfirstname, agentlastname)
    if policy_success:
        return True, "policy created successfully: "+policynumber, policy_data
    else:
        return False, "error creating policy: "+policynumber, policy_data

async def async_cccp_many(session, arg_arr):
    at = []
    for tup in arg_arr:
        task = asyncio.create_task(async_check_check_create_policy(session, *tup))
        at.append(task)
    return await asyncio.gather(*at)

def cc_create_policies(arg_arr, **kargs):
    return asyncio_wrap(async_cccp_many, arg_arr, **kargs)

def search_individuals(arr, **kargs):
    return asyncio_wrap(async_search_individuals, arr, **kargs)

async def async_search_individuals(session, arr):
    at = []
    for x in arr:
        task = asyncio.create_task(ab_customer_data(session, x.get('firstName'), x.get('lastName'), x.get('ssn')))
        at.append(task)
    return await asyncio.gather(*at)

# inner function for checking for customer data
async def ab_customer_data(session, firstname, lastname, dob):
    out = {}
    body = {
        'firstName': firstname,
        'lastName': lastname,
        'birthDate': dob
    }
    async with session.post(ab_path("individuals/search"), headers=ab_header(), data=ab_body(body)) as r:
        try:
            r = await r.text()
            j = json.loads(r)
            if len(j) > 0:
                out = j[0]
            return True, out
        except:
            return False, out

# inner function to create new individual
async def ab_create_individual(session, firstname, lastname, dob, ssn):
    body = {
        'firstName': firstname,
        'lastName': lastname,
        'birthDate': dob,
        'ssn': ssn
    }
    path = "individuals/create"
    async with session.post(ab_path(path), headers=ab_header(), data=ab_body(body)) as r:
        if r.status == 200:
            print("success for", firstname, lastname)
        out = await r.text()
        try:
            j = json.loads(out)
            return j
        except:
            pprint(out)
            return out


# Get AB agents
async def async_get_agents(session):
    async with session.post(ab_path("agents/search"), headers=ab_header(), data = ab_body({})) as r:
        if r.status == 200:
            print("successfully pulled ab agents")
        out = await r.text()
        try:
            return json.loads(out)
        except:
            print("json error")
            return out

# convenience function
def get_agents(**kargs):
    return asyncio_wrap(async_get_agents, **kargs)

# update_agents
async def async_update_agent(session, dic):
    async with session.post(ab_path("agents/update"), headers=ab_header(), data=ab_body(dic)) as r:
        out = await r.text()
        try:
            return json.loads(out)
        except:
            return out

async def async_update_agents(session, arr):
    at = []
    for dic in arr:
        assert 'agentID' in dic
        task = asyncio.create_task(async_update_agent(session, dic))
        at.append(task)
    return await asyncio.gather(*at)

def update_agents(arr, **kargs):
    return asyncio_wrap(async_update_agents, arr, **kargs)

async def async_create_agent(session, firstname, lastname, email):
    path = ab_path("agents/create")
    data = {'firstName': firstname, 'lastName': lastname, 'email': email}
    async with session.post(path, headers=ab_header(), data = ab_body(data)) as r:
        out = await r.text()
        try:
            return json.loads(out)

        except:
            return out

async def async_create_agents(session, arr):
    at = []
    for t in arr:
        task = asyncio.create_task(async_create_agent(session, *t))
        at.append(task)
    return await asyncio.gather(*at)

def create_agents(arr, **kargs):
    return asyncio_wrap(async_create_agents, arr, **kargs)

def policy_details(*pid_array, **kargs):
    k = .2
    if 'k' in kargs:
        k = kargs.pop('k')
    if 'limit_per_host' in kargs:
        limit_per_host = kargs.pop('limit_per_host')
    return asyncio_wrap(async_policy_details, pid_array, k=k, **kargs)

async def async_policy_details(session, pid_array, k, **kargs):
    at = []
    for pid in pid_array:
        data = ab_body({"policyID": pid})
        task = asyncio.create_task(async_pd_inner(session, data))
        at.append(task)
    return await asyncio.gather(*at)

async def async_pd_inner(session, data):
    async with session.post(ab_path("policies/detail"), data=data, headers=ab_header()) as r:
        out = await r.text()
        try:
            return json.loads(out)
        except:
            return out

async def async_update_policy(session, body, **kargs):
    path = "policies/update"
    msg_key = "policyID"
    return await batch_inner(session, path, body, msg_key=msg_key)

async def batch_inner(session, path, body, **kargs):
    sleep_ = kargs.get('sleep')
    if sleep_ and sleep_ > 0:
        await asyncio.sleep(sleep_)
    msg_key = kargs.get('msg_key')
    if msg_key:
        print("updating", body.get(msg_key))
    async with session.post(ab_path(path), headers=ab_header(), data =ab_body(body)) as r:
        out = await r.text()
        if msg_key:
            print('finishing', body.get(msg_key))
        try:
            out = json.loads(out)
            if out:
                return True, out
            else:
                return False, out
        except Exception as ee:
            return False, {"error": ee}

def update_policy_args(d):
    cont = d.get('contact')
    props = d['properties']
    status = props.get('status')
    if not status:
        status = props.get('plan__status')
    body = {
        'policyID': str(d.get('ab_policy_id')).strip(),
        'policyStatus': status,
        'effectiveDate': props.get('date_effective'),
        'custom_State': props.get('state_of_issue'),
        'custom_hubspot_id': d.get('id'),
        'custom_hubspot_link': d.get('link'),
    }
    return body

async def async_update_policies(session, arr, **kargs):
    return await batch_outer(session, arr, update_policy_args, async_update_policy, **kargs)

async def batch_outer(session, arr, process_func, inner_func, **kargs):
    at = []
    k = kargs.get('k', 0)
    for i,dic in enumerate(arr):
        sleep_ = i*k
        body = process_func(dic)
        task = asyncio.create_task(inner_func(session, body, sleep=sleep_))
        at.append(task)
    return await asyncio.gather(*at)

def update_policies(*args, **kargs):
    return asyncio_wrap(async_update_policies, *args, **kargs)

async def async_update_contact(session, body, **kargs):
    path = "individuals/update"
    msg_key = "individualID"
    return await batch_inner(session, path, body, msg_key=msg_key)


# pre-process the array to have the body dic
async def async_update_contacts(session, arr, **kargs):
    return await batch_outer(session, arr, lambda x: x, async_update_contact, **kargs)

def update_contacts(*args, **kargs):
    return asyncio_wrap(async_update_contacts, *args, **kargs)
