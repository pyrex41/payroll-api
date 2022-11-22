from helpers import foldl, safemap, hs_get, ab_post, hs_path
import agency_bloc as ab
import hubspot as hs
import hubspot_migrate as hsm
import json
import time


def agent_checker(pdic, hb_dict, ab_dict):
    hb_agent_id = pdic['properties'].get('agent_originating')
    if not hb_agent_id:
        print("No agent specified in Hubspot")
    email = hb_dict.get(hb_agent_id, {}).get('email')
    return hb_agent_id, ab_dict.get(email)


msp_id = "2-8483761"
dvh_id = "2-8483915"
map_id = "2-7775359"
copay_id = "2-8483892"
pdp_id = "2-8567541"

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

commission_field = 'commission_id__if_applicable_'

def get_status(props):
    out = props.get('status')
    if not out:
        out = props.get('plan__status')
    return out


def load_hubspot(object_label, k=.2):
    object_label = object_label.upper()
    object_id = oid_dic.get(object_label)
    assert object_id
    props_raw = hs_get("properties/"+object_id).json()["results"]
    props = sorted([x["name"] for x in props_raw])
    path = hs_path("objects/"+object_id+"/search")
    data = {
        'limit': 100,
        'sorts': [{'propertyName': 'hs_object_id', 'direction': 'ASCENDING'}],
        'properties': props
    }
    existing_map, _ = hsm.batch_search(path, data, k = k)
    for x in existing_map:
        x['link'] = 'https://app.hubspot.com/contacts/7879306/record/'+oid_dic[object_label]+'/'+x['id']
    return existing_map

def load_agency_bloc(object_label):
    path = "policies/search"
    object_label = object_label.upper()
    assert object_label in oid_dic
    r = ab_post(path, policyCoverageType=object_label)
    return json.loads(r.text)
    """
    try:
        aa = json.loads(r.text)
        batches = [aa[i:i+2000] for i in range(0,len(aa),2000)]
        deets = []
        for i,batch in enumerate(batches):
            ai = [x['policyID'] for x in batch]
            deet1 = ab.policy_details(*ai)
            deets = deets + deet1
            print("loaded batch", i+1, "of",len(batches))
            print("waiting for", timeout,"seconds because of antiquated rate limit")
            time.sleep(timeout)
        return deets
    except Exception as ee:
        print("default")
        print(ee)
        return r.text
    """


def upstrip(maybe_string):
    if type(maybe_string) == str:
        return maybe_string.strip().upper()
    else:
        return maybe_string


def parse_hubspot_fields(h_array, olabel):
    hvp = {}
    hdic = {}
    dups = {}
    for x in h_array:
        props = x['properties']
        k = upstrip(get_commission_field(props, olabel))
        k2 = upstrip(props.get('carrier'))
        if k and k2:
            kk = (k.upper().strip(), k2.upper().strip())
            if kk in hvp:
                hvp.pop(kk)
                x0 = hdic.pop(kk)
                dups[kk] = [x0,x]
            else:
                hvp[kk] = get_status(props)
                hdic[kk] = x
    valid_dup_keys = [k for k in dups.keys() if any(c.isdigit() for c in k[0])]
    dup_errors = {}
    for k in valid_dup_keys:
        ss = []
        v = dups[k]
        for i in range(0, len(v)):
            p_ = v[i]['properties']
            ss.append(get_status(p_))
        if not foldl(lambda a,b: bool(a) and bool(b), True, ss):
            dup_errors[k] = x
        else:
            x = dups.get(k)
            props = x[0]['properties']
            hvp[k] = get_status(props)
            hdic[k] = x[0]
    return hvp, hdic, dup_errors

def parse_agencybloc_fields(a_array):
    avp = {}
    adic = {}
    dups = {}
    for x in a_array:
        k = x.get('policyNumber')
        k2 = x.get('carrier')
        if k:
            kk = (k.upper().strip(), k2.upper().strip())
            if kk in avp:
                avp.pop(kk)
                x1 = adic.pop(kk)
                dups[kk] = [x,x1]
            elif kk in dups:
                dups[kk].append(x)
            else:
                avp[kk] = x.get('policyStatus')
                adic[kk] = x
    to_archive = []
    for k, arr in dups.items():
        added = False
        for x in arr:
            status = x.get('policyStatus')
            if status == '200' and not added:
                adic[k] = x
                avp[k] = '200'
            else:
                to_archive.append(x)
    to_archive = list(filter(lambda x: not bool(x.get('policyStatus') == 'Archive'), to_archive))
    return avp, adic, to_archive



def get_contacts(hdic):
    cids = []
    for x in hdic.values():
        if type(x) == list:
            for d in x:
                cid = d.get('properties', {}).get('originally_associated_contact_id')
                if cid:
                    cids.append(cid)
        else:
            cid = x.get('properties', {}).get('originally_associated_contact_id')
            if cid:
                cids.append(cid)
    fdata_raw,_ = hs.batch_contacts(cids)
    fdata = {x['id']:x for x in fdata_raw}
    for x in hdic.values():
        if x:
            if type(x) == dict:
                ii = x.get('properties', {}).get('originally_associated_contact_id')
                x['contact'] = fdata.get(ii)
            else:
                for d in x:
                    ii = d.get('properties', {}).get('originally_associated_contact_id')
                    d['contact'] = fdata.get(ii)
    return fdata



def contact_filter(dic, hdic):
    return dict(filter(lambda tup: hdic[tup[0]].get('contact'), dic.items()))


def create_batch_inner(aa, olabel, **kargs):
    oid = oid_dic[olabel.upper()]
    rr = ab.create_policies(aa, olabel.upper(), oid, limit_per_host=10, **kargs)
    success, err = [], []
    for r, s in zip(rr, aa):
        if r[0]:
            success.append((r[1], s))
        else:
            err.append((r[1], s))
    return success, err

def create_batch(aa, olabel, **kargs):
    s, e = create_batch_inner(aa, olabel, **kargs)
    for i in range(0,10):
        if e:
            s1, e = create_batch_inner(e, olabel, **kargs)
            s = s + s1
    return s, e

def update_batch_inner(uu):
    rr = ab.update_policies(uu, limit_per_host=10)
    success, err = [],[]
    for r,s in zip(rr, uu):
        if r[0]:
            success.append((r[1], s))
        else:
            err.append(s)
    return success, err

def update_batch(uu):
    s,e = update_batch_inner(uu)
    for i in range(0,10):
        if e:
            s1,e = update_batch_inner(e)
            s = s + s1
    return s,e

def parse_duplicates_to_remove(adups):
    out = []
    for k,v in adups.items():
        if len(v) > 1:
            out = out + v[1:]
    return out

def archive_batch(archiveids):
    aa = []
    for pid in archiveids:
        dic = {
            'ab_policy_id': pid,
            'properties': {'status': 'ARCHIVE'}
        }
        aa.append(dic)
    s,e = update_batch_inner(aa)
    for i in range(0,10):
        if e:
            s1, e = update_batch_inner(e)
            s = s + s1
    return s,e

def dedup_batch(dups_to_remove):
    aa = []
    for a in dups_to_remove:
        pid = a.get('policyID')
        if not pid:
            pid = a.get('PolicyID')
        if pid:
            dic = {
                'ab_policy_id': pid,
                'properties': {'status': 'ARCHIVE'}
            }
            aa.append(dic)
    s,e = update_batch_inner(aa)
    for i in range(0,10):
        if e:
            s1,e = update_batch_inner(e)
            s = s + s1
    return s,e

def show_active_hb_only(hvp, avp):
    out = {}
    ss = set(hvp) - set(avp)
    for k in ss:
        v = hvp[k]
        if v == '200':
            out[k] = v
    return out

def show_matches(hvp, avp):
    ss = set(avp).union(set(hvp))
    out = {}
    for k in ss:
        i = strip(avp.get(k))
        j = strip(avp.get(k))
        if i and j and i==j:
            out[k] = i
    return out

def show_ab_only(hvp, avp):
    ss = set(avp) - set(hvp)
    return {k:v for k,v in avp.items() if k in ss and v.lower() != 'archive'}

def strip(x):
    if type(x) == str:
        return x.strip()
    else:
        return x
def show_mismatching_status(hvp, avp):
    ss = set(hvp).intersection(set(avp))
    out = {}

    for k in ss:
        i = strip(hvp[k])
        j = strip(avp[k])
        if j.lower() == 'none':
            j = None
        if i != j:
            out[k] = (i,j)
    return out

def process_matches(hvp, avp):
    create = show_active_hb_only(hvp, avp)
    change_status = show_mismatching_status(hvp, avp)
    archive = show_ab_only(hvp, avp)
    archive = archive
    return create, change_status, archive

def load_and_process_hubspot(olabel):
    hh = load_hubspot(olabel)
    hvp, hdic, hdups = parse_hubspot_fields(hh, olabel)
    return hvp, hdic, hdups, hh

def load_and_proces_ab(olabel):
    aa = load_agency_bloc(olabel)
    avp, adic, to_archive = parse_agencybloc_fields(aa)
    return avp, adic, to_archive, aa

def combine_load_process(olabel):
    hvp, hdic, _, hh = load_and_process_hubspot(olabel)
    get_contacts(hdic)
    avp, adic, to_archive, aa = load_and_proces_ab(olabel)
    for x in avp:
        ax = adic.get(x)
        hx = hdic.get(x)
        if hx:
            hdic[x]['ab_policy_id'] = ax.get('policyID')

    create0, update, archive = process_matches(hvp, avp)
    create = contact_filter(create0, hdic)
    return create, create0, update, archive, hdic, adic, to_archive, hh, aa

def safe_up(s):
    if type(s) == str:
        ss = s.upper().strip()
        if ss == 'NONE':
            return None
        else:
            return ss
    else:
        return s

def hb_load_policies(olabel, hb_show_active_only):
    hs_check = load_hubspot(olabel)
    hid = {}
    hdic = {}
    for x in hs_check:
        carrier = x['properties']['carrier']
        pnumber = ab.get_commission_field(x['properties'], olabel)
        k = (safe_up(carrier), pnumber)
        hdic[k] = hdic.get(k, []) + [x]
        arr = hid.get(k, [])
        pid = x['id']
        status = x['properties'].get('status')
        if not status:
            status = x['properties'].get('plan__status')
        contact_id = x['properties']['originally_associated_contact_id']
        if status == '200' or not hb_show_active_only:
            arr.append((pid, status, contact_id))
            hid[k] = arr
    return hdic, hid

def policy_groups(olabel, ab_show_active_only = True, hb_show_active_only = True):
    object_id = oid_dic[olabel.upper()]
    ab_check = load_agency_bloc(olabel)
    abd = {}
    adic = {}
    for x in ab_check:
        carrier, pnumber = (x['carrier'],x['policyNumber'])
        k = (safe_up(carrier), pnumber.strip())
        arr = abd.get(k, [])
        adic[k] = adic.get(k, []) + [x]
        pid, status, contact_id = (x['policyID'], x['policyStatus'], x['entityID'])
        if status.lower() == 'active' or status == '200' or not ab_show_active_only:
            arr.append((pid, status, contact_id))
            abd[k] = arr

    hdic, hid = hs_load_policies(olabel, hb_show_active_only)

    out = []
    for k in set(abd).union(set(hid)):
        car, pnumber = k
        ab_arr = abd.get(k, [])
        hb_arr = hid.get(k, [])
        group = []
        for rid, status, contact_id in ab_arr:
            d = {
                'carrier': car,
                'policy_number': pnumber,
                'type': 'agency_bloc',
                'status': status,
                'id': rid,
                'link':'https://app.agencybloc.com/policies/'+str(rid)+'/detail',
                'contact_id': contact_id,
                'contact_link': 'https://app.agencybloc.com/individuals/'+str(contact_id)+'/detail'
            }
            group.append(d)
        for rid, status, contact_id in hb_arr:
            if contact_id:
                contact_link = 'https://app.hubspot.com/contacts/7879306/contact/'+contact_id
            else:
                contact_link = None
            d = {
                'carrier': car,
                'policy_number': pnumber,
                'type': 'hubspot',
                'status': status,
                'id': rid,
                'link':'https://app.hubspot.com/contacts/7879306/record/'+object_id+'/'+str(rid),
                'contact_id': contact_id,
                'contact_link': contact_link
            }
            group.append(d)

        out.append(group)
    return out, adic, hdic

def split_error_groups(arr):
    ok, err = [],[]
    for group in arr:
        types = set([x['type'] for x in group])
        if len(types) == 2 and len(group) == 2:
            ok.append(group)
        else:
            err.append(group)
    single_h = []
    single_a = []
    other = []
    for group in err:
        if len(group) == 1:
            if group[0]['type'] == 'agency_bloc':
                single_a = single_a + group
            else:
                assert group[0]['type'] == 'hubspot'
                single_h = single_h + group
        else:
            other.append(group)

    no_action, update = [],[]
    for group in ok:
        x,y = group
        if x['status'] == y['status']:
            no_action.append(group)
        else:
            update.append(group)

    return no_action, update, single_h, single_a, other

def agent_routine(arr):
    default_agent_id = '168441184'   # house account for deactivated agents

    print("checking that egents exist")
    ab_agents = ab.get_agents()
    ab_agents_by_email = {x['email']: x for x in ab_agents}
    default_agent = ab_agents_by_email['house.enlightnu.noemail@medicareschool.com']

    hb_agents = hs.get_agents()

    agents_to_add = []
    for x in arr:
        hb_agent_id, has_agent = agent_checker(x, hb_agents, ab_agents_by_email)
        if not has_agent:
            agents_to_add.append(hb_agent_id)
    agents_to_add = list(set(agents_to_add))
    agents_use_house = []
    agent_args = []
    print("Trying to create missing agents")
    for i in agents_to_add:
        d = hb_agents.get(i, {})
        firstname = d.get('firstName')
        lastname = d.get('lastName')
        email = d.get('email')
        if firstname and lastname and email:
            agent_args.append((firstname, lastname, email))
            print('created agent', firstname, lastname)
        else:
            # using house account:
            print("Using house account for agent id", i)
            agents_use_house.append(i)

    if agent_args:
        ab.create_agents(agent_args)

    for x in arr:
        _, has_agent = agent_checker(x, hb_agents, ab_agents_by_email)
        if has_agent:
            x['agent'] = has_agent
        else:
            x['agent'] = default_agent

report_columns = [
    'policy_number',
    'carrier',
    'hubspot_status',
    'agencybloc_status_old',
    'agencybloc_status_new',
    'action',
    'hubspot_id',
    'agencybloc_id',
    'hubspot_link',
    'agencybloc_link',
]

def format_for_review(create, update, archive, hdic, adic, out1, object_id):
    fcreate = format_response_create(create, out1.get('create_response',({}, None)), object_id)
    fupdate = format_response_update(update, out1.get('update_response',({}, None)), object_id)
    farchive = format_response_archive(archive, out1.get('archive_response',({},None)), hdic, adic, object_id)
    return foldl(lambda a,b: a + list(b.values()), [], [fcreate, fupdate, farchive])

def format_response_create(arr, resp, object_id):
    dd = {}
    for d1,d2 in resp[0]:
        pid = d1.get('Agencybloc Response', {}).get('policyID')
        d = {
            'agencybloc_id': pid,
            'agencybloc_status_new': d1.get('Agencybloc Response', {}).get('Status'),
            'action': d1.get('Agencybloc Response', {}).get('Action'),
            'agencybloc_link': 'https://app.agencybloc.com/policies/'+pid+'/detail',
        }
        hubspot_id = d2.get('id')
        dd[hubspot_id] = d
    for dic in arr:
        d = {
            'policy_number': dic['policy_number'],
            'carrier': dic['carrier'],
            'hubspot_status': dic['status'],
            'agencybloc_status_old': None,
            'hubspot_id': dic['id'],
            'hubspot_link': 'https://app.hubspot.com/contacts/7879306/record/'+object_id+'/'+dic['id']
        }
        hid = dic['id']
        d2 = dd.get(hid, {})
        dnew = d | d2
        dd[hid] = dnew
    return dd


def format_response_update(arr, resp, object_id):
    dd = {}
    for d1,d2 in resp[0]:
        pid = d1.get('Agencybloc Response', {}).get('policyID')
        d = {
            'agencybloc_id': pid,
            'agencybloc_status_new': d1.get('Agencybloc Response', {}).get('Status'),
            'action': d1.get('Agencybloc Response', {}).get('Action', 'default_update?'),
            'agencybloc_link': 'https://app.agencybloc.com/policies/'+pid+'/detail',
        }
        hubspot_id = d2.get('id')
        dd[hubspot_id] = d
    for group in arr:
        x,y = group
        if x['type'] == 'hubspot':
            h,a = x,y
        else:
            a,h = x,y
        d = {
            'policy_number': h['policy_number'],
            'carrier': h['policy_number'],
            'hubspot_status': h['status'],
            'agencybloc_status_old': a['status'],
            'hubspot_id': h['id'],
            'hubspot_link': 'https://app.hubspot.com/contacts/7879306/record/'+object_id+'/'+h['id']
        }
        hubspot_id = h['id']
        d2 = dd.get(hubspot_id, {})
        dnew = d | d2
        dd[hubspot_id] = dnew
    return dd

def format_response_archive(arr, resp, hdic, adic, object_id):
    dd = {}
    adic_new = {x[0]['policyID']: x[0] for x in adic.values()}
    for d1,d2 in resp[0]:
        pid = d1.get('Agencybloc Response', {}).get('policyID')
        d = {
            'agencybloc_id': pid,
            'agencybloc_status_new': 'Archive',
            'action': 'archive',
            'agencybloc_link': 'https://app.agencybloc.com/policies/'+pid+'/detail',
        }
        dd[pid] = d
    for aid in [x['id'] for x in arr]:
        print(aid)
        dic = adic_new.get(aid, {})
        hubspot_status = None
        hubspot_link = None
        d = {
            'policy_number': dic.get('policyNumber'),
            'carrier': dic.get('carrier'),
            'hubspot_status': None,
            'agencybloc_status_old': dic.get('status'),
            'hubspot_id': None,
            'hubspot_link': None,
        }
        d2 = dd.get(aid, {})
        dnew = d | d2
        dd[aid] = dnew
    return dd

def flatten_records(arr, out_arr):
    for x in arr:
        if type(x) == dict:
            out_arr.append(x)
        else:
            out_arr = flatten_records(x, out_arr)
    return out_arr
