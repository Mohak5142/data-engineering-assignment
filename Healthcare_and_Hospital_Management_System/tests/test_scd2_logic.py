import pandas as pd

def apply_scd2(existing,incoming,key,attrs):
    existing=existing.copy(); incoming=incoming.copy()
    existing['_hash']=existing[attrs].astype(str).agg('|'.join,axis=1).map(lambda x: __import__('hashlib').sha256(x.encode()).hexdigest())
    incoming['_hash']=incoming[attrs].astype(str).agg('|'.join,axis=1).map(lambda x: __import__('hashlib').sha256(x.encode()).hexdigest())
    cur=existing[existing['_is_current']==True].set_index(key)
    changes=incoming[(~incoming[key].isin(cur.index)) | (incoming.set_index(key)['_hash'].reindex(incoming[key]).values != cur['_hash'].reindex(incoming[key]).values)]
    return changes

def test_scd_detects_change():
    e=pd.DataFrame([{'patient_id':'P1','gender':'F','_is_current':True}]); i=pd.DataFrame([{'patient_id':'P1','gender':'M'}])
    assert len(apply_scd2(e,i,'patient_id',['gender']))==1
