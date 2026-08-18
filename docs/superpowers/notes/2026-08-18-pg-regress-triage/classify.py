import re, sys, json, collections
diffs=open(sys.argv[1]).read().split('\n')
# split into files
files={}
cur=None
for line in diffs:
    m=re.match(r'^diff -U3 .*expected/([\w.]+)\.out .*results/', line)
    if m:
        cur=m.group(1); files[cur]=[]; continue
    if cur is not None:
        files[cur].append(line)
def changed(lines):
    out=[]
    for l in lines:
        if re.match(r'^(\+\+\+|---) /', l): continue
        if l.startswith('+') or l.startswith('-'): out.append(l)
    return out
tot=0
per={}
for f,lines in files.items():
    ch=changed(lines); tot+=len(ch); per[f]=len(ch)
print('files',len(files),'changed',tot)
# classify lines: explain-plan lines = lines within a hunk that follows a "QUERY PLAN" header, or that look like plan nodes
plan_re=re.compile(r'^[+-]\s*(->  |)(Seq Scan|Index Scan|Index Only Scan|Bitmap|Hash|Nested Loop|Merge|Sort|Aggregate|HashAggregate|GroupAggregate|Append|Merge Append|Subquery Scan|CTE Scan|Function Scan|Values Scan|Result|Limit|Unique|WindowAgg|Materialize|Memoize|Gather|Incremental Sort|Group|SetOp|Recursive Union|WorkTable Scan|Foreign Scan|Sample Scan|Tid Scan|Tid Range Scan|Table Function Scan|Insert|Update|Delete|Merge on|ModifyTable|LockRows|ProjectSet|Custom Scan|Named Tuplestore|Parallel|Finalize|Partial|Hash Join|Hash Left Join|Hash Right Join|Hash Full Join|Hash Anti Join|Hash Semi Join|Merge Join|Nested Loop Left|Index Cond|Filter|Join Filter|Hash Cond|Merge Cond|Sort Key|Group Key|Recheck Cond|Heap Fetches|One-Time Filter|Output|Presorted Key|Workers|Rows Removed|Cache Key|Cache Mode|Buckets|Planning|Execution|Subplans Removed|InitPlan|SubPlan|Conflict|Tuples Inserted|Conflicting Tuples|Storage|Maximum Storage|Full-sort Groups|Pre-sorted Groups|Disabled|Order By|TID Cond|Table Function Call|Sampling|Actual|Batches|Peak Memory|Worker|Total|Trigger|Update on|Delete on|Insert on|Merge on|Settings|Query Identifier|Function Call|CTE |Filter:|Sort Method|JIT|Options|Timing|Grouped|Partitions|Presorted|Rows: |Loops:|Cache Hits|Cache Misses|Cache Evictions|Cache Overflows|Heap Blocks|Buffers)')
def is_plan(l):
    return bool(plan_re.match(l))
cats=collections.Counter()
percat=collections.defaultdict(collections.Counter)
err_fam=collections.Counter()
for f,lines in files.items():
    in_plan=False
    for l in lines:
        if re.match(r'^(\+\+\+|---) /', l): continue
        body=l[1:] if l[:1] in '+- ' else l
        if 'QUERY PLAN' in body: in_plan=True
        elif body.strip()=='' or re.match(r'^\(\d+ rows?\)',body.strip()): in_plan=False
        if l[:1] not in '+-': continue
        if in_plan or is_plan(l):
            c='explain'
        elif re.match(r'^[+-]ERROR:',l):
            c='error'
            if l.startswith('+'):
                m=re.match(r'^\+ERROR:\s+(.*)',l); msg=m.group(1)
                # normalize
                msg=re.sub(r'"[^"]*"','"…"',msg); msg=re.sub(r'\d+','N',msg)
                err_fam[msg[:90]]+=1
        elif re.match(r'^[+-](LINE \d+:|\s+\^)',l):
            c='caret'
        elif re.match(r'^[+-](DETAIL|HINT|CONTEXT|NOTICE|WARNING|INFO):',l):
            c='diag'
        elif re.match(r'^[+-]\(\d+ rows?\)',l) or re.match(r'^[+-][-+ ]*$',l):
            c='frame'
        elif re.match(r'^[+-]-+(\+-+)*$', l):
            c='frame'
        else:
            c='content'
        cats[c]+=1; percat[f][c]+=1
print(cats)
print()
print('top gres-side error families:')
for m,c in err_fam.most_common(80): print(f'{c:6d} {m}')
print()
print('per-file explain share (files with >100 explain lines):')
rows=sorted(percat.items(), key=lambda kv:-kv[1]['explain'])
for f,c in rows:
    if c['explain']>=100: print(f"{c['explain']:6d} of {per[f]:6d} {f}")
json.dump({f:dict(c) for f,c in percat.items()}, open(sys.argv[2],'w'), indent=1)
