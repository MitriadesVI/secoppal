from pathlib import Path
import subprocess,os,json,datetime,hashlib,sys
ROOT=Path('/Users/rodrigoortiz/Documents/secoppal-live-publication-r1-2');BASE=ROOT.parent/'secoppal-live-publication-r1-2-base';D=ROOT/'docs/audits/foundation_v3_live_publication_r1_2';E=D/'evidence';S=ROOT/'scripts/audits/foundation_v3_live_publication_r1_2';PY='/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python';STATE='r-20260930-184140-647b'
def run(name,argv,cwd=ROOT,timeout=180):
 env=os.environ.copy();env.update(PYTHONDONTWRITEBYTECODE='1',BURR_TELEMETRY_ENABLED='false',SEMGREP_SEND_METRICS='off',SEMGREP_ENABLE_VERSION_CHECK='0',SECOP_FEEDBACK_PATH='/tmp/r12-feedback.jsonl',SECOP_FEEDBACK_DB_PATH='/tmp/r12-feedback.db',CONVERSATION_STORE_DIR='/tmp/r12-conversations',HYPOTHESIS_STORAGE_DIRECTORY='/tmp/r12-hypothesis',PYTHONPATH=str(cwd))
 cmd=['sandbox-exec','-f',str(S/'offline.sb'),*map(str,argv)]
 i=0
 while (E/f'{name}.{i}.stdout').exists():i+=1
 out=E/f'{name}.{i}.stdout';err=E/f'{name}.{i}.stderr';start=datetime.datetime.now(datetime.timezone.utc).isoformat()
 try:p=subprocess.run(cmd,cwd=cwd,env=env,capture_output=True,text=True,timeout=timeout);rc=p.returncode;stdout=p.stdout;stderr=p.stderr
 except subprocess.TimeoutExpired as ex:rc=124;stdout=str(ex.stdout);stderr=str(ex.stderr)
 out.write_text(stdout);err.write_text(stderr)
 with (D/'COMMANDS.jsonl').open('a') as f:f.write(json.dumps({'name':name,'time_utc':start,'cwd':str(cwd),'argv':cmd,'exit':rc,'stdout':str(out.relative_to(ROOT)),'stderr':str(err.relative_to(ROOT))})+'\n')
 print(name,rc,(stdout if rc==0 else stdout+stderr)[-1400:],flush=True)
 return rc,stdout,stderr
