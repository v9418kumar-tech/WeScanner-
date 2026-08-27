import os,gzip,json,time,logging
from datetime import datetime
from urllib.parse import quote
import requests
from fastapi import FastAPI,HTTPException
from fastapi.responses import FileResponse
TOKEN=os.getenv("UPSTOX_ACCESS_TOKEN","").strip(); BASE="https://api.upstox.com"; INSTR_URL="https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"; s=requests.Session(); app=FastAPI(); instruments=[]; loaded=0
H=lambda:{"Accept":"application/json","Authorization":f"Bearer {TOKEN}"}
def load():
 global instruments,loaded
 if instruments and time.time()-loaded<21600:return
 r=s.get(INSTR_URL,timeout=30);r.raise_for_status();d=json.loads(gzip.decompress(r.content))
  trading_symbol") or "")+" "+(x.get("name") or "")).upper()]
def chunks(a,n): 
 for i in range(0,len(a),n):yield a[i:i+n]
def quotes():
 out=[]
 for c in chunks(instruments,500):
  r=s.get(BASE+"/v3/market-quote/ohlc",headers=H(),params={"instrument_key":",".join(x["instrument_key"] for x in c),"interval":"I1"},timeout=30);r.raise_for_status();out+=list(r.json().get("data",{}).values())
 return out
def hist(key,date):
 u=BASE+"/v3/historical-candle/"+quote(key,safe="|")+"/minutes/1/"+date
 r=s.get(u,headers=H(),timeout=15);r.raise_for_status();cs=r.json().get("data",{}).get("candles",[])
 if len(cs)<2:return None
 vs=[float(c[5]) for c in cs[-6:]];cur=vs[-1];avg=sum(vs[:-1])/len(vs[:-1]);return cur,avg,cs[-1][0]
@app.get("/")
def root():return FileResponse("index.html")
@app.get("/api/scan")
def scan(rows:int=10,mult:float=3):
 if not TOKEN:raise HTTPException(500,"UPSTOX_ACCESS_TOKEN is not configured")
 rows=max(1,min(100,rows));mult=max(.1,min(1000,mult));load();qs=quotes();by={x["instrument_key"]:x for x in instruments};cand=[]
 for q in qs:
  p=float(q.get("last_price") or 0);v=float((q.get("prev_ohlc") or {}).get("volume") or 0)
  if p>=100 and v>0:cand.append((v,p,q.get("instrument_token"),q))
 cand.sort(reverse=True);res=[];date=datetime.now().date().isoformat()
 for v,p,key,q in cand[:max(100,rows*5)]:
  try:
   x=hist(key,date)
   if x and x[1]>0 and x[0]>=x[1]*mult:
    sym=by.get(key,{}).get("trading_symbol") or q.get("symbol") or key;res.append({"symbol":sym,"price":p,"volume":x[0],"avg5":x[1],"multiplier":x[0]/x[1],"time":str(x[2])[11:19]})
  except Exception as e:logging.warning("history %s %s",key,e)
 res.sort(key=lambda x:x["multiplier"],reverse=True)
 return {"connected":True,"mode":"Historical 1-minute scan","scanned":len(instruments),"updated_at":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"results":res[:rows]}
