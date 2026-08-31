import React,{useEffect,useState} from 'react'
import {createRoot} from 'react-dom/client'
import './style.css'

// Same-origin paths, reverse-proxied by nginx (see frontend/nginx.conf).
// Calling the backend ports directly from the browser fails CORS preflight.
const API='/payment'
const NOTIFY='/notify'
const CLIENTS={amazon:'/amazon',udemy:'/udemy'}

function App(){
 const [client,setClient]=useState('amazon'),[order,setOrder]=useState('ORD-'+Date.now()),[amount,setAmount]=useState(5000)
 const [payment,setPayment]=useState(null),[rows,setRows]=useState([]),[circuits,setCircuits]=useState({})
 const [up,setUp]=useState({amazon:true,udemy:true}),[loading,setLoading]=useState(false)

 async function pay(){
   setLoading(true)
   try{
    const r=await fetch(API+'/api/payments',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({clientId:client,orderId:order,amount:Number(amount),currency:'INR'})})
    setPayment(await r.json())
    setOrder('ORD-'+Date.now())
   }catch(e){setPayment({error:e.message})}
   setLoading(false)
 }
 async function refresh(){
   try{setRows(await (await fetch(NOTIFY+'/api/notifications')).json())}catch{}
   for(const c of ['amazon','udemy']){
     try{const s=await (await fetch(NOTIFY+'/api/circuit/'+c)).json();setCircuits(x=>({...x,[c]:s}))
     const cs=await (await fetch(CLIENTS[c]+'/api/status')).json();setUp(x=>({...x,[c]:!cs.down}))}catch{}
   }
 }
 async function toggle(c){
   await fetch(CLIENTS[c]+'/api/toggle',{method:'POST'}); refresh()
 }
 useEffect(()=>{refresh();const i=setInterval(refresh,1000);return()=>clearInterval(i)},[])
 const latest=payment?rows.find(x=>x.eventId===payment.eventId):null
 return <div className="app">
  <header><div><h1>Payment Notification Gateway</h1><p>Kafka • Redis • PostgreSQL • Webhooks • Circuit Breaker</p></div><span className="live">● LIVE</span></header>
  <main>
   <section className="card payment">
    <h2>Make Payment</h2>
    <div className="grid">
     <label>Client<select value={client} onChange={e=>setClient(e.target.value)}><option value="amazon">Amazon</option><option value="udemy">Udemy</option></select></label>
     <label>Order ID<input value={order} onChange={e=>setOrder(e.target.value)}/></label>
     <label>Amount<input type="number" value={amount} onChange={e=>setAmount(e.target.value)}/></label>
    </div>
    <button onClick={pay} disabled={loading}>{loading?'Processing...':'MAKE PAYMENT'}</button>
    {payment&&<div className="result"><b>Payment {payment.status||'ERROR'}</b>
      {payment.error
        ?<span>{payment.error}</span>
        :<><span>Payment ID: {payment.paymentId||'-'}</span><span>Event ID: {payment.eventId||'-'}</span></>}
    </div>}
   </section>

   <section className="two">
    <div className="card">
      <h2>Client Simulator</h2>
      {['amazon','udemy'].map(c=><div className="client" key={c}><b>{c.toUpperCase()}</b><span className={up[c]?'ok':'bad'}>● {up[c]?'UP':'DOWN'}</span><button className="small" onClick={()=>toggle(c)}>{up[c]?'MAKE DOWN':'MAKE UP'}</button></div>)}
    </div>
    <div className="card">
      <h2>Circuit Breakers</h2>
      {['amazon','udemy'].map(c=><div className="client" key={c}><b>{c.toUpperCase()}</b><span className={(circuits[c]?.state||'CLOSED')==='CLOSED'?'ok':'warn'}>{circuits[c]?.state||'CLOSED'}</span><small> failures: {circuits[c]?.failures||0} {circuits[c]?.cooldownRemainingSeconds?`• ${circuits[c].cooldownRemainingSeconds}s`:''}</small></div>)}
    </div>
   </section>

   <section className="card">
    <h2>Notification Deliveries</h2>
    <table><thead><tr><th>Client</th><th>Event</th><th>Status</th><th>Attempt</th><th>Next Retry</th><th>Error</th></tr></thead>
    <tbody>{rows.map(r=><tr key={r.eventId}><td>{r.clientId}</td><td>{r.eventId.slice(0,16)}...</td><td><span className={'pill '+r.status.toLowerCase()}>{r.status}</span></td><td>{r.attempt}</td><td>{r.nextRetryAt?new Date(r.nextRetryAt).toLocaleTimeString():'-'}</td><td>{r.lastError||'-'}</td></tr>)}</tbody></table>
   </section>

   <section className="card">
    <h2>How this demo works</h2>
    <div className="flow"><span>Payment</span><i>→</i><span>Kafka</span><i>→</i><span>Notification</span><i>→</i><span>Redis/DB</span><i>→</i><span>Webhook</span></div>
    <p className="hint">Turn a client DOWN, make a payment, and watch exponential retries. After repeated failures the per-client circuit opens. Bring the client UP and wait for HALF_OPEN → CLOSED.</p>
   </section>
  </main>
 </div>
}
createRoot(document.getElementById('root')).render(<App/>)
