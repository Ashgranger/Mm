const API="https://api.arcus.xyz/v1/leaderboard";
const walletInput=document.querySelector("#wallet"),checkBtn=document.querySelector("#check"),errorEl=document.querySelector("#error"),results=document.querySelector("#results"),addressEl=document.querySelector("#address"),statusEl=document.querySelector("#status");
let currentWallet="",currentWindow="all",cache={};
const validAddress=v=>/^0x[a-fA-F0-9]{40}$/.test(v.trim());
const fmt=n=>new Intl.NumberFormat("en-US",{maximumFractionDigits:2}).format(n);
function usd(n){const s=n<0?"-":"",x=Math.abs(n);if(x>=1e12)return s+"$"+fmt(x/1e12)+"T";if(x>=1e9)return s+"$"+fmt(x/1e9)+"B";if(x>=1e6)return s+"$"+fmt(x/1e6)+"M";if(x>=1e3)return s+"$"+fmt(x/1e3)+"K";return s+"$"+fmt(x)}
function showError(m){errorEl.textContent=m;errorEl.style.display=m?"block":"none"}
async function fetchStats(wallet,win){
 const key=wallet.toLowerCase()+":"+win;if(cache[key])return cache[key];
 const u=new URL(API);u.searchParams.set("window",win);u.searchParams.set("sortBy","volume");u.searchParams.set("address",wallet);
 const r=await fetch(u,{headers:{Accept:"application/json"}});if(!r.ok)throw new Error(`Arcus API returned HTTP ${r.status}`);
 const d=await r.json();if(!d.entries?.length)throw new Error("No leaderboard entry found for this wallet.");return cache[key]=d.entries[0];
}
function render(e){
 document.querySelector("#rank").textContent=e.rank!=null?"#"+e.rank:"—";
 document.querySelector("#pnl").textContent=usd(Number(e.pnl)/1e6);
 document.querySelector("#fees").textContent=usd(Number(e.feesPaid)/1e6);
 document.querySelector("#volume").textContent=usd(Number(e.volume)/1e6);
 document.querySelector("#period").textContent=currentWindow==="all"?"All time":currentWindow;
 document.querySelector("#rawVolume").textContent=Number(e.volume).toLocaleString();
 document.querySelector("#rawFees").textContent=Number(e.feesPaid).toLocaleString();
 document.querySelector("#rawPnl").textContent=Number(e.pnl).toLocaleString();
}
async function check(win=currentWindow){showError("");statusEl.textContent="Loading…";try{const e=await fetchStats(currentWallet,win);currentWindow=win;render(e);statusEl.textContent="Updated just now"}catch(err){showError(err.message||"Unable to load Arcus data.");statusEl.textContent=""}}
async function start(){const w=walletInput.value.trim();if(!validAddress(w)){showError("Enter a valid 0x wallet address.");return}currentWallet=w;cache={};addressEl.textContent=w;results.classList.remove("hidden");await check("all")}
checkBtn.addEventListener("click",start);walletInput.addEventListener("keydown",e=>{if(e.key==="Enter")start()});
document.querySelectorAll(".tabs button").forEach(b=>b.addEventListener("click",()=>{document.querySelectorAll(".tabs button").forEach(x=>x.classList.remove("active"));b.classList.add("active");check(b.dataset.window)}));
document.querySelector("#copy").addEventListener("click",async()=>{const text=`Arcus stats\nWallet: ${currentWallet}\nPeriod: ${currentWindow}\nRank: ${document.querySelector("#rank").textContent}\nPnL: ${document.querySelector("#pnl").textContent}\nFees: ${document.querySelector("#fees").textContent}\nVolume: ${document.querySelector("#volume").textContent}`;try{await navigator.clipboard.writeText(text);document.querySelector("#copy").textContent="Copied!";setTimeout(()=>document.querySelector("#copy").textContent="Copy summary",1200)}catch{showError("Clipboard access was blocked by the browser.")}});
