import { useState, useEffect, useRef, useCallback } from "react";
import { AreaChart, Area, XAxis, YAxis, Tooltip as RechartTooltip, ResponsiveContainer, CartesianGrid } from "recharts";
import * as d3 from "d3";

// ══════════════════════════════════════════════════════════════════════════════
// GLOBAL STYLES
// ══════════════════════════════════════════════════════════════════════════════
const GlobalStyles = () => (
  <style>{`
    @import url('https://fonts.googleapis.com/css2?family=Teko:wght@400;500;600;700&family=DM+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500;700&display=swap');
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    html { scroll-behavior: smooth; }
    body { background: #060a0e; }
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: #060a0e; }
    ::-webkit-scrollbar-thumb { background: #1a2535; border-radius: 4px; }
    .live-pulse { animation: livePulse 1.8s ease-in-out infinite; }
    @keyframes livePulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:0.2;transform:scale(0.8)} }
    .ticker-track { display:flex; animation: tickerRoll 80s linear infinite; }
    .ticker-track:hover { animation-play-state:paused; }
    @keyframes tickerRoll { from{transform:translateX(0)} to{transform:translateX(-50%)} }
    .news-feed { animation: newsFeed 65s linear infinite; }
    .news-feed:hover { animation-play-state:paused; }
    @keyframes newsFeed { from{transform:translateY(0)} to{transform:translateY(-50%)} }
    .cell-h { transition: filter .15s ease; cursor:pointer; }
    .cell-h:hover { filter: brightness(1.45) saturate(1.3); }
    .fade-bot { background: linear-gradient(transparent, #0d1219); }
    .glow-india { box-shadow: 0 0 0 1px rgba(255,153,51,0.15), 0 4px 20px rgba(255,153,51,0.05); }
    .glow-us    { box-shadow: 0 0 0 1px rgba(65,132,228,0.15),  0 4px 20px rgba(65,132,228,0.05); }
    .glow-gold  { box-shadow: 0 0 0 1px rgba(212,175,55,0.2),   0 4px 20px rgba(212,175,55,0.07); }
    @keyframes fadeUp { from{opacity:0;transform:translateY(10px)} to{opacity:1;transform:translateY(0)} }
    .fi { animation: fadeUp 0.4s ease forwards; }
    @keyframes ttIn { from{opacity:0;transform:translateY(4px)} to{opacity:1;transform:translateY(0)} }
    .tt-in { animation: ttIn 0.1s ease forwards; }
    .tab-btn { transition: all 0.18s ease; }
    .tab-btn:hover { color: #ff9933 !important; }
  `}</style>
);

// ══════════════════════════════════════════════════════════════════════════════
// MARKET PRICE DATA
// ══════════════════════════════════════════════════════════════════════════════
const mkS = (base, n = 24, vol = 0.003) => {
  let v = base;
  return Array.from({ length: n }, (_, i) => {
    v = v * (1 + (Math.random() - 0.49) * vol);
    return { t: `${String(i+1).padStart(2,"0")}:00`, v: parseFloat(v.toFixed(2)) };
  });
};
const INIT = {
  giftNifty: { p:23847.5,  ch:+0.42, data:mkS(23700,24,0.004) },
  nifty50:   { p:23640.30, ch:+0.38, data:mkS(23500,24,0.003) },
  sensex:    { p:78120.45, ch:+0.41, data:mkS(77800,24,0.003) },
  bankNifty: { p:50840.25, ch:-0.22, data:mkS(51000,24,0.004) },
  niftyIT:   { p:38420.60, ch:+1.12, data:mkS(38000,24,0.005) },
  goldMCX:   { p:92480,    ch:+0.58, data:mkS(92000,24,0.003) },
  silverMCX: { p:96250,    ch:-0.21, data:mkS(96700,24,0.004) },
  copper:    { p:841.20,   ch:+0.33, data:mkS(838,  24,0.004) },
  aluminium: { p:228.40,   ch:-0.55, data:mkS(230,  24,0.003) },
  zinc:      { p:265.80,   ch:+0.21, data:mkS(264,  24,0.003) },
  nickel:    { p:1388.50,  ch:-0.88, data:mkS(1402, 24,0.005) },
  dji:       { p:41872.3,  ch:+0.37, data:mkS(41600,24,0.003) },
  sp500:     { p:5612.78,  ch:+0.29, data:mkS(5580, 24,0.003) },
  dowFut:    { p:41920.0,  ch:+0.41, data:mkS(41700,24,0.003) },
  dxy:       { p:103.42,   ch:-0.18, data:mkS(103.8,24,0.002) },
  bond10y:   { p:4.387,    ch:+0.04, data:mkS(4.35, 24,0.004) },
  usdinr:    { p:83.24,    ch:+0.12, data:mkS(83.1, 24,0.002) },
  crudeMCX:  { p:6810,     ch:-0.44, data:mkS(6842, 24,0.004) },
};

// ══════════════════════════════════════════════════════════════════════════════
// HEATMAP SECTOR DATA (Nifty 500)
// ══════════════════════════════════════════════════════════════════════════════
const RAW_SECTORS = [
  { id:"banking",   label:"BANKING",         mcap:280, stocks:[
    {sym:"HDFCBANK",  mcap:120,ch:+1.20},{sym:"ICICIBANK",  mcap:100,ch:+0.82},
    {sym:"SBIN",      mcap:80, ch:+1.85},{sym:"KOTAKBANK",  mcap:70, ch:-0.41},
    {sym:"AXISBANK",  mcap:60, ch:+0.35},{sym:"INDUSINDBK", mcap:30, ch:-1.10},
    {sym:"BANKBARODA",mcap:25, ch:+2.10},{sym:"PNB",        mcap:22, ch:+1.60},
    {sym:"CANBK",     mcap:18, ch:+0.95},{sym:"FEDERALBNK", mcap:15, ch:-0.30},
  ]},
  { id:"it",        label:"IT & TECH",        mcap:240, stocks:[
    {sym:"TCS",       mcap:120,ch:+2.12},{sym:"INFY",       mcap:95, ch:+1.65},
    {sym:"HCLTECH",   mcap:65, ch:+1.30},{sym:"WIPRO",      mcap:55, ch:+0.92},
    {sym:"TECHM",     mcap:40, ch:-0.22},{sym:"LTIM",       mcap:38, ch:+1.80},
    {sym:"MPHASIS",   mcap:22, ch:+0.65},{sym:"PERSISTENT", mcap:20, ch:+2.50},
    {sym:"COFORGE",   mcap:18, ch:+1.40},{sym:"KPIT",       mcap:14, ch:-0.60},
  ]},
  { id:"energy",    label:"ENERGY & O&G",     mcap:210, stocks:[
    {sym:"RELIANCE",  mcap:130,ch:+0.62},{sym:"ONGC",       mcap:55, ch:-0.88},
    {sym:"NTPC",      mcap:52, ch:+0.74},{sym:"POWERGRID",  mcap:44, ch:+0.42},
    {sym:"ADANIGREEN",mcap:40, ch:+1.45},{sym:"TATAPOWER",  mcap:32, ch:+0.88},
    {sym:"IOC",       mcap:30, ch:-1.20},{sym:"BPCL",       mcap:28, ch:-0.75},
    {sym:"GAIL",      mcap:26, ch:+0.55},{sym:"PETRONET",   mcap:18, ch:+0.30},
  ]},
  { id:"nbfc",      label:"NBFC & INSURANCE", mcap:155, stocks:[
    {sym:"BAJFINANCE",mcap:80, ch:+1.92},{sym:"BAJAJFINSV", mcap:55, ch:+1.10},
    {sym:"HDFCLIFE",  mcap:42, ch:-0.32},{sym:"SBILIFE",    mcap:40, ch:+0.44},
    {sym:"ICICIGI",   mcap:35, ch:+0.78},{sym:"MUTHOOTFIN", mcap:22, ch:+2.40},
    {sym:"CHOLAFIN",  mcap:20, ch:+1.15},{sym:"MANAPPURAM", mcap:12, ch:-0.85},
  ]},
  { id:"auto",      label:"AUTO & ANCIL.",    mcap:150, stocks:[
    {sym:"MARUTI",    mcap:80, ch:-0.30},{sym:"TATAMOTORS", mcap:75, ch:+1.42},
    {sym:"M&M",       mcap:70, ch:+2.20},{sym:"BAJAJ-AUTO", mcap:55, ch:+0.52},
    {sym:"EICHERMOT", mcap:45, ch:-0.58},{sym:"HEROMOTOCO", mcap:40, ch:+0.38},
    {sym:"BOSCHLTD",  mcap:30, ch:+1.10},{sym:"MOTHERSON",  mcap:22, ch:+1.85},
    {sym:"BALKRISIND",mcap:18, ch:-0.42},{sym:"APOLLOTYRE", mcap:15, ch:+0.68},
  ]},
  { id:"fmcg",      label:"FMCG",             mcap:130, stocks:[
    {sym:"HINDUNILVR",mcap:80, ch:-0.72},{sym:"ITC",        mcap:72, ch:+0.42},
    {sym:"NESTLEIND", mcap:40, ch:-0.18},{sym:"BRITANNIA",  mcap:30, ch:+0.32},
    {sym:"DABUR",     mcap:28, ch:-0.55},{sym:"MARICO",     mcap:22, ch:+0.28},
    {sym:"GODREJCP",  mcap:20, ch:+0.62},{sym:"EMAMILTD",   mcap:14, ch:-0.38},
  ]},
  { id:"capgoods",  label:"CAPITAL GOODS",    mcap:120, stocks:[
    {sym:"LT",        mcap:90, ch:+0.95},{sym:"HAL",        mcap:55, ch:+3.20},
    {sym:"BEL",       mcap:42, ch:+2.80},{sym:"SIEMENS",    mcap:38, ch:+1.30},
    {sym:"ABB",       mcap:35, ch:+0.72},{sym:"BHEL",       mcap:28, ch:+1.65},
    {sym:"POLYCAB",   mcap:28, ch:-0.22},{sym:"CUMMINSIND", mcap:22, ch:+1.10},
    {sym:"KAYNES",    mcap:16, ch:+2.40},{sym:"THERMAX",    mcap:14, ch:+0.48},
  ]},
  { id:"pharma",    label:"PHARMA",           mcap:115, stocks:[
    {sym:"SUNPHARMA", mcap:80, ch:-0.42},{sym:"DRREDDY",    mcap:55, ch:+0.62},
    {sym:"CIPLA",     mcap:42, ch:+0.22},{sym:"DIVISLAB",   mcap:38, ch:-0.78},
    {sym:"APOLLOHOSP",mcap:36, ch:+0.88},{sym:"LUPIN",      mcap:30, ch:+1.35},
    {sym:"MAXHEALTH", mcap:22, ch:+0.72},{sym:"AUROPHARMA", mcap:18, ch:+0.55},
    {sym:"FORTIS",    mcap:16, ch:+1.20},{sym:"ABBOTINDIA", mcap:14, ch:-0.18},
  ]},
  { id:"metals",    label:"METALS & MINING",  mcap:100, stocks:[
    {sym:"TATASTEEL", mcap:60, ch:+1.12},{sym:"JSWSTEEL",   mcap:55, ch:+0.82},
    {sym:"HINDALCO",  mcap:50, ch:-0.52},{sym:"VEDL",       mcap:42, ch:+1.52},
    {sym:"COALINDIA", mcap:55, ch:+0.34},{sym:"NMDC",       mcap:28, ch:+0.68},
    {sym:"NATIONALUM",mcap:20, ch:+1.90},{sym:"SAIL",       mcap:18, ch:-0.44},
  ]},
  { id:"consumer",  label:"CONSUMER DISC.",   mcap:95, stocks:[
    {sym:"TITAN",     mcap:65, ch:+0.72},{sym:"ZOMATO",     mcap:55, ch:+1.92},
    {sym:"DMART",     mcap:52, ch:-0.38},{sym:"TRENT",      mcap:40, ch:+2.80},
    {sym:"SWIGGY",    mcap:30, ch:+0.88},{sym:"NYKAA",      mcap:22, ch:+1.45},
    {sym:"KALYANKJIL",mcap:18, ch:+2.10},{sym:"SENCO",      mcap:12, ch:+1.35},
  ]},
  { id:"realty",    label:"REAL ESTATE",      mcap:75, stocks:[
    {sym:"DLF",       mcap:55, ch:+1.45},{sym:"MACROTECH",  mcap:35, ch:+0.68},
    {sym:"GODREJPROP",mcap:30, ch:+2.10},{sym:"PRESTIGE",   mcap:25, ch:+1.80},
    {sym:"OBEROIRLTY",mcap:22, ch:-0.35},{sym:"PHOENIXLTD", mcap:20, ch:+1.20},
    {sym:"BRIGADE",   mcap:16, ch:+0.95},
  ]},
  { id:"infra",     label:"INFRA & CEMENT",   mcap:90, stocks:[
    {sym:"ULTRACEMCO",mcap:65, ch:+0.55},{sym:"GRASIM",     mcap:50, ch:+0.68},
    {sym:"AMBUJACEM", mcap:42, ch:+0.82},{sym:"SHREECEM",   mcap:40, ch:-0.28},
    {sym:"ACCLTD",    mcap:30, ch:+0.44},{sym:"IRB",        mcap:18, ch:+1.90},
    {sym:"KNRCON",    mcap:12, ch:+1.10},
  ]},
  { id:"chemicals", label:"CHEMICALS",        mcap:65, stocks:[
    {sym:"PIDILITIND",mcap:45, ch:+0.48},{sym:"SRF",        mcap:32, ch:+0.92},
    {sym:"UPL",       mcap:28, ch:-1.35},{sym:"DEEPAKNITRI",mcap:22, ch:+1.65},
    {sym:"NAVINFLUOR",mcap:18, ch:+1.20},{sym:"AARTIIND",   mcap:16, ch:+0.78},
    {sym:"FINEORG",   mcap:12, ch:+0.55},
  ]},
  { id:"telecom",   label:"TELECOM & MEDIA",  mcap:60, stocks:[
    {sym:"BHARTIARTL",mcap:80, ch:+0.88},{sym:"INDUSTOWER", mcap:30, ch:+0.44},
    {sym:"VODAFONE",  mcap:20, ch:-2.40},{sym:"SUNTV",      mcap:18, ch:+0.35},
    {sym:"PVRINOX",   mcap:12, ch:-1.10},{sym:"ZEEL",       mcap:10, ch:-0.92},
  ]},
  { id:"logistics", label:"LOGISTICS & AERO", mcap:50, stocks:[
    {sym:"INDIGO",    mcap:50, ch:-0.62},{sym:"CONTAINER",  mcap:30, ch:+0.75},
    {sym:"DELHIVERY", mcap:20, ch:+1.20},{sym:"BLUEDART",   mcap:18, ch:+0.42},
    {sym:"VRL",       mcap:10, ch:+0.68},{sym:"SPICEJET",   mcap:8,  ch:-1.80},
  ]},
];

const WORLD = [
  {n:"Nikkei 225",  v:"35,682",   ch:+0.88}, {n:"Hang Seng",  v:"19,230",   ch:-0.44},
  {n:"Shanghai",    v:"3,371",    ch:+0.12}, {n:"FTSE 100",   v:"8,612",    ch:+0.33},
  {n:"DAX",         v:"22,418",   ch:+0.51}, {n:"CAC 40",     v:"8,021",    ch:-0.09},
  {n:"KOSPI",       v:"2,631",    ch:-0.15}, {n:"ASX 200",    v:"7,941",    ch:+0.27},
  {n:"Taiwan Wt.",  v:"19,842",   ch:+0.63}, {n:"Jakarta",    v:"7,203",    ch:+0.19},
  {n:"STI (SGX)",   v:"3,825",    ch:+0.35}, {n:"IBOVESPA",   v:"1,28,420", ch:-0.28},
];

const NEWS = [
  {t:"09:52",src:"ET Markets",   h:"RBI likely to cut repo rate 25 bps in April MPC; bond yields soften"},
  {t:"09:44",src:"Moneycontrol", h:"Nifty 50 opens gap-up 180 pts; Bank Nifty surges on HDFC earnings beat"},
  {t:"09:36",src:"Business Std.",h:"Gold MCX hits ₹92,500 all-time high on global geopolitical jitters"},
  {t:"09:28",src:"Reuters India",h:"FII net buyers at ₹3,240 Cr; DII support ₹1,820 Cr — NSE data"},
  {t:"09:20",src:"Bloomberg",    h:"Fed signals patience; Powell says two cuts possible in 2025 if data allows"},
  {t:"09:12",src:"CNBC TV18",    h:"Rupee opens stronger at ₹83.19/$; exports data due this afternoon"},
  {t:"09:02",src:"ET Markets",   h:"IT stocks rally as TCS Q4 revenue beats estimates, guides higher for FY26"},
  {t:"08:52",src:"Moneycontrol", h:"Crude steady near ₹6,820/bbl; OPEC+ output cut extension eyed"},
  {t:"08:42",src:"Business Std.",h:"Silver consolidates near ₹96,000 support; open interest rises on MCX"},
  {t:"08:32",src:"Reuters India",h:"Domestic auto sales surge 12% YoY in Feb; Maruti, M&M lead gains"},
  {t:"08:22",src:"CNBC TV18",    h:"US 10Y yield climbs to 4.39%; Indian bonds eye RBI open market ops"},
  {t:"08:12",src:"ET Markets",   h:"HAL, BEL surge on record defence capex allocation for FY26 budget"},
  {t:"08:00",src:"Moneycontrol", h:"Zomato Q4 profit beats street — quick commerce margin expansion driver"},
  {t:"07:48",src:"Bloomberg",    h:"Dollar index slips to 103.2; traders reduce long positions pre-FOMC"},
  {t:"07:35",src:"Business Std.",h:"SEBI tightens F&O framework from May; weekly expiry curbs spark debate"},
];

// ══════════════════════════════════════════════════════════════════════════════
// HELPERS
// ══════════════════════════════════════════════════════════════════════════════
const heatCol = ch =>
  ch> 3.0?"#00e676":ch> 2.0?"#00c853":ch> 1.0?"#43a047":
  ch> 0.3?"#2e7d32":ch> 0.0?"#1b5e20":ch>-0.3?"#7f1d1d":
  ch>-1.0?"#b71c1c":ch>-2.0?"#e53935":"#ff1744";

const upTxt = ch => ch>=0?"#4caf50":"#f44336";
const upBg  = ch => ch>=0?"rgba(76,175,80,0.10)":"rgba(244,67,54,0.10)";
const upBdr = ch => ch>=0?"rgba(76,175,80,0.18)":"rgba(244,67,54,0.18)";
const fmtCh = ch => `${ch>=0?"▲":"▼"} ${Math.abs(ch).toFixed(2)}%`;
const fmtINR= v  => `₹${v.toLocaleString("en-IN")}`;
const sAvg  = stocks => stocks.reduce((a,s)=>a+s.ch,0)/stocks.length;

// ══════════════════════════════════════════════════════════════════════════════
// SMALL UI COMPONENTS
// ══════════════════════════════════════════════════════════════════════════════
const ChartTip = ({active,payload}) => {
  if(!active||!payload?.length) return null;
  return (
    <div style={{background:"#080d13",border:"1px solid #1a2535",borderRadius:6,padding:"5px 10px",fontSize:11}}>
      <div style={{color:"#3d5166",fontFamily:"JetBrains Mono"}}>{payload[0]?.payload?.t}</div>
      <div style={{color:"#e2e8f0",fontWeight:700,fontFamily:"JetBrains Mono"}}>{payload[0]?.value?.toLocaleString("en-IN")}</div>
    </div>
  );
};

const MiniChart = ({data,color,h=55}) => (
  <ResponsiveContainer width="100%" height={h}>
    <AreaChart data={data} margin={{top:2,right:0,left:0,bottom:0}}>
      <defs><linearGradient id={`m${color.replace(/\W/g,"")}`} x1="0" y1="0" x2="0" y2="1">
        <stop offset="5%"  stopColor={color} stopOpacity={0.3}/>
        <stop offset="95%" stopColor={color} stopOpacity={0}/>
      </linearGradient></defs>
      <Area type="monotone" dataKey="v" stroke={color} strokeWidth={1.5} fill={`url(#m${color.replace(/\W/g,"")})`} dot={false}/>
      <RechartTooltip content={<ChartTip/>}/>
    </AreaChart>
  </ResponsiveContainer>
);

const BigChart = ({data,color,h=120}) => (
  <ResponsiveContainer width="100%" height={h}>
    <AreaChart data={data} margin={{top:4,right:4,left:-24,bottom:0}}>
      <defs><linearGradient id={`b${color.replace(/\W/g,"")}`} x1="0" y1="0" x2="0" y2="1">
        <stop offset="5%"  stopColor={color} stopOpacity={0.35}/>
        <stop offset="95%" stopColor={color} stopOpacity={0}/>
      </linearGradient></defs>
      <CartesianGrid strokeDasharray="3 3" stroke="#0c1520" vertical={false}/>
      <XAxis dataKey="t" tick={{fill:"#3d5166",fontSize:9,fontFamily:"JetBrains Mono"}} tickLine={false} interval={4}/>
      <YAxis tick={{fill:"#3d5166",fontSize:9,fontFamily:"JetBrains Mono"}} tickLine={false} domain={["auto","auto"]}/>
      <RechartTooltip content={<ChartTip/>}/>
      <Area type="monotone" dataKey="v" stroke={color} strokeWidth={2} fill={`url(#b${color.replace(/\W/g,"")})`} dot={false}/>
    </AreaChart>
  </ResponsiveContainer>
);

const Chip = ({ch}) => (
  <span style={{
    background:upBg(ch),color:upTxt(ch),border:`1px solid ${upBdr(ch)}`,
    borderRadius:4,padding:"2px 7px",fontSize:11,
    fontFamily:"JetBrains Mono,monospace",fontWeight:700,whiteSpace:"nowrap"
  }}>{fmtCh(ch)}</span>
);

const SH = ({icon,title,accent,sub}) => (
  <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:14}}>
    <span style={{fontSize:16}}>{icon}</span>
    <div>
      <div style={{fontFamily:"Teko,sans-serif",fontSize:19,letterSpacing:2,color:accent,lineHeight:1}}>{title}</div>
      {sub&&<div style={{fontSize:9,color:"#3d5166",letterSpacing:1}}>{sub}</div>}
    </div>
    <div style={{flex:1,height:1,background:`linear-gradient(90deg,${accent}33,transparent)`}}/>
  </div>
);

const Card = ({children,cls="",style={}}) => (
  <div className={`fi ${cls}`} style={{background:"#0d1219",border:"1px solid #141e29",borderRadius:10,padding:"14px 16px",...style}}>
    {children}
  </div>
);

// ══════════════════════════════════════════════════════════════════════════════
// D3 TREEMAP HEATMAP
// ══════════════════════════════════════════════════════════════════════════════
function buildTreemap(sectors, W, H) {
  const root = d3.hierarchy({
    id:"root",
    children: sectors.map(s=>({...s, children:s.stocks, value:s.mcap}))
  }).sum(d=>d.mcap||0).sort((a,b)=>b.value-a.value);
  d3.treemap().size([W,H]).paddingOuter(3).paddingTop(22).paddingInner(2).round(true)(root);
  return root;
}

const HeatmapTooltip = ({node,x,y}) => {
  if(!node) return null;
  const isSec = !node.data.sym;
  const ch = isSec ? sAvg(node.children.map(c=>c.data)) : node.data.ch;
  return (
    <div className="tt-in" style={{
      position:"fixed",left:x+14,top:y-10,
      background:"#080d13",border:"1px solid #1a2535",
      borderRadius:8,padding:"10px 14px",zIndex:9999,
      pointerEvents:"none",minWidth:150,
      boxShadow:"0 8px 32px rgba(0,0,0,0.75)",
    }}>
      <div style={{fontFamily:"Teko",fontSize:17,color:"#e2e8f0",letterSpacing:1}}>
        {isSec ? node.data.label : node.data.sym}
      </div>
      {!isSec && <div style={{fontSize:10,color:"#3d5166",marginBottom:3}}>{node.parent?.data?.label}</div>}
      <div style={{
        fontFamily:"JetBrains Mono",fontSize:14,fontWeight:700,
        color:upTxt(ch),background:upBg(ch),
        display:"inline-block",padding:"2px 8px",borderRadius:4,marginTop:2,
      }}>
        {ch>=0?"+":""}{ch.toFixed(2)}%
      </div>
      {isSec && <div style={{fontSize:10,color:"#3d5166",marginTop:6}}>{node.children.length} companies</div>}
    </div>
  );
};

const TreemapSVG = ({sectors}) => {
  const containerRef = useRef(null);
  const [dims, setDims] = useState({w:1100,h:600});
  const [root, setRoot] = useState(null);
  const [tip, setTip] = useState({node:null,x:0,y:0});

  useEffect(()=>{
    const ro = new ResizeObserver(entries=>{
      const w = Math.floor(entries[0].contentRect.width);
      setDims({w, h: Math.floor(w*0.55)});
    });
    if(containerRef.current) ro.observe(containerRef.current);
    return ()=>ro.disconnect();
  },[]);

  useEffect(()=>{
    if(dims.w>0) setRoot(buildTreemap(sectors, dims.w, dims.h));
  },[sectors,dims]);

  const onMove = useCallback((e,node)=>setTip({node,x:e.clientX,y:e.clientY}),[]);
  const onLeave = useCallback(()=>setTip({node:null,x:0,y:0}),[]);

  if(!root) return <div ref={containerRef} style={{width:"100%",height:600}}/>;
  const sectorNodes = root.children||[];

  return (
    <div ref={containerRef} style={{width:"100%",position:"relative"}}>
      <svg width={dims.w} height={dims.h} style={{display:"block",borderRadius:6,overflow:"hidden"}}>
        {sectorNodes.map(sec=>{
          const sx=sec.x0,sy=sec.y0,sw=sec.x1-sec.x0,sh=sec.y1-sec.y0;
          const avg = sec.children ? sAvg(sec.children.map(c=>c.data)) : 0;
          return (
            <g key={sec.data.id}>
              <rect x={sx} y={sy} width={sw} height={sh}
                fill={`${heatCol(avg)}20`} stroke="#0a0f16" strokeWidth={1} rx={3}/>
              <rect x={sx} y={sy} width={sw} height={20}
                fill={`${heatCol(avg)}60`} rx={3}
                style={{cursor:"pointer"}}
                onMouseMove={e=>onMove(e,sec)} onMouseLeave={onLeave}/>
              <text x={sx+6} y={sy+14}
                fontSize={Math.min(11,sw/8)} fontFamily="Teko,sans-serif"
                fontWeight={600} letterSpacing={1.2} fill="rgba(255,255,255,0.9)"
                style={{pointerEvents:"none",userSelect:"none"}}>
                {sw>60 ? sec.data.label : sec.data.label.slice(0,Math.max(3,Math.floor(sw/8)))}
              </text>
              {sw>90&&(
                <text x={sx+sw-4} y={sy+14} fontSize={Math.min(10,sw/10)}
                  fontFamily="JetBrains Mono,monospace" fontWeight={700}
                  fill={upTxt(avg)} textAnchor="end"
                  style={{pointerEvents:"none",userSelect:"none"}}>
                  {avg>=0?"+":""}{avg.toFixed(2)}%
                </text>
              )}
              {(sec.children||[]).map(stock=>{
                const cw=stock.x1-stock.x0, ch2=stock.y1-stock.y0;
                const bg=heatCol(stock.data.ch);
                const showSym = cw>26&&ch2>18;
                const showPct = cw>36&&ch2>34;
                const fs = Math.min(11,Math.max(7,cw/5.5));
                return (
                  <g key={stock.data.sym} className="cell-h"
                    onMouseMove={e=>onMove(e,stock)} onMouseLeave={onLeave}>
                    <rect x={stock.x0} y={stock.y0} width={cw} height={ch2}
                      fill={bg} stroke="rgba(0,0,0,0.3)" strokeWidth={0.8} rx={2}/>
                    {showSym&&(
                      <text x={stock.x0+cw/2} y={stock.y0+(showPct?ch2/2-2:ch2/2+4)}
                        textAnchor="middle" dominantBaseline="middle"
                        fontSize={fs} fontFamily="Teko,sans-serif" fontWeight={600}
                        fill="rgba(255,255,255,0.92)"
                        style={{pointerEvents:"none",userSelect:"none"}}>
                        {stock.data.sym.length>Math.floor(cw/(fs*0.68))
                          ?stock.data.sym.slice(0,Math.max(3,Math.floor(cw/(fs*0.68))))
                          :stock.data.sym}
                      </text>
                    )}
                    {showPct&&(
                      <text x={stock.x0+cw/2} y={stock.y0+ch2/2+fs+1}
                        textAnchor="middle" dominantBaseline="middle"
                        fontSize={Math.max(6.5,fs*0.82)} fontFamily="JetBrains Mono,monospace"
                        fontWeight={700} fill="rgba(255,255,255,0.75)"
                        style={{pointerEvents:"none",userSelect:"none"}}>
                        {stock.data.ch>=0?"+":""}{stock.data.ch.toFixed(1)}%
                      </text>
                    )}
                  </g>
                );
              })}
            </g>
          );
        })}
      </svg>
      <HeatmapTooltip node={tip.node} x={tip.x} y={tip.y}/>
    </div>
  );
};

const HeatmapTab = ({sectors}) => {
  const [live, setLive] = useState(sectors);
  const [filter, setFilter] = useState("all");
  const [sortBy, setSortBy] = useState("mcap");
  const baseRef = useRef(sectors);

  useEffect(()=>{
    const id = setInterval(()=>{
      setLive(prev=>prev.map(sec=>({
        ...sec,
        stocks:sec.stocks.map(s=>({...s,ch:parseFloat((s.ch+(Math.random()-0.49)*0.08).toFixed(2))}))
      })));
    },2500);
    return ()=>clearInterval(id);
  },[]);

  const visible = live
    .filter(s=>{
      if(filter==="gainers") return sAvg(s.stocks)>0;
      if(filter==="losers")  return sAvg(s.stocks)<0;
      return true;
    })
    .sort((a,b)=>{
      if(sortBy==="gainers") return sAvg(b.stocks)-sAvg(a.stocks);
      if(sortBy==="losers")  return sAvg(a.stocks)-sAvg(b.stocks);
      return b.mcap-a.mcap;
    });

  const allStocks = live.flatMap(s=>s.stocks);
  const gainers = allStocks.filter(s=>s.ch>0).length;
  const losers  = allStocks.length-gainers;

  const btn = (active) => ({
    padding:"4px 12px",borderRadius:5,border:"none",cursor:"pointer",
    fontSize:11,fontFamily:"DM Sans",fontWeight:active?700:400,
    background:active?"rgba(255,153,51,0.18)":"transparent",
    color:active?"#ff9933":"#3d5166",transition:"all 0.15s",
  });

  return (
    <div style={{display:"grid",gap:16}}>
      {/* Stats + Controls */}
      <div style={{
        background:"#0a0f16",border:"1px solid #141e29",borderRadius:10,
        padding:"12px 16px",display:"flex",alignItems:"center",justifyContent:"space-between",flexWrap:"wrap",gap:12,
      }}>
        {/* Stats */}
        <div style={{display:"flex",gap:20,alignItems:"center"}}>
          {[
            {l:"ADVANCING",v:gainers,  c:"#4caf50"},
            {l:"DECLINING",v:losers,   c:"#f44336"},
            {l:"ADV/DEC",  v:(gainers/Math.max(1,losers)).toFixed(2), c:"#ff9933"},
            {l:"STOCKS",   v:allStocks.length, c:"#c9d8e8"},
          ].map(({l,v,c})=>(
            <div key={l} style={{textAlign:"center"}}>
              <div style={{fontFamily:"JetBrains Mono",fontSize:18,fontWeight:700,color:c,lineHeight:1}}>{v}</div>
              <div style={{fontSize:8,color:"#2d4257",letterSpacing:1,marginTop:2}}>{l}</div>
            </div>
          ))}
        </div>
        {/* Legend */}
        <div style={{display:"flex",alignItems:"center",gap:5}}>
          <span style={{fontSize:9,color:"#2d4257",fontFamily:"JetBrains Mono"}}>−3%</span>
          {["#ff1744","#e53935","#b71c1c","#7f1d1d","#1b5e20","#2e7d32","#43a047","#00c853","#00e676"].map((c,i)=>(
            <div key={i} style={{width:18,height:11,background:c,borderRadius:2}}/>
          ))}
          <span style={{fontSize:9,color:"#2d4257",fontFamily:"JetBrains Mono"}}>+3%</span>
          <span style={{fontSize:9,color:"#2d4257",marginLeft:8}}>size = mcap</span>
        </div>
        {/* Controls */}
        <div style={{display:"flex",gap:16,alignItems:"center",flexWrap:"wrap"}}>
          <div style={{display:"flex",gap:4}}>
            <span style={{fontSize:10,color:"#2d4257",letterSpacing:1,alignSelf:"center",marginRight:2}}>SHOW</span>
            {[["all","All"],["gainers","Gainers"],["losers","Losers"]].map(([k,l])=>(
              <button key={k} style={btn(filter===k)} onClick={()=>setFilter(k)}>{l}</button>
            ))}
          </div>
          <div style={{display:"flex",gap:4}}>
            <span style={{fontSize:10,color:"#2d4257",letterSpacing:1,alignSelf:"center",marginRight:2}}>SORT</span>
            {[["mcap","Mcap"],["gainers","Gainers"],["losers","Losers"]].map(([k,l])=>(
              <button key={k} style={btn(sortBy===k)} onClick={()=>setSortBy(k)}>{l}</button>
            ))}
          </div>
        </div>
      </div>

      {/* Treemap */}
      <Card style={{padding:"16px"}}>
        <SH icon="🔥" title="NIFTY 500 · SECTORAL HEATMAP" accent="#ff9933" sub="NSE · 15 SECTORS · HOVER FOR DETAILS · LIVE"/>
        <TreemapSVG sectors={visible}/>
      </Card>

      {/* Sector summary pills */}
      <div>
        <div style={{fontSize:9,color:"#2d4257",letterSpacing:2,marginBottom:8,fontFamily:"Teko"}}>SECTOR SUMMARY</div>
        <div style={{display:"flex",flexWrap:"wrap",gap:7}}>
          {live.map(sec=>{
            const avg=sAvg(sec.stocks);
            return (
              <div key={sec.id} style={{
                display:"flex",alignItems:"center",gap:7,
                background:`${heatCol(avg)}15`,border:`1px solid ${heatCol(avg)}38`,
                borderRadius:6,padding:"5px 11px",
              }}>
                <div style={{width:8,height:8,borderRadius:2,background:heatCol(avg),flexShrink:0}}/>
                <span style={{fontFamily:"Teko",fontSize:12,letterSpacing:1,color:"rgba(255,255,255,0.8)"}}>{sec.label}</span>
                <span style={{fontFamily:"JetBrains Mono",fontSize:11,fontWeight:700,color:upTxt(avg)}}>{avg>=0?"+":""}{avg.toFixed(2)}%</span>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
};

// ══════════════════════════════════════════════════════════════════════════════
// NEWS + WORLD PANELS
// ══════════════════════════════════════════════════════════════════════════════
const NewsPanel = () => (
  <Card style={{overflow:"hidden",position:"relative"}}>
    <SH icon="📰" title="FINANCIAL NEWS" accent="#ff9933" sub="India · Global · Live Feed"/>
    <div style={{height:430,overflow:"hidden",position:"relative"}}>
      <div className="news-feed">
        {[...NEWS,...NEWS].map((n,i)=>(
          <div key={i} style={{display:"flex",gap:10,alignItems:"flex-start",padding:"11px 0",borderBottom:"1px solid #0f1822"}}>
            <div style={{flexShrink:0,minWidth:54,textAlign:"right"}}>
              <div style={{fontFamily:"JetBrains Mono",fontSize:9,color:"#2d4257"}}>{n.t}</div>
              <div style={{fontSize:8,fontWeight:700,marginTop:3,padding:"1px 5px",background:"rgba(255,153,51,0.1)",color:"#ff9933",borderRadius:3,whiteSpace:"nowrap"}}>{n.src}</div>
            </div>
            <div style={{fontSize:12.5,lineHeight:1.52,color:"#7a96ab"}}>{n.h}</div>
          </div>
        ))}
      </div>
      <div className="fade-bot" style={{position:"absolute",bottom:0,left:0,right:0,height:72,pointerEvents:"none"}}/>
    </div>
  </Card>
);

const WorldPanel = () => (
  <Card>
    <SH icon="🌏" title="WORLD MARKETS" accent="#4caf50" sub="Live Global Indices"/>
    <div style={{display:"flex",flexDirection:"column",gap:5}}>
      {WORLD.map(m=>(
        <div key={m.n} style={{
          display:"flex",justifyContent:"space-between",alignItems:"center",
          padding:"8px 10px",borderRadius:6,
          background:upBg(m.ch),border:`1px solid ${upBdr(m.ch)}`,
        }}>
          <span style={{fontSize:13,fontWeight:500}}>{m.n}</span>
          <div style={{display:"flex",gap:14,alignItems:"center"}}>
            <span style={{fontFamily:"JetBrains Mono",fontSize:12,color:"#c9d8e8"}}>{m.v}</span>
            <span style={{fontFamily:"JetBrains Mono",fontSize:11,fontWeight:700,color:upTxt(m.ch),minWidth:64,textAlign:"right"}}>{fmtCh(m.ch)}</span>
          </div>
        </div>
      ))}
    </div>
  </Card>
);

// ══════════════════════════════════════════════════════════════════════════════
// MAIN DASHBOARD
// ══════════════════════════════════════════════════════════════════════════════
export default function BazaarWatch() {
  const [D, setD] = useState(INIT);
  const [tab, setTab] = useState("overview");
  const base = useRef(INIT);

  useEffect(()=>{
    const id = setInterval(()=>{
      setD(prev=>{
        const u=key=>{
          const vol=key==="bond10y"?0.0003:["dxy","usdinr"].includes(key)?0.0002:0.0006;
          const np=parseFloat((prev[key].p*(1+(Math.random()-0.49)*vol)).toFixed(key==="bond10y"?3:2));
          const ch=parseFloat(((np-base.current[key].p)/base.current[key].p*100).toFixed(2));
          return {p:np,ch,data:[...prev[key].data.slice(1),{t:prev[key].data.at(-1).t,v:np}]};
        };
        return Object.fromEntries(Object.keys(prev).map(k=>[k,u(k)]));
      });
    },2200);
    return ()=>clearInterval(id);
  },[]);

  const ticker = [
    ["GIFT NIFTY",D.giftNifty, v=>v.toLocaleString("en-IN")],
    ["NIFTY 50",  D.nifty50,   v=>v.toLocaleString("en-IN")],
    ["SENSEX",    D.sensex,    v=>v.toLocaleString("en-IN")],
    ["BANK NIFTY",D.bankNifty, v=>v.toLocaleString("en-IN")],
    ["GOLD MCX",  D.goldMCX,   v=>fmtINR(v)],
    ["SILVER MCX",D.silverMCX, v=>fmtINR(v)],
    ["CRUDE MCX", D.crudeMCX,  v=>fmtINR(v)],
    ["USD/INR",   D.usdinr,    v=>`₹${v.toFixed(2)}`],
    ["DOW JONES", D.dji,       v=>v.toLocaleString()],
    ["S&P 500",   D.sp500,     v=>v.toLocaleString()],
    ["DXY",       D.dxy,       v=>v.toFixed(2)],
    ["US 10Y",    D.bond10y,   v=>`${v.toFixed(3)}%`],
  ];

  const TABS = [["overview","Overview"],["heatmap","🔥 Heatmap"],["commodities","Commodities"],["global","Global"]];

  return (
    <div style={{minHeight:"100vh",background:"#060a0e",color:"#c9d8e8",fontFamily:"DM Sans,sans-serif"}}>
      <GlobalStyles/>

      {/* ── TOP BAR ── */}
      <div style={{
        background:"#060a0e",borderBottom:"1px solid #0f1822",
        padding:"0 20px",height:54,
        display:"flex",alignItems:"center",justifyContent:"space-between",
        position:"sticky",top:0,zIndex:300,
      }}>
        <div style={{display:"flex",alignItems:"center",gap:12}}>
          <div style={{display:"flex",flexDirection:"column",gap:2.5}}>
            {["#ff9933","#fff","#138808"].map((c,i)=>(
              <div key={i} style={{width:4,height:6,background:c,borderRadius:2}}/>
            ))}
          </div>
          <div>
            <div style={{fontFamily:"Teko",fontSize:26,letterSpacing:3,color:"#e2e8f0",lineHeight:1}}>BAZAAR WATCH</div>
            <div style={{fontSize:9,color:"#2d4257",letterSpacing:2}}>INDIA · NSE · BSE · MCX · GLOBAL MARKETS</div>
          </div>
        </div>

        <div style={{display:"flex",gap:3}}>
          {TABS.map(([k,l])=>(
            <button key={k} className="tab-btn" onClick={()=>setTab(k)} style={{
              padding:"5px 16px",borderRadius:6,border:"none",cursor:"pointer",
              fontSize:12,fontFamily:"DM Sans",
              background:tab===k?"rgba(255,153,51,0.18)":"transparent",
              color:tab===k?"#ff9933":"#3d5166",
              fontWeight:tab===k?700:400,
              transition:"all 0.18s",
            }}>{l}</button>
          ))}
        </div>

        <div style={{display:"flex",alignItems:"center",gap:14}}>
          <div style={{display:"flex",alignItems:"center",gap:5}}>
            <div className="live-pulse" style={{width:7,height:7,borderRadius:"50%",background:"#4caf50"}}/>
            <span style={{fontSize:11,color:"#4caf50",fontFamily:"JetBrains Mono",letterSpacing:1}}>LIVE</span>
          </div>
          <span style={{fontSize:11,color:"#2d4257",fontFamily:"JetBrains Mono"}}>
            {new Date().toLocaleTimeString("en-IN",{hour12:true})} IST
          </span>
        </div>
      </div>

      {/* ── TICKER BELT ── */}
      <div style={{background:"#080d13",borderBottom:"1px solid #0f1822",padding:"7px 0",overflow:"hidden"}}>
        <div style={{overflow:"hidden"}}>
          <div className="ticker-track" style={{gap:44}}>
            {[...ticker,...ticker].map(([label,item,fmt],i)=>(
              <div key={i} style={{display:"flex",alignItems:"center",gap:7,flexShrink:0}}>
                <span style={{fontSize:9,color:"#2d4257",fontFamily:"JetBrains Mono",letterSpacing:1}}>{label}</span>
                <span style={{fontSize:12,fontFamily:"JetBrains Mono",fontWeight:700,color:"#c9d8e8"}}>{fmt(item.p)}</span>
                <span style={{fontSize:10,fontFamily:"JetBrains Mono",color:upTxt(item.ch)}}>{fmtCh(item.ch)}</span>
                <span style={{color:"#0f1822"}}>│</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* ── PAGE CONTENT ── */}
      <div style={{padding:"18px 20px 40px",display:"grid",gap:18}}>

        {/* ════ OVERVIEW ════ */}
        {tab==="overview"&&(<>
          {/* Indian Indices */}
          <div>
            <div style={{fontFamily:"Teko",fontSize:11,color:"#2d4257",letterSpacing:2,marginBottom:10}}>🇮🇳 INDIAN INDICES</div>
            <div style={{display:"grid",gridTemplateColumns:"repeat(5,1fr)",gap:12}}>
              {[
                {key:"giftNifty",label:"GIFT NIFTY",  sub:"SGX · USD",  color:"#ff9933",fmt:v=>v.toLocaleString("en-IN")},
                {key:"nifty50",  label:"NIFTY 50",    sub:"NSE · INR",  color:"#f0c040",fmt:v=>v.toLocaleString("en-IN")},
                {key:"sensex",   label:"SENSEX",      sub:"BSE · INR",  color:"#4fc3f7",fmt:v=>v.toLocaleString("en-IN")},
                {key:"bankNifty",label:"BANK NIFTY",  sub:"NSE · INR",  color:"#81c784",fmt:v=>v.toLocaleString("en-IN")},
                {key:"niftyIT",  label:"NIFTY IT",    sub:"NSE · INR",  color:"#ce93d8",fmt:v=>v.toLocaleString("en-IN")},
              ].map(({key,label,sub,color,fmt})=>(
                <Card key={key} cls="glow-india">
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:8}}>
                    <div>
                      <div style={{fontFamily:"Teko",fontSize:15,letterSpacing:1.5,color}}>{label}</div>
                      <div style={{fontSize:9,color:"#2d4257"}}>{sub}</div>
                    </div>
                    <Chip ch={D[key].ch}/>
                  </div>
                  <div style={{fontFamily:"JetBrains Mono",fontSize:22,fontWeight:700,color,marginBottom:8}}>{fmt(D[key].p)}</div>
                  <BigChart data={D[key].data} color={color} h={90}/>
                </Card>
              ))}
            </div>
          </div>

          {/* Key Metrics */}
          <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:12}}>
            {[
              {key:"usdinr",  label:"USD / INR",    sub:"RBI Ref. Rate", color:"#4fc3f7",fmt:v=>`₹${v.toFixed(2)}`},
              {key:"crudeMCX",label:"CRUDE OIL",    sub:"MCX · ₹/bbl",  color:"#ffb74d",fmt:fmtINR},
              {key:"dxy",     label:"DOLLAR INDEX", sub:"DXY · ICE",     color:"#80cbc4",fmt:v=>v.toFixed(2)},
              {key:"bond10y", label:"US 10Y YIELD", sub:"T-Note · %",    color:"#ef9a9a",fmt:v=>`${v.toFixed(3)}%`},
            ].map(({key,label,sub,color,fmt})=>(
              <Card key={key}>
                <div style={{display:"flex",justifyContent:"space-between",marginBottom:6}}>
                  <div>
                    <div style={{fontFamily:"Teko",fontSize:15,letterSpacing:1.5,color}}>{label}</div>
                    <div style={{fontSize:9,color:"#2d4257"}}>{sub}</div>
                  </div>
                  <Chip ch={D[key].ch}/>
                </div>
                <div style={{fontFamily:"JetBrains Mono",fontSize:20,fontWeight:700,color,marginBottom:6}}>{fmt(D[key].p)}</div>
                <MiniChart data={D[key].data} color={color} h={52}/>
              </Card>
            ))}
          </div>

          {/* News (left) + World (right) */}
          <div style={{display:"grid",gridTemplateColumns:"1.5fr 1fr",gap:16}}>
            <NewsPanel/><WorldPanel/>
          </div>

          {/* US Markets */}
          <div>
            <div style={{fontFamily:"Teko",fontSize:11,color:"#2d4257",letterSpacing:2,marginBottom:10}}>🇺🇸 US MARKETS</div>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:12}}>
              {[
                {key:"dji",   label:"DOW JONES",   sub:"DJIA · NYSE", color:"#4184e4",fmt:v=>v.toLocaleString()},
                {key:"sp500", label:"S&P 500",     sub:"SPX · CBOE",  color:"#a78bfa",fmt:v=>v.toLocaleString()},
                {key:"dowFut",label:"DOW FUTURES", sub:"YM · CBOT",   color:"#67e8f9",fmt:v=>v.toLocaleString()},
              ].map(({key,label,sub,color,fmt})=>(
                <Card key={key} cls="glow-us">
                  <div style={{display:"flex",justifyContent:"space-between",marginBottom:8}}>
                    <div>
                      <div style={{fontFamily:"Teko",fontSize:17,letterSpacing:1.5,color}}>{label}</div>
                      <div style={{fontSize:9,color:"#2d4257"}}>{sub}</div>
                    </div>
                    <Chip ch={D[key].ch}/>
                  </div>
                  <div style={{fontFamily:"JetBrains Mono",fontSize:26,fontWeight:700,color,marginBottom:10}}>{fmt(D[key].p)}</div>
                  <BigChart data={D[key].data} color={color} h={110}/>
                </Card>
              ))}
            </div>
          </div>
        </>)}

        {/* ════ HEATMAP TAB ════ */}
        {tab==="heatmap"&&<HeatmapTab sectors={RAW_SECTORS}/>}

        {/* ════ COMMODITIES ════ */}
        {tab==="commodities"&&(<>
          <div>
            <div style={{fontFamily:"Teko",fontSize:11,color:"#2d4257",letterSpacing:2,marginBottom:10}}>🪙 PRECIOUS METALS — MCX</div>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
              {[
                {key:"goldMCX",  label:"GOLD",   sub:"MCX · ₹ per 10 grams", color:"#d4af37", fmt:fmtINR,
                 sub2:v=>`₹${(v/10).toFixed(0)} per gram`},
                {key:"silverMCX",label:"SILVER", sub:"MCX · ₹ per kg",       color:"#adb5bd", fmt:fmtINR,
                 sub2:v=>`₹${(v/1000).toFixed(2)} per gram`},
              ].map(({key,label,sub,color,fmt,sub2})=>(
                <Card key={key} cls="glow-gold">
                  <SH icon={key==="goldMCX"?"🥇":"🥈"} title={label} accent={color} sub={sub}/>
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:12}}>
                    <div>
                      <div style={{fontFamily:"JetBrains Mono",fontSize:34,fontWeight:700,color}}>{fmt(D[key].p)}</div>
                      <div style={{fontSize:11,color:"#2d4257",marginTop:2}}>{sub2(D[key].p)} · MCX Active</div>
                    </div>
                    <Chip ch={D[key].ch}/>
                  </div>
                  <BigChart data={D[key].data} color={color} h={140}/>
                </Card>
              ))}
            </div>
          </div>
          <Card>
            <SH icon="⚙️" title="BASE METALS" accent="#ffb74d" sub="MCX · ₹ per kg"/>
            <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:16}}>
              {[
                {key:"copper",   label:"COPPER",    sub:"MCX · ₹/kg", color:"#e07b4a",fmt:v=>`₹${v.toFixed(2)}`},
                {key:"aluminium",label:"ALUMINIUM", sub:"MCX · ₹/kg", color:"#90caf9",fmt:v=>`₹${v.toFixed(2)}`},
                {key:"zinc",     label:"ZINC",      sub:"MCX · ₹/kg", color:"#4fc3f7",fmt:v=>`₹${v.toFixed(2)}`},
                {key:"nickel",   label:"NICKEL",    sub:"MCX · ₹/kg", color:"#b39ddb",fmt:v=>`₹${v.toFixed(2)}`},
              ].map(({key,label,sub,color,fmt})=>(
                <div key={key}>
                  <div style={{display:"flex",justifyContent:"space-between",marginBottom:6}}>
                    <div>
                      <div style={{fontFamily:"Teko",fontSize:16,letterSpacing:1.5,color}}>{label}</div>
                      <div style={{fontSize:9,color:"#2d4257"}}>{sub}</div>
                    </div>
                    <Chip ch={D[key].ch}/>
                  </div>
                  <div style={{fontFamily:"JetBrains Mono",fontSize:20,fontWeight:700,color,marginBottom:8}}>{fmt(D[key].p)}</div>
                  <BigChart data={D[key].data} color={color} h={110}/>
                </div>
              ))}
            </div>
          </Card>
        </>)}

        {/* ════ GLOBAL ════ */}
        {tab==="global"&&(
          <div style={{display:"grid",gridTemplateColumns:"1.5fr 1fr",gap:16}}>
            <NewsPanel/><WorldPanel/>
          </div>
        )}

      </div>

      <div style={{textAlign:"center",padding:"0 24px 20px",fontSize:9,color:"#0f1822",letterSpacing:1}}>
        PROTOTYPE · DATA SIMULATED · LIVE REQUIRES NSE / BSE / MCX / REUTERS DATAFEED
      </div>
    </div>
  );
}
