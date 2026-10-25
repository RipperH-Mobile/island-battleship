'use strict';
// Island Battleship: static file server + tiny WebSocket room relay. No dependencies.
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const PORT=+process.env.PORT||3000,GUID='258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const ORIGINS=(process.env.ALLOWED_ORIGINS||'').split(',').map(s=>s.trim()).filter(Boolean);
const TOPICS=new Set(['start','st','plan','board','mem']),MAXMSG=8192,MAXROOM=8;
const rooms=new Map();

const srv=http.createServer((q,r)=>{
  if(q.url==='/health'){r.writeHead(200,{'content-type':'text/plain'});return r.end('ok')}
  if(q.url==='/'||q.url.startsWith('/?')||q.url.startsWith('/index.html')){
    try{const b=fs.readFileSync(path.join(__dirname,'index.html'));
      r.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-cache','x-content-type-options':'nosniff'});return r.end(b)}
    catch(e){r.writeHead(500);return r.end('index.html missing')}}
  r.writeHead(404);r.end('not found')});

function frame(op,buf){const n=buf.length;let h;
  if(n<126)h=Buffer.from([0x80|op,n]);
  else if(n<65536){h=Buffer.alloc(4);h[0]=0x80|op;h[1]=126;h.writeUInt16BE(n,2)}
  else{h=Buffer.alloc(10);h[0]=0x80|op;h[1]=127;h.writeBigUInt64BE(BigInt(n),2)}
  return Buffer.concat([h,buf])}

function peersMsg(room){return JSON.stringify({k:'peers',peers:[...room.values()].map(c=>({id:c.id,p:c.p}))})}
function broadcast(room,txt){for(const c of room.values())c.send(txt)}

function onMsg(c,txt){
  const now=Date.now();c.tok=Math.min(80,c.tok+(now-c.last)*0.04);c.last=now;
  if(c.tok<1)return;c.tok--;
  let m;try{m=JSON.parse(txt)}catch(e){return}
  if(!m||typeof m!=='object')return;
  if(m.k==='p'&&m.p&&typeof m.p==='object'&&JSON.stringify(m.p).length<=512){c.p=m.p;broadcast(c.room,peersMsg(c.room))}
  else if(m.k==='e'&&TOPICS.has(m.t))broadcast(c.room,JSON.stringify({k:'e',t:m.t,d:m.d,from:c.id}))}

function onData(c,d){
  c.buf=Buffer.concat([c.buf,d]);
  for(;;){
    const b=c.buf;if(b.length<2)return;
    const fin=b[0]&0x80,op=b[0]&15,masked=b[1]&0x80;let len=b[1]&0x7f,off=2;
    if(!masked)return c.kill();
    if(len===126){if(b.length<4)return;len=b.readUInt16BE(2);off=4}
    else if(len===127){if(b.length<10)return;const L=b.readBigUInt64BE(2);if(L>BigInt(MAXMSG))return c.kill();len=Number(L);off=10}
    if(len>MAXMSG)return c.kill();
    if(b.length<off+4+len)return;
    const mask=b.subarray(off,off+4),pl=Buffer.from(b.subarray(off+4,off+4+len));
    for(let i=0;i<len;i++)pl[i]^=mask[i&3];
    c.buf=b.subarray(off+4+len);
    if(op===8)return c.kill();
    if(op===9){c.raw(frame(10,pl));continue}
    if(op===10){c.alive=true;continue}
    if(op===0||op===1){
      if(op===1){c.frag=[];c.fl=0}
      c.frag.push(pl);c.fl+=len;if(c.fl>MAXMSG)return c.kill();
      if(fin){const t=Buffer.concat(c.frag).toString('utf8');c.frag=[];c.fl=0;onMsg(c,t)}}}}

srv.on('upgrade',(q,sock)=>{
  const fail=()=>{try{sock.write('HTTP/1.1 400 Bad Request\r\n\r\n')}catch(e){}sock.destroy()};
  const u=new URL(q.url,'http://x'),key=q.headers['sec-websocket-key'];
  if(u.pathname!=='/ws'||!key)return fail();
  if(ORIGINS.length&&!ORIGINS.includes(q.headers.origin))return fail();
  const rn=u.searchParams.get('room')||'';
  if(!/^[a-z0-9][a-z0-9_.-]{0,47}$/.test(rn))return fail();
  const room=rooms.get(rn)||new Map();
  if(room.size>=MAXROOM)return fail();
  const acc=crypto.createHash('sha1').update(key+GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+acc+'\r\n\r\n');
  sock.setNoDelay(true);
  const c={id:crypto.randomBytes(4).toString('hex'),room,p:{},buf:Buffer.alloc(0),frag:[],fl:0,alive:true,tok:60,last:Date.now(),
    raw:b=>{if(!sock.destroyed)sock.write(b)},send:t=>c.raw(frame(1,Buffer.from(t))),kill:()=>sock.destroy()};
  room.set(c.id,c);rooms.set(rn,room);
  sock.on('data',d=>onData(c,d));
  sock.on('error',()=>{});
  sock.on('close',()=>{room.delete(c.id);if(!room.size)rooms.delete(rn);else broadcast(room,peersMsg(room))});
  c.send(JSON.stringify({k:'hello',id:c.id}));
  broadcast(room,peersMsg(room))});

setInterval(()=>{for(const room of rooms.values())for(const c of room.values()){
  if(!c.alive)c.kill();else{c.alive=false;c.raw(frame(9,Buffer.alloc(0)))}}},25000).unref();

if(require.main===module)srv.listen(PORT,()=>console.log('Island Battleship on http://localhost:'+PORT));
module.exports=srv;
