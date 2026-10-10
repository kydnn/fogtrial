// Fog Trial multiplayer server — bundled build. Run: node server.mjs (PORT env var, default 8080)
import { createServer } from 'node:http';
import { readFileSync, existsSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';
const __defs = {}, __cache = {};
function __req(id){ if (__cache[id]) return __cache[id]; const f = __defs[id]; if (!f) throw new Error('missing module ' + id); return (__cache[id] = f()); }
__defs["server/ws.js"] = function(){
// Minimal WebSocket server (RFC 6455) on top of node:http — text frames, ping/pong, close,
// fragmented messages. No dependencies, so the server runs with plain `node`.



const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

class Socket extends EventEmitter {
  constructor(sock, maxPayload) {
    super(); this.sock = sock; this.readyState = 1; this.buf = Buffer.alloc(0); this.frags = []; this.maxPayload = maxPayload;
    sock.setNoDelay(true);
    sock.on('data', (d) => { this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d; try { this.parse(); } catch (e) { this.terminate(); } });
    sock.on('close', () => this.closed());
    sock.on('error', () => this.closed());
  }
  closed() { if (this.readyState === 3) return; this.readyState = 3; this.emit('close'); }
  parse() {
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = b[0] & 0x80, op = b[0] & 0x0f, masked = b[1] & 0x80; let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > this.maxPayload) { this.close(1009); return; }
      const mOff = off; if (masked) off += 4;
      if (b.length < off + len) return;
      let data = b.subarray(off, off + len);
      if (masked) { const mask = b.subarray(mOff, mOff + 4); data = Buffer.from(data); for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3]; }
      this.buf = b.subarray(off + len);
      if (op === 0x8) { this.close(); return; }
      if (op === 0x9) { this.frame(0xA, data); continue; }
      if (op === 0xA) { this.emit('pong'); continue; }
      if (op === 0x1 || op === 0x2 || op === 0x0) {
        this.frags.push(data);
        if (fin) { const msg = Buffer.concat(this.frags); this.frags = []; this.emit('message', msg.toString('utf8')); }
      }
    }
  }
  frame(op, payload) {
    if (this.readyState !== 1) return;
    const n = payload.length; let head;
    if (n < 126) { head = Buffer.alloc(2); head[1] = n; }
    else if (n < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(n, 2); }
    else { head = Buffer.alloc(10); head[1] = 127; head.writeBigUInt64BE(BigInt(n), 2); }
    head[0] = 0x80 | op;
    this.sock.write(Buffer.concat([head, payload]));
  }
  send(text) { this.frame(0x1, Buffer.from(text, 'utf8')); }
  ping() { this.frame(0x9, Buffer.alloc(0)); }
  close(code = 1000) { if (this.readyState !== 1) return; const p = Buffer.alloc(2); p.writeUInt16BE(code, 0); this.frame(0x8, p); this.readyState = 2; this.sock.end(); setTimeout(() => this.sock.destroy(), 1000); }
  terminate() { this.sock.destroy(); this.closed(); }
}

class WebSocketServer extends EventEmitter {
  constructor({ server, maxPayload = 1 << 20 }) {
    super(); this.clients = new Set();
    server.on('upgrade', (req, sock) => {
      const key = req.headers['sec-websocket-key'];
      if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.end('HTTP/1.1 400 Bad Request\r\n\r\n'); return; }
      const accept = createHash('sha1').update(key + GUID).digest('base64');
      sock.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
      const ws = new Socket(sock, maxPayload); this.clients.add(ws);
      ws.on('close', () => this.clients.delete(ws));
      this.emit('connection', ws, req);
    });
  }
}

return { WebSocketServer };
};
__defs["js/killers/powerData.js"] = function(){
// Killer power profiles: a data description of every power — its abilities, cooldowns and
// charges, how survivors interact with it and which statuses it applies. The UI shows these
// in the loadout; the survivor AI's matching counter profile lives in ai/killerProfiles.js
// (joined here as aiCounterProfile by powerProfile()). Numbers mirror the power modules.
const POWER_DATA = {
  trapper: {
    abilities: [{ key: 'Q/RMB', name: 'Set Bear Trap', desc: 'Set a trap in front of you; tall grass hides it.' }, { key: 'E', name: 'Pick up / reset', desc: 'Re-arm or collect a trap.' }],
    cooldowns: { carried: '2 traps (3 with Trapper Bag)', setTime: '2.5 s' },
    survivorInteractions: ['Step on a trap → trapped until escape attempts succeed', 'Disarm a seen trap', 'Teammates free trapped survivors'],
    statuses: [],
  },
  hillbilly: {
    abilities: [{ key: 'Q/RMB', name: 'Chainsaw', desc: 'Rev, then sprint; a hit downs instantly.' }],
    cooldowns: { rev: '~1.6 s', overheat: 'heat builds while revving; overheat locks the saw' },
    survivorInteractions: ['Break line of sight while he revs', 'Tight loops make him collide'],
    statuses: [],
  },
  wraith: {
    abilities: [{ key: 'Q/RMB', name: 'Wailing Bell', desc: 'Cloak (fast, undetectable, cannot attack); uncloak rings the bell.' }],
    cooldowns: { cloak: '~1.5 s', uncloak: '~1.5 s' },
    survivorInteractions: ['Hear the bell', 'Spot the shimmer up close'],
    statuses: ['undetectable'],
  },
  ghostface: {
    abilities: [{ key: 'RMB', name: 'Night Shroud', desc: 'Undetectable and hard to spot.' }, { key: 'Hold RMB', name: 'Stalk', desc: 'Stalk survivors in view to Mark them.' }, { key: 'E', name: 'Crouch', desc: 'Lower profile.' }],
    cooldowns: { reveal: '20 s after being revealed', marked: '35 s' },
    survivorInteractions: ['Stare at him for ~1.3 s within 14 m to reveal him', 'Break line of sight to deny stalk'],
    statuses: ['marked', 'exposed', 'undetectable'],
  },
  clown: {
    abilities: [{ key: 'Hold RMB', name: 'Afterpiece Tonic', desc: 'Lob a bottle; the gas slows and intoxicates.' }, { key: 'E', name: 'Antidote', desc: 'Switch to speed gas.' }],
    cooldowns: { bottles: '6', reload: '3.2 s', cloud: '8 s' },
    survivorInteractions: ['Leave the gas', 'Route around clouds on loops'],
    statuses: ['intoxicated', 'haste'],
  },
  executioner: {
    abilities: [{ key: 'RMB', name: 'Rites of Judgement', desc: 'Punishment mode: Torment Trails.' }, { key: 'LMB', name: 'Punishment of the Damned', desc: 'Ground wave through walls.' }, { key: 'SPACE', name: 'Cage of Atonement', desc: 'Cage a Tormented downed survivor.' }],
    cooldowns: { windup: '1.1 s', wave: '2.2 s', trail: '60 s' },
    survivorInteractions: ['Side-step the wave', 'Avoid trails', 'Rescue caged survivors'],
    statuses: ['tormented'],
  },
  singularity: {
    abilities: [
      { key: 'Hold & release RMB', name: 'Biopod', desc: 'Shoot a biopod (50 m/s, 22 m). It sticks to the first wall or obstacle; a survivor hit is Slipstreamed, or teleported to if already Slipstreamed.' },
      { key: 'E', name: 'Biopod control', desc: 'View through the pod nearest your aim (F / wheel: next pod). Hold the reticle on a survivor in 20 m for 0.8 s to Slipstream them.' },
      { key: 'LMB (in a pod)', name: 'Quantum Instantiation', desc: 'Locked on a Slipstreamed survivor: teleport to the pod, then Overclock for 5.7 s.' },
    ],
    cooldowns: { pods: '8 active (the 9th recycles the oldest)', reload: '0.5 s', lock: '0.8 s · 3 s between locks · 10× slower near hooks', overclock: '5.7 s: +3% speed, +75% kick/break/vault, pallet stun → pallet breaks + 3 s Overheat' },
    survivorInteractions: ['Print EMPs at the 4 printers (they fill to 97% by themselves)', 'An EMP (2.5 s to arm) clears Slipstream and disables pods in 10 m for 45 s', 'Slipstream spreads in 6 m after 2 s — spread out', 'Break pod sightlines; pallets still work after Overclock ends'],
    statuses: ['slipstream'],
  },
  huntress: {
    abilities: [{ key: 'Hold RMB', name: 'Hunting Hatchets', desc: 'Wind up and throw.' }, { key: 'E', name: 'Reload', desc: 'Restock at a locker.' }],
    cooldowns: { hatchets: '5', windup: '0.35–1.3 s', reload: '2.4 s' },
    survivorInteractions: ['Dodge thrown hatchets', 'Break line of sight', 'Use cover on long runs'],
    statuses: [],
  },
  legion: {
    abilities: [{ key: 'RMB', name: 'Feral Frenzy', desc: '5.2 m/s, fast vaults, survivors in the terror radius are revealed.' }, { key: 'LMB', name: 'Feral Slash', desc: 'Inflicts Deep Wound and refills the gauge; a miss ends Frenzy.' }],
    cooldowns: { frenzy: '~10 s', recharge: '~20 s', fatigue: '4 s' },
    survivorInteractions: ['Mend Deep Wound before it bleeds out (20 s)', 'Spread out to deny chains', 'Frenzy cannot down — only a Deep Wound bleed-out or a basic hit can'],
    statuses: ['deepWound', 'deepWoundFrenzy'],
  },
  hag: {
    abilities: [{ key: 'Hold RMB', name: 'Phantasm Trap', desc: 'Draw a trap on the ground (max 10).' }, { key: 'E', name: 'Teleport', desc: 'Teleport to a triggered trap within 40 m.' }],
    cooldowns: { draw: '1.5 s', teleportWindow: '4 s', maxTraps: '10' },
    survivorInteractions: ['Crouch through traps to avoid triggering them', 'Wipe known traps', 'Leave the area after triggering one'],
    statuses: [],
  },
  cannibal: {
    abilities: [
      { key: 'Hold RMB', name: 'Rev', desc: '2 s rev (slowed). Release a full rev to sweep; release early to cancel for free. Holding the rev 3 s = tantrum.' },
      { key: '(auto)', name: 'Chainsaw Sweep', desc: 'Costs 1 charge. Accelerates to 5.35 m/s over 2.5 s, downs everyone in the wide arc (multi-downs). Mouse steers slowly; press RMB again to stop.' },
      { key: 'LMB while sweeping', name: 'Dash', desc: 'Spend another charge: top speed instantly and more sweep time.' },
    ],
    cooldowns: { charges: '3, refilling 0.25/s while idle', hit: '2 s', miss: '1.5 s + 1 s per charge spent', pallet: '1 s (the sweep saws through dropped pallets)', tantrum: '3 s + 1 s per remaining charge, Hindered 90%, all charges lost, then 3 s' },
    survivorInteractions: ['Keep more than 6 m away when you hear the rev', 'Turn tight around walls so the sweep rams them (tantrum)', 'Windows: he cannot vault while sawing', 'Spread out — one sweep downs everyone it touches', 'Dropping a pallet ends the sweep'],
    statuses: ['hindered'],
  },
  deathslinger: {
    abilities: [{ key: 'Hold RMB', name: 'Aim', desc: 'Slower while aiming.' }, { key: 'LMB', name: 'Fire harpoon', desc: 'Projectile; a hit spears and reels the survivor in.' }],
    cooldowns: { range: '18 m', reload: '2 s', reel: 'up to 6 s' },
    survivorInteractions: ['Juke the aim', 'Break the chain on obstacles (Deep Wound)', 'Avoid open sightlines'],
    statuses: ['grabbed', 'deepWound'],
  },
  dredge: {
    abilities: [{ key: 'RMB', name: 'Gloaming', desc: 'Leave a Remnant and pick a locker.' }, { key: 'RMB again', name: 'Teleport', desc: 'Travel to the locker.' }, { key: 'LMB', name: 'Return', desc: 'Snap back to the Remnant.' }],
    cooldowns: { tokens: '3', teleport: '10 s (4 s in Nightfall)', nightfall: '60 s of darkness' },
    survivorInteractions: ['Lock lockers near objectives', 'Avoid lockers during Nightfall', 'Track the Remnant'],
    statuses: ['nightfall', 'undetectable'],
  },
  knight: {
    abilities: [{ key: 'RMB', name: 'Patrol Path', desc: 'Draw a path up to 30 m.' }, { key: 'RMB again', name: 'Summon Guard', desc: 'The guard patrols and hunts.' }, { key: 'E', name: 'Cycle Guard', desc: 'Carnifex · Assassin · Jailer.' }],
    cooldowns: { recharge: '8 s', carnifex: 'breaks pallets in 1.8 s', assassin: 'hunts at 4.4 m/s, Deep Wound', jailer: '24 s patrol, 16 m detection' },
    survivorInteractions: ['Grab the Standard to end a hunt', 'Outlast the hunt timer', 'Lead guards through pallets'],
    statuses: ['deepWound'],
  },
  cenobite: {
    abilities: [{ key: 'Hold RMB', name: 'Possessed Chain', desc: 'Steerable chain that binds survivors.' }, { key: 'E', name: 'Gateway / Summon', desc: 'Teleport to a survivor solving the box.' }],
    cooldowns: { chain: '5 s', chainHunt: 'after 90 s without a solve' },
    survivorInteractions: ['Break chains (or tear them on obstacles)', 'Pick up and solve the Lament Configuration', 'Solve where it is safe'],
    statuses: ['chained', 'oblivious'],
  },
  mastermind: {
    abilities: [{ key: 'Hold RMB', name: 'Virulent Bound', desc: 'Charge and leap at 14 m/s.' }, { key: 'RMB within 2 s', name: 'Second Bound', desc: 'Longer chained leap.' }],
    cooldowns: { tokens: '2', recharge: '5 s per token' },
    survivorInteractions: ['Stay out of dash lanes', 'Use First Aid Spray from supply cases to cure infection', 'Grabbed survivors are slammed into walls'],
    statuses: ['infected', 'grabbed', 'hindered'],
  },
  houndmaster: {
    abilities: [{ key: 'Hold RMB', name: 'Chase Command', desc: 'Plot a line and send the dog; it grabs survivors and drags them to you.' }, { key: 'E', name: 'Search Command', desc: 'Send the dog to search an area.' }],
    cooldowns: { dog: '3 s', drag: 'up to 8 s' },
    survivorInteractions: ['Drop a pallet on the dog (stun)', 'Break line of sight from the dog', 'Track the dog separately'],
    statuses: ['grabbed', 'hindered', 'incapacitated'],
  },
  lich: {
    abilities: [{ key: 'Wheel/1-4', name: 'Select spell', desc: 'Choose one of four spells.' }, { key: 'RMB', name: 'Flight of the Damned', desc: 'Five flying entities; crouch to avoid.' }, { key: 'RMB', name: 'Fly', desc: 'Fly over pallets and windows; no attack for 2.75 s after.' }, { key: 'RMB', name: 'Mage Hand', desc: 'Lift a dropped pallet or block a standing one.' }, { key: 'RMB', name: 'Dispelling Sphere', desc: 'Slow orb that reveals and dispels Magic Items.' }],
    cooldowns: { flightOfTheDamned: '30 s', fly: '20 s', mageHand: '35 s', dispellingSphere: '30 s' },
    survivorInteractions: ['Crouch under Flight of the Damned', 'Loot Magic Items that counter each spell', 'Watch which spell is held'],
    statuses: ['dispelled', 'cannotAttack'],
  },
  skull_merchant: {
    abilities: [{ key: 'Hold RMB', name: 'Deploy drone', desc: 'Place a drone ≤10 m ahead (7 s cooldown, 6 max). Its scan line sweeps a 10 m zone at 105°/s.' }, { key: 'E', name: 'Recall', desc: 'Recall the drone nearest your aim (1 s): Undetectable 8 s.' }],
    cooldowns: { deploy: '7 s', lockOn: '3 stacks → injure / Deep Wound + Broken + Claw Trap (45 s)', hack: 'disables a drone 45 s' },
    survivorInteractions: ['Crouch or stand still as the scan line passes', 'Hack drones (3 s) — removes your claw trap', 'A botched hack adds a lock-on'],
    statuses: ['lockOn', 'clawTrap', 'broken', 'hindered'],
  },
  blight: {
    abilities: [{ key: 'RMB', name: 'Blighted Rush', desc: '9.2 m/s for up to 3 s (1 token). Slow turning.' }, { key: 'RMB after a slam', name: 'Lethal Rush', desc: 'Chain within 1.25 s of a head-on slam (≥45°).' }, { key: 'LMB', name: 'Special attack', desc: 'Lunge out of a lethal rush.' }],
    cooldowns: { tokens: '5, one per 2 s', fatigue: '2.5 s when the chain ends', pallet: 'a lethal rush smashes dropped pallets (3 tokens)' },
    survivorInteractions: ['Side-step late — he turns slowly', 'Loops with tall walls force slams', 'Punish fatigue'],
    statuses: [],
  },
  nurse: {
    abilities: [{ key: 'Hold RMB', name: 'Blink', desc: 'Charge 2 s: 1.5–20 m straight ahead through walls.' }, { key: 'RMB in the chain window', name: 'Chain blink', desc: 'Second blink, up to 12 m (1.5 s window).' }, { key: 'LMB', name: 'Blink attack', desc: 'Attack out of a blink.' }],
    cooldowns: { tokens: '2, 3 s recharge each', fatigue: '2 s (1 blink) / 2.5 s (2 blinks), +1 s after an attack' },
    survivorInteractions: ['Change direction when her charge sound stops', 'Double back behind walls', 'Windows/pallets do little — she ignores them', 'Run during her fatigue'],
    statuses: [],
  },
  unknown: {
    abilities: [{ key: 'Hold RMB', name: 'UVX', desc: '1 s charge; bouncing shot, 2.25 m burst. Weakens, hurts the Weakened.' }, { key: 'E', name: 'Hallucination', desc: 'Leave a hallucination (max 4).' }, { key: 'F', name: 'Teleport', desc: 'Teleport to a hallucination (25 s cooldown).' }],
    cooldowns: { uvx: '1.5 s', teleport: '25 s' },
    survivorInteractions: ['Look at him 10 s (≤25 m) to clear Weakened', 'Dispel hallucinations by staring 4 s (≤12 m)', 'Walking into one: Weakened + revealed'],
    statuses: ['weakened'],
  },
  nemesis: {
    abilities: [{ key: 'Hold RMB', name: 'Tentacle Strike', desc: '0.35 s charge: 5 m line (6.5 m at MR 3). MR 2+ smashes dropped pallets.' }, { key: '—', name: 'Zombies', desc: 'Two zombies roam; they injure and contaminate on sight.' }],
    cooldowns: { tentacle: '1.2 s', mutation: 'MR 2 at 1 point, MR 3 at 3 points (+1 contamination, +0.5 zombie kill)', zombie: 'respawn 15 s' },
    survivorInteractions: ['Stay out of tentacle range', 'Take a vaccine (1 s) and inject (3 s, Q)', 'Avoid zombie sight cones; stun them with pallets'],
    statuses: ['contaminated'],
  },
  ghoul: {
    abilities: [{ key: 'RMB', name: 'Kagune Leap', desc: '14 m leap. Latch onto a survivor or cling to a wall, then leap again.' }, { key: 'RMB while latched', name: 'Grab attack', desc: 'Injure + Deep Wound + Kagune Mark (40 s).' }],
    cooldowns: { tokens: '2 (4 s) · Enraged: 3 (2.5 s)', latch: '2 s window', cling: '1.5 s window' },
    survivorInteractions: ['Mend to clear the Kagune Mark', 'Tight corners beat straight leaps', 'Break away fast after a latch'],
    statuses: ['latched', 'kaguneMark', 'deepWound'],
  },
};

function powerProfile(def, counterProfiles = {}) {
  const d = POWER_DATA[def.id] || { abilities: [], cooldowns: {}, survivorInteractions: [], statuses: [] };
  const cp = counterProfiles[def.id];
  return { id: def.id, power: { name: def.power, ...d, aiCounterProfile: cp ? { strengths: cp.strengths, weaknesses: cp.weaknesses, avoid: cp.avoid, exploit: cp.exploit, knobs: cp.knobs } : null } };
}

return { POWER_DATA, powerProfile };
};
__defs["js/core/util.js"] = function(){
// Shared math + helpers. 1 meter = M pixels in world space.
const M = 32;

const clamp = (v, a, b) => (v < a ? a : v > b ? b : v);
const lerp = (a, b, t) => a + (b - a) * t;
const invLerp = (a, b, v) => clamp((v - a) / (b - a), 0, 1);
const smoothstep = (a, b, v) => { const t = invLerp(a, b, v); return t * t * (3 - 2 * t); };
const dist = (ax, ay, bx, by) => Math.hypot(bx - ax, by - ay);
const dist2 = (ax, ay, bx, by) => { const dx = bx - ax, dy = by - ay; return dx * dx + dy * dy; };
const dE = (a, b) => Math.hypot(b.x - a.x, b.y - a.y);
const angleTo = (a, b) => Math.atan2(b.y - a.y, b.x - a.x);
const normAngle = (a) => { while (a > Math.PI) a -= Math.PI * 2; while (a < -Math.PI) a += Math.PI * 2; return a; };
const angleDiff = (a, b) => Math.abs(normAngle(a - b));
const len = (x, y) => Math.hypot(x, y);
const norm = (x, y) => { const l = Math.hypot(x, y) || 1; return { x: x / l, y: y / l }; };

// Seeded RNG so maps are reproducible from a seed.
function mulberry32(seed) {
  let a = seed >>> 0;
  return function () {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const rr = (rng, a, b) => a + (b - a) * rng();
const ri = (rng, a, b) => Math.floor(rr(rng, a, b + 1));
const pick = (rng, arr) => arr[Math.floor(rng() * arr.length)];
function shuffle(rng, arr) {
  for (let i = arr.length - 1; i > 0; i--) { const j = Math.floor(rng() * (i + 1)); [arr[i], arr[j]] = [arr[j], arr[i]]; }
  return arr;
}
// Gaussian-ish noise
const gauss = (rng = Math.random) => (rng() + rng() + rng() - 1.5) / 1.5;

// Response curves for utility AI considerations.
const curves = {
  linear: (x) => clamp(x, 0, 1),
  inverse: (x) => 1 - clamp(x, 0, 1),
  quad: (x) => { x = clamp(x, 0, 1); return x * x; },
  invQuad: (x) => { x = 1 - clamp(x, 0, 1); return x * x; },
  sqrt: (x) => Math.sqrt(clamp(x, 0, 1)),
  logistic: (x, k = 10, mid = 0.5) => 1 / (1 + Math.exp(-k * (x - mid))),
};

let _id = 1;
const uid = (p = 'e') => `${p}${_id++}`;

function fmtTime(s) {
  s = Math.max(0, Math.floor(s));
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;
}

// Point-in-rect and rect helpers. Rect = {x,y,w,h} with x,y top-left.
const inRect = (px, py, r, pad = 0) => px >= r.x - pad && px <= r.x + r.w + pad && py >= r.y - pad && py <= r.y + r.h + pad;
const rectCenter = (r) => ({ x: r.x + r.w / 2, y: r.y + r.h / 2 });
const rectsOverlap = (a, b, pad = 0) => a.x - pad < b.x + b.w && a.x + a.w + pad > b.x && a.y - pad < b.y + b.h && a.y + a.h + pad > b.y;

return { M, clamp, lerp, invLerp, smoothstep, dist, dist2, dE, angleTo, normAngle, angleDiff, len, norm, mulberry32, rr, ri, pick, shuffle, gauss, curves, uid, fmtTime, inRect, rectCenter, rectsOverlap };
};
__defs["js/core/events.js"] = function(){
// Central event bus. Perks, scoring, audio, AI memory and UI all subscribe here.
const EV = {
  GENERATOR_START: 'onGeneratorStart',
  GENERATOR_PROGRESS: 'onGeneratorProgress',
  GENERATOR_REPAIRED: 'onGeneratorComplete',
  GENERATOR_DAMAGED: 'onGeneratorDamaged',
  SKILL_CHECK: 'onSkillCheck',
  SKILL_CHECK_SUCCESS: 'onSkillCheckSuccess',
  SKILL_CHECK_FAILURE: 'onSkillCheckFailure',
  SURVIVOR_INJURED: 'onSurvivorInjured',
  SURVIVOR_DOWNED: 'onSurvivorDowned',
  SURVIVOR_PICKED_UP: 'onSurvivorPickedUp',
  SURVIVOR_DROPPED: 'onSurvivorDropped',
  SURVIVOR_HOOKED: 'onSurvivorHooked',
  SURVIVOR_UNHOOKED: 'onSurvivorUnhooked',
  SURVIVOR_HEALED: 'onSurvivorHealed',
  SURVIVOR_KILLED: 'onSurvivorKilled',
  SURVIVOR_ESCAPED: 'onSurvivorEscaped',
  HOOK_STAGE: 'onHookStage',
  PALLET_DROPPED: 'onPalletDropped',
  PALLET_DESTROYED: 'onPalletDestroyed',
  KILLER_STUNNED: 'onKillerStunned',
  VAULT_PERFORMED: 'onVault',
  KILLER_HIT: 'onKillerHit',
  KILLER_MISS: 'onKillerMiss',
  EXIT_GATES_POWERED: 'onExitGatesPowered',
  EXIT_GATE_OPENED: 'onExitGateOpened',
  ENDGAME_COLLAPSE: 'onEndgameCollapse',
  HATCH_OPENED: 'onHatchOpened',
  HATCH_CLOSED: 'onHatchClosed',
  TRAP_SET: 'onTrapSet',
  TRAP_TRIGGERED: 'onTrapTriggered',
  TRAP_DISARMED: 'onTrapDisarmed',
  TOTEM_CLEANSED: 'onTotemCleansed',
  CHEST_OPENED: 'onChestOpened',
  CHASE_START: 'onChaseStart',
  CHASE_END: 'onChaseEnd',
  LOUD_NOISE: 'onLoudNoise',
  SOUND: 'onSound', // generic world sound survivors may hear
  POWER_USED: 'onPowerUsed',
  SCORE: 'onScore',
};

class EventBus {
  constructor() { this.map = new Map(); this.log = []; }
  on(name, fn, owner = null) {
    if (!this.map.has(name)) this.map.set(name, []);
    this.map.get(name).push({ fn, owner });
    return () => this.off(name, fn);
  }
  off(name, fn) {
    const l = this.map.get(name); if (!l) return;
    const i = l.findIndex((h) => h.fn === fn); if (i >= 0) l.splice(i, 1);
  }
  offOwner(owner) { for (const l of this.map.values()) for (let i = l.length - 1; i >= 0; i--) if (l[i].owner === owner) l.splice(i, 1); }
  emit(name, data = {}) {
    const l = this.map.get(name);
    if (l) for (const h of [...l]) { try { h.fn(data); } catch (e) { console.error('event handler', name, e); } }
    const any = this.map.get('*');
    if (any) for (const h of [...any]) h.fn({ name, ...data });
  }
}

return { EV, EventBus };
};
__defs["js/entities/objects.js"] = function(){
// Interactable map objects. Positions are world pixels.
const { M, uid } = __req("js/core/util.js");

const GEN_CHARGES = 90;        // seconds of solo repair
const PALLET_DROP_TIME = 0.35;

// A vaultable low wall. alongX: obstacle line runs along x, so crossing is along y.
class Window {
  constructor({ cx, cy, len, alongX }) {
    this.id = uid('win'); this.kind = 'window';
    this.x = cx * M; this.y = cy * M; this.alongX = alongX; this.len = len * M;
    const t = 1 * M; // matches template cell thickness
    this.rect = alongX ? { x: this.x - this.len / 2, y: this.y - t / 2, w: this.len, h: t } : { x: this.x - t / 2, y: this.y - this.len / 2, w: t, h: this.len };
    this.solid = { ...this.rect, blocksMove: true, blocksLOS: false, kind: 'window', ref: this };
    this.blockedUntil = 0;        // entity block (survivors cannot vault)
    this.killerBlockedUntil = 0;  // e.g. Bamboozle
    this.nx = alongX ? 0 : 1; this.ny = alongX ? 1 : 0; // crossing normal
    this.tile = null;
  }
  sides(off = 0.95 * M) { return [{ x: this.x - this.nx * off, y: this.y - this.ny * off }, { x: this.x + this.nx * off, y: this.y + this.ny * off }]; }
  sideOf(p) { return (p.x - this.x) * this.nx + (p.y - this.y) * this.ny < 0 ? 0 : 1; }
}

class Pallet {
  constructor({ cx, cy, len, alongX }) {
    this.id = uid('pal'); this.kind = 'pallet';
    this.x = cx * M; this.y = cy * M; this.alongX = alongX; this.len = len * M;
    const t = 0.7 * M;
    this.rect = alongX ? { x: this.x - this.len / 2, y: this.y - t / 2, w: this.len, h: t } : { x: this.x - t / 2, y: this.y - this.len / 2, w: t, h: this.len };
    this.solid = { ...this.rect, blocksMove: true, blocksLOS: false, kind: 'pallet', ref: this };
    this.state = 'up'; this.dropT = 0; this.dropper = null;
    this.nx = alongX ? 0 : 1; this.ny = alongX ? 1 : 0;
    this.tile = null;
  }
  sides(off = 1.0 * M) { return [{ x: this.x - this.nx * off, y: this.y - this.ny * off }, { x: this.x + this.nx * off, y: this.y + this.ny * off }]; }
  sideOf(p) { return (p.x - this.x) * this.nx + (p.y - this.y) * this.ny < 0 ? 0 : 1; }
  // distance from point to the pallet's line segment (used for stun zone)
  stunZone(p) {
    const lx = Math.abs((p.x - this.x) * (this.alongX ? 1 : 0) + (p.y - this.y) * (this.alongX ? 0 : 1));
    const ln = Math.abs((p.x - this.x) * this.nx + (p.y - this.y) * this.ny);
    return lx < this.len / 2 + 0.2 * M && ln < 1.0 * M;
  }
}

class Generator {
  constructor(x, y, name) {
    this.id = uid('gen'); this.kind = 'generator'; this.name = name;
    this.x = x; this.y = y; this.charges = 0; this.done = false;
    this.repairers = new Set(); this.regressing = false; this.regressRate = 0;
    this.blockedUntil = 0; this.lastRepairT = -999; this.kickT = -999; this.popArmed = false;
    const s = 1.4 * M;
    this.solid = { x: x - s / 2, y: y - s / 2, w: s, h: s, blocksMove: true, blocksLOS: false, kind: 'generator', ref: this };
  }
  get progress() { return this.charges / GEN_CHARGES; }
  slots() {
    const o = 1.25 * M;
    return [{ x: this.x, y: this.y - o }, { x: this.x + o, y: this.y }, { x: this.x, y: this.y + o }, { x: this.x - o, y: this.y }];
  }
}

class Hook {
  constructor(x, y) { this.id = uid('hook'); this.kind = 'hook'; this.x = x; this.y = y; this.survivor = null; this.broken = false; }
}

class Chest {
  constructor(x, y) { this.id = uid('chest'); this.kind = 'chest'; this.x = x; this.y = y; this.opened = false; this.progress = 0; this.user = null; }
}

class Totem {
  constructor(x, y) { this.id = uid('totem'); this.kind = 'totem'; this.x = x; this.y = y; this.hex = null; this.cleansed = false; this.progress = 0; this.user = null; }
}

class ExitGate {
  // side: 'n','s','e','w'. doorRect blocks the corridor until open; exitRect = escape zone.
  constructor({ x, y, side, doorRect, exitRect, name }) {
    this.id = uid('gate'); this.kind = 'gate'; this.name = name;
    this.x = x; this.y = y; this.side = side; this.doorRect = doorRect; this.exitRect = exitRect;
    this.powered = false; this.progress = 0; this.open = false; this.user = null;
    this.solid = { ...doorRect, blocksMove: true, blocksLOS: true, kind: 'gate', ref: this };
  }
}

class Hatch {
  constructor(x, y) { this.kind = 'hatch'; this.x = x; this.y = y; this.state = 'hidden'; this.keyProgress = 0; }
}

class Trap {
  constructor(x, y, state = 'unset') {
    this.id = uid('trap'); this.kind = 'trap'; this.x = x; this.y = y; this.state = state; this.victim = null;
    this.inGrass = false; this.disarmProgress = 0; this.setT = 0;
  }
}

return { GEN_CHARGES, PALLET_DROP_TIME, Window, Pallet, Generator, Hook, Chest, Totem, ExitGate, Hatch, Trap };
};
__defs["js/core/balance.js"] = function(){
// Balance configuration: tunable numbers that designers change often live here instead of
// deep inside power or rules code.
const BALANCE = {
  trapper: {
    // Haste after a bear trap is successfully set (never on a failed/cancelled placement).
    trapSetHaste: 0.10,        // +10% movement speed
    trapSetHasteDuration: 4,   // seconds
    trapSetHasteStacks: false, // false: a new placement refreshes the timer instead of stacking
  },
  cenobite: { chainRecoverySpeed: 0.25 },
};

return { BALANCE };
};
__defs["js/killers/trapper.js"] = function(){
// THE TRAPPER — Bear Traps. Hold power to set a carried trap; E picks up / resets a trap.
const { M, dist } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { Trap } = __req("js/entities/objects.js");
const { BALANCE } = __req("js/core/balance.js");

class TrapperPower {
  constructor(killer, addons = []) {
    this.k = killer; this.id = 'traps'; this.name = 'Bear Trap';
    this.maxCarry = 2 + (addons.includes('trapper_bag') ? 1 : 0);
    this.carried = 2; this.setting = null; this.picking = null;
    this.setTime = addons.includes('fast_set') ? 1.4 : 2.0;
    this.serrated = addons.includes('serrated_jaws'); this.ironCoil = addons.includes('iron_coil');
    this.aiProfile = { type: 'trap', label: 'Bear traps', counter: ['avoid known traps', 'disarm near loops', 'avoid tall grass near tiles'] };
  }
  speedMult() { return this.setting || this.picking ? 0 : 1; }
  canAttack() { return !this.setting && !this.picking; }
  onInit(m) {
    // Map trap spawns: traps lying on the ground, unset.
    for (const p of m.map.trapSpawns || []) m.traps.push(new Trap(p.x, p.y, 'unset'));
  }
  frontPoint() { return { x: this.k.x + Math.cos(this.k.facing) * 1.1 * M, y: this.k.y + Math.sin(this.k.facing) * 1.1 * M }; }
  update(dt, m, inp, locked) {
    if (locked) { this.setting = null; this.picking = null; return; }
    const k = this.k;
    if (this.setting) {
      this.setting.t += dt;
      if (!inp.rmb) { this.setting = null; }
      else if (this.setting.t >= this.setTime) {
        const p = this.setting.p; const t = new Trap(p.x, p.y, 'set'); t.inGrass = m.map.isGrass(p.x, p.y); t.setT = m.time;
        m.traps.push(t); this.carried--; this.setting = null;
        m.events.emit(EV.TRAP_SET, { trap: t }); m.audio?.play('trapset');
        // Balance: Haste after a successful placement (refreshes, never stacks unless configured).
        const B = BALANCE.trapper; if (!B.trapSetHasteStacks) k.status.remove('haste', 'trap_set');
        k.status.add('haste', { duration: B.trapSetHasteDuration, value: B.trapSetHaste, source: 'trap_set' }); m.fx.text(k.x, k.y - 40, 'HASTE', '#9fd0a0');
      }
      return;
    }
    if (this.picking) {
      this.picking.t += dt;
      if (this.picking.t >= 1.0) {
        const t = this.picking.trap; const i = m.traps.indexOf(t); if (i >= 0 && t.state !== 'triggered') { m.traps.splice(i, 1); this.carried++; }
        this.picking = null;
      }
      return;
    }
    if (inp.rmbP && !k.carrying && this.carried > 0) {
      const p = this.frontPoint();
      if (!m.world.pointBlocked(p.x, p.y, 0.4 * M) && !m.traps.some((t) => dist(t.x, t.y, p.x, p.y) < 1.2 * M)) this.setting = { t: 0, p };
      else m.fx.text(k.x, k.y - 40, 'No room', '#cfc6b8');
    }
    if (inp.eP && !k.carrying && this.carried < this.maxCarry) {
      const t = this.nearTrap(m); if (t) this.picking = { t: 0, trap: t };
    }
  }
  nearTrap(m) {
    let best = null, bd = 1.8 * M;
    for (const t of m.traps) { if (t.state === 'triggered') continue; const d = dist(t.x, t.y, this.k.x, this.k.y); if (d < bd) { bd = d; best = t; } }
    return best;
  }
  hud(m) {
    const st = this.setting ? `Setting ${Math.round(this.setting.t / this.setTime * 100)}%` : this.picking ? 'Picking up…' : this.carried ? 'Ready' : 'No traps carried';
    return { title: 'Bear Trap', value: `${this.carried}/${this.maxCarry}`, meter: this.setting ? this.setting.t / this.setTime : this.picking ? this.picking.t : this.carried / this.maxCarry, state: st,
      hints: [['Q / RMB', 'Hold to set trap'], ['E', 'Pick up / reset trap']] };
  }
  prompt(m) { const t = this.nearTrap(m); return t && this.carried < this.maxCarry ? { key: 'E', label: t.state === 'set' ? 'Pick up set trap' : 'Pick up trap' } : null; }
}

return { TrapperPower };
};
__defs["js/killers/hillbilly.js"] = function(){
// THE HILLBILLY — Chainsaw. Hold power to rev; at full charge he sprints at high speed
// with limited turning. Hitting a survivor downs them instantly; hitting an obstacle stuns him.
const { M, clamp, normAngle, dist } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");

class HillbillyPower {
  constructor(killer, addons = []) {
    this.k = killer; this.id = 'chainsaw'; this.name = 'Chainsaw';
    this.state = 'idle'; this.charge = 0; this.heat = 0; this.t = 0; this.dir = 0; this.overheatT = 0;
    this.revTime = addons.includes('primer_bulb') ? 2.0 : 2.5;
    this.turnRate = addons.includes('heavy_clutch') ? 1.9 : 1.35;
    this.heatRate = addons.includes('speed_limiter') ? 0.07 : 0.1;
    this.sprintSpeed = 9.2 * M; this.maxSprint = 3.2;
    this.soundT = 0; this.lockFacing = false;
    this.aiProfile = { type: 'chainsaw', label: 'Chainsaw sprint', counter: ['break line of sight when revving', 'avoid long straight runs', 'sidestep the sprint'] };
  }
  speedMult() {
    if (this.state === 'revving') return 0.7;
    if (this.state === 'sprint') return this.sprintSpeed / (this.k.def.speed * M);
    if (this.state === 'recover') return 0.35;
    if (this.state === 'collide') return 0;
    return 1;
  }
  canAttack() { return this.state === 'idle' || this.state === 'overheat'; }
  overrideMove() {
    if (this.state !== 'sprint') return null;
    return { x: Math.cos(this.dir), y: Math.sin(this.dir) };
  }
  update(dt, m, inp, locked) {
    const k = this.k;
    if (this.overheatT > 0) { this.overheatT -= dt; if (this.overheatT <= 0) { this.state = 'idle'; this.heat = 0; } }
    else this.heat = Math.max(0, this.heat - dt * 0.04);
    if (locked) { if (this.state === 'revving' || this.state === 'sprint') this.state = 'idle'; this.charge = 0; this.lockFacing = false; return; }
    this.soundT -= dt;
    switch (this.state) {
      case 'idle':
        this.charge = Math.max(0, this.charge - dt);
        if (inp.rmb && !k.carrying && this.overheatT <= 0) { this.state = 'revving'; m.events.emit(EV.POWER_USED, { killer: k, power: 'rev' }); }
        break;
      case 'revving':
        this.charge += dt / this.revTime; this.heat += dt * this.heatRate * 0.5;
        if (this.soundT <= 0) { this.soundT = 0.3; m.sound('chainsaw_rev', k.x, k.y, 40 * M, { charge: this.charge }); m.audio?.play('rev', { charge: this.charge }); }
        if (!inp.rmb) { this.state = 'idle'; break; }
        if (this.charge >= 1) { this.state = 'sprint'; this.t = 0; this.dir = k.facing; this.lockFacing = true; m.events.emit(EV.POWER_USED, { killer: k, power: 'sprint' }); m.audio?.play('saw'); }
        if (this.heat >= 1) this.overheat(m);
        break;
      case 'sprint': {
        this.t += dt; this.heat += dt * this.heatRate;
        if (this.soundT <= 0) { this.soundT = 0.2; m.sound('chainsaw_sprint', k.x, k.y, 40 * M, { dir: this.dir }); }
        // limited steering toward aim
        const d = normAngle(inp.aim - this.dir); this.dir += clamp(d, -this.turnRate * dt, this.turnRate * dt);
        k.facing = this.dir;
        // survivor hit check: instant down
        for (const s of m.survivors) {
          if (!s.standing) continue;
          if (dist(s.x, s.y, k.x + Math.cos(this.dir) * 0.6 * M, k.y + Math.sin(this.dir) * 0.6 * M) < s.r + k.r + 0.25 * M) {
            if (s.dodgeT && s.dodgeT > m.time) continue;
            const res = s.takeHit(m, { instadown: true, source: 'chainsaw' }); k.stats.hits++; k.bloodlust = 0;
            m.events.emit(EV.KILLER_HIT, { killer: k, survivor: s, result: res, power: true }); m.fx.hit(s, res); m.fx.shake(10);
            this.state = 'recover'; this.t = 0; this.lockFacing = false; this.charge = 0; return;
          }
        }
        if (!inp.rmb || this.t > this.maxSprint) { this.state = 'recover'; this.t = 0; this.lockFacing = false; this.charge = 0; }
        if (this.heat >= 1) this.overheat(m);
        break;
      }
      case 'recover': this.t += dt; if (this.t > 1.4) this.state = 'idle'; break;
      case 'collide': this.t += dt; if (this.t > 2.5) this.state = 'idle'; break;
    }
  }
  onCollide(m) {
    if (this.state !== 'sprint') return;
    this.state = 'collide'; this.t = 0; this.lockFacing = false; this.charge = 0;
    m.fx.shake(14); m.fx.sparks(this.k.x + Math.cos(this.dir) * 20, this.k.y + Math.sin(this.dir) * 20); m.audio?.play('clang');
    m.events.emit(EV.POWER_USED, { killer: this.k, power: 'collide' });
  }
  overheat(m) { this.state = 'overheat'; this.overheatT = 12; this.lockFacing = false; this.charge = 0; m.fx.text(this.k.x, this.k.y - 40, 'OVERHEAT', '#ff8a4a'); }
  onStun() { this.state = 'idle'; this.charge = 0; this.lockFacing = false; }
  hud() {
    const label = { idle: 'Ready', revving: 'Revving', sprint: 'SPRINTING', recover: 'Recovering', collide: 'Collision!', overheat: 'Overheated' }[this.state];
    return { title: 'Chainsaw', value: this.state === 'overheat' ? Math.ceil(this.overheatT) + 's' : Math.round(this.heat * 100) + '° heat', meter: this.state === 'revving' ? this.charge : this.state === 'overheat' ? 1 - this.overheatT / 12 : 1 - this.heat, state: label,
      hot: this.heat, hints: [['Q / RMB', 'Hold to rev, keep holding to sprint'], ['Mouse', 'Steer the sprint']] };
  }
  prompt() { return null; }
}

return { HillbillyPower };
};
__defs["js/killers/wraith.js"] = function(){
// THE WRAITH — Wailing Bell. Toggle cloak: while cloaked he is fast, undetectable and
// nearly invisible, but cannot attack. Cloaking and uncloaking ring a bell survivors can hear.
const { M } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");

class WraithPower {
  constructor(killer, addons = []) {
    this.k = killer; this.id = 'cloak'; this.name = 'Wailing Bell';
    this.state = 'visible'; this.t = 0;
    const f = addons.includes('blind_warrior') ? 0.75 : 1;
    this.cloakTime = 1.5 * f; this.uncloakTime = 1.6 * f;
    this.bellRange = (addons.includes('bone_clapper') ? 12 : 24) * M;
    this.shadowDance = addons.includes('shadow_dance');
    this.aiProfile = { type: 'stealth', label: 'Cloaking', counter: ['react to the bell', 'watch for the shimmer', 'stay near tiles while repairing'] };
  }
  get cloaked() { return this.state === 'cloaked' || this.state === 'uncloaking'; }
  speedMult() {
    if (this.state === 'cloaked') return 6.0 / 4.6;
    if (this.state === 'cloaking') return 0.6;
    if (this.state === 'uncloaking') return 0.75;
    return 1;
  }
  canAttack() { return this.state === 'visible'; }
  canInteract() { return this.state === 'visible'; }
  update(dt, m, inp, locked) {
    if (this.state === 'cloaking') { this.t += dt; if (this.t >= this.cloakTime) { this.state = 'cloaked'; this.k.status.add('undetectable', { source: 'cloak' }); } }
    else if (this.state === 'uncloaking') { this.t += dt; if (this.t >= this.uncloakTime) {
      this.state = 'visible'; this.k.status.remove('undetectable', 'cloak');
      this.k.status.add('haste', { duration: this.shadowDance ? 1.5 : 0.8, value: this.shadowDance ? 0.2 : 0.12, source: 'uncloak' });
      m.events.emit(EV.POWER_USED, { killer: this.k, power: 'uncloaked' });
    } }
    if (locked) return;
    if (inp.rmbP && !this.k.carrying) {
      if (this.state === 'visible') { this.state = 'cloaking'; this.t = 0; this.bell(m, 'cloak'); }
      else if (this.state === 'cloaked') { this.state = 'uncloaking'; this.t = 0; this.bell(m, 'uncloak'); }
    }
  }
  bell(m, kind) {
    m.sound('wraith_bell', this.k.x, this.k.y, this.bellRange, { kind });
    m.audio?.play('bell');
    m.events.emit(EV.POWER_USED, { killer: this.k, power: kind });
  }
  onStun() { if (this.state !== 'visible') { this.state = 'visible'; this.k.status.remove('undetectable', 'cloak'); } }
  hud() {
    const label = { visible: 'Visible', cloaking: 'Cloaking…', cloaked: 'CLOAKED', uncloaking: 'Uncloaking…' }[this.state];
    const meter = this.state === 'cloaking' ? this.t / this.cloakTime : this.state === 'uncloaking' ? 1 - this.t / this.uncloakTime : this.state === 'cloaked' ? 1 : 0;
    return { title: 'Wailing Bell', value: label, meter, state: this.state === 'cloaked' ? 'Undetectable · cannot attack' : 'Press to cloak', hints: [['Q / RMB', 'Cloak / uncloak (rings bell)']] };
  }
  prompt() { return null; }
}

return { WraithPower };
};
__defs["js/killers/framework.js"] = function(){
// Killer power framework. Every killer power extends BasePower and may use the shared
// world systems below — projectiles, area zones and placeable devices — without the
// match loop knowing anything about a specific killer.
//
// Killer
// ├── Power (BasePower)
// │   ├── activation / update(dt, m, input, locked)
// │   ├── cooldowns + charges (helpers here)
// │   ├── effects        → statuses on survivors / killer
// │   ├── projectiles    → m.spawnProjectile(new Projectile({...}))
// │   ├── area effects   → m.addZone(new Zone({...}))
// │   ├── devices        → m.addDevice(new Device({...}))  (survivors can interact)
// │   └── survivor hooks → visibilityTo(), context(), onSurvivorHit()…
// ├── Add-ons (this.has(id))
// ├── Perks (event driven, see perks/)
// └── AI interactions → this.aiProfile + ai/killerProfiles.js
const { M, dist, uid } = __req("js/core/util.js");

class BasePower {
  constructor(killer, addons = []) {
    this.k = killer; this.addons = addons; this.state = 'idle'; this.cooldowns = {}; this.lockFacing = false;
  }
  has(id) { return this.addons.includes(id); }
  cd(name) { return this.cooldowns[name] || 0; }
  setCd(name, t) { this.cooldowns[name] = t; }
  tickCooldowns(dt) { for (const k in this.cooldowns) this.cooldowns[k] = Math.max(0, this.cooldowns[k] - dt); }
  // defaults every power may override
  speedMult() { return 1; }
  canAttack() { return true; }
  canInteract() { return true; }
  update() {}
  hud() { return { title: this.name, value: '', meter: 0, state: '', hints: [] }; }
  prompt() { return null; }
}

// Straight-flying projectile. Walls (sight-blocking solids) stop it unless passWalls.
class Projectile {
  constructor(o) {
    this.id = uid('pr'); this.kind = o.kind; this.owner = o.owner;
    this.x = o.x; this.y = o.y; this.vx = o.vx; this.vy = o.vy; this.r = o.r ?? 0.25 * M;
    this.range = o.range ?? 30 * M; this.traveled = 0; this.dead = false; this.passWalls = !!o.passWalls;
    this.onHit = o.onHit; this.pierce = !!o.pierce; this.onWall = o.onWall; this.onEnd = o.onEnd; this.color = o.color || '#ddd'; this.t = 0;
    this.arc = o.arc || null; // lobbed projectiles: { tx, ty, dur } land at a point, ignore walls
    this.data = o.data || {}; this.filter = o.filter || null; this.hitOnce = o.hitOnce ?? true; this.hitSet = new Set();
  }
  update(dt, m) {
    this.t += dt;
    if (this.arc) {
      const k = Math.min(1, this.t / this.arc.dur);
      this.x = this.arc.sx + (this.arc.tx - this.arc.sx) * k; this.y = this.arc.sy + (this.arc.ty - this.arc.sy) * k;
      this.height = Math.sin(k * Math.PI);
      if (k >= 1) { this.dead = true; this.onEnd?.(this, m); }
      return;
    }
    const ax = this.x, ay = this.y; const bx = ax + this.vx * dt, by = ay + this.vy * dt;
    let endT = 1;
    if (!this.passWalls) { const h = m.world.firstHit(ax, ay, bx, by, this.r, (s) => s.blocksLOS); if (h) endT = h.t; }
    // survivors along the segment
    let hitS = null, hitT = endT;
    for (const s of m.survivors) {
      if (!s.standing || this.hitSet.has(s) || (this.filter && !this.filter(s, m))) continue;
      const t = segCircle(ax, ay, bx, by, s.x, s.y, s.r + this.r);
      if (t >= 0 && t <= hitT) { hitT = t; hitS = s; }
    }
    this.x = ax + (bx - ax) * hitT; this.y = ay + (by - ay) * hitT;
    this.traveled += Math.hypot(bx - ax, by - ay) * hitT;
    if (hitS) { if (this.pierce) { this.hitSet.add(hitS); this.onHit?.(this, hitS, m); } else { this.dead = true; this.onHit?.(this, hitS, m); return; } }
    if (endT < 1) { this.dead = true; this.onWall?.(this, m); return; }
    if (this.traveled >= this.range) { this.dead = true; this.onEnd?.(this, m); }
  }
  // seconds until closest approach to a point, and the miss distance there
  approach(px, py) {
    const sp2 = this.vx * this.vx + this.vy * this.vy; if (!sp2) return { t: Infinity, d: Infinity };
    const t = ((px - this.x) * this.vx + (py - this.y) * this.vy) / sp2;
    const cx = this.x + this.vx * t, cy = this.y + this.vy * t;
    return { t, d: Math.hypot(px - cx, py - cy) };
  }
}

function segCircle(ax, ay, bx, by, cx, cy, r) {
  const dx = bx - ax, dy = by - ay, fx = ax - cx, fy = ay - cy;
  const a = dx * dx + dy * dy, b = 2 * (fx * dx + fy * dy), c = fx * fx + fy * fy - r * r;
  if (c <= 0) return 0;
  if (a < 1e-9) return -1;
  const disc = b * b - 4 * a * c; if (disc < 0) return -1;
  const t = (-b - Math.sqrt(disc)) / (2 * a);
  return t >= 0 && t <= 1 ? t : -1;
}

// Area effect: circle zone with a per-frame effect on survivors (and optionally the killer).
// aiCost tells survivor pathing how much to avoid it; seenRange how far away survivors notice it.
class Zone {
  constructor(o) {
    this.id = uid('zn'); this.kind = o.kind; this.x = o.x; this.y = o.y; this.r = o.r;
    this.t0 = o.t0; this.until = o.until; this.owner = o.owner; this.color = o.color || 'rgba(160,80,200,0.3)';
    this.effect = o.effect; this.killerEffect = o.killerEffect; this.armT = o.armT || 0; this.aiCost = o.aiCost ?? 8;
    this.seenRange = o.seenRange ?? 30 * M; this.label = o.label || o.kind; this.data = o.data || {};
  }
  armed(m) { return m.time - this.t0 >= this.armT; }
  contains(x, y, pad = 0) { return (x - this.x) ** 2 + (y - this.y) ** 2 < (this.r + pad) ** 2; }
}

// Placeable / interactable world object (pods, lockers, printers…).
class Device {
  constructor(o) {
    this.id = uid('dv'); this.kind = o.kind; this.x = o.x; this.y = o.y; this.r = o.r ?? 0.5 * M;
    this.owner = o.owner; this.label = o.label || o.kind; this.useTime = o.useTime ?? 2;
    this.survivorUsable = o.survivorUsable ?? false; this.usable = o.usable || (() => this.survivorUsable);
    this.onUse = o.onUse || (() => {}); this.dead = false; this.data = o.data || {}; this.seenRange = o.seenRange ?? 26 * M;
    this.aiCost = o.aiCost ?? 0; this.dangerRadius = o.dangerRadius ?? 0; this.useLabel = o.useLabel || null; this.manualComplete = !!o.manualComplete;
  }
}

// Helper used by several powers: find the best open ground point in front of the killer.
function frontPoint(k, d = 1.2 * M) { return { x: k.x + Math.cos(k.facing) * d, y: k.y + Math.sin(k.facing) * d }; }
function nearestOf(list, x, y, max = Infinity) { let best = null, bd = max; for (const o of list) { const d = dist(o.x, o.y, x, y); if (d < bd) { bd = d; best = o; } } return best; }

// Power-controlled AI entity (the Houndmaster's dog, the Knight's guards…). It has its own
// position, nav-grid pathfinding and state; survivors perceive it as a separate threat.
class Minion {
  constructor(o) {
    this.id = uid('mn'); this.kind = o.kind; this.owner = o.owner; this.x = o.x; this.y = o.y; this.r = o.r ?? 0.4 * M;
    this.vx = 0; this.vy = 0; this.facing = o.facing ?? 0; this.state = o.state || 'idle'; this.speed = o.speed ?? 4 * M;
    this.target = null; this.path = null; this.pathI = 0; this.dead = false; this.t = 0; this.stunT = 0; this.data = o.data || {};
    this.color = o.color || '#ccc'; this.label = o.label || o.kind; this.threat = o.threat ?? 1; this.visRange = o.visRange ?? 30 * M;
  }
  // A* over the nav grid. Minions never vault (no links), dropped pallets block them.
  pathTo(m, x, y) {
    const p = m.map.nav.findPath(this.x, this.y, x, y, {}, 6000);
    this.path = p ? m.map.nav.smooth(m.world, p.pts, this.r) : null; this.pathI = 1; this.pathGoal = { x, y };
    return !!this.path;
  }
  // Follow the current path; returns true when the end is reached (or no path).
  followPath(dt, m, speed = this.speed) {
    if (!this.path || this.pathI >= this.path.length) { this.vx = this.vy = 0; return true; }
    const p = this.path[this.pathI]; const dx = p.x - this.x, dy = p.y - this.y, d = Math.hypot(dx, dy);
    if (d < 0.35 * M) { this.pathI++; return this.pathI >= this.path.length; }
    this.moveDir(dt, m, dx / d, dy / d, speed);
    return false;
  }
  moveDir(dt, m, ux, uy, speed = this.speed) {
    const ox = this.x, oy = this.y;
    const hit = m.world.move(this, ux * speed * dt, uy * speed * dt, this.r, (s) => !(s.kind === 'gate' && s.ref.open));
    this.vx = (this.x - ox) / dt; this.vy = (this.y - oy) / dt; this.facing = Math.atan2(uy, ux);
    return hit;
  }
}

// A power landing a hit on a survivor (respects Dead Hard style dodges). Returns the result.
function strike(m, k, s, { source = 'power', instadown = false, score = null, label = 'Hit' } = {}) {
  if (!s.standing) return null;
  if (s.dodgeT && s.dodgeT > m.time) { m.fx.text(s.x, s.y - 30, 'DODGED', '#ffd27a'); return 'dodged'; }
  s.lastHitBy = k; const res = s.takeHit(m, { source, instadown }); k.stats.hits++; k.stats.powerHits++; k.bloodlust = 0; k.chaseT = 0;
  m.events.emit('onKillerHit', { killer: k, survivor: s, result: res, power: true }); m.fx.hit(s, res);
  if (score) m.addScore(score, 300, label);
  return res;
}
// Killer Instinct: the killer sees where survivors are (shown like a short aura reveal).
function killerInstinct(m, s, dur = 2, label = 'Killer Instinct') { m.reveal(s, dur, label); }

return { BasePower, Projectile, Zone, Device, frontPoint, nearestOf, Minion, strike, killerInstinct };
};
__defs["js/killers/ghostface.js"] = function(){
// GHOST FACE — Night Shroud. Activate to become undetectable and hard to spot.
// Hold the power while shrouded to stalk survivors in view; a fully stalked survivor is
// Marked (Exposed: one hit downs). Survivors who look straight at him long enough reveal
// him, which breaks the shroud and puts it on cooldown. E toggles a crouch.
const { M, dist, angleDiff, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower } = __req("js/killers/framework.js");

class GhostFacePower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'shroud'; this.name = 'Night Shroud'; this.state = 'visible';
    this.crouched = false; this.stalking = false; this.markTime = this.has('philly') ? 50 : 35;
    this.revealCd = this.has('outdoor_rope') ? 14 : 20; this.stalkRate = this.has('drop_leg_sheath') ? 1.25 : 1;
    this.revealRange = 14 * M; this.stalkTarget = null;
    this.aiProfile = { type: 'stalker', label: 'Stalk & expose', counter: ['look at him to reveal', 'break line of sight when stalked', 'play safe when marked'] };
  }
  get shrouded() { return this.state === 'shrouded'; }
  speedMult() { if (this.stalking) return 0.45; if (this.crouched) return 0.7; return 1; }
  canAttack() { return true; }
  // Survivors' visual detection range multiplier while shrouded.
  visibilityTo(s, m) {
    if (!this.shrouded) return 1;
    let v = this.crouched ? 0.32 : 0.55;
    if (m.map.isGrass(this.k.x, this.k.y) && this.crouched) v *= 0.6;
    return v;
  }
  onAttack(m) { if (this.shrouded) this.breakShroud(m, null, 6); }
  onStun(m) { if (this.shrouded) this.breakShroud(m, null, this.revealCd); }
  breakShroud(m, by, cd) {
    this.state = 'cooldown'; this.setCd('shroud', cd); this.stalking = false; this.crouched = false;
    this.k.status.remove('undetectable', 'shroud');
    if (by) {
      m.notifyK(`${by.name} revealed you`, '#e05a5a'); m.loudNoise(this.k.x, this.k.y, 'reveal');
      m.sound('killer_action', this.k.x, this.k.y, 40 * M, { what: 'revealed' });
      m.bb.report(by, this.k.x, this.k.y, this.k.vx, this.k.vy, this.k);
      by.ai?.note('Revealed Ghost Face by watching him', 'gfreveal');
      m.events.emit(EV.POWER_USED, { killer: this.k, power: 'revealed', by });
    }
  }
  update(dt, m, inp, locked) {
    this.tickCooldowns(dt);
    const k = this.k;
    if (this.state === 'cooldown' && this.cd('shroud') <= 0) this.state = 'visible';
    for (const s of m.survivors) if (s.alive && s.stalk > 0 && !this.stalking) s.stalk = Math.max(0, s.stalk - dt * 0.05);
    if (locked) { this.stalking = false; return; }
    if (inp.eP) this.crouched = !this.crouched;
    if (this.state === 'visible' && inp.rmbP && !k.carrying) {
      this.state = 'shrouded'; k.status.add('undetectable', { source: 'shroud' });
      m.events.emit(EV.POWER_USED, { killer: k, power: 'shroud' });
      return;
    }
    if (!this.shrouded) { this.stalking = false; return; }
    // Stalking
    this.stalking = inp.rmb && !k.carrying; this.stalkTarget = null;
    if (this.stalking) {
      for (const s of m.survivors) {
        if (!s.standing) continue;
        const d = dist(k.x, k.y, s.x, s.y); if (d > 26 * M) continue;
        if (angleDiff(Math.atan2(s.y - k.y, s.x - k.x), k.facing) > 0.55) continue;
        if (!m.world.losClear(k.x, k.y, s.x, s.y)) continue;
        if (s.status.has('marked')) continue;
        const rate = (d < 10 * M ? 1 / 3.5 : 1 / 6.5) * this.stalkRate * (this.crouched ? 1.25 : 1);
        s.stalk = clamp(s.stalk + rate * dt, 0, 1); this.stalkTarget = s; s.lastStalkedT = m.time;
        if (s.stalk >= 1) {
          s.stalk = 0; s.status.add('marked', { duration: this.markTime, source: 'ghostface' });
          m.fx.text(s.x, s.y - 30, 'MARKED', '#ff4b5c'); m.notifyK(`${s.name} is marked`, '#ff4b5c');
          m.events.emit(EV.POWER_USED, { killer: k, power: 'marked', survivor: s }); m.addScore('deviousness', 300, 'Marked');
        }
      }
    }
    // Reveal: survivors staring at him from close range
    for (const s of m.survivors) {
      if (!s.standing) { s.revealT = 0; continue; }
      const d = dist(k.x, k.y, s.x, s.y);
      const range = this.revealRange * (this.crouched ? 0.6 : 1);
      const looking = d < range && angleDiff(s.facing, Math.atan2(k.y - s.y, k.x - s.x)) < 0.7 && m.world.losClear(s.x, s.y, k.x, k.y);
      s.revealT = looking ? (s.revealT || 0) + dt : Math.max(0, (s.revealT || 0) - dt * 2);
      if (s.revealT > 1.3) { s.revealT = 0; this.breakShroud(m, s, this.revealCd); break; }
    }
  }
  hud() {
    const st = { visible: 'Ready', shrouded: this.stalking ? (this.stalkTarget ? `Stalking ${this.stalkTarget.name}` : 'Stalking…') : 'Night Shroud', cooldown: 'Revealed' }[this.state];
    return { title: 'Night Shroud', value: this.state === 'cooldown' ? Math.ceil(this.cd('shroud')) + 's' : this.crouched ? 'Crouch' : this.state === 'shrouded' ? 'Shroud' : 'Ready',
      meter: this.state === 'cooldown' ? 1 - this.cd('shroud') / this.revealCd : this.stalkTarget ? this.stalkTarget.stalk : this.state === 'shrouded' ? 1 : 0, state: st,
      hints: [['Q / RMB', 'Shroud · hold to stalk'], ['E', 'Crouch']] };
  }
}

return { GhostFacePower };
};
__defs["js/killers/clown.js"] = function(){
// THE CLOWN — The Afterpiece Tonic. Hold the power to aim, release to lob a bottle.
// Tonic (purple) gas Intoxicates survivors: Hindered while inside and briefly after,
// and they cough. Antidote (yellow) gas arms after a moment and gives everyone inside
// (the Clown too) a burst of speed. E switches bottle type. Bottles reload when empty.
const { M, dist, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, Projectile, Zone } = __req("js/killers/framework.js");

class ClownPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'tonic'; this.name = 'Afterpiece Tonic'; this.state = 'idle';
    this.max = this.has('fingerless_parade') ? 8 : 6; this.bottles = this.max; this.type = 'tonic';
    this.aimT = 0; this.reloadT = 0; this.reloadTime = this.has('robin_feather') ? 2.4 : 3.2;
    this.cloudR = (this.has('redheads_pinkie') ? 3.8 : 3.2) * M; this.slow = this.has('ether') ? 0.26 : 0.2;
    this.aiProfile = { type: 'area', label: 'Gas clouds', counter: ['avoid purple gas', 'reroute when herded', 'use pallets before the gas lands'] };
  }
  speedMult() { if (this.state === 'aiming') return 0.85; if (this.state === 'reloading') return 0.55; return 1; }
  canAttack() { return this.state !== 'reloading'; }
  update(dt, m, inp, locked) {
    this.tickCooldowns(dt);
    if (locked) { if (this.state === 'aiming') this.state = 'idle'; return; }
    if (this.state === 'reloading') { this.reloadT += dt; if (this.reloadT >= this.reloadTime) { this.bottles = this.max; this.state = 'idle'; } return; }
    if (inp.eP) this.type = this.type === 'tonic' ? 'antidote' : 'tonic';
    if (this.state === 'idle' && inp.rmb && !this.k.carrying && this.bottles > 0 && this.cd('throw') <= 0) { this.state = 'aiming'; this.aimT = 0; }
    if (this.state === 'aiming') {
      this.aimT += dt;
      if (!inp.rmb) { if (this.aimT > 0.25) this.throw(m, inp); this.state = 'idle'; }
    }
    if (this.bottles <= 0 && this.state === 'idle') { this.state = 'reloading'; this.reloadT = 0; }
  }
  target(inp) {
    const k = this.k; const d = clamp(inp.aimDist ?? 10 * M, 3 * M, 16 * M);
    return { x: k.x + Math.cos(k.facing) * d, y: k.y + Math.sin(k.facing) * d, d };
  }
  throw(m, inp) {
    const k = this.k, t = this.target(inp), type = this.type;
    this.bottles--; this.setCd('throw', 0.5);
    m.sound('power_windup', k.x, k.y, 24 * M, { what: 'bottle' });
    m.spawnProjectile(new Projectile({
      kind: 'bottle', owner: k, x: k.x, y: k.y, arc: { sx: k.x, sy: k.y, tx: t.x, ty: t.y, dur: 0.25 + t.d / (18 * M) }, color: type === 'tonic' ? '#b07ad8' : '#e8c35a',
      data: { type }, onEnd: (p, mm) => this.burst(mm, p.x, p.y, type),
    }));
    m.events.emit(EV.POWER_USED, { killer: k, power: 'bottle', type });
  }
  burst(m, x, y, type) {
    m.audio?.play('glass'); m.fx.burst(x, y, 24, type === 'tonic' ? '#b07ad8' : '#e8c35a', 120, 0.8, 3, 3);
    m.sound('gas_cloud', x, y, 28 * M, { type });
    if (type === 'tonic') {
      m.addZone(new Zone({ kind: 'tonic', x, y, r: this.cloudR, t0: m.time, until: m.time + 8, owner: this.k, color: 'rgba(150,80,200,0.32)', aiCost: 14, label: 'Tonic gas',
        effect: (s, dt, mm) => {
          s.status.add('intoxicated', { duration: 2.5, value: this.slow, source: 'clown' });
          s._coughT = (s._coughT ?? 0) - dt; if (s._coughT <= 0) { s._coughT = 1.2 + Math.random(); mm.addGrunt(s, '#b07ad8'); }
        } }));
    } else {
      m.addZone(new Zone({ kind: 'antidote', x, y, r: this.cloudR * 0.9, t0: m.time, until: m.time + 8, armT: 1.5, owner: this.k, color: 'rgba(230,195,90,0.28)', aiCost: -2, label: 'Antidote gas',
        effect: (s) => { s.status.remove('intoxicated'); s.status.add('haste', { duration: 4, value: 0.1, source: 'antidote' }); },
        killerEffect: (kk) => kk.status.add('haste', { duration: 6, value: 0.12, source: 'antidote' }) }));
    }
  }
  hud() {
    const label = this.state === 'reloading' ? `Reloading ${Math.round(this.reloadT / this.reloadTime * 100)}%` : this.state === 'aiming' ? 'Aiming…' : this.type === 'tonic' ? 'Tonic ready' : 'Antidote ready';
    return { title: this.type === 'tonic' ? 'Afterpiece Tonic' : 'Antidote', value: `${this.bottles}/${this.max}`, meter: this.state === 'reloading' ? this.reloadT / this.reloadTime : this.bottles / this.max, state: label,
      hints: [['Q / RMB', 'Hold to aim, release to throw'], ['E', 'Switch tonic / antidote']] };
  }
}

return { ClownPower };
};
__defs["js/killers/executioner.js"] = function(){
// THE EXECUTIONER — Rites of Judgement. Toggle the power to enter Punishment mode:
// slower, leaving Torment Trails that Torment any survivor who steps on them.
// In Punishment mode LMB fires Punishment of the Damned: a telegraphed ground wave that
// travels through walls in a straight line and injures/tortures survivors it touches.
// A dying, Tormented survivor can be sent to a Cage of Atonement (SPACE) on a far hook.
const { M, dist, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, Zone } = __req("js/killers/framework.js");

class ExecutionerPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'judgement'; this.name = 'Rites of Judgement'; this.state = 'normal';
    this.windT = 0; this.wave = null; this.lastTrail = null; this.windup = this.has('burning_man') ? 0.85 : 1.1;
    this.waveLen = (this.has('lead_ring') ? 19 : 16) * M; this.trailLife = this.has('rust_shackles') ? 80 : 60;
    this.aiProfile = { type: 'zone_ranged', label: 'Torment trails & ground wave', counter: ['avoid torment trails', 'side-step the wave telegraph', 'walls do not block the wave'] };
  }
  speedMult() { if (this.state === 'windup' || this.state === 'recover') return 0.15; if (this.state === 'punish') return 0.82; return 1; }
  canAttack() { return this.state === 'normal'; }
  update(dt, m, inp, locked) {
    this.tickCooldowns(dt);
    const k = this.k;
    if (this.wave) this.updateWave(dt, m);
    if (locked) { if (this.state === 'windup') this.state = 'punish'; return; }
    if (inp.rmbP && !k.carrying && (this.state === 'normal' || this.state === 'punish')) {
      this.state = this.state === 'normal' ? 'punish' : 'normal'; this.lastTrail = null;
      m.events.emit(EV.POWER_USED, { killer: k, power: this.state === 'punish' ? 'rites' : 'rites_end' });
    }
    if (this.state === 'punish') {
      // lay torment trail
      if (!this.lastTrail || dist(this.lastTrail.x, this.lastTrail.y, k.x, k.y) > 0.8 * M) {
        this.lastTrail = { x: k.x, y: k.y };
        m.addZone(new Zone({ kind: 'torment', x: k.x, y: k.y, r: 0.85 * M, t0: m.time, until: m.time + this.trailLife, owner: k, color: 'rgba(200,60,30,0.28)', aiCost: 9, seenRange: 22 * M, label: 'Torment trail',
          effect: (s, dt2, mm) => { if (!s.status.has('tormented')) { s.status.add('tormented', { duration: 60, source: 'exe' }); mm.fx.text(s.x, s.y - 26, 'TORMENTED', '#e2683c'); mm.loudNoise(s.x, s.y, 'torment'); s.ai?.note('Stepped on a torment trail', 'torment'); } } }));
      }
      if (inp.lmbP && this.cd('wave') <= 0) {
        this.state = 'windup'; this.windT = 0; this.windT0 = m.time; this.lockFacing = false; this.dir = k.facing;
        m.sound('power_windup', k.x, k.y, 30 * M, { what: 'wave', dir: k.facing });
      }
    }
    if (this.state === 'windup') {
      this.windT += dt; this.dir = k.facing;
      if (this.windT >= this.windup) { this.fireWave(m); this.state = 'recover'; this.windT = 0; }
    } else if (this.state === 'recover') { this.windT += dt; if (this.windT > 0.6) this.state = 'punish'; }
  }
  fireWave(m) {
    const k = this.k; this.setCd('wave', 2.2);
    this.wave = { x: k.x, y: k.y, dir: this.dir, d: 0, hit: new Set() };
    m.fx.shake(6); m.audio?.play('wave');
    m.sound('killer_action', k.x, k.y, 36 * M, { what: 'wave' });
  }
  updateWave(dt, m) {
    const w = this.wave; const step = 15 * M * dt; w.d += step;
    const hx = w.x + Math.cos(w.dir) * w.d, hy = w.y + Math.sin(w.dir) * w.d;
    // the wave front is a short segment perpendicular to its travel direction
    for (const s of m.survivors) {
      if (!s.standing || w.hit.has(s)) continue;
      const along = (s.x - w.x) * Math.cos(w.dir) + (s.y - w.y) * Math.sin(w.dir);
      const side = Math.abs(-(s.x - w.x) * Math.sin(w.dir) + (s.y - w.y) * Math.cos(w.dir));
      if (along < w.d + 0.3 * M && along > w.d - step - 0.6 * M && side < 1.25 * M) {
        if (s.dodgeT && s.dodgeT > m.time) continue;
        w.hit.add(s);
        const res = s.takeHit(m, { source: 'wave' }); s.status.add('tormented', { duration: 60, source: 'exe' });
        this.k.stats.hits++; this.k.stats.powerHits++;
        m.events.emit(EV.KILLER_HIT, { killer: this.k, survivor: s, result: res, power: true }); m.fx.hit(s, res);
      }
    }
    m.fx.burst(hx, hy, 2, '#e2683c', 40, 0.4, 3, 2);
    if (w.d >= this.waveLen) this.wave = null;
  }
  context(m) {
    const k = this.k; if (k.carrying) return null;
    for (const s of m.survivors) {
      if (s.health === 'dying' && s.status.has('tormented') && dist(s.x, s.y, k.x, k.y) < 2 * M) {
        return { type: 'custom', label: `Cage of Atonement · ${s.name}`, target: { dur: 1.0, label: 'Caging', tick: () => s.health === 'dying', done: (mm) => { if (mm.cageSurvivor(s)) { mm.addScore('sacrifice', 300, 'Caged'); s.status.remove('tormented'); } } } };
      }
    }
    return null;
  }
  hud() {
    const st = { normal: 'Normal', punish: 'Punishment mode', windup: 'Winding up…', recover: 'Recovering' }[this.state];
    return { title: 'Rites of Judgement', value: this.state === 'normal' ? 'Off' : 'Rites', meter: this.state === 'windup' ? this.windT / this.windup : this.state === 'normal' ? 0 : 1 - this.cd('wave') / 2.2, state: st,
      hints: [['Q / RMB', 'Toggle Punishment mode'], ['LMB', 'In mode: Punishment of the Damned'], ['SPACE', 'Cage a Tormented downed survivor']] };
  }
}

return { ExecutionerPower };
};
__defs["js/killers/singularity.js"] = function(){
// THE SINGULARITY — Quantum Instantiation (adapted to top-down from the current DBD power).
//  • Biopods: hold RMB to charge (0.15 s) and release to SHOOT a biopod (50 m/s, 22 m). It
//    sticks to the first wall / obstacle it hits; missing everything wastes the shot. 8 pods,
//    the 9th recycles the oldest. Reload 0.5 s. A shot that hits a survivor Slipstreams them;
//    hitting someone already Slipstreamed teleports you straight to them.
//  • Pod control: E jumps into the nearest pod to your aim (F cycles pods). Your body stands
//    still. Aim the reticle at a survivor the pod can see (20 m): 0.8 s of lock Slipstreams
//    them (10× slower near a hooked survivor). Locked on an already Slipstreamed survivor,
//    LMB Quantum-Instantiates you to the pod. E leaves the pod (0.5 s wake-up).
//  • Temporal Slipstream lasts until an EMP removes it and spreads to anyone within 6 m for 2 s.
//  • Overclock (5.7 s after a teleport): +3% speed, +75% break/kick/vault speed, immune to
//    pallet stuns — a stun attempt breaks the pallet and Overheats you instead.
//  • Overheat (3 s): Hindered 50%, pods unusable.
//  • EMP printers (4 supply cases): they print slowly by themselves up to 97%; a survivor
//    finishes them (×4). An EMP takes 2.5 s to arm (Hindered 10%) and in 10 m removes
//    Slipstream and disables every pod for 45 s. A disabled pod can be destroyed from inside
//    by holding LMB for 0.75 s.
const { M, dist, angleDiff, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, Device, Projectile } = __req("js/killers/framework.js");

const POD_RANGE = 22 * M, LOCK_RANGE = 20 * M, LOCK_TIME = 0.8, SPREAD_R = 6 * M, SPREAD_T = 2, OVERCLOCK = 5.7, OVERHEAT = 3;
class SingularityPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'quantum'; this.name = 'Quantum Instantiation'; this.state = 'idle';
    this.maxPods = this.has('cold_dirt') ? 10 : 8; this.charge = 0; this.lock = null; this.ctrl = null; this.overclockT = 0; this.overheatT = 0;
    this.spread = new Map(); this.lockCdT = 0; this.wakeT = 0;
    this.lockMult = this.has('denied_requisition') ? 1.25 : 1;
    this.aiProfile = { type: 'surveillance', label: 'Biopods, Slipstream & teleports', counter: ['stay out of pod sightlines', 'spread out (Slipstream spreads)', 'print and use EMPs', 'pallets break on Overclock'] };
  }
  get pods() { return this._m ? this._m.devices.filter((d) => d.kind === 'biopod' && d.owner === this.k) : []; }
  get podsCarried() { return this.maxPods - this.pods.length; } // compatibility (HUD / older AI)
  podActive(p, m) { return !p.dead && (p.data.disabledUntil || 0) <= m.time; }
  speedMult() {
    if (this.state === 'control' || this.state === 'teleporting' || this.wakeT > 0) return 0;
    if (this.state === 'aiming') return 0.85;
    return 1;
  }
  canAttack() { return this.state === 'idle' && this.wakeT <= 0; }
  noContext() { return this.state === 'control'; }
  stunImmune() { return this.overclockT > 0; }
  onStunImmune(m, pallet) { m.breakPallet(pallet, this.k); this.overheat(m); }
  actionMult(type) { return this.overclockT > 0 && ['kick', 'break', 'vault'].includes(type) ? 1 / 1.75 : 1; }
  // pod-control view: the camera and line of sight move to the pod
  viewPoint() { return this.state === 'control' && this.ctrl && !this.ctrl.dead ? { x: this.ctrl.x, y: this.ctrl.y } : null; }
  onInit(m) {
    this._m = m;
    // four supply cases spread across the map
    const cand = m.map.openSpots(16, 8, m.map.gens), spots = [];
    while (spots.length < 4 && cand.length) { cand.sort((a, b) => Math.min(...spots.map((q) => dist(q.x, q.y, b.x, b.y)), 1e9) - Math.min(...spots.map((q) => dist(q.x, q.y, a.x, a.y)), 1e9)); spots.push(cand.shift()); }
    for (const p of spots) {
      const pr = m.addDevice(new Device({ kind: 'printer', x: p.x, y: p.y, r: 0.7 * M, label: 'EMP printer', useLabel: 'Print an EMP', survivorUsable: true, seenRange: 28 * M, useTime: 25, manualComplete: true, data: { charge: Math.random() * 40 } }));
      pr.usable = (s) => !s.emp && !this.disabledPrinter(pr);
      pr.onUse = (s, mm) => { pr.data.charge = 0; s.emp = true; mm.notifyK(`${s.name} printed an EMP`, '#9fd0d0'); s.ai?.note('Printed an EMP', 'emp'); mm.warn(s, 'EMP ready — Q to arm it'); };
    }
  }
  disabledPrinter() { return false; }
  // ------------------------------------------------------------------ per frame
  update(dt, m, inp, locked) {
    this._m = m; this.tickCooldowns(dt);
    const k = this.k;
    this.overclockT = Math.max(0, this.overclockT - dt); this.overheatT = Math.max(0, this.overheatT - dt); this.lockCdT = Math.max(0, this.lockCdT - dt); this.wakeT = Math.max(0, this.wakeT - dt);
    if (this.overclockT > 0) k.status.add('haste', { duration: 0.2, value: 0.03, source: 'overclock' });
    if (this.state === 'teleporting') { this.tpT -= dt; if (this.tpT <= 0) this.finishTeleport(m); return; }
    if (locked) { if (this.state === 'aiming') this.state = 'idle'; if (this.state === 'control') this.leaveControl(m); return; }
    if (this.state === 'control') return this.updateControl(dt, m, inp);
    // shoot pods
    if (this.state === 'idle' && inp.rmb && !k.carrying && this.overheatT <= 0 && this.cd('reload') <= 0) { this.state = 'aiming'; this.charge = 0; }
    if (this.state === 'aiming') {
      this.charge += dt;
      if (!inp.rmb) { if (this.charge >= 0.15) this.shoot(m); this.state = 'idle'; }
    }
    if (inp.eP && !k.carrying && this.overheatT <= 0 && this.state === 'idle') this.enterControl(m, inp);
  }
  // Always-running clocks (also while stunned / acting): printers, Slipstream spread, pods.
  tick(dt, m) {
    for (const pr of m.devices) if (pr.kind === 'printer') {
      const users = m.survivors.filter((s) => s.action?.type === 'device' && s.action.target === pr).length;
      const c = pr.data.charge; pr.data.charge = users ? Math.min(100, c + 4 * dt) : c < 97 ? Math.min(97, c + dt) : c;
      pr.useTime = Math.max(0.3, (100 - pr.data.charge) / 4); // remaining time (AI estimates)
      if (users && pr.data.charge >= 100) { const s = m.survivors.find((q) => q.action?.type === 'device' && q.action.target === pr); s.stopAction(m); pr.onUse(s, m); m.events.emit('deviceUsed', { survivor: s, device: pr }); }
    }
    // Slipstream spreads to survivors standing near a Slipstreamed one for 2 s
    const slipped = m.survivors.filter((s) => s.standing && s.status.has('slipstream'));
    for (const s of m.survivors) {
      if (!s.standing || s.status.has('slipstream') || (s.slipImmuneUntil || 0) > m.time) { this.spread.delete(s); continue; }
      if (slipped.some((o) => dist(o.x, o.y, s.x, s.y) < SPREAD_R)) {
        const t = (this.spread.get(s) || 0) + dt; this.spread.set(s, t);
        if (t >= SPREAD_T) { this.slipstream(m, s, 'spread'); this.spread.delete(s); }
      } else this.spread.delete(s);
    }
  }
  // ------------------------------------------------------------------ pods
  shoot(m) {
    const k = this.k; this.setCd('reload', 0.5); const a = k.facing;
    m.audio?.play('throw'); m.sound('killer_action', k.x, k.y, 20 * M, { what: 'biopod' });
    m.spawnProjectile(new Projectile({
      kind: 'biopod_shot', owner: k, x: k.x + Math.cos(a) * k.r, y: k.y + Math.sin(a) * k.r, vx: Math.cos(a) * 50 * M, vy: Math.sin(a) * 50 * M, r: 0.15 * M, range: POD_RANGE, color: '#5fe0e0',
      onHit: (p, s, mm) => { if (s.status.has('slipstream')) this.teleportTo(mm, s, null); else this.slipstream(mm, s, 'shot'); },
      onWall: (p, mm) => this.attachPod(mm, p.x - Math.cos(a) * 0.35 * M, p.y - Math.sin(a) * 0.35 * M),
      onEnd: (p, mm) => { mm.fx.text(p.x, p.y - 20, 'no surface', '#7fb8b8'); },
    }));
    m.events.emit(EV.POWER_USED, { killer: k, power: 'pod' });
  }
  attachPod(m, x, y) {
    // spacing rules: not on top of generators, windows or other pods
    if (m.map.gens.some((g) => dist(g.x, g.y, x, y) < 2.5 * M) || m.map.windows.some((w) => dist(w.x, w.y, x, y) < 2.5 * M) || this.pods.some((p) => dist(p.x, p.y, x, y) < 5 * M)) { m.fx.text(x, y - 20, 'too close', '#7fb8b8'); return; }
    const pods = this.pods; if (pods.length >= this.maxPods) m.removeDevice(pods[0]);
    m.addDevice(new Device({ kind: 'biopod', x, y, r: 0.35 * M, owner: this.k, label: 'Biopod', seenRange: 18 * M, dangerRadius: LOCK_RANGE * 0.6, aiCost: 4, data: { placedT: m.time, disabledUntil: 0 } }));
    m.fx.burst(x, y, 10, '#5fe0e0', 60, 0.5, 2, 2);
  }
  enterControl(m, inp) {
    const pods = this.pods.filter((p) => !p.dead); if (!pods.length) { m.fx.text(this.k.x, this.k.y - 40, 'No biopods', '#9fd0d0'); return; }
    const ax = inp.aimX ?? this.k.x + Math.cos(this.k.facing) * (inp.aimDist ?? 10 * M), ay = inp.aimY ?? this.k.y + Math.sin(this.k.facing) * (inp.aimDist ?? 10 * M);
    this.ctrl = pods.sort((a, b) => dist(a.x, a.y, ax, ay) - dist(b.x, b.y, ax, ay))[0];
    this.state = 'control'; this.lock = null; this.ctrlT = 0; this.k.vx = this.k.vy = 0;
    m.events.emit(EV.POWER_USED, { killer: this.k, power: 'pod_control' });
  }
  leaveControl(m) { this.state = 'idle'; this.ctrl = null; this.lock = null; this.wakeT = 0.5; }
  cyclePod(dir = 1) { const pods = this.pods; if (pods.length < 2) return; const i = pods.indexOf(this.ctrl); this.ctrl = pods[(i + dir + pods.length) % pods.length]; this.lock = null; }
  // reticle: what the pod is pointing at (mouse in world space for the player, aim for bots)
  reticle(inp) {
    const c = this.ctrl;
    if (inp.aimX !== undefined) return { x: inp.aimX, y: inp.aimY };
    return { x: this.k.x + Math.cos(inp.aim) * (inp.aimDist ?? 10 * M), y: this.k.y + Math.sin(inp.aim) * (inp.aimDist ?? 10 * M), c };
  }
  updateControl(dt, m, inp) {
    const c = this.ctrl; this.ctrlT += dt;
    if (!c || c.dead || inp.eP) { this.leaveControl(m); return; }
    if (inp.cycleP || inp.wheel) this.cyclePod(inp.wheel > 0 ? 1 : -1);
    const r = this.reticle(inp);
    // disabled pod: hold LMB 0.75 s to destroy it from inside
    if (!this.podActive(c, m)) {
      this.lock = null; c.data.destroyT = inp.lmb ? (c.data.destroyT || 0) + dt : 0;
      if (c.data.destroyT >= 0.75) { m.removeDevice(c); this.leaveControl(m); }
      return;
    }
    // candidate: survivor nearest the reticle that the pod can see
    let best = null, bd = 1.8 * M;
    for (const s of m.survivors) {
      if (!s.standing) continue;
      const dr = dist(s.x, s.y, r.x, r.y); if (dr > bd) continue;
      if (dist(s.x, s.y, c.x, c.y) > LOCK_RANGE || !m.world.losClear(c.x, c.y, s.x, s.y)) continue;
      bd = dr; best = s;
    }
    if (this.lockCdT > 0) best = null;
    if (best) {
      const nearHook = m.survivors.some((h) => h.health === 'hooked' && dist(h.x, h.y, best.x, best.y) < 10 * M);
      if (!this.lock || this.lock.s !== best) this.lock = { s: best, t: 0, lost: 0 };
      this.lock.t += dt * this.lockMult / (nearHook ? 10 : 1); this.lock.lost = 0;
      if (this.lock.t >= LOCK_TIME && !best.status.has('slipstream')) { this.slipstream(m, best, 'pod'); this.lockCdT = 3; this.lock = null; }
      else if (this.lock && this.lock.t >= LOCK_TIME && best.status.has('slipstream') && inp.lmbP) this.teleportTo(m, best, c);
    } else if (this.lock) {
      this.lock.lost += dt; if (this.lock.lost > 0.25) { this.lock.t -= dt / 0.5 * LOCK_TIME; if (this.lock.t <= 0) this.lock = null; }
    }
  }
  slipstream(m, s, how) {
    if (s.status.has('slipstream')) return;
    s.status.add('slipstream', { source: 'sing' });
    m.reveal(s, 3, 'Killer Instinct'); // Killer Instinct for 3 s
    m.fx.ring(s.x, s.y, 'rgba(95,224,224,0.9)', 40, 0.6); m.addScore('deviousness', 200, 'Slipstream');
    s.ai?.note(how === 'spread' ? 'Slipstream spread to me' : how === 'shot' ? 'Shot with a biopod — Slipstreamed' : 'Tagged by a biopod — Slipstreamed', 'slip');
    m.warn(s, 'You are Slipstreamed — the Singularity can teleport to you');
    m.events.emit('powerTriggered', { survivor: s, x: s.x, y: s.y, killer: this.k });
  }
  // Quantum Instantiation: to the controlled pod (pod lock) or to the survivor (direct shot)
  teleportTo(m, s, pod) {
    const k = this.k; const at = pod || s;
    this.tpDest = { x: at.x, y: at.y, s }; this.tpT = 0.3; this.state = 'teleporting'; this.ctrl = null; this.lock = null;
    m.sound('teleport_warn', at.x, at.y, 26 * M, { pod, killer: k });
    m.fx.ring(at.x, at.y, 'rgba(95,224,224,0.9)', 3 * M, 0.5);
  }
  finishTeleport(m) {
    const k = this.k, d = this.tpDest; this.state = 'idle';
    // land on open ground next to the destination, facing the target
    let x = d.x, y = d.y; const s = d.s;
    for (let i = 0; i < 12; i++) { const a = (s ? Math.atan2(k.y - s.y, k.x - s.x) : 0) + i * 0.5; const tx = d.x + Math.cos(a) * 1.4 * M, ty = d.y + Math.sin(a) * 1.4 * M; if (!m.world.pointBlocked(tx, ty, k.r)) { x = tx; y = ty; break; } }
    m.fx.burst(k.x, k.y, 20, '#5fe0e0', 120, 0.5, 2, 2);
    k.x = x; k.y = y; m.world.resolve(k, k.r); if (s) k.facing = Math.atan2(s.y - k.y, s.x - k.x);
    this.wakeT = 0.6; this.overclockT = OVERCLOCK; this.lockCdT = 3; this.setCd('tp', 2);
    m.fx.burst(k.x, k.y, 40, '#5fe0e0', 220, 0.8, 3, 3); m.audio?.play('teleport'); m.fx.shake(6);
    m.sound('killer_action', k.x, k.y, 36 * M, { what: 'teleport' });
    m.events.emit(EV.POWER_USED, { killer: k, power: 'teleport' });
  }
  overheat(m) { this.overclockT = 0; this.overheatT = OVERHEAT; this.k.status.add('hindered', { duration: OVERHEAT, value: 0.5, source: 'overheat' }); m.fx.text(this.k.x, this.k.y - 40, 'OVERHEAT', '#ff8a4a'); if (this.state === 'control') this.leaveControl(m); }
  // ------------------------------------------------------------------ EMP
  useEmp(s, m) {
    let n = 0;
    for (const pod of this.pods) if (dist(pod.x, pod.y, s.x, s.y) < 10 * M && this.podActive(pod, m)) { pod.data.disabledUntil = m.time + (this.has('broken_key') ? 38 : 45); n++; m.fx.sparks(pod.x, pod.y); if (pod === this.ctrl) this.leaveControl(m); }
    for (const o of m.survivors) if (dist(o.x, o.y, s.x, s.y) < 10 * M) { o.status.remove('slipstream'); o.slipImmuneUntil = m.time + 1; }
    m.fx.ring(s.x, s.y, 'rgba(160,240,255,0.9)', 10 * M, 0.8); m.audio?.play('emp');
    m.notifyK(`EMP disabled ${n} biopod${n === 1 ? '' : 's'}`, '#9fd0d0'); s.ai?.note(`Used an EMP (${n} pods disabled)`, 'empuse');
    s.status.remove('hindered', 'emp');
  }
  // compatibility for the survivor AI (pod knowledge / threat of a teleport)
  teleportTarget(m) { const s = m.survivors.find((q) => q.standing && q.status.has('slipstream')); return s ? { s, pod: this.pods[0] } : null; }
  // ------------------------------------------------------------------ drawing
  drawDevice(ctx, d, m) {
    if (d.kind === 'biopod' && d.owner === this.k) {
      const on = this.podActive(d, m);
      ctx.fillStyle = '#20343c'; ctx.beginPath(); ctx.arc(d.x, d.y, d.r + 3, 0, Math.PI * 2); ctx.fill();
      ctx.fillStyle = on ? (d === this.ctrl ? '#d8ffff' : '#5fe0e0') : '#a03a3a'; ctx.beginPath(); ctx.arc(d.x, d.y, d.r * 0.6, 0, Math.PI * 2); ctx.fill();
      if (on) { ctx.strokeStyle = 'rgba(95,224,224,0.10)'; ctx.lineWidth = 1; ctx.beginPath(); ctx.arc(d.x, d.y, LOCK_RANGE, 0, Math.PI * 2); ctx.stroke(); }
    } else if (d.kind === 'printer') {
      ctx.fillStyle = '#2c3a40'; ctx.fillRect(d.x - 14, d.y - 10, 28, 20); ctx.fillStyle = 'rgba(0,0,0,0.6)'; ctx.fillRect(d.x - 12, d.y + 12, 24, 4);
      ctx.fillStyle = d.data.charge >= 97 ? '#ffd27a' : '#9fe8ff'; ctx.fillRect(d.x - 12, d.y + 12, 24 * d.data.charge / 100, 4); ctx.fillRect(d.x - 8, d.y - 4, 16, 3);
    }
  }
  drawOver(ctx, m) {
    const k = this.k;
    if (this.state === 'aiming') { ctx.strokeStyle = 'rgba(95,224,224,0.5)'; ctx.setLineDash([5, 6]); ctx.lineWidth = 1.5; ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.lineTo(k.x + Math.cos(k.facing) * POD_RANGE, k.y + Math.sin(k.facing) * POD_RANGE); ctx.stroke(); ctx.setLineDash([]); }
    if (this.state === 'control' && this.ctrl) {
      const c = this.ctrl; ctx.strokeStyle = 'rgba(95,224,224,0.35)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(c.x, c.y, LOCK_RANGE, 0, Math.PI * 2); ctx.stroke();
      const r = this.reticle(k.input);
      ctx.strokeStyle = this.lock ? '#ff5b6b' : '#d8ffff'; ctx.beginPath(); ctx.arc(r.x, r.y, 16, 0, Math.PI * 2); ctx.moveTo(r.x - 24, r.y); ctx.lineTo(r.x + 24, r.y); ctx.moveTo(r.x, r.y - 24); ctx.lineTo(r.x, r.y + 24); ctx.stroke();
      if (this.lock) { const s = this.lock.s; ctx.strokeStyle = '#ff5b6b'; ctx.lineWidth = 3; ctx.beginPath(); ctx.arc(s.x, s.y, s.r + 10, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * Math.min(1, this.lock.t / LOCK_TIME)); ctx.stroke(); }
    }
    if (this.state === 'teleporting' && this.tpDest) { ctx.strokeStyle = 'rgba(95,224,224,0.8)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.lineTo(this.tpDest.x, this.tpDest.y); ctx.stroke(); }
  }
  hud(m) {
    const slip = m ? m.survivors.filter((s) => s.standing && s.status.has('slipstream')).length : 0;
    const st = this.state === 'control' ? (this.ctrl && !this.podActive(this.ctrl, m) ? 'Pod disabled — hold LMB to destroy' : this.lock ? (this.lock.s.status.has('slipstream') && this.lock.t >= LOCK_TIME ? `LMB: teleport to ${this.lock.s.name}` : `Locking ${this.lock.s.name}…`) : 'Pod view — aim at a survivor')
      : this.overheatT > 0 ? `OVERHEAT ${Math.ceil(this.overheatT)}s` : this.overclockT > 0 ? `OVERCLOCK ${this.overclockT.toFixed(1)}s` : `${slip} slipstreamed`;
    return { title: 'Quantum Instantiation', value: `${this.podsCarried}/${this.maxPods}`, meter: this.overheatT > 0 ? 1 - this.overheatT / OVERHEAT : this.state === 'control' && this.lock ? Math.min(1, this.lock.t / LOCK_TIME) : this.podsCarried / this.maxPods, state: st,
      hints: this.state === 'control' ? [['Aim', 'Lock on (0.8 s)'], ['LMB', 'Teleport to a locked Slipstreamed survivor'], ['F / Wheel', 'Next pod'], ['E', 'Leave the pod']]
        : [['Q / RMB', 'Hold & release: shoot a biopod onto a wall'], ['E', 'Control a biopod']] };
  }
}

return { SingularityPower };
};
__defs["js/killers/huntress.js"] = function(){
// THE HUNTRESS — Hunting Hatchets. Hold the power to wind up, release to throw.
// Longer wind-ups throw faster and further. Hatchets fly over pallets and windows but
// stop on walls, rocks and trees. Limited hatchets; reload at lockers (E).
const { M, dist, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, Projectile, Device, nearestOf } = __req("js/killers/framework.js");

class HuntressPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'hatchets'; this.name = 'Hunting Hatchets'; this.state = 'idle';
    this.max = this.has('leather_loop') ? 7 : 5; this.count = this.max; this.charge = 0;
    this.minWind = 0.35; this.maxWind = this.has('shiny_pin') ? 1.0 : 1.3; this.range = (this.has('flower_babushka') ? 36 : 30) * M;
    this.aiProfile = { type: 'ranged', label: 'Hatchet throws', counter: ['break line of sight', 'avoid long open runs', 'dodge when she winds up'] };
  }
  speedMult() { return this.state === 'winding' ? 0.62 : this.state === 'recover' ? 0.5 : 1; }
  canAttack() { return this.state === 'idle'; }
  onInit(m) {
    for (const p of m.map.openSpots(5, 24, m.map.gens)) m.addDevice(new Device({ kind: 'locker', x: p.x, y: p.y, r: 0.7 * M, label: 'Locker', seenRange: 20 * M }));
  }
  update(dt, m, inp, locked) {
    this.tickCooldowns(dt);
    const k = this.k;
    if (locked) { if (this.state === 'winding') this.state = 'idle'; return; }
    if (this.state === 'recover') { if (this.cd('recover') <= 0) this.state = 'idle'; }
    if (this.state === 'idle' && inp.rmb && !k.carrying && this.count > 0) {
      this.state = 'winding'; this.charge = 0;
      m.sound('power_windup', k.x, k.y, 26 * M, { what: 'hatchet' });
    }
    if (this.state === 'winding') {
      this.charge = Math.min(this.maxWind, this.charge + dt);
      if (!inp.rmb) { if (this.charge >= this.minWind) this.throw(m); this.state = this.charge >= this.minWind ? 'recover' : 'idle'; this.setCd('recover', 0.55); }
    }
    if (inp.eP && !k.carrying && this.count < this.max) {
      const locker = nearestOf(m.devices.filter((d) => d.kind === 'locker'), k.x, k.y, 1.9 * M);
      if (locker) k.startAction('custom', { dur: 2.4, label: 'Reloading hatchets', done: () => { this.count = this.max; m.audio?.play('reload'); } }, m);
    }
  }
  throw(m) {
    const k = this.k, f = (this.charge - this.minWind) / (this.maxWind - this.minWind);
    const speed = (22 + 20 * clamp(f, 0, 1)) * M; this.count--;
    const a = k.facing; const sx = k.x + Math.cos(a) * k.r, sy = k.y + Math.sin(a) * k.r;
    m.spawnProjectile(new Projectile({
      kind: 'hatchet', owner: k, x: sx, y: sy, vx: Math.cos(a) * speed, vy: Math.sin(a) * speed, r: 0.22 * M, range: this.range, color: '#cfd3d6',
      onHit: (p, s, mm) => {
        if (s.dodgeT && s.dodgeT > mm.time) { mm.fx.text(s.x, s.y - 30, 'DODGED', '#ffd27a'); return; }
        const res = s.takeHit(mm, { source: 'hatchet' }); k.stats.hits++; k.stats.powerHits++; k.bloodlust = 0;
        mm.events.emit(EV.KILLER_HIT, { killer: k, survivor: s, result: res, power: true }); mm.fx.hit(s, res);
        mm.addScore('hunter', 300, 'Hatchet hit'); s.ai?.note('Hit by a hatchet', 'hatchet');
      },
      onWall: (p, mm) => { mm.fx.sparks(p.x, p.y); },
    }));
    m.audio?.play('throw'); m.events.emit(EV.POWER_USED, { killer: k, power: 'hatchet' });
  }
  prompt(m) {
    const locker = nearestOf(m.devices.filter((d) => d.kind === 'locker'), this.k.x, this.k.y, 1.9 * M);
    return locker && this.count < this.max ? { key: 'E', label: 'Reload hatchets' } : null;
  }
  hud() {
    return { title: 'Hunting Hatchets', value: `${this.count}/${this.max}`, meter: this.state === 'winding' ? this.charge / this.maxWind : this.count / this.max,
      state: this.state === 'winding' ? (this.charge >= this.minWind ? 'Release to throw' : 'Winding up…') : this.count ? 'Ready' : 'Out of hatchets — find a locker',
      hints: [['Q / RMB', 'Hold to wind up, release to throw'], ['E', 'Reload at a locker']] };
  }
}

return { HuntressPower };
};
__defs["js/killers/legion.js"] = function(){
// THE LEGION — Feral Frenzy. With a full power gauge, RMB starts a Frenzy: much faster,
// fast vaults over windows AND dropped pallets, and LMB becomes a quick Frenzy slash.
// A Frenzy hit injures a healthy survivor and always inflicts Deep Wound, but never downs —
// on an already injured survivor it eats into their Deep Wound timer instead. Every hit
// refills the gauge and reveals other survivors in the terror radius (Killer Instinct),
// so the Legion chains from survivor to survivor; the same survivor cannot be hit twice
// in a row. A miss, or the gauge running dry, ends the Frenzy in Fatigue.
const { M, dist, angleDiff } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, killerInstinct } = __req("js/killers/framework.js");

class LegionPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'frenzy'; this.name = 'Feral Frenzy'; this.state = 'ready';
    this.gauge = 1; this.lastHit = null; this.slashT = 0; this.fatigueT = 0;
    this.frenzySpeed = (this.has('stab_wounds') ? 5.4 : 5.2) * M;
    this.drain = this.has('mural_sketch') ? 0.085 : 0.1;          // gauge per second in Frenzy (~10 s)
    this.recharge = this.has('never_sleep') ? 1 / 16 : 1 / 20;   // gauge per second while recharging
    this.fatigueTime = this.has('cold_dirt') ? 3.2 : 4;
    this.aiProfile = { type: 'frenzy', label: 'Feral Frenzy', counter: ['spread out', 'mend deep wounds', 'do not group up while he frenzies'] };
  }
  get frenzy() { return this.state === 'frenzy'; }
  speedMult() {
    if (this.state === 'frenzy') return this.frenzySpeed / (this.k.def.speed * M);
    if (this.state === 'fatigue') return 0.25;
    return 1;
  }
  canAttack() { return this.state === 'ready' || this.state === 'recharge'; }
  actionMult(type, target) { return this.state === 'frenzy' && type === 'vault' ? (target.kind === 'pallet' ? 0.35 : 0.3) : 1; }
  // During Frenzy only vaulting is possible (windows and dropped pallets alike).
  context(m) {
    if (this.state !== 'frenzy') return null;
    const k = this.k;
    for (const p of m.map.pallets) if (p.state === 'down' && dist(k.x, k.y, p.x, p.y) < Math.max(p.len / 2, 1.2 * M) + 0.6 * M && Math.abs((k.x - p.x) * p.nx + (k.y - p.y) * p.ny) < 1.6 * M) return { type: 'vault', target: p, label: 'Vault pallet (Frenzy)' };
    for (const w of m.map.windows) if (dist(k.x, k.y, w.x, w.y) < 1.7 * M && Math.abs((k.x - w.x) * w.nx + (k.y - w.y) * w.ny) > 0.4 * M) return { type: 'vault', target: w, label: 'Vault (Frenzy)' };
    return null;
  }
  noContext() { return this.state === 'frenzy'; }
  update(dt, m, inp, locked) {
    this.tickCooldowns(dt);
    const k = this.k;
    if (this.state === 'fatigue') { this.fatigueT -= dt; if (this.fatigueT <= 0) this.state = 'recharge'; return; }
    if (this.state === 'recharge') { this.gauge = Math.min(1, this.gauge + dt * this.recharge); if (this.gauge >= 1) this.state = 'ready'; }
    if (this.state === 'frenzy') {
      this.gauge -= dt * this.drain;
      // auto-continue through vaults; Frenzy pauses its drain while vaulting
      if (k.action?.type === 'vault') this.gauge += dt * this.drain * 0.8;
      if (this.gauge <= 0) { this.endFrenzy(m, 'Frenzy ended'); return; }
      if (this.slashT > 0) { this.slashT -= dt; this.trySlash(m); if (this.slashT <= 0 && !this.slashHit) this.endFrenzy(m, 'Missed — Fatigue'); }
    }
    if (locked) return;
    if (inp.rmbP && this.state === 'ready' && !k.carrying) {
      this.state = 'frenzy'; this.lastHit = null; this.hits = 0;
      m.sound('power_windup', k.x, k.y, 32 * M, { what: 'frenzy' }); m.sound('killer_action', k.x, k.y, 24 * M, { what: 'frenzy' });
      m.events.emit(EV.POWER_USED, { killer: k, power: 'frenzy' }); m.fx.text(k.x, k.y - 40, 'FERAL FRENZY', '#d8e05a');
      return;
    }
    if (this.state === 'frenzy' && inp.lmbP && this.slashT <= 0 && !k.action) {
      this.slashT = 0.28; this.slashHit = false; this.slashDir = k.facing; m.audio?.play('swing'); m.fx.swing(k);
    }
  }
  trySlash(m) {
    if (this.slashHit) return;
    const k = this.k;
    for (const s of m.survivors) {
      if (!s.standing || s === this.lastHit) continue;
      const d = dist(k.x, k.y, s.x, s.y); if (d > k.r + s.r + 1.15 * M) continue;
      if (angleDiff(Math.atan2(s.y - k.y, s.x - k.x), this.slashDir) > 0.95 && d > k.r + s.r + 4) continue;
      if (!m.world.losClear(k.x, k.y, s.x, s.y)) continue;
      if (s.dodgeT && s.dodgeT > m.time) { m.fx.text(s.x, s.y - 30, 'DODGED', '#ffd27a'); continue; }
      this.frenzyHit(m, s); this.slashHit = true; this.slashT = 0; return;
    }
  }
  frenzyHit(m, s) {
    const k = this.k; this.lastHit = s; this.hits++;
    let res;
    if (s.health === 'healthy' && !s.status.has('exposed')) res = s.takeHit(m, { source: 'frenzy' });
    else {
      // injured (or exposed): never downs — the Deep Wound timer takes the damage instead
      s.stopAction(m); s.vault = null; s.lastHitT = m.time;
      s.status.add('haste', { duration: 1.6, value: 0.5, source: 'hit' });
      if (s.health === 'healthy') { s.health = 'injured'; m.events.emit(EV.SURVIVOR_INJURED, { survivor: s, source: 'frenzy' }); }
      res = 'injured';
    }
    const dw = s.status.get('deepWound');
    if (dw && s.status.has('deepWoundFrenzy')) dw.expiration = Math.max(1.5, dw.expiration - 10);
    else s.status.add('deepWound', { duration: this.has('filthy_blade') ? 16 : 20 });
    s.status.add('deepWoundFrenzy', { duration: 30 }); s.mend = 0;
    k.stats.hits++; k.stats.powerHits++; k.chaseT = 0;
    m.events.emit(EV.KILLER_HIT, { killer: k, survivor: s, result: res, power: true }); m.fx.hit(s, res);
    m.addScore('brutality', 250, 'Frenzy hit');
    s.ai?.note('Hit by the Legion\'s Frenzy — Deep Wound', 'frenzyhit');
    this.gauge = Math.min(1, this.gauge + 0.3);
    // Killer Instinct on everyone else in the terror radius: find the next victim
    for (const o of m.survivors) if (o !== s && o.standing && dist(o.x, o.y, k.x, k.y) < k.terrorRadius) killerInstinct(m, o, 3, 'Frenzy');
    if (this.hits >= 4 && this.has('iridescent_button')) this.gauge = Math.min(1, this.gauge + 0.2);
  }
  endFrenzy(m, why) {
    this.state = 'fatigue'; this.fatigueT = this.fatigueTime; this.gauge = Math.max(0, this.gauge); this.slashT = 0;
    m.fx.text(this.k.x, this.k.y - 40, why.toUpperCase(), '#c9b04a'); m.events.emit(EV.POWER_USED, { killer: this.k, power: 'frenzy_end' });
  }
  onStun(m) { if (this.state === 'frenzy') this.endFrenzy(m, 'Stunned'); }
  drawWeapon(ctx) {
    const k = this.k; ctx.fillStyle = '#c9ccd0'; ctx.beginPath(); ctx.moveTo(k.r - 2, 5); ctx.lineTo(k.r + 13, 8); ctx.lineTo(k.r - 2, 10); ctx.closePath(); ctx.fill();
    if (this.slashT > 0) { ctx.strokeStyle = 'rgba(216,224,90,0.7)'; ctx.lineWidth = 3; ctx.beginPath(); ctx.arc(0, 0, k.r + 22, -0.8, 0.8); ctx.stroke(); }
    return true;
  }
  drawBody(ctx) { const k = this.k; ctx.fillStyle = '#e8e4d8'; ctx.beginPath(); ctx.arc(6, 0, 5, 0, Math.PI * 2); ctx.fill(); ctx.fillStyle = '#9a2030'; ctx.fillRect(7, -1, 3, 2); if (this.frenzy) { ctx.strokeStyle = 'rgba(216,224,90,0.5)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(0, 0, k.r + 4, 0, Math.PI * 2); ctx.stroke(); } }
  hud() {
    const st = { ready: 'Ready — RMB for Frenzy', frenzy: `FRENZY · ${this.hits || 0} hit${this.hits === 1 ? '' : 's'}`, fatigue: 'Fatigued', recharge: 'Recharging' }[this.state];
    return { title: 'Feral Frenzy', value: Math.round(this.gauge * 100) + '%', meter: this.gauge, state: st,
      hints: [['Q / RMB', 'Feral Frenzy (full gauge)'], ['LMB', 'In Frenzy: Frenzy slash (Deep Wound, chains)'], ['SPACE', 'In Frenzy: vault windows and dropped pallets']] };
  }
}

return { LegionPower };
};
__defs["js/killers/hag.js"] = function(){
// THE HAG — Blackened Catalyst. Hold RMB to draw a Phantasm Trap on the ground (max 10).
// A survivor who walks or runs over a trap (crouching does not trigger it) sets it off:
// a mud phantasm rises, the survivor screams and is revealed, and for a few seconds the Hag
// can press E to teleport to it if she is within 40 m. Triggered traps are consumed.
// Survivors can burn traps with a flashlight or sneak past by crouching.
const { M, dist } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, Device, nearestOf } = __req("js/killers/framework.js");

class HagPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'phantasm'; this.name = 'Blackened Catalyst'; this.state = 'idle';
    this.maxTraps = this.has('rusty_shackles') ? 12 : 10; this.drawT = 0; this.drawTime = this.has('bloodied_mud') ? 1.1 : 1.5;
    this.tpRange = (this.has('mint_rag') ? 50 : 40) * M; this.tpWindow = this.has('dried_cicada') ? 6 : 4;
    this.triggered = []; this.aiProfile = { type: 'traps_teleport', label: 'Phantasm traps', counter: ['crouch over known traps', 'burn traps with a flashlight', 'avoid trapped loops'] };
  }
  get traps() { return this._m ? this._m.devices.filter((d) => d.kind === 'phantasm') : []; }
  speedMult() { return this.state === 'drawing' ? 0 : 1; }
  canAttack() { return this.state === 'idle'; }
  onInit(m) { this._m = m; }
  update(dt, m, inp, locked) {
    this._m = m; this.tickCooldowns(dt);
    const k = this.k;
    this.triggered = this.triggered.filter((t) => m.time - t.t < this.tpWindow);
    if (locked) { if (this.state === 'drawing') this.state = 'idle'; return; }
    if (this.state === 'idle' && inp.rmb && !k.carrying) { this.state = 'drawing'; this.drawT = 0; }
    if (this.state === 'drawing') {
      this.drawT += dt;
      if (!inp.rmb) { this.state = 'idle'; return; }
      if (this.drawT >= this.drawTime) { this.placeTrap(m, k.x, k.y); this.state = 'cooldown'; this.setCd('draw', 0.6); }
    }
    if (this.state === 'cooldown' && this.cd('draw') <= 0 && !inp.rmb) this.state = 'idle';
    if (inp.eP && !k.carrying) {
      const t = this.teleportTarget(m);
      if (t) this.teleport(m, t);
    }
  }
  placeTrap(m, x, y) {
    const traps = this.traps; if (traps.length >= this.maxTraps) m.removeDevice(traps[0]);
    m.addDevice(new Device({ kind: 'phantasm', x, y, r: 0.9 * M, owner: this.k, label: 'Phantasm Trap', seenRange: (this.has('scarred_hand') ? 4 : 6) * M, aiCost: 10, dangerRadius: 1.4 * M,
      data: { t0: m.time, armed: false } }));
    m.events.emit(EV.POWER_USED, { killer: this.k, power: 'trap' }); m.audio?.play('trapset');
  }
  // Called from the match each frame: survivors stepping over a trap trigger it.
  tick(dt, m) {
    for (const d of this.traps) {
      if (m.time - d.data.t0 < 1) continue;
      for (const s of m.survivors) {
        if (!s.standing || s.moveMode === 'crouch' && !s.vault) continue;
        if (dist(s.x, s.y, d.x, d.y) > d.r) continue;
        this.trigger(m, d, s); break;
      }
    }
  }
  trigger(m, d, s) {
    m.removeDevice(d);
    this.triggered.push({ x: d.x, y: d.y, t: m.time, s });
    m.reveal(s, 3, 'Phantasm'); m.addGrunt(s, '#7a9a5a'); m.audio?.play('scream');
    m.fx.burst(d.x, d.y, 26, '#4a5a3a', 90, 1.2, 3, 1.5);
    m.notifyK(`${s.name} triggered a Phantasm Trap — E to teleport`, '#a9c27a');
    m.sound('power_warn', d.x, d.y, 18 * M, { what: 'hag', note: 'Triggered a Phantasm — she can teleport here' });
    m.events.emit(EV.TRAP_TRIGGERED, { trap: d, survivor: s, phantasm: true });
    s.ai?.onPhantasm?.(d);
    (s.ai?.memory.devices)?.delete(d.id);
  }
  teleportTarget(m) {
    const k = this.k; let best = null, bt = -1;
    for (const t of this.triggered) { const d = dist(k.x, k.y, t.x, t.y); if (d <= this.tpRange && t.t > bt) { bt = t.t; best = t; } }
    return best;
  }
  teleport(m, t) {
    const k = this.k; this.triggered = this.triggered.filter((q) => q !== t);
    // appear just behind the phantasm (away from where she came from)
    const a = Math.atan2(t.y - k.y, t.x - k.x); let x = t.x + Math.cos(a) * 0.6 * M, y = t.y + Math.sin(a) * 0.6 * M;
    if (m.world.pointBlocked(x, y, k.r)) { x = t.x; y = t.y; }
    m.fx.burst(k.x, k.y, 20, '#3a4a2a', 80, 0.8, 3, 2);
    k.x = x; k.y = y; m.world.resolve(k, k.r); k.facing = a;
    m.fx.burst(k.x, k.y, 30, '#5a6a3a', 120, 0.9, 3, 2); m.audio?.play('teleport'); m.fx.shake(5);
    m.sound('killer_action', k.x, k.y, 24 * M, { what: 'teleport' });
    m.events.emit(EV.POWER_USED, { killer: k, power: 'teleport' });
  }
  // flashlights burn Phantasm Traps
  burnAt(m, x, y, r) { for (const d of this.traps) if (dist(d.x, d.y, x, y) < r) { m.removeDevice(d); m.fx.burst(d.x, d.y, 12, '#e8e0a0', 60, 0.6, 2, 2); return true; } return false; }
  drawDevice(ctx, d, m) {
    if (d.kind !== 'phantasm') return;
    const a = m.time - d.data.t0 < 1 ? 0.35 : 0.8; ctx.strokeStyle = `rgba(120,150,90,${a})`; ctx.lineWidth = 2;
    ctx.beginPath(); ctx.arc(d.x, d.y, d.r * 0.8, 0, Math.PI * 2); ctx.stroke();
    ctx.beginPath(); for (let i = 0; i < 5; i++) { const q = i * 2.513 - Math.PI / 2; const x = d.x + Math.cos(q) * d.r * 0.8, y = d.y + Math.sin(q) * d.r * 0.8; if (i) ctx.lineTo(x, y); else ctx.moveTo(x, y); } ctx.closePath(); ctx.stroke();
  }
  drawGround(ctx, m) {
    for (const t of this.triggered) {
      const f = 1 - (m.time - t.t) / this.tpWindow; ctx.fillStyle = `rgba(60,70,40,${0.8 * f})`;
      ctx.beginPath(); ctx.ellipse(t.x, t.y, 10, 16, 0, 0, Math.PI * 2); ctx.fill();
      ctx.strokeStyle = `rgba(169,194,122,${f})`; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(t.x, t.y, 22, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * f); ctx.stroke();
    }
  }
  drawOver(ctx) {
    if (this.state !== 'drawing') return; const k = this.k;
    ctx.strokeStyle = 'rgba(169,194,122,0.8)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(k.x, k.y, 0.8 * M, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * this.drawT / this.drawTime); ctx.stroke();
  }
  drawWeapon(ctx) { const k = this.k; ctx.strokeStyle = '#6a5a40'; ctx.lineWidth = 2; for (let i = -1; i <= 1; i++) { ctx.beginPath(); ctx.moveTo(k.r - 2, i * 4); ctx.lineTo(k.r + 12, i * 6); ctx.stroke(); } return true; }
  prompt(m) { const t = this.teleportTarget(m); return t ? { key: 'E', label: `Teleport to ${t.s.name}'s phantasm` } : null; }
  hud(m) {
    const n = m ? this.traps.length : 0; const t = m ? this.teleportTarget(m) : null;
    return { title: 'Blackened Catalyst', value: `${n}/${this.maxTraps}`, meter: this.state === 'drawing' ? this.drawT / this.drawTime : n / this.maxTraps,
      state: this.state === 'drawing' ? 'Drawing trap…' : t ? 'TRIGGERED — E to teleport' : 'Draw traps with Q / RMB',
      hints: [['Q / RMB', 'Hold to draw a Phantasm Trap'], ['E', 'Teleport to a triggered trap (40 m)']] };
  }
}

return { HagPower };
};
__defs["js/killers/cannibal.js"] = function(){
// THE CANNIBAL — Bubba's Chainsaw (current design, adapted to top-down).
//  • 3 chainsaw charges, refilling at 0.25 charge/s while the saw is idle.
//  • Hold RMB to REV (2 s, slowed). Releasing early cancels for free. Releasing a full rev
//    starts the CHAINSAW SWEEP (costs 1 charge): Bubba carves forward in a wide arc, accelerating over
//    2.5 s up to 5.35 m/s. Mouse steers (slowly). Every survivor the blade touches is downed —
//    one sweep can down several.
//  • Press LMB during a sweep to DASH: spend another charge for a burst of top speed and
//    more sweep time.
//  • Holding the full rev too long (3 s total) without letting the saw go, or ramming a wall, rock or other
//    solid obstacle, throws him into a TANTRUM: 3 s + 1 s per remaining charge, Hindered 90%,
//    flailing around him (anyone right next to him is still hit). All charges are lost.
//  • Dropped pallets and breakable walls in the sweep's path are sawn through (1 s cooldown).
//  • Cooldowns after the sweep ends: hit 2 s · miss 1.5 s + 1 s per charge spent · pallet 1 s.
// Unlike the Hillbilly (long, fast, straight sprint with overheat), Bubba is short range,
// wide and slow to turn: survivors beat him by keeping distance, forcing the sweep into
// walls and windows, and by spreading out so one sweep can't catch two.
const { M, dist, angleDiff, normAngle, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, strike } = __req("js/killers/framework.js");

const REV = 2.0, OVERREV = 3.0, RAMP = 2.5, TOP = 5.35, START = 3.6;
class CannibalPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'bubba'; this.name = "Bubba's Chainsaw"; this.state = 'idle';
    this.maxCharges = this.has('award_winning_chili') ? 4 : 3; this.charges = this.maxCharges; this.rev = 0; this.revHeld = 0;
    this.revTime = this.has('primer_bulb') ? 1.7 : REV; this.turn = this.has('light_chassis') ? 1.6 : 1.25;
    this.reach = (this.has('long_guide_bar') ? 2.6 : 2.2) * M; this.tantrumT = 0; this.sweepT = 0; this.downed = new Set(); this.soundT = 0;
    this.regenRate = this.has('knife_scratches') ? 0.33 : 0.25; this.cdT = 0; this.spent = 0; this.speedNow = START;
    this.aiProfile = { type: 'sweep', label: 'Chainsaw sweep', counter: ['keep more than 6 m away', 'make him sweep into walls (tantrum)', 'windows — he can\'t vault while sawing', 'spread out: one sweep downs everyone it touches'] };
  }
  speedMult() {
    const base = this.k.def.speed * M;
    if (this.state === 'revving') return 0.6;
    if (this.state === 'sweep') return this.speedNow * M / base;
    if (this.state === 'tantrum') return 1; // Hindered 90% status does the slowing
    return 1;
  }
  canAttack() { return this.state === 'idle' || this.state === 'cooldown'; }
  canInteract() { return this.state === 'idle' || this.state === 'cooldown'; }
  overrideMove() { return this.state === 'sweep' ? { x: Math.cos(this.dir), y: Math.sin(this.dir) } : null; }
  noContext() { return this.state === 'sweep' || this.state === 'tantrum'; }
  update(dt, m, inp, locked) {
    this.tickCooldowns(dt);
    const k = this.k; this.soundT -= dt;
    if ((this.state === 'idle' || this.state === 'cooldown') && this.charges < this.maxCharges) this.charges = Math.min(this.maxCharges, this.charges + this.regenRate * dt);
    if (this.state === 'cooldown') { this.cdT -= dt; if (this.cdT <= 0) this.state = 'idle'; }
    if (this.state === 'tantrum') { // runs even while "locked" by the tantrum itself
      this.tantrumT -= dt; k.facing += dt * 9; this.sawHits(m, Math.PI, 1.4 * M);
      if (this.tantrumT <= 0) { this.state = 'cooldown'; this.cdT = 3; this.lockFacing = false; }
      return;
    }
    if (locked) { if (this.state === 'revving' || this.state === 'sweep') this.endSweep(m, 'interrupted'); return; }
    switch (this.state) {
      case 'idle':
        if (inp.rmb && !k.carrying && this.charges >= 1) { this.state = 'revving'; this.rev = 0; this.revHeld = 0; m.events.emit(EV.POWER_USED, { killer: k, power: 'rev' }); }
        break;
      case 'revving':
        this.rev += dt / this.revTime; this.revHeld += dt;
        if (this.soundT <= 0) { this.soundT = 0.3; m.sound('chainsaw_rev', k.x, k.y, 36 * M, { charge: Math.min(1, this.rev), bubba: true }); m.audio?.play('rev', { charge: Math.min(1, this.rev) }); }
        if (!inp.rmb) { if (this.rev >= 1) this.startSweep(m); else { this.state = 'idle'; this.rev = 0; } break; } // early release: free cancel
        if (this.revHeld >= OVERREV) this.tantrum(m, 'overrev'); // held the full rev too long
        break;
      case 'sweep': {
        this.sweepT += dt; k.facing = this.dir;
        this.speedNow = this.dashT > 0 ? TOP : START + (TOP - START) * Math.min(1, this.sweepT / RAMP); this.dashT = Math.max(0, (this.dashT || 0) - dt);
        const d = normAngle(inp.aim - this.dir); this.dir += clamp(d, -this.turn * dt, this.turn * dt);
        if (this.soundT <= 0) { this.soundT = 0.2; m.sound('chainsaw_sprint', k.x, k.y, 36 * M, { dir: this.dir, bubba: true }); }
        this.sawHits(m, Math.PI * 0.42);
        // dash: spend another charge for top speed and more sweep time
        if (inp.lmbP && this.charges >= 1) { this.charges -= 1; this.spent++; this.dashT = 0.8; this.left += 1.2; m.fx.text(k.x, k.y - 40, 'DASH', '#ff8a4a'); m.events.emit(EV.POWER_USED, { killer: k, power: 'dash' }); }
        // saw through dropped pallets / breakable walls in front
        const fx = k.x + Math.cos(this.dir) * 0.9 * M, fy = k.y + Math.sin(this.dir) * 0.9 * M;
        for (const p of m.map.pallets) if (p.state === 'down' && p.stunZone({ x: fx, y: fy })) { m.breakPallet(p, k); this.endSweep(m, 'pallet'); return; }
        this.left -= dt;
        if (this.left <= 0 || (inp.rmbP && this.sweepT > 0.3)) this.endSweep(m, this.downed.size ? 'hit' : 'miss');
        break;
      }
    }
  }
  startSweep(m) {
    const k = this.k; this.state = 'sweep'; this.dir = k.facing; this.lockFacing = true; this.sweepT = 0; this.left = 2.6; this.dashT = 0;
    this.charges -= 1; this.spent = 1; this.downed = new Set(); this.speedNow = START;
    m.audio?.play('saw'); m.events.emit(EV.POWER_USED, { killer: k, power: 'sweep' });
  }
  endSweep(m, why) {
    this.lockFacing = false; this.state = 'cooldown';
    this.cdT = why === 'hit' ? 2 : why === 'pallet' ? 1 : why === 'interrupted' ? 1 : 1.5 + this.spent;
    if (this.downed.size) this.cdT = 2;
    this.lastEnd = { why, t: m.time, hits: this.downed.size };
  }
  sawHits(m, arc, reach = this.reach) {
    const k = this.k;
    for (const s of m.survivors) {
      if (!s.standing || this.downed.has(s)) continue;
      const d = dist(k.x, k.y, s.x, s.y); if (d > reach + s.r) continue;
      if (angleDiff(Math.atan2(s.y - k.y, s.x - k.x), k.facing) > arc && d > k.r + s.r + 6) continue;
      if (!m.world.losClear(k.x, k.y, s.x, s.y)) continue;
      const r = strike(m, k, s, { instadown: true, source: 'chainsaw', score: 'brutality', label: 'Chainsaw' });
      if (r && r !== 'dodged') { this.downed.add(s); m.fx.shake(10); s.ai?.note('Sawed by the Cannibal', 'bubbasaw'); if (this.downed.size > 1) m.fx.text(k.x, k.y - 50, `MULTI-DOWN ×${this.downed.size}`, '#ff5b3b'); }
    }
  }
  onCollide(m) { if (this.state === 'sweep' && this.sweepT > 0.15) this.tantrum(m, 'wall'); }
  tantrum(m, what) {
    const k = this.k;
    this.tantrumT = (this.has('the_beast_marks') ? 2.2 : 3) + Math.floor(this.charges); this.charges = 0;
    this.state = 'tantrum'; this.lockFacing = true;
    k.status.add('hindered', { duration: this.tantrumT, value: 0.9, source: 'tantrum' });
    m.fx.shake(12); m.audio?.play('clang'); m.fx.text(k.x, k.y - 40, 'TANTRUM', '#ff8a4a');
    m.sound('killer_action', k.x, k.y, 30 * M, { what: 'tantrum' });
    m.events.emit(EV.POWER_USED, { killer: k, power: 'tantrum', what });
    for (const s of m.survivors) if (s.standing && dist(s.x, s.y, k.x, k.y) < 16 * M) s.ai?.note(what === 'overrev' ? 'Bubba over-revved into a tantrum' : 'Made Bubba tantrum on the wall', 'tantrum');
  }
  onStun() { this.state = 'idle'; this.lockFacing = false; }
  drawWeapon(ctx) {
    const k = this.k, on = this.state === 'sweep' || this.state === 'revving' || this.state === 'tantrum';
    const j = on ? (Math.random() - 0.5) * 2 : 0;
    ctx.fillStyle = on ? '#d06a2a' : '#6b6e72'; ctx.fillRect(k.r - 6, -5 + j, 30, 10); ctx.fillStyle = '#3a2a1e'; ctx.fillRect(k.r - 10, -8, 10, 16);
    if (on) { ctx.strokeStyle = 'rgba(255,170,80,0.85)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(k.r + 24, -5); ctx.lineTo(k.r + 24, 5); ctx.stroke(); }
    return true;
  }
  drawBody(ctx) { ctx.fillStyle = '#d8c8a8'; ctx.beginPath(); ctx.arc(6, 0, 6, 0, Math.PI * 2); ctx.fill(); ctx.fillStyle = '#7a3a2a'; ctx.fillRect(8, -3, 3, 6); }
  drawOver(ctx) {
    const k = this.k; if (this.state !== 'sweep' && this.state !== 'tantrum' && this.state !== 'revving') return;
    if (this.state === 'revving') { // rev meter ring + exhaust
      ctx.strokeStyle = this.revHeld > 2.3 ? '#ff5b3b' : '#d06a2a'; ctx.lineWidth = 3; ctx.beginPath(); ctx.arc(k.x, k.y, k.r + 8, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * Math.min(1, this.rev)); ctx.stroke();
    }
    ctx.fillStyle = this.state === 'revving' ? 'rgba(208,106,42,0.08)' : this.state === 'tantrum' ? 'rgba(255,90,60,0.22)' : 'rgba(208,106,42,0.18)';
    ctx.beginPath(); ctx.moveTo(k.x, k.y); const arc = this.state === 'tantrum' ? Math.PI : Math.PI * 0.42; const r = this.state === 'tantrum' ? 1.4 * M : this.reach;
    ctx.arc(k.x, k.y, r, k.facing - arc, k.facing + arc); ctx.closePath(); ctx.fill();
  }
  hud() {
    const label = { idle: 'Ready', revving: this.revHeld > 2.3 ? 'OVER-REVVING!' : 'Revving', sweep: this.dashT > 0 ? 'DASH' : `SWEEP ${this.speedNow.toFixed(2)} m/s`, tantrum: `TANTRUM ${Math.ceil(this.tantrumT)}s`, cooldown: `Cooldown ${this.cdT.toFixed(1)}s` }[this.state];
    return { title: "Bubba's Chainsaw", value: `${Math.floor(this.charges)}/${this.maxCharges}`, meter: this.state === 'revving' ? Math.min(1, this.rev) : this.charges / this.maxCharges, state: label,
      hints: [['Q / RMB', 'Hold 2 s to rev, release to sweep (early release cancels, 3 s = tantrum)'], ['Mouse', 'Steer the sweep'], ['LMB (sweeping)', 'Dash: spend a charge for top speed'], ['RMB (sweeping)', 'Stop the sweep']] };
  }
}

return { CannibalPower };
};
__defs["js/killers/deathslinger.js"] = function(){
// THE DEATHSLINGER — The Redeemer. Hold RMB to aim down the sights (slowed, shown as a
// laser line), LMB fires a harpoon: a real projectile with travel time (not hitscan),
// stopped by walls. A speared survivor is chained to him and reeled in while he holds
// LMB; up close he can strike them normally. The chain snaps if it is pulled against an
// obstacle (the survivor is injured with Deep Wound), if the reel runs too long, or if he
// is stunned. After every shot he has to reload (about 2 s, slowed).
const { M, dist, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, Projectile, strike } = __req("js/killers/framework.js");

class DeathslingerPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'redeemer'; this.name = 'The Redeemer'; this.state = 'idle'; this.loaded = true;
    this.range = (this.has('rusted_spike') ? 20 : 18) * M; this.speed = 55 * M; this.reelSpeed = 3.6 * M;
    this.reloadTime = this.has('speed_loader') ? 1.5 : 2.0; this.reelMax = this.has('chewing_tobacco') ? 8 : 6; this.speared = null;
    this.aiProfile = { type: 'harpoon', label: 'Harpoon gun', counter: ['break line of sight', 'zig-zag when he aims', 'pull the chain against obstacles'] };
  }
  speedMult() { if (this.state === 'aiming') return 0.7; if (this.state === 'reload') return 0.55; if (this.state === 'reel') return 0.5; return 1; }
  canAttack() { return this.state === 'idle' || (this.state === 'reel' && this.speared && dist(this.k.x, this.k.y, this.speared.x, this.speared.y) < 2.6 * M); }
  update(dt, m, inp, locked) {
    this.tickCooldowns(dt);
    const k = this.k;
    if (this.state === 'reel') this.updateReel(dt, m, inp, locked);
    if (locked) { if (this.state === 'aiming') this.state = 'idle'; return; }
    if (this.state === 'reload') { if (this.cd('reload') <= 0) { this.loaded = true; this.state = 'idle'; m.audio?.play('reload'); } return; }
    if (this.state === 'idle' && inp.rmb && !k.carrying && this.loaded) { this.state = 'aiming'; m.sound('power_windup', k.x, k.y, 20 * M, { what: 'aim', dir: k.facing }); }
    if (this.state === 'aiming') {
      if (!inp.rmb) { this.state = 'idle'; return; }
      this.aimT = (this.aimT || 0) + dt;
      if (inp.lmbP) this.fire(m);
    } else this.aimT = 0;
    if (this.state === 'idle' && !this.loaded) { this.state = 'reload'; this.setCd('reload', this.reloadTime); }
  }
  fire(m) {
    const k = this.k, a = k.facing; this.loaded = false; this.state = 'flying';
    k.attack.state = 'hit'; k.attack.t = 2.0; // no immediate slash after the shot
    m.spawnProjectile(new Projectile({ kind: 'harpoon', owner: k, x: k.x + Math.cos(a) * k.r, y: k.y + Math.sin(a) * k.r, vx: Math.cos(a) * this.speed, vy: Math.sin(a) * this.speed, r: 0.18 * M, range: this.range, color: '#b9a37a',
      onHit: (p, s, mm) => { if (s.dodgeT && s.dodgeT > mm.time) { mm.fx.text(s.x, s.y - 30, 'DODGED', '#ffd27a'); this.toReload(); return; } this.spear(mm, s); },
      onWall: (p, mm) => { mm.fx.sparks(p.x, p.y); this.toReload(); }, onEnd: () => this.toReload() }));
    m.audio?.play('throw'); m.fx.shake(4); m.sound('killer_action', k.x, k.y, 32 * M, { what: 'shot' });
    m.events.emit(EV.POWER_USED, { killer: k, power: 'harpoon' });
  }
  toReload() { if (this.state === 'flying') { this.state = 'reload'; this.setCd('reload', this.reloadTime); } }
  spear(m, s) {
    const k = this.k; this.state = 'reel'; this.speared = s; this.reelT = 0;
    s.stopAction(m); s.vault = null; s.status.add('grabbed', { source: 'harpoon' }); m.events.emit('powerTriggered', { survivor: s, x: s.x, y: s.y }); 
    m.fx.hit(s, 'injured'); m.addScore('hunter', 300, 'Speared'); k.stats.powerHits++;
    s.ai?.note('Speared by the Deathslinger', 'speared');
    const self = this;
    s.control = { kind: 'harpoon', allow: ['dropPallet'], update(dt, mm, sv) {
      if (self.speared !== sv) return false;
      // the survivor fights the pull: moves away at walking pace while being reeled
      const dx = k.x - sv.x, dy = k.y - sv.y, d = Math.hypot(dx, dy) || 1;
      let ux = dx / d * self.reelSpeed, uy = dy / d * self.reelSpeed;
      if (sv.moveTo) { const ax = sv.moveTo.x - sv.x, ay = sv.moveTo.y - sv.y, al = Math.hypot(ax, ay) || 1; ux += ax / al * 1.6 * M; uy += ay / al * 1.6 * M; }
      if (d > 1.6 * M) mm.world.move(sv, ux * dt, uy * dt, sv.r);
      sv.facing = Math.atan2(-dy, -dx);
      return true;
    } };
  }
  updateReel(dt, m, inp, locked) {
    const s = this.speared, k = this.k; this.reelT += dt;
    if (!s || !s.standing) return this.release(m, null);
    if (locked && k.stunT > 0) return this.release(m, 'stunned');
    // chain snaps against obstacles between them
    if (!m.world.losClear(k.x, k.y, s.x, s.y)) return this.release(m, 'snapped');
    if (this.reelT > this.reelMax || dist(k.x, k.y, s.x, s.y) > this.range + 4 * M) return this.release(m, 'snapped');
    if (k.attack.state === 'hit' && k.attack.t < 0.1 && this.reelT > 0.3) { /* basic hit landed — release */ }
  }
  onBasicHit(m, s) { if (s === this.speared) this.release(m, null); }
  release(m, why) {
    const s = this.speared; this.speared = null; this.state = 'reload'; this.setCd('reload', this.reloadTime);
    if (!s) return;
    if (s.control?.kind === 'harpoon') s.control = null; s.status.remove('grabbed', 'harpoon');
    if (why === 'snapped' && s.standing) {
      // breaking free tears the spear out: injured + Deep Wound
      if (s.health === 'healthy') { s.health = 'injured'; m.events.emit(EV.SURVIVOR_INJURED, { survivor: s, source: 'harpoon' }); }
      s.status.add('deepWound', { duration: 20 }); s.status.add('haste', { duration: 1.5, value: 0.3, source: 'snap' });
      m.fx.text(s.x, s.y - 30, 'CHAIN SNAPPED', '#ffd27a'); s.ai?.note('Broke the harpoon chain on an obstacle', 'snap');
    }
  }
  onStun(m) { if (this.state === 'reel') this.release(m, 'stunned'); if (this.state === 'aiming') this.state = 'idle'; }
  // basic attack while reeling: hooked into the killer's attack via onAttack
  onAttack(m) { if (this.state === 'reel') { this._releaseAfterSwing = true; } }
  tick(dt, m) { if (this._releaseAfterSwing && this.k.attack.state !== 'lunge') { this._releaseAfterSwing = false; if (this.state === 'reel') this.release(m, null); } }
  drawWeapon(ctx) {
    const k = this.k; ctx.fillStyle = '#5a4a36'; ctx.fillRect(k.r - 6, -3, 30, 6); ctx.fillStyle = '#9a9ca0'; ctx.fillRect(k.r + 18, -2, 10, 4);
    if (this.loaded) { ctx.fillStyle = '#b9a37a'; ctx.beginPath(); ctx.moveTo(k.r + 28, -3); ctx.lineTo(k.r + 34, 0); ctx.lineTo(k.r + 28, 3); ctx.fill(); }
    return true;
  }
  drawOver(ctx, m) {
    const k = this.k;
    if (this.state === 'aiming') { ctx.strokeStyle = 'rgba(255,60,60,0.55)'; ctx.lineWidth = 1.2; ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.lineTo(k.x + Math.cos(k.facing) * this.range, k.y + Math.sin(k.facing) * this.range); ctx.stroke(); }
    const s = this.speared; if (s) { ctx.strokeStyle = '#b9a37a'; ctx.lineWidth = 2; ctx.setLineDash([5, 3]); ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.lineTo(s.x, s.y); ctx.stroke(); ctx.setLineDash([]); }
    for (const p of m.projectiles) if (p.kind === 'harpoon') { ctx.strokeStyle = 'rgba(185,163,122,0.7)'; ctx.lineWidth = 1.5; ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.lineTo(p.x, p.y); ctx.stroke(); }
  }
  hud() {
    const st = { idle: this.loaded ? 'Loaded' : 'Empty', aiming: 'Aiming — LMB to fire', flying: 'Harpoon in flight', reel: 'Reeling — LMB to strike up close', reload: 'Reloading…' }[this.state];
    return { title: 'The Redeemer', value: this.loaded ? '1/1' : '0/1', meter: this.state === 'reload' ? 1 - this.cd('reload') / this.reloadTime : this.loaded ? 1 : 0, state: st,
      hints: [['Q / RMB', 'Hold to aim down sights'], ['LMB', 'While aiming: fire the harpoon'], ['LMB', 'While reeling: strike']] };
  }
}

return { DeathslingerPower };
};
__defs["js/killers/dredge.js"] = function(){
// THE DREDGE — Reign of Darkness. Tap RMB to enter the Gloaming: a Remnant is left
// where you stood, you move slower and cannot attack. In the Gloaming, aim at any locker
// on the map and tap RMB to teleport into it (costs a token); LMB snaps you back to the
// Remnant; E leaves the Gloaming. Teleports travel through the dark at 19 m/s (38 m/s at
// Nightfall) and survivors near the destination locker hear it rattle. Survivors can
// lock lockers: a locked locker takes much longer to burst out of and the lock breaks.
// Nightfall builds from hits, hooks, locker time and the injured; when full, the map goes
// dark for 60 s: survivors see almost nothing, the Dredge is Undetectable and teleports
// faster with a short cooldown.
const { M, dist } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, Device } = __req("js/killers/framework.js");

const NIGHT_MAX = 300;
class DredgePower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'nightfall'; this.name = 'Reign of Darkness'; this.state = 'idle';
    this.tokens = this.has('boat_key') ? 4 : 3; this.maxTokens = this.tokens; this.remnant = null; this.target = null;
    this.night = 0; this.nightfall = false; this.nightT = 0; this.travel = null; this.tokenT = 0;
    this.nightLen = this.has('malthinkers_skull') ? 70 : 60;
    this.aiProfile = { type: 'lockers', label: 'Locker teleports & Nightfall', counter: ['lock lockers near objectives', 'watch lockers', 'play safe during Nightfall'] };
  }
  get lockers() { return this._m ? this._m.devices.filter((d) => d.kind === 'locker') : []; }
  onInit(m) {
    this._m = m;
    for (const p of m.map.openSpots(14, 12, [...m.map.gens, ...m.map.hooks])) m.addDevice(new Device({ kind: 'locker', x: p.x, y: p.y, r: 0.7 * M, label: 'Locker', seenRange: 22 * M, useTime: 1.5, survivorUsable: true, data: { locked: false } }));
    m.events.on(EV.SURVIVOR_HOOKED, () => { if (!this.nightfall) this.night += 20; });
    m.events.on(EV.KILLER_HIT, (d) => { if (!this.nightfall && d.result === 'injured' && !d.power) this.night += 20; });
    // bind usable/onUse to each locker instance
    for (const d of this.lockers) { d.usable = () => !d.data.locked && !(this.travel?.to === d); d.onUse = (s, mm) => { d.data.locked = true; mm.fx.text(d.x, d.y - 26, 'LOCKED', '#cfd3d6'); s.ai?.note('Locked a locker against the Dredge', 'lock'); mm.events.emit('lockerLocked', { locker: d, survivor: s }); }; }
  }
  speedMult() { if (this.state === 'gloaming') return 4.0 / this.k.def.speed; if (this.state === 'travel' || this.state === 'exit') return 0; return 1; }
  canAttack() { return this.state === 'idle'; }
  noContext() { return this.state !== 'idle'; }
  alpha() { return this.state === 'travel' ? 0 : this.state === 'gloaming' ? 0.35 : this.state === 'exit' ? 0.6 : 1; }
  visibilityTo() { return this.state === 'gloaming' ? 0.4 : 1; }
  get cooldown() { return this.nightfall ? 4 : 10; }
  // a locker the player is pointing at: nearest locker to the aim point
  aimLocker(m, inp) {
    const k = this.k; const d = Math.min(inp.aimDist ?? 10 * M, 60 * M) * 2.2;
    const ax = k.x + Math.cos(k.facing) * d, ay = k.y + Math.sin(k.facing) * d;
    let best = null, bd = Infinity;
    for (const l of this.lockers) { const dd = dist(l.x, l.y, ax, ay) + dist(l.x, l.y, k.x, k.y) * 0.15; if (dist(l.x, l.y, k.x, k.y) < 3 * M) continue; if (dd < bd) { bd = dd; best = l; } }
    return best;
  }
  update(dt, m, inp, locked) {
    this._m = m; this.tickCooldowns(dt);
    const k = this.k;
    if (this.tokens < this.maxTokens) { this.tokenT += dt; if (this.tokenT > 20) { this.tokenT = 0; this.tokens++; } }
    if (this.state === 'travel') {
      const t = this.travel; t.t += dt; const f = Math.min(1, t.t / t.dur);
      k.x = t.fx + (t.to.x - t.fx) * f; k.y = t.fy + (t.to.y - t.fy) * f;
      if (f >= 1) { this.state = 'exit'; this.exitT = t.to.data.locked ? 2.25 : 0.45; this.night += 0; k.x = t.to.x; k.y = t.to.y; }
      return;
    }
    if (this.state === 'exit') {
      this.exitT -= dt; this.night += dt * 6;
      if (this.exitT <= 0) this.emerge(m);
      return;
    }
    if (locked) { if (this.state === 'gloaming') this.leaveGloaming(m); return; }
    if (this.state === 'idle' && inp.rmbP && !k.carrying) { this.enterGloaming(m); return; }
    if (this.state === 'gloaming') {
      this.gloamT += dt; this.target = this.aimLocker(m, inp);
      if (inp.lmbP && this.remnant) { this.returnToRemnant(m); return; }
      if (inp.eP || this.gloamT > 15) { this.leaveGloaming(m); return; }
      if (inp.rmbP && this.target && this.tokens > 0 && this.cd('tp') <= 0) this.teleport(m, this.target);
    }
  }
  enterGloaming(m) {
    const k = this.k; this.state = 'gloaming'; this.gloamT = 0;
    this.remnant = { x: k.x, y: k.y, t: m.time };
    m.events.emit(EV.POWER_USED, { killer: k, power: 'gloaming' }); m.audio?.play('teleport');
  }
  leaveGloaming() { this.state = 'idle'; this.remnant = null; this.target = null; }
  returnToRemnant(m) {
    const k = this.k, r = this.remnant; this.remnant = null; this.state = 'idle';
    m.fx.burst(k.x, k.y, 16, '#20202a', 80, 0.6, 3, 2); k.x = r.x; k.y = r.y; m.world.resolve(k, k.r);
    this.night += 10; m.audio?.play('teleport'); m.sound('killer_action', k.x, k.y, 20 * M, { what: 'remnant' });
  }
  teleport(m, l) {
    const k = this.k; this.tokens--; this.setCd('tp', this.cooldown); this.remnant = null; this.target = null;
    const d = dist(k.x, k.y, l.x, l.y);
    this.travel = { to: l, t: 0, dur: Math.max(0.5, d / ((this.nightfall ? 38 : 19) * M)), fx: k.x, fy: k.y };
    this.state = 'travel'; k.hidden = true;
    m.sound('power_warn', l.x, l.y, 12 * M, { what: 'locker', note: 'A locker nearby rattles — the Dredge is coming' });
    m.events.emit(EV.POWER_USED, { killer: k, power: 'teleport' }); this.night += 0;
  }
  emerge(m) {
    const k = this.k, l = this.travel.to; this.travel = null; this.state = 'idle'; k.hidden = false;
    if (l.data.locked) { l.data.locked = false; m.fx.text(l.x, l.y - 26, 'LOCK BROKEN', '#ffd27a'); }
    // step out in front of the locker toward open ground
    let best = null; for (let a = 0; a < 8; a++) { const x = l.x + Math.cos(a * Math.PI / 4) * 1.1 * M, y = l.y + Math.sin(a * Math.PI / 4) * 1.1 * M; if (!m.world.pointBlocked(x, y, k.r)) { best = { x, y, a: a * Math.PI / 4 }; break; } }
    if (best) { k.x = best.x; k.y = best.y; k.facing = best.a; }
    m.world.resolve(k, k.r); m.audio?.play('kick');
    m.sound('killer_action', k.x, k.y, 24 * M, { what: 'locker' });
    // anyone right at the locker is grabbed straight out of the dark
    for (const s of m.survivors) if (s.standing && dist(s.x, s.y, l.x, l.y) < 1.2 * M) { m.reveal(s, 2, 'Locker'); }
  }
  // Nightfall clock (runs even while stunned / acting)
  tick(dt, m) {
    const injured = m.survivors.filter((s) => s.health === 'injured').length;
    if (this.nightfall) {
      this.nightT -= dt;
      for (const s of m.survivors) if (s.alive) s.status.add('nightfall', { duration: 0.5, source: 'dredge' });
      this.k.status.add('undetectable', { duration: 0.5, source: 'nightfall' });
      if (this.state === 'exit' || this.state === 'travel') for (const s of m.survivors) if (s.standing && dist(s.x, s.y, this.k.x, this.k.y) < 16 * M) m.reveal(s, 0.5, 'Nightfall', true);
      if (this.nightT <= 0) { this.nightfall = false; this.night = 0; m.notifyK('Daylight returns', '#cfd3d6'); m.events.emit('nightfallEnd', {}); }
      return;
    }
    this.night += dt * (0.25 + Math.min(4, injured));
    if (this.night >= NIGHT_MAX) {
      this.nightfall = true; this.nightT = this.nightLen;
      m.notifyK('NIGHTFALL — the map is plunged into darkness', '#9a8fe0'); m.audio?.play('collapse'); m.fx.shake(6);
      m.events.emit('nightfall', {}); m.sound('power_warn', this.k.x, this.k.y, 1e9, { what: 'nightfall', note: 'Nightfall!' });
      for (const s of m.survivors) s.ai?.note('Nightfall — it is pitch black', 'nightfall');
    }
  }
  drawDevice(ctx, d) {
    if (d.kind !== 'locker') return;
  }
  drawGround(ctx, m) {
    for (const l of this.lockers) {
      if (l === this.target) { ctx.strokeStyle = 'rgba(154,143,224,0.95)'; ctx.lineWidth = 3; ctx.strokeRect(l.x - 16, l.y - 20, 32, 40); }
      if (l.data.locked) { ctx.fillStyle = '#cfd3d6'; ctx.fillRect(l.x - 4, l.y - 3, 8, 7); ctx.strokeStyle = '#cfd3d6'; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(l.x, l.y - 4, 3, Math.PI, 0); ctx.stroke(); }
    }
    if (this.remnant) { const r = this.remnant; ctx.fillStyle = 'rgba(30,26,46,0.85)'; ctx.beginPath(); ctx.arc(r.x, r.y, this.k.r, 0, Math.PI * 2); ctx.fill(); ctx.strokeStyle = 'rgba(154,143,224,0.6)'; ctx.lineWidth = 2; ctx.stroke(); }
  }
  drawOver(ctx, m) {
    const k = this.k;
    if (this.state === 'gloaming' && this.target) { ctx.strokeStyle = 'rgba(154,143,224,0.5)'; ctx.setLineDash([6, 6]); ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.lineTo(this.target.x, this.target.y); ctx.stroke(); ctx.setLineDash([]); }
    if (this.travel) { const t = this.travel; ctx.strokeStyle = 'rgba(80,70,140,0.6)'; ctx.lineWidth = 6; ctx.beginPath(); ctx.moveTo(t.fx, t.fy); ctx.lineTo(k.x, k.y); ctx.stroke(); }
  }
  drawBody(ctx) { const k = this.k; ctx.fillStyle = '#121218'; ctx.beginPath(); ctx.arc(2, 0, k.r * 0.75, 0, Math.PI * 2); ctx.fill(); ctx.fillStyle = '#9a8fe0'; ctx.beginPath(); ctx.arc(7, -3, 1.6, 0, Math.PI * 2); ctx.arc(7, 3, 1.6, 0, Math.PI * 2); ctx.fill(); }
  drawWeapon(ctx) { const k = this.k; ctx.strokeStyle = '#3a3448'; ctx.lineWidth = 4; for (const y of [-7, 7]) { ctx.beginPath(); ctx.moveTo(k.r - 4, y); ctx.lineTo(k.r + 14, y * 1.3); ctx.stroke(); } return true; }
  prompt() { return this.state === 'gloaming' ? { key: 'Q', label: this.target ? (this.tokens ? (this.cd('tp') > 0 ? `Teleport in ${Math.ceil(this.cd('tp'))}s` : 'Teleport to locker') : 'No tokens') : 'Aim at a locker' } : null; }
  hud() {
    const st = this.nightfall ? `NIGHTFALL ${Math.ceil(this.nightT)}s` : { idle: 'Daytime', gloaming: 'The Gloaming', travel: 'Travelling…', exit: 'Emerging' }[this.state];
    return { title: 'Reign of Darkness', value: `${this.tokens}◆`, meter: this.nightfall ? this.nightT / this.nightLen : this.night / NIGHT_MAX, state: st,
      hints: [['Q / RMB', 'Gloaming · then Q: teleport to aimed locker'], ['LMB', 'In Gloaming: return to Remnant'], ['E', 'Leave the Gloaming']] };
  }
}

return { DredgePower };
};
__defs["js/killers/knight.js"] = function(){
// THE KNIGHT — Guardia Compagnia. Tap RMB to start drawing a patrol path (walk it out),
// tap RMB again to summon the current Guard at the start of the path (a path drawn shorter
// than 4 m sends the Guard straight toward where you aim instead). E cycles the Guard;
// after each summon the order also rotates Carnifex → Assassin → Jailer.
// The Guard patrols the path with a 180° vision cone. When it spots a survivor it moves to
// that spot (phasing through obstacles), plants a Standard and HUNTS the survivor: it
// chases with its own pathfinding, breaks pallets in its way and injures on contact. The
// hunt ends when its timer runs out (drains 3× faster while the Knight is within 8 m), when
// the survivor grabs the Standard, or when the survivor is downed.
//   Carnifex — breaks pallets/generators fast.  Assassin — fastest hunter, Deep Wound.
//   Jailer — 16 m detection, long patrol and hunt.
const { M, dist, angleDiff, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, Minion, Device, strike } = __req("js/killers/framework.js");

const GUARDS = {
  carnifex: { id: 'carnifex', name: 'The Carnifex', color: '#b04a3a', patrolSpeed: 3.4, huntSpeed: 4.1, patrolTime: 12, huntTime: 12, detect: 10, breakTime: 1.8, standardDelay: 5, deepWound: false },
  assassin: { id: 'assassin', name: 'The Assassin', color: '#4a6ab0', patrolSpeed: 3.4, huntSpeed: 4.4, patrolTime: 12, huntTime: 12, detect: 10, breakTime: 5, standardDelay: 5, deepWound: true },
  jailer: { id: 'jailer', name: 'The Jailer', color: '#c09a3a', patrolSpeed: 4.1, huntSpeed: 4.1, patrolTime: 24, huntTime: 24, detect: 16, breakTime: 3, standardDelay: 10, deepWound: false },
};
const ORDER = ['carnifex', 'assassin', 'jailer'];

class Guard extends Minion {
  constructor(power, type, path) {
    super({ kind: 'guard', owner: power.k, x: path[0].x, y: path[0].y, r: 0.5 * M, color: type.color, label: type.name, threat: 1, visRange: 34 * M });
    this.p = power; this.type = type; this.patrol = path; this.pi = 1; this.dirSign = 1; this.state = 'patrol'; this.time = type.patrolTime;
    this.stunnable = true; this.stunTime = 2; this.breaking = null; this.kicked = new Set();
  }
  update(dt, m) {
    const T = this.type, k = this.p.k;
    if (this.breaking) { this.breaking.t -= dt; if (this.breaking.t <= 0) { if (this.breaking.p.state === 'down') m.breakPallet(this.breaking.p); this.breaking = null; } return; }
    if (this.state === 'patrol') {
      this.time -= dt; if (this.time <= 0) return this.p.endGuard(m, 'patrol over');
      const tgt = this.patrol[this.pi]; const dx = tgt.x - this.x, dy = tgt.y - this.y, d = Math.hypot(dx, dy);
      if (d < 0.3 * M) { this.pi += this.dirSign; if (this.pi >= this.patrol.length || this.pi < 0) { this.dirSign *= -1; this.pi = clamp(this.pi + this.dirSign * 2, 0, this.patrol.length - 1); } }
      else this.moveDir(dt, m, dx / d, dy / d, T.patrolSpeed * M);
      // generators along the patrol take a kick
      for (const g of m.map.gens) if (!g.done && g.charges > 0 && !this.kicked.has(g) && g.repairers.size === 0 && dist(g.x, g.y, this.x, this.y) < 2.4 * M) { this.kicked.add(g); m.kickGenerator(g); }
      this.breakAhead(m);
      // vision: 180° cone in the walking direction
      for (const s of m.survivors) {
        if (!s.standing) continue; const sd = dist(s.x, s.y, this.x, this.y);
        if (sd > T.detect * M || (sd > 2 * M && angleDiff(Math.atan2(s.y - this.y, s.x - this.x), this.facing) > Math.PI / 2)) continue;
        if (!m.world.losClear(this.x, this.y, s.x, s.y)) continue;
        this.state = 'spotted'; this.target = s; this.spot = { x: s.x, y: s.y }; this.spotT = 2.5;
        m.notifyK(`${T.name} spotted ${s.name}`, T.color); m.reveal(s, 2.5, T.name);
        m.sound('power_warn', this.x, this.y, 20 * M, { what: 'guard', note: `${T.name} spotted ${s.name}` });
        s.ai?.onGuardSpotted?.(this);
        break;
      }
    } else if (this.state === 'spotted') {
      // move to where the survivor was seen, phasing through obstacles
      this.spotT -= dt; const dx = this.spot.x - this.x, dy = this.spot.y - this.y, d = Math.hypot(dx, dy);
      if (d > 0.2 * M) { const st = Math.min(d, Math.max(d / Math.max(this.spotT, 0.1), 3 * M) * dt); this.x += dx / d * st; this.y += dy / d * st; this.facing = Math.atan2(dy, dx); }
      if (this.spotT <= 0) {
        this.state = 'hunt'; this.time = T.huntTime; this.huntRepath = 0;
        this.p.plantStandard(m, this.spot, T.standardDelay, this);
        m.world.resolve(this, this.r);
      }
    } else if (this.state === 'hunt') {
      const s = this.target; const near = dist(k.x, k.y, this.x, this.y) < 8 * M;
      this.time -= dt * (near ? 3 : 1);
      if (!s || !s.standing || this.time <= 0) return this.p.endGuard(m, this.time <= 0 ? 'hunt over' : 'target down');
      this.huntRepath -= dt;
      if (this.huntRepath <= 0) { this.huntRepath = 0.35; this.pathTo(m, s.x, s.y); }
      if (!this.followPath(dt, m, T.huntSpeed * M) && this.path) this.breakAhead(m);
      if (dist(s.x, s.y, this.x, this.y) < this.r + s.r + 0.5 * M && m.world.losClear(this.x, this.y, s.x, s.y)) {
        const res = strike(m, k, s, { source: 'guard', score: 'brutality', label: `${T.name} hit` });
        if (res && res !== 'dodged') { if (T.deepWound && s.health === 'injured') s.status.add('deepWound', { duration: 20 }); s.ai?.note(`Caught by ${T.name}`, 'guardhit'); }
        m.events.emit('powerTriggered', { survivor: s, x: s.x, y: s.y }); return this.p.endGuard(m, 'caught');
      }
    }
  }
  breakAhead(m) {
    for (const p of m.map.pallets) if (p.state === 'down' && dist(p.x, p.y, this.x, this.y) < Math.max(p.len / 2, 1) + 1.0 * M && this.path?.length) {
      const ahead = { x: this.x + Math.cos(this.facing) * 0.9 * M, y: this.y + Math.sin(this.facing) * 0.9 * M };
      if (p.stunZone(ahead) || dist(p.x, p.y, ahead.x, ahead.y) < 1.2 * M) { this.breaking = { p, t: this.type.breakTime }; return; }
    }
  }
}

class KnightPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'guardia'; this.name = 'Guardia Compagnia'; this.state = 'idle'; this.order = 0; this.drawn = []; this.guard = null; this.standards = [];
    this.maxLen = (this.has('broken_hilt') ? 36 : 30) * M; this.recharge = this.has('ironworks_tongs') ? 6 : 8;
    this.aiProfile = { type: 'guards', label: 'Guard patrols', counter: ['leave a guard\'s patrol area', 'grab the Standard', 'break line of sight from the patrol', 'drop pallets on guards'] };
  }
  get type() { return GUARDS[ORDER[this.order % 3]]; }
  speedMult() { return this.state === 'drawing' ? 1.0 : 1; }
  pathLen() { let L = 0; for (let i = 1; i < this.drawn.length; i++) L += dist(this.drawn[i].x, this.drawn[i].y, this.drawn[i - 1].x, this.drawn[i - 1].y); return L; }
  update(dt, m, inp, locked) {
    this.tickCooldowns(dt);
    const k = this.k;
    for (const st of this.standards) { if (st.materializeT > 0) { st.materializeT -= dt; if (st.materializeT <= 0) this.spawnStandard(m, st); } }
    if (locked) return;
    if (inp.eP && this.state !== 'drawing' && !this.guard) { this.order++; m.audio?.play('ui'); }
    if (this.state === 'idle' && inp.rmbP && !k.carrying && !this.guard && this.cd('power') <= 0) { this.state = 'drawing'; this.drawn = [{ x: k.x, y: k.y }]; this.aimAtStart = k.facing; m.events.emit(EV.POWER_USED, { killer: k, power: 'draw' }); return; }
    if (this.state === 'drawing') {
      const last = this.drawn[this.drawn.length - 1];
      if (dist(last.x, last.y, k.x, k.y) > 1 * M) this.drawn.push({ x: k.x, y: k.y });
      if (inp.rmbP || this.pathLen() >= this.maxLen) this.summon(m);
    }
  }
  summon(m) {
    const k = this.k; let path = this.drawn;
    if (this.pathLen() < 4 * M) {
      // Guard Order: walk straight toward the aim point
      const d = Math.min(14 * M, Math.max(5 * M, k.input.aimDist ?? 8 * M)); const tx = k.x + Math.cos(k.facing) * d, ty = k.y + Math.sin(k.facing) * d;
      const p = m.map.nav.findPath(k.x, k.y, tx, ty, {}, 4000); path = p ? m.map.nav.smooth(m.world, p.pts, 0.5 * M).map((q) => ({ x: q.x, y: q.y })) : [{ x: k.x, y: k.y }, { x: tx, y: ty }];
    }
    const T = this.type; this.order++;
    this.guard = m.addMinion(new Guard(this, T, path)); this.state = 'idle'; this.drawn = [];
    m.fx.burst(path[0].x, path[0].y, 24, T.color, 90, 0.8, 3, 2); m.audio?.play('bell');
    m.sound('power_warn', path[0].x, path[0].y, 24 * M, { what: 'guard', note: `${T.name} summoned` });
    m.notifyK(`${T.name} patrols`, T.color); m.events.emit(EV.POWER_USED, { killer: this.k, power: 'guard', guard: T.id });
  }
  plantStandard(m, spot, delay, guard) { this.standards.push({ x: spot.x, y: spot.y, materializeT: delay, guard }); }
  spawnStandard(m, st) {
    if (this.guard !== st.guard || st.guard.state !== 'hunt') { this.standards = this.standards.filter((q) => q !== st); return; }
    st.device = m.addDevice(new Device({ kind: 'standard', x: st.x, y: st.y, r: 0.5 * M, label: 'Standard', useTime: 0.5, survivorUsable: true, seenRange: 30 * M,
      usable: (s) => st.guard.target === s && this.guard === st.guard,
      onUse: (s, mm) => { s.status.add('haste', { duration: 3, value: 0.5, source: 'standard' }); s.status.add('endurance', { duration: 3, source: 'standard' }); s.ai?.note('Grabbed the Standard — hunt over', 'standard'); this.endGuard(mm, 'standard'); } }));
    st.guard.target?.ai?.onStandard?.(st.device);
  }
  endGuard(m, why) {
    const g = this.guard; if (!g) return; this.guard = null;
    m.removeMinion(g); m.fx.burst(g.x, g.y, 18, g.type.color, 80, 0.6, 3, 2);
    for (const st of this.standards) if (st.device) m.removeDevice(st.device);
    this.standards = []; this.setCd('power', this.recharge);
    m.events.emit(EV.POWER_USED, { killer: this.k, power: 'guard_end', why });
  }
  // unhooking someone ends a hunt on the rescuer
  onInit(m) { m.events.on(EV.SURVIVOR_UNHOOKED, (d) => { if (this.guard?.state === 'hunt' && this.guard.target === d.rescuer) this.endGuard(m, 'unhook'); }); }
  drawMinion(ctx, n, m) {
    if (n.kind !== 'guard') return false;
    ctx.save(); ctx.translate(n.x, n.y);
    if (n.state === 'patrol') { ctx.fillStyle = 'rgba(255,255,255,0.05)'; ctx.beginPath(); ctx.moveTo(0, 0); ctx.arc(0, 0, n.type.detect * M, n.facing - Math.PI / 2, n.facing + Math.PI / 2); ctx.closePath(); ctx.fill(); }
    ctx.rotate(n.facing); ctx.globalAlpha = n.state === 'spotted' ? 0.6 : 1;
    ctx.fillStyle = 'rgba(0,0,0,0.4)'; ctx.beginPath(); ctx.ellipse(3, 4, n.r + 2, n.r, 0, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = n.type.color; ctx.beginPath(); ctx.arc(0, 0, n.r, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = '#c8ccd2'; ctx.fillRect(n.r - 2, -2, 14, 4);
    ctx.fillStyle = '#2a2a2a'; ctx.beginPath(); ctx.arc(4, 0, 5, 0, Math.PI * 2); ctx.fill();
    ctx.restore();
    if (n.state === 'hunt') { ctx.fillStyle = '#ff5a5a'; ctx.font = '700 12px sans-serif'; ctx.textAlign = 'center'; ctx.fillText(`HUNT ${Math.ceil(n.time)}`, n.x, n.y - n.r - 8); if (n.target) { ctx.strokeStyle = 'rgba(255,90,90,0.35)'; ctx.setLineDash([4, 6]); ctx.beginPath(); ctx.moveTo(n.x, n.y); ctx.lineTo(n.target.x, n.target.y); ctx.stroke(); ctx.setLineDash([]); } }
    if (n.breaking) { ctx.fillStyle = '#ffd27a'; ctx.fillText('BREAKING', n.x, n.y - n.r - 8); }
    return true;
  }
  drawDevice(ctx, d) {
    if (d.kind !== 'standard') return;
    ctx.strokeStyle = '#8a7a5a'; ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(d.x, d.y + 10); ctx.lineTo(d.x, d.y - 18); ctx.stroke();
    ctx.fillStyle = '#a03a2a'; ctx.beginPath(); ctx.moveTo(d.x, d.y - 18); ctx.lineTo(d.x + 14, d.y - 13); ctx.lineTo(d.x, d.y - 8); ctx.fill();
  }
  drawGround(ctx, m) {
    const pts = this.state === 'drawing' ? [...this.drawn, { x: this.k.x, y: this.k.y }] : this.guard?.patrol;
    if (pts && pts.length > 1) { ctx.strokeStyle = this.state === 'drawing' ? 'rgba(230,200,120,0.7)' : `${this.guard.type.color}88`; ctx.lineWidth = 3; ctx.setLineDash([8, 6]); ctx.beginPath(); ctx.moveTo(pts[0].x, pts[0].y); for (const p of pts) ctx.lineTo(p.x, p.y); ctx.stroke(); ctx.setLineDash([]); }
    for (const st of this.standards) if (st.materializeT > 0) { ctx.strokeStyle = 'rgba(160,58,42,0.6)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(st.x, st.y, 12, 0, Math.PI * 2 * (1 - st.materializeT / st.guard.type.standardDelay)); ctx.stroke(); }
  }
  drawWeapon(ctx) { const k = this.k; ctx.fillStyle = '#b8bcc2'; ctx.fillRect(k.r - 4, -3, 34, 6); ctx.fillStyle = '#5a4a36'; ctx.fillRect(k.r - 6, -7, 4, 14); return true; }
  hud(m) {
    const T = this.type; const g = this.guard;
    const st = g ? `${g.type.name}: ${g.state === 'patrol' ? 'patrolling' : g.state === 'spotted' ? 'spotted!' : `HUNTING ${g.target?.name}`} ${Math.ceil(g.time)}s` : this.state === 'drawing' ? `Drawing path ${Math.round(this.pathLen() / M)} m — Q to summon` : this.cd('power') > 0 ? `Recharging ${Math.ceil(this.cd('power'))}s` : `Next: ${T.name}`;
    return { title: 'Guardia Compagnia', value: T.name.replace('The ', ''), meter: this.state === 'drawing' ? this.pathLen() / this.maxLen : g ? g.time / (g.state === 'patrol' ? g.type.patrolTime : g.type.huntTime) : 1 - this.cd('power') / this.recharge, state: st,
      hints: [['Q / RMB', 'Start a patrol path · again to summon'], ['E', 'Cycle guard (Carnifex / Assassin / Jailer)']] };
  }
}

return { GUARDS, KnightPower };
};
__defs["js/killers/cenobite.js"] = function(){
// THE CENOBITE — Summons of Pain.
//  • Possessed Chain (hold RMB): a chain shoots out, starting slow and accelerating to
//    40 m/s, steered with the mouse while RMB is held (max 24 m). It stops on walls. A hit
//    binds the survivor with 3 chains: slowed, cannot run or interact, and blocked from
//    the exit gates. Each chain takes 1 s to break free.
//  • Gateway (E): conjure a gateway up to 16 m away and possess it for 6 s; your next
//    chain shot comes out of the gateway.
//  • Lament Configuration: the puzzle box lies on the map. If nobody picks it up within
//    90 s, a Chain Hunt starts — chains hunt every survivor until someone takes the box.
//    The holder is Oblivious and hunted by chains until they solve it (6 s). The Cenobite
//    can summon himself to a survivor who is solving (E, 3.25 s), or pick the box up
//    himself (SPACE): every survivor is bound with 3 chains and screams.
const { M, dist, angleDiff, normAngle, clamp, rr } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, Device } = __req("js/killers/framework.js");

const CHAIN_RANGE = 24 * M;
class CenobitePower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'lament'; this.name = 'Summons of Pain'; this.state = 'idle';
    this.chain = null; this.hunters = []; this.box = null; this.boxCharge = 0; this.boxHolder = null; this.respawnT = 0; this.hunt = false; this.huntT = 0;
    this.huntCharge = this.has('impaling_wire') ? 80 : 90; this.gateway = null; this.summonT = 0;
    this.aiProfile = { type: 'chains_box', label: 'Chains & the Lament Configuration', counter: ['break line of sight from chains', 'pick up and solve the box when safe', 'break chains quickly'] };
  }
  speedMult() { if (this.state === 'chain') return 0.25; if (this.state === 'gateway' || this.state === 'summon') return 0; return 1; }
  canAttack() { return this.state === 'idle'; }
  noContext() { return this.state !== 'idle'; }
  onInit(m) { this._m = m; this.spawnBox(m); }
  spawnBox(m) {
    // the box appears somewhere away from the Cenobite
    const spots = m.map.openSpots(8, 6, []); const k = this.k;
    const p = spots.sort((a, b) => dist(b.x, b.y, k.x, k.y) - dist(a.x, a.y, k.x, k.y))[Math.floor(Math.random() * Math.min(3, spots.length))] || { x: m.map.w / 2, y: m.map.h / 2 };
    this.box = m.addDevice(new Device({ kind: 'lament', x: p.x, y: p.y, r: 0.4 * M, label: 'Lament Configuration', useTime: 1.0, survivorUsable: true, seenRange: 60 * M,
      usable: (s) => !s.box && !s.status.has('chained'), onUse: (s, mm) => this.pickBox(mm, s) }));
    this.boxCharge = 0; this.hunt = false;
    for (const s of m.survivors) s.ai?.memory.devices.set(this.box.id, this.box); // survivors always know where the box is (white aura)
  }
  pickBox(m, s) {
    m.removeDevice(this.box); this.box = null; this.boxHolder = s; s.box = true; this.hunt = false; this.huntT = 4;
    s.status.add('oblivious', { source: 'box' });
    m.notifyK(`${s.name} picked up the Lament Configuration`, '#e0b65a'); s.ai?.note('Picked up the Lament Configuration', 'box');
    m.events.emit('boxPicked', { survivor: s });
  }
  solveBox(s, m) {
    if (this.boxHolder !== s) return;
    s.box = false; s.status.remove('oblivious', 'box'); this.boxHolder = null; this.respawnT = 45;
    m.reveal(s, 3, 'Box solved'); m.notifyK(`${s.name} solved the box`, '#e0b65a'); m.audio?.play('bell');
    s.ai?.note('Solved the Lament Configuration', 'boxsolve'); m.events.emit('boxSolved', { survivor: s });
    for (const h of this.hunters) h.dead = true;
  }
  update(dt, m, inp, locked) {
    this.tickCooldowns(dt);
    const k = this.k;
    // chains also snap on their own: one every 5 s (breaking them by hand is faster)
    for (const s of m.survivors) {
      const c = s.status.get('chained'); if (!c) { s._chainT = 0; continue; }
      s._chainT = (s._chainT || 0) + dt;
      if (s._chainT > 5) { s._chainT = 0; c.value = (c.value ?? 3) - 1; if (c.value <= 0) { s.status.remove('chained'); m.events.emit('chainsBroken', { survivor: s }); } }
    }
    if (this.state === 'summon') { this.summonT -= dt; if (this.summonT <= 0) this.finishSummon(m); return; }
    if (this.state === 'gateway') { this.gwT -= dt; if (this.gwT <= 0) { this.state = 'idle'; this.gateway = null; this.setCd('gateway', 3); } }
    if (this.chain) this.updateChain(dt, m, inp);
    // Locked (stunned, vaulting, picking up…): a chain in flight is cancelled through the normal
    // end path so the power leaves the 'chain' state (it used to be dropped silently, leaving
    // the Cenobite stuck at chain speed with attacks disabled).
    if (locked) { if (this.chain || this.state === 'chain') this.endChain(); return; }
    if ((this.state === 'idle' || this.state === 'gateway') && inp.rmb && !this.chain && !k.carrying && this.cd('chain') <= 0) this.shoot(m);
    if (inp.eP && !k.carrying) {
      const solver = m.survivors.find((s) => s.action?.type === 'solveBox');
      if (solver && this.state === 'idle') { this.state = 'summon'; this.summonT = 3.25; this.summonTo = solver; m.sound('power_warn', solver.x, solver.y, 14 * M, { what: 'summon', note: 'The Cenobite is coming for the box' }); }
      else if (this.state === 'idle' && this.cd('gateway') <= 0) this.placeGateway(m, inp);
    }
  }
  placeGateway(m, inp) {
    const k = this.k; const d = clamp(inp.aimDist ?? 10 * M, 1.5 * M, 16 * M);
    let x = k.x, y = k.y; const st = 0.5 * M;
    for (let t = st; t <= d; t += st) { const nx = k.x + Math.cos(k.facing) * t, ny = k.y + Math.sin(k.facing) * t; if (m.world.pointBlocked(nx, ny, 0.3 * M)) break; x = nx; y = ny; }
    this.gateway = { x, y, a: k.facing }; this.state = 'gateway'; this.gwT = 6; m.audio?.play('teleport');
    m.events.emit(EV.POWER_USED, { killer: k, power: 'gateway' });
  }
  shoot(m) {
    const k = this.k, g = this.gateway; const ox = g ? g.x : k.x + Math.cos(k.facing) * k.r, oy = g ? g.y : k.y + Math.sin(k.facing) * k.r;
    this.chain = { x: ox, y: oy, ox, oy, dir: k.facing, v: 10 * M, t: 0, len: 0, fromGateway: !!g }; this.state = 'chain';
    m.sound('power_windup', ox, oy, 24 * M, { what: 'chain', dir: k.facing });
    m.events.emit(EV.POWER_USED, { killer: k, power: 'chain' }); m.audio?.play('throw');
  }
  updateChain(dt, m, inp) {
    const c = this.chain; c.t += dt;
    c.v = c.t < 0.4 ? 10 * M : c.t < 0.7 ? 15 * M + (c.t - 0.4) / 0.3 * 22.5 * M : Math.min(40 * M, 37.5 * M + (c.t - 0.7) * 0.8 * M);
    // steer toward the mouse (from the chain head), limited turn rate
    const k = this.k; const ax = k.x + Math.cos(inp.aim) * (inp.aimDist ?? 10 * M), ay = k.y + Math.sin(inp.aim) * (inp.aimDist ?? 10 * M);
    const want = Math.atan2(ay - c.y, ax - c.x); c.dir += clamp(normAngle(want - c.dir), -4.2 * dt, 4.2 * dt);
    const step = c.v * dt; const nx = c.x + Math.cos(c.dir) * step, ny = c.y + Math.sin(c.dir) * step;
    const hit = m.world.firstHit(c.x, c.y, nx, ny, 0.15 * M, (s) => s.blocksLOS);
    for (const s of m.survivors) {
      if (!s.standing) continue;
      if (dist(s.x, s.y, nx, ny) < s.r + 0.25 * M || dist(s.x, s.y, (c.x + nx) / 2, (c.y + ny) / 2) < s.r + 0.25 * M) { this.bind(m, s, 3, 'Possessed Chain'); this.endChain(); return; }
    }
    if (hit) { m.fx.sparks(c.x, c.y); this.endChain(); return; }
    c.x = nx; c.y = ny; c.len += step;
    if (c.len >= CHAIN_RANGE || !inp.rmb && c.t > 0.15) this.endChain();
  }
  // A chain always returns control to the Cenobite's body: a chain fired from a Gateway closes
  // the Gateway (it used to drop him back into the motionless 'gateway' state for the rest of
  // its 6 s, which looked like being stuck after every hit).
  endChain() {
    const fromGateway = this.chain?.fromGateway || this.state === 'gateway';
    this.chain = null;
    if (this.state === 'chain' || this.state === 'gateway') this.state = 'idle';
    if (fromGateway && this.gateway) { this.gateway = null; this.setCd('gateway', 3); }
    this.setCd('chain', this.has('chatterer_tooth') ? 4 : 5);
  }
  bind(m, s, n, label) {
    if (s.dodgeT && s.dodgeT > m.time) { m.fx.text(s.x, s.y - 30, 'DODGED', '#ffd27a'); return; }
    const cur = s.status.get('chained'); const v = Math.min(3, (cur?.value ?? 0) + n);
    s.status.add('chained', { value: v, source: 'cenobite' }); const c2 = s.status.get('chained'); c2.value = v; c2.expiration = Infinity;
    s.stopAction(m); s.vault = null; s.gateBlockUntil = m.time + 5; if (!cur) m.events.emit('powerTriggered', { survivor: s, x: s.x, y: s.y }); 
    m.fx.burst(s.x, s.y, 14, '#b8b8c0', 80, 0.6, 2, 2); m.audio?.play('clang');
    m.addScore('deviousness', 200, label); this.k.stats.powerHits++;
    s.ai?.note(`Bound by ${label}`, 'chained'); m.events.emit('survivorChained', { survivor: s });
  }
  finishSummon(m) {
    const s = this.summonTo, k = this.k; this.state = 'idle';
    if (!s || !s.standing) return;
    const a = Math.random() * Math.PI * 2; let best = null;
    for (let i = 0; i < 12; i++) { const r = rr(Math.random, 9, 12) * M; const x = s.x + Math.cos(a + i) * r, y = s.y + Math.sin(a + i) * r; if (!m.world.pointBlocked(x, y, k.r) && x > 0 && y > 0 && x < m.map.w && y < m.map.h) { best = { x, y }; break; } }
    if (best) { k.x = best.x; k.y = best.y; m.world.resolve(k, k.r); k.facing = Math.atan2(s.y - k.y, s.x - k.x); }
    if (s.action?.type === 'solveBox') s.stopAction(m);
    this.bind(m, s, 1, 'Summons'); m.audio?.play('teleport');
  }
  // Space near the box: the Cenobite claims it — every survivor is chained and screams.
  context(m) {
    if (!this.box || this.state !== 'idle' || this.k.carrying) return null;
    if (dist(this.k.x, this.k.y, this.box.x, this.box.y) > 1.8 * M) return null;
    return { type: 'custom', label: 'Take the Lament Configuration', target: { dur: 1.0, label: 'Claiming the box', done: (mm) => this.claimBox(mm) } };
  }
  claimBox(m) {
    if (!this.box) return; m.removeDevice(this.box); this.box = null; this.respawnT = 10;
    for (const s of m.survivors) if (s.standing) { this.bind(m, s, 3, 'Lament'); m.reveal(s, 3, 'Scream'); m.addGrunt(s); }
    m.audio?.play('scream'); m.notifyK('Every survivor is bound', '#e0b65a');
  }
  // Always-running clocks: box charge, Chain Hunt, hunting chains, respawns.
  tick(dt, m) {
    if (!this.box && !this.boxHolder && this.respawnT > 0) { this.respawnT -= dt; if (this.respawnT <= 0) this.spawnBox(m); }
    if (this.boxHolder && !this.boxHolder.standing) {
      // downing (or losing) the holder: everyone is bound, the box comes back soon
      const h = this.boxHolder; h.box = false; h.status.remove('oblivious', 'box'); this.boxHolder = null; this.respawnT = 10;
      if (h.health === 'dying') for (const s of m.survivors) if (s.standing) { this.bind(m, s, 3, 'Lament'); m.reveal(s, 3, 'Scream'); }
    }
    if (this.box) { this.boxCharge += dt; if (this.boxCharge >= this.huntCharge && !this.hunt) { this.hunt = true; this.huntT = 2; m.notifyK('CHAIN HUNT', '#e0b65a'); m.events.emit('chainHunt', {}); for (const s of m.survivors) s.ai?.note('Chain Hunt started — someone must get the box', 'chainhunt'); } }
    const targets = this.hunt ? m.survivors.filter((s) => s.standing) : this.boxHolder ? [this.boxHolder] : [];
    if (targets.length && m.aliveCount() > 1) {
      this.huntT -= dt;
      if (this.huntT <= 0) { this.huntT = rr(Math.random, 9, 12) / (this.hunt ? 1 : 1.2); for (const s of targets) this.spawnHunter(m, s); }
    }
    for (const h of this.hunters) this.updateHunter(dt, m, h);
    this.hunters = this.hunters.filter((h) => !h.dead);
  }
  spawnHunter(m, s) {
    const a = Math.random() * Math.PI * 2; const x = clamp(s.x + Math.cos(a) * 14 * M, 4 * M, m.map.w - 4 * M), y = clamp(s.y + Math.sin(a) * 14 * M, 4 * M, m.map.h - 4 * M);
    const h = { x, y, ox: x, oy: y, target: s, dir: Math.atan2(s.y - y, s.x - x), v: 11 * M, t: 0, dead: false };
    this.hunters.push(h); m.sound('power_windup', x, y, 18 * M, { what: 'hunt_chain', dir: h.dir });
  }
  updateHunter(dt, m, h) {
    const s = h.target; h.t += dt; if (!s.standing || h.t > 3) { h.dead = true; return; }
    const want = Math.atan2(s.y - h.y, s.x - h.x); h.dir += clamp(normAngle(want - h.dir), -1.6 * dt, 1.6 * dt);
    const nx = h.x + Math.cos(h.dir) * h.v * dt, ny = h.y + Math.sin(h.dir) * h.v * dt;
    if (h.t > 0.6 && m.world.firstHit(h.x, h.y, nx, ny, 0.15 * M, (q) => q.blocksLOS)) { h.dead = true; m.fx.sparks(h.x, h.y); return; }
    h.x = nx; h.y = ny;
    if (dist(s.x, s.y, h.x, h.y) < s.r + 0.3 * M) { h.dead = true; this.bind(m, s, 1, 'Chain Hunt'); }
  }
  drawDevice(ctx, d, m) {
    if (d.kind !== 'lament') return;
    ctx.save(); ctx.translate(d.x, d.y); ctx.rotate(m.time * 0.6); ctx.fillStyle = '#c9a23a'; ctx.fillRect(-7, -7, 14, 14); ctx.strokeStyle = '#3a2a10'; ctx.lineWidth = 1.5; ctx.strokeRect(-7, -7, 14, 14); ctx.beginPath(); ctx.arc(0, 0, 3, 0, Math.PI * 2); ctx.stroke(); ctx.restore();
    if (this.hunt) { ctx.strokeStyle = 'rgba(224,182,90,0.5)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(d.x, d.y, 18 + Math.sin(m.time * 6) * 4, 0, Math.PI * 2); ctx.stroke(); }
  }
  drawGround(ctx, m) {
    if (this.gateway) { const g = this.gateway; ctx.strokeStyle = 'rgba(224,182,90,0.9)'; ctx.lineWidth = 3; ctx.beginPath(); ctx.arc(g.x, g.y, 14, 0, Math.PI * 2); ctx.stroke(); ctx.fillStyle = 'rgba(60,30,10,0.6)'; ctx.fill(); }
  }
  drawOver(ctx, m) {
    const k = this.k; const draw = (c, ox, oy) => { ctx.strokeStyle = '#c8c8d0'; ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(ox, oy); ctx.lineTo(c.x, c.y); ctx.stroke(); ctx.fillStyle = '#e0e0e8'; ctx.beginPath(); ctx.arc(c.x, c.y, 4, 0, Math.PI * 2); ctx.fill(); };
    if (this.chain) draw(this.chain, this.chain.ox, this.chain.oy);
    for (const h of this.hunters) draw(h, h.ox, h.oy);
    if (this.boxHolder) { const s = this.boxHolder; ctx.fillStyle = '#c9a23a'; ctx.fillRect(s.x - 4, s.y - s.r - 14, 8, 8); }
    if (this.state === 'summon') { ctx.strokeStyle = 'rgba(224,182,90,0.8)'; ctx.lineWidth = 3; ctx.beginPath(); ctx.arc(k.x, k.y, k.r + 10, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * (1 - this.summonT / 3.25)); ctx.stroke(); }
  }
  drawBody(ctx) { ctx.fillStyle = '#d8d0c8'; ctx.beginPath(); ctx.arc(5, 0, 6, 0, Math.PI * 2); ctx.fill(); ctx.strokeStyle = '#555'; ctx.lineWidth = 1; for (let i = -1; i <= 1; i++) for (let j = -1; j <= 1; j++) { ctx.beginPath(); ctx.arc(5 + i * 3, j * 3, 0.8, 0, Math.PI * 2); ctx.stroke(); } }
  drawWeapon(ctx) { const k = this.k; ctx.strokeStyle = '#9a9aa2'; ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(k.r - 4, 6); ctx.quadraticCurveTo(k.r + 10, 14, k.r + 18, 8); ctx.stroke(); return true; }
  prompt(m) { const solver = m.survivors.find((s) => s.action?.type === 'solveBox'); return solver && this.state === 'idle' ? { key: 'E', label: `Summon to ${solver.name} (solving the box)` } : null; }
  hud(m) {
    const st = this.state === 'summon' ? 'Summoning…' : this.state === 'gateway' ? `Gateway ${Math.ceil(this.gwT)}s` : this.hunt ? 'CHAIN HUNT' : this.boxHolder ? `${this.boxHolder.name} holds the box` : this.box ? `Chain Hunt in ${Math.ceil(this.huntCharge - this.boxCharge)}s` : 'Box respawning';
    return { title: 'Summons of Pain', value: this.cd('chain') > 0 ? Math.ceil(this.cd('chain')) + 's' : 'Ready', meter: this.box ? this.boxCharge / this.huntCharge : 1 - this.cd('chain') / 5, state: st,
      hints: [['Q / RMB', 'Hold: Possessed Chain (steer with mouse)'], ['E', 'Gateway · Summon to a box solver'], ['SPACE', 'Take the box']] };
  }
}

return { CenobitePower };
};
__defs["js/killers/mastermind.js"] = function(){
// THE MASTERMIND — Virulent Bound. Hold RMB to charge (1.5 s, slowed), release to bound
// forward at 14 m/s. With a second token you can bound again within 2 s of the first,
// further (1 s). A bound that hits a survivor infects them with Uroboros (+20) and grabs
// them: he carries them through the rest of the bound and throws them. Slamming the
// survivor into a wall or rock while carrying (or when thrown) injures them. A fully
// infected survivor is Hindered, and a bound into them deals double damage (downs).
// Infection grows slowly on its own. Six supply cases hold First Aid Sprays that cure it
// (using one makes the survivor scream for 4 s of Killer Instinct).
const { M, dist, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, Device, killerInstinct } = __req("js/killers/framework.js");

class MastermindPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'virulent'; this.name = 'Virulent Bound'; this.state = 'idle'; this.tokens = 2; this.maxTokens = 2;
    this.charge = 0; this.chargeTime = this.has('cell_divider') ? 1.25 : 1.5; this.boundSpeed = 14 * M; this.chainWin = 0; this.held = null; this.tokenT = 0;
    this.aiProfile = { type: 'dash', label: 'Virulent Bound', counter: ['stay out of straight dash lanes', 'hug obstacles and windows', 'cure infection with sprays'] };
  }
  speedMult() {
    if (this.state === 'charging') return 3.68 / this.k.def.speed;
    if (this.state === 'bound') return this.boundSpeed / (this.k.def.speed * M);
    if (this.state === 'recover') return 0.35;
    return 1;
  }
  canAttack() { return this.state === 'idle'; }
  noContext() { return this.state === 'bound' || this.state === 'charging'; }
  overrideMove() { return this.state === 'bound' ? { x: Math.cos(this.dir), y: Math.sin(this.dir) } : null; }
  onInit(m) {
    for (const p of m.map.openSpots(6, 18, m.map.gens)) m.addDevice(new Device({ kind: 'supply', x: p.x, y: p.y, r: 0.6 * M, label: 'Supply case', useTime: 1.2, survivorUsable: true, seenRange: 20 * M, data: { sprays: 2 },
      usable: (s, mm) => true, onUse: () => {} }));
    for (const d of m.devices) if (d.kind === 'supply') { d.usable = (s) => d.data.sprays > 0 && (s.sprays || 0) < 1; d.onUse = (s, mm) => { d.data.sprays--; s.sprays = (s.sprays || 0) + 1; s.ai?.note('Took a First Aid Spray', 'spray'); }; }
  }
  update(dt, m, inp, locked) {
    this.tickCooldowns(dt);
    const k = this.k;
    if (this.tokens < this.maxTokens && this.state === 'idle') { this.tokenT += dt; if (this.tokenT > 5) { this.tokenT = 0; this.tokens++; } }
    if (this.state === 'bound') return this.updateBound(dt, m);
    if (this.state === 'recover') { this.t += dt; if (this.t > 1.1) this.state = 'idle'; return; }
    if (this.chainWin > 0) this.chainWin -= dt;
    if (locked) { if (this.state === 'charging') this.state = 'idle'; return; }
    const canStart = this.tokens > 0 && !k.carrying && (this.state === 'idle' || this.state === 'between');
    if (canStart && inp.rmb && this.state === 'idle') { this.state = 'charging'; this.charge = 0; this.second = false; m.sound('power_windup', k.x, k.y, 24 * M, { what: 'bound', dir: k.facing }); }
    if (this.state === 'between') {
      if (this.chainWin <= 0) { this.state = 'recover'; this.t = 0; return; }
      if (inp.rmbP && this.tokens > 0) this.startBound(m, 1.0, true);
      return;
    }
    if (this.state === 'charging') {
      this.charge += dt / this.chargeTime;
      if (!inp.rmb) { if (this.charge >= 1) this.startBound(m, 0.5, false); else this.state = 'idle'; }
    }
  }
  startBound(m, dur, second) {
    const k = this.k; this.tokens--; this.state = 'bound'; this.dir = k.facing; this.boundT = dur; this.second = second; this.lockFacing = true; this.hitThisBound = false;
    m.audio?.play('swing'); m.fx.shake(4); m.events.emit(EV.POWER_USED, { killer: k, power: 'bound', second });
    m.sound('killer_action', k.x, k.y, 24 * M, { what: 'bound' });
  }
  updateBound(dt, m) {
    const k = this.k; this.boundT -= dt; k.facing = this.dir;
    if (this.held) { const s = this.held; s.x = k.x + Math.cos(this.dir) * (k.r + s.r); s.y = k.y + Math.sin(this.dir) * (k.r + s.r); }
    else for (const s of m.survivors) {
      if (!s.standing || s.vault) continue;
      if (dist(s.x, s.y, k.x + Math.cos(this.dir) * k.r, k.y + Math.sin(this.dir) * k.r) > s.r + k.r * 0.8 + 0.2 * M) continue;
      if (!m.world.losClear(k.x, k.y, s.x, s.y)) continue;
      if (s.dodgeT && s.dodgeT > m.time) { m.fx.text(s.x, s.y - 30, 'DODGED', '#ffd27a'); continue; }
      this.collide(m, s); break;
    }
    if (this.boundT <= 0) this.endBound(m);
  }
  infect(m, s, n) {
    const st = s.status.get('infected'); const v = Math.min(100, (st?.value ?? 0) + n);
    s.status.add('infected', { value: v, source: 'uroboros' }); const c = s.status.get('infected'); c.value = v; c.expiration = Infinity;
  }
  collide(m, s) {
    const k = this.k; this.hitThisBound = true;
    const crit = (s.status.get('infected')?.value ?? 0) >= 100;
    this.infect(m, s, 20); m.addScore('brutality', 300, 'Virulent Bound'); k.stats.powerHits++;
    if (crit || s.health === 'injured') {
      // infected & injured, or critically infected: the bound itself downs them
      const res = s.takeHit(m, { source: 'bound', instadown: crit }); k.stats.hits++;
      m.events.emit(EV.KILLER_HIT, { killer: k, survivor: s, result: res, power: true }); m.fx.hit(s, res);
      this.state = 'recover'; this.t = 0; this.lockFacing = false; return;
    }
    if (s.action) {
      // interacting survivors are not grabbed — they take the hit where they stand
      const res = s.takeHit(m, { source: 'bound' }); k.stats.hits++; m.events.emit(EV.KILLER_HIT, { killer: k, survivor: s, result: res, power: true }); m.fx.hit(s, res);
      this.state = 'recover'; this.t = 0; this.lockFacing = false; return;
    }
    // grab and carry
    this.held = s; s.stopAction(m); s.status.add('grabbed', { source: 'wesker' }); m.events.emit('powerTriggered', { survivor: s, x: s.x, y: s.y }); 
    const self = this; s.control = { kind: 'wesker', allow: [], update: () => self.held === s || self.thrown?.s === s };
    m.fx.burst(s.x, s.y, 10, '#7fae3a', 60, 0.4, 2, 2); s.ai?.note('Grabbed by the Mastermind', 'wesker');
    this.boundT = Math.max(this.boundT, 0.35);
  }
  onCollide(m) {
    if (this.state !== 'bound') return;
    if (this.held) this.slam(m, this.held, 'wall'); else this.endBound(m, true);
  }
  slam(m, s, why) {
    const k = this.k; this.held = null; this.release(m, s);
    const res = s.takeHit(m, { source: 'bound' }); k.stats.hits++;
    if (res === 'injured') s.status.add('deepWound', { duration: 20 });
    m.events.emit(EV.KILLER_HIT, { killer: k, survivor: s, result: res, power: true }); m.fx.hit(s, res); m.fx.shake(10); m.audio?.play('clang');
    s.ai?.note('Slammed into a wall by the Mastermind', 'slam');
    this.state = 'recover'; this.t = 0; this.lockFacing = false;
  }
  release(m, s) { s.status.remove('grabbed', 'wesker'); if (s.control?.kind === 'wesker') s.control = null; }
  endBound(m, hitWall = false) {
    const k = this.k; this.lockFacing = false;
    if (this.held) {
      // throw: the survivor flies 3 m forward; hitting something on the way hurts
      const s = this.held; this.held = null; this.release(m, s);
      const tx = s.x + Math.cos(this.dir) * 3 * M, ty = s.y + Math.sin(this.dir) * 3 * M;
      if (!m.world.sweepClear(s.x, s.y, tx, ty, s.r)) { this.slam(m, s, 'throw'); return; }
      s.x = tx; s.y = ty; m.world.resolve(s, s.r);
      // being grabbed and thrown costs a health state (like DBD), a wall slam adds Deep Wound
      const res = s.takeHit(m, { source: 'bound' }); k.stats.hits++; m.events.emit(EV.KILLER_HIT, { killer: k, survivor: s, result: res, power: true }); m.fx.hit(s, res);
      if (res === 'injured') s.status.add('haste', { duration: 1.5, value: 0.2, source: 'thrown' });
      m.fx.text(s.x, s.y - 30, 'THROWN', '#9fd06a'); this.state = 'recover'; this.t = 0; return;
    }
    if (!this.second && !this.hitThisBound && this.tokens > 0 && !hitWall) { this.state = 'between'; this.chainWin = 2; return; }
    this.state = 'recover'; this.t = 0;
  }
  // infection creep and the supply-case sprays
  tick(dt, m) {
    for (const s of m.survivors) {
      const st = s.status.get('infected'); if (!st) continue;
      if (s.health === 'hooked') { st.value = 1; continue; }
      if (s.standing && st.value < 100) st.value = Math.min(100, st.value + dt * 0.8);
    }
  }
  onSpray(s, m) { killerInstinct(m, s, 4, 'First Aid Spray'); s.ai?.note('Cured the infection with a spray', 'cure'); }
  onStun(m) { if (this.held) { const s = this.held; this.held = null; this.release(m, s); } this.state = 'idle'; this.lockFacing = false; }
  drawDevice(ctx, d) {
    if (d.kind !== 'supply') return;
    ctx.fillStyle = '#2a3a4a'; ctx.fillRect(d.x - 12, d.y - 8, 24, 16); ctx.fillStyle = d.data.sprays ? '#7fd0a0' : '#445'; ctx.fillRect(d.x - 3, d.y - 6, 6, 12); ctx.fillRect(d.x - 6, d.y - 3, 12, 6);
  }
  drawOver(ctx) {
    const k = this.k;
    if (this.state === 'charging' || this.state === 'between') { const len = (this.state === 'between' ? 14 : 7) * M; ctx.fillStyle = `rgba(127,174,58,${this.state === 'charging' ? 0.08 + 0.18 * Math.min(1, this.charge) : 0.22})`; ctx.save(); ctx.translate(k.x, k.y); ctx.rotate(k.facing); ctx.fillRect(0, -0.6 * M, len, 1.2 * M); ctx.restore(); }
  }
  drawWeapon(ctx) { const k = this.k; ctx.fillStyle = '#1a1a1a'; ctx.beginPath(); ctx.arc(k.r + 4, 6, 5, 0, Math.PI * 2); ctx.fill(); return true; }
  drawBody(ctx) { ctx.fillStyle = '#e8d8a0'; ctx.fillRect(2, -6, 6, 12); ctx.fillStyle = '#111'; ctx.fillRect(7, -5, 3, 10); }
  hud() {
    const st = { idle: 'Ready', charging: this.charge >= 1 ? 'Release to bound' : 'Charging…', bound: 'BOUND', between: 'Q again: second bound', recover: 'Recovering' }[this.state];
    return { title: 'Virulent Bound', value: `${this.tokens}/${this.maxTokens}`, meter: this.state === 'charging' ? Math.min(1, this.charge) : this.tokens / this.maxTokens, state: st,
      hints: [['Q / RMB', 'Hold to charge, release to bound'], ['Q / RMB', 'Within 2 s: second, longer bound']] };
  }
}

return { MastermindPower };
};
__defs["js/killers/houndmaster.js"] = function(){
// THE HOUNDMASTER — Hunting. Her dog, Snug, is a real entity with its own pathfinding.
//  • Chase Command (hold RMB to plot, release): Snug charges along the plotted line at
//    high speed. Tap RMB again while it runs to take control and redirect it once.
//  • Search Command (E): aim the cane 5–40 m away; Snug paths there with a lullaby that
//    grows as it travels, giving Killer Instinct on survivors near it. A survivor it spots
//    is pursued. While the search runs and she stays near Snug's route, the Houndmaster
//    moves at 6 m/s.
//  • A dog that reaches a survivor pounces and drags them toward the Houndmaster for up to
//    8 s (2 s with Endurance): Incapacitated, Hindered, aura revealed, gates blocked.
//    Dropping a pallet on Snug stuns it for 3 s and frees the survivor.
const { M, dist, angleDiff, normAngle, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, Minion } = __req("js/killers/framework.js");

class Dog extends Minion {
  constructor(power) {
    super({ kind: 'dog', owner: power.k, x: power.k.x, y: power.k.y, r: 0.36 * M, color: '#8a6a4a', label: 'dog', threat: 1, visRange: 30 * M });
    this.p = power; this.state = 'follow'; this.stunnable = true; this.stunTime = 3; this.repath = 0;
  }
  onStun(m) { if (this.grabbed) this.p.releaseGrab(m, 'pallet'); this.state = 'return'; this.path = null; }
  update(dt, m) {
    const k = this.p.k; this.t += dt;
    switch (this.state) {
      case 'follow': {
        const bx = k.x - Math.cos(k.facing) * 1.1 * M, by = k.y - Math.sin(k.facing) * 1.1 * M; const d = dist(bx, by, this.x, this.y);
        if (d > 6 * M) { this.x = bx; this.y = by; } else if (d > 0.3 * M) this.moveDir(dt, m, (bx - this.x) / d, (by - this.y) / d, Math.min(6 * M, d * 4));
        break;
      }
      case 'chase': {
        this.left -= this.speed * dt;
        const hit = this.moveDir(dt, m, Math.cos(this.dir), Math.sin(this.dir), 8.2 * M);
        if (this.tryGrab(m)) return;
        if (hit || this.left <= 0) { this.state = 'return'; this.path = null; }
        break;
      }
      case 'search': {
        this.travel += Math.hypot(this.vx, this.vy) * dt; this.radius = Math.min(14 * M, 4 * M + this.travel * 0.3);
        if (this.followPath(dt, m, 6.2 * M) || this.t > 9) { this.state = 'return'; this.path = null; break; }
        for (const s of m.survivors) {
          if (!s.standing) continue; const d = dist(s.x, s.y, this.x, this.y);
          if (d < this.p.lullaby) m.reveal(s, 0.4, 'Lullaby', true);
          if (d < this.radius && m.world.losClear(this.x, this.y, s.x, s.y)) { this.state = 'pursue'; this.target = s; this.pursueT = 5; m.reveal(s, 2, 'Snug'); s.ai?.onDogPursuit?.(this); break; }
        }
        break;
      }
      case 'pursue': {
        const s = this.target; this.pursueT -= dt;
        if (!s.standing || this.pursueT <= 0) { this.state = 'return'; this.path = null; break; }
        this.repath -= dt; if (this.repath <= 0) { this.repath = 0.3; this.pathTo(m, s.x, s.y); }
        this.followPath(dt, m, 7.4 * M);
        this.tryGrab(m);
        break;
      }
      case 'grab': {
        // dragging the survivor toward the Houndmaster
        const s = this.grabbed; this.grabT -= dt;
        if (!s || !s.standing || this.grabT <= 0) { this.p.releaseGrab(m, 'time'); break; }
        const d = dist(k.x, k.y, this.x, this.y);
        if (d > 1.6 * M) {
          if (!this.path || (this.repath -= dt) <= 0) { this.repath = 0.4; this.pathTo(m, k.x, k.y); }
          this.followPath(dt, m, 2.6 * M);
        }
        s.x = this.x - Math.cos(this.facing) * (this.r + s.r * 0.8); s.y = this.y - Math.sin(this.facing) * (this.r + s.r * 0.8); m.world.resolve(s, s.r);
        m.reveal(s, 0.3, 'Grabbed', true);
        // dragged across a dropped pallet: survivor can slam it on the dog (handled by match stun)
        break;
      }
      case 'return': {
        const d = dist(k.x, k.y, this.x, this.y);
        if (d < 1.5 * M) { this.state = 'follow'; this.p.setCd('dog', 3); break; }
        this.repath -= dt; if (this.repath <= 0 || !this.path) { this.repath = 0.6; this.pathTo(m, k.x, k.y); }
        if (this.followPath(dt, m, 7 * M) && d > 1.5 * M) this.path = null;
        if (this.t > 12) { this.x = k.x; this.y = k.y; this.state = 'follow'; }
        break;
      }
    }
  }
  tryGrab(m) {
    for (const s of m.survivors) {
      if (!s.standing || s.vault) continue;
      if (dist(s.x, s.y, this.x, this.y) > s.r + this.r + 0.25 * M) continue;
      if (s.dodgeT && s.dodgeT > m.time) continue;
      this.p.grab(m, s); return true;
    }
    return false;
  }
}

class HoundmasterPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'hound'; this.name = 'Hunting'; this.state = 'idle'; this.dog = null; this.plot = 0; this.lullaby = 32 * M;
    this.chaseLen = (this.has('training_whistle') ? 24 : 20) * M; this.grabTime = this.has('rope_leash') ? 9 : 8;
    this.aiProfile = { type: 'dog', label: 'The dog', counter: ['track the dog separately', 'use windows (the dog cannot vault)', 'drop pallets on the dog', 'leave the dog\'s lane'] };
  }
  onInit(m) { this.dog = m.addMinion(new Dog(this)); }
  speedMult(m) {
    if (this.state === 'plotting') return 0.75;
    const d = this.dog; if (d && (d.state === 'search' || d.state === 'pursue') && dist(this.k.x, this.k.y, d.x, d.y) < 14 * M) return 6 / this.k.def.speed;
    return 1;
  }
  canAttack() { return this.state === 'idle'; }
  update(dt, m, inp, locked) {
    this.tickCooldowns(dt);
    const k = this.k, d = this.dog;
    if (locked) { if (this.state === 'plotting') this.state = 'idle'; return; }
    const ready = d && d.state === 'follow' && this.cd('dog') <= 0 && !k.carrying;
    if (this.state === 'idle' && inp.rmb && ready) { this.state = 'plotting'; this.plot = 0; }
    if (this.state === 'plotting') {
      this.plot = Math.min(1, this.plot + dt / 0.6);
      if (!inp.rmb) { this.sendChase(m); this.state = 'idle'; }
      return;
    }
    if (d && d.state === 'chase' && inp.rmbP && !d.redirected) { d.redirected = true; d.dir = k.facing; d.left = Math.max(d.left, 8 * M); m.audio?.play('ui'); }
    if (inp.eP && ready) this.sendSearch(m, inp);
  }
  sendChase(m) {
    const d = this.dog, k = this.k; d.state = 'chase'; d.t0 = m.time; d.dir = k.facing; d.left = this.chaseLen * (0.5 + 0.5 * this.plot); d.speed = 8.2 * M; d.redirected = false; d.t = 0;
    d.x = k.x + Math.cos(k.facing) * 0.8 * M; d.y = k.y + Math.sin(k.facing) * 0.8 * M; m.world.resolve(d, d.r);
    m.audio?.play('swing'); m.sound('power_windup', d.x, d.y, 26 * M, { what: 'dog_chase', dir: d.dir });
    m.events.emit(EV.POWER_USED, { killer: k, power: 'chase_command' });
  }
  sendSearch(m, inp) {
    const d = this.dog, k = this.k; const r = clamp(inp.aimDist ?? 15 * M, 5 * M, 40 * M);
    const tx = k.x + Math.cos(k.facing) * r, ty = k.y + Math.sin(k.facing) * r;
    if (!d.pathTo(m, tx, ty)) return;
    d.state = 'search'; d.travel = 0; d.radius = 4 * M; d.t = 0; this.searchGoal = { x: tx, y: ty };
    m.sound('dog_lullaby', d.x, d.y, this.lullaby, { what: 'search', tx, ty });
    m.events.emit(EV.POWER_USED, { killer: k, power: 'search_command' });
  }
  grab(m, s) {
    const d = this.dog; d.state = 'grab'; d.grabbed = s; d.grabT = s.status.has('endurance') ? 2 : this.grabTime; d.path = null; d.repath = 0;
    s.stopAction(m); s.vault = null; s.status.add('grabbed', { source: 'dog' }); m.events.emit('powerTriggered', { survivor: s, x: s.x, y: s.y });  s.status.add('hindered', { value: 0.1, duration: d.grabT, source: 'dog' });
    s.gateBlockUntil = m.time + d.grabT + 1.5; s.ai?.note('Grabbed by the dog!', 'doggrab');
    const self = this; s.control = { kind: 'dog', allow: ['dropPallet'], update: () => self.dog?.grabbed === s };
    m.addScore('hunter', 300, 'Dog grab'); this.k.stats.powerHits++; m.audio?.play('hit'); m.fx.burst(s.x, s.y, 12, '#8a6a4a', 70, 0.5, 2, 2);
    m.events.emit('dogGrab', { survivor: s });
  }
  releaseGrab(m, why) {
    const d = this.dog; const s = d.grabbed; d.grabbed = null; d.state = 'return'; d.path = null; d.t = 0;
    if (s) { s.status.remove('grabbed', 'dog'); if (s.control?.kind === 'dog') s.control = null; if (why === 'pallet') s.ai?.note('Freed myself — pallet on the dog', 'dogpal'); }
  }
  // a basic hit on the grabbed survivor releases the dog's hold
  tick(dt, m) { const d = this.dog; if (d?.grabbed && d.grabbed.lastHitT === m.time) this.releaseGrab(m, 'hit'); }
  drawMinion(ctx, n, m) {
    if (n.kind !== 'dog') return false;
    if (n.state === 'search' || n.state === 'pursue') { ctx.strokeStyle = 'rgba(200,170,120,0.25)'; ctx.lineWidth = 1.5; ctx.beginPath(); ctx.arc(n.x, n.y, n.radius || 4 * M, 0, Math.PI * 2); ctx.stroke(); }
    ctx.save(); ctx.translate(n.x, n.y); ctx.rotate(n.facing);
    ctx.fillStyle = 'rgba(0,0,0,0.4)'; ctx.beginPath(); ctx.ellipse(2, 3, 13, 7, 0, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = '#7a5a3c'; ctx.beginPath(); ctx.ellipse(0, 0, 12, 6.5, 0, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = '#5a3f28'; ctx.beginPath(); ctx.ellipse(11, 0, 5, 4.5, 0, 0, Math.PI * 2); ctx.fill();
    ctx.strokeStyle = '#5a3f28'; ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(-11, 0); ctx.lineTo(-17, Math.sin(m.time * 14) * 3); ctx.stroke();
    ctx.restore();
    if (n.stunT > 0) { ctx.fillStyle = '#ffd27a'; ctx.beginPath(); ctx.arc(n.x, n.y - 14, 3, 0, Math.PI * 2); ctx.fill(); }
    return true;
  }
  drawOver(ctx, m) {
    const k = this.k;
    if (this.state === 'plotting') { const L = this.chaseLen * (0.5 + 0.5 * this.plot); ctx.strokeStyle = 'rgba(200,170,120,0.8)'; ctx.setLineDash([6, 5]); ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.lineTo(k.x + Math.cos(k.facing) * L, k.y + Math.sin(k.facing) * L); ctx.stroke(); ctx.setLineDash([]); }
    const d = this.dog; if (d?.state === 'search' && this.searchGoal) { ctx.strokeStyle = 'rgba(200,170,120,0.5)'; ctx.beginPath(); ctx.arc(this.searchGoal.x, this.searchGoal.y, 10, 0, Math.PI * 2); ctx.stroke(); }
    if (d?.grabbed) { ctx.strokeStyle = 'rgba(255,90,90,0.6)'; ctx.lineWidth = 1.5; ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.lineTo(d.x, d.y); ctx.stroke(); }
  }
  drawWeapon(ctx) { const k = this.k; ctx.strokeStyle = '#6a4a2a'; ctx.lineWidth = 3; ctx.beginPath(); ctx.moveTo(k.r - 4, 6); ctx.lineTo(k.r + 20, 10); ctx.stroke(); return true; }
  hud() {
    const d = this.dog; const ds = d ? { follow: this.cd('dog') > 0 ? `Snug ready in ${Math.ceil(this.cd('dog'))}s` : 'Snug ready', chase: 'Chase Command', search: 'Search Command', pursue: `Pursuing ${d.target?.name}`, grab: `Dragging ${d.grabbed?.name}`, return: 'Snug returning' }[d.state] : '';
    return { title: 'Hunting', value: d?.state === 'grab' ? Math.ceil(d.grabT) + 's' : d?.state === 'follow' ? 'Ready' : '…', meter: this.state === 'plotting' ? this.plot : d?.state === 'follow' ? 1 - this.cd('dog') / 3 : 0.3, state: ds,
      hints: [['Q / RMB', 'Hold to plot, release: Chase Command · tap again to redirect'], ['E', 'Search Command at the aimed spot']] };
  }
}

return { HoundmasterPower };
};
__defs["js/killers/lich.js"] = function(){
// THE LICH (Vecna) — Vile Darkness: four spells, chosen with the mouse wheel, 1–4 or F,
// and cast with Q / RMB. Each spell has its own cooldown.
//  1 Flight of the Damned (30 s): five spectral entities sweep forward in a fan, passing
//    through walls; they injure survivors they touch — but not crouching survivors.
//  2 Fly (20 s): levitate for up to 5 s at 8 m/s over pallets and windows (not walls).
//    After landing he cannot attack for 2.75 s. Tap Q again to land early.
//  3 Mage Hand (35 s): an invisible hand grabs the pallet you aim at (7 m): a dropped
//    pallet is lifted back up; a standing pallet is held so it cannot be dropped for 4 s.
//  4 Dispelling Sphere (30 s): an invisible sphere rolls forward (9.75 → 6.5 m/s) through
//    walls; survivors it touches trigger Killer Instinct and their Magic Items are
//    disabled for 45 s.
// Magic Items come from six treasure chests and each counters one spell:
//    Skyguard (see him while he flies) · Nightwatch (see the damned through walls)
//    Archivist (see the sphere) · Interloper (warned of Mage Hand + haste).
const { M, dist, angleDiff, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, Projectile, Device, killerInstinct } = __req("js/killers/framework.js");

const SPELLS = [
  { id: 'fotd', name: 'Flight of the Damned', icon: '☠', cd: 30 },
  { id: 'fly', name: 'Fly', icon: '⇡', cd: 20 },
  { id: 'magehand', name: 'Mage Hand', icon: '✋', cd: 35 },
  { id: 'sphere', name: 'Dispelling Sphere', icon: '◉', cd: 30 },
];
const MAGIC_ITEMS = {
  skyguard: { name: 'Boots of Skyguard', counters: 'fly' },
  nightwatch: { name: 'Gauntlets of Nightwatch', counters: 'fotd' },
  archivist: { name: 'Boots of the Archivist', counters: 'sphere' },
  interloper: { name: 'Gauntlets of the Interloper', counters: 'magehand' },
};

class LichPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'vecna'; this.name = 'Vile Darkness'; this.state = 'idle'; this.sel = 0; this.selectable = true;
    this.flyT = 0; this.sphere = null; this.hand = null; this.castLog = [];
    this.aiProfile = { type: 'spells', label: 'Four spells', counter: ['crouch against Flight of the Damned', 'do not rely on dropped pallets while Mage Hand is up', 'step out of the sphere\'s path', 'loot chests for Magic Items'] };
  }
  get spell() { return SPELLS[this.sel]; }
  ready(id) { return this.cd(id) <= 0; }
  onInit(m) {
    for (const p of m.map.openSpots(6, 16, m.map.gens)) m.addDevice(new Device({ kind: 'magic_chest', x: p.x, y: p.y, r: 0.6 * M, label: 'Treasure chest', useTime: 6, survivorUsable: true, seenRange: 18 * M, data: { opened: false } }));
    for (const d of m.devices) if (d.kind === 'magic_chest') {
      d.usable = (s) => !d.data.opened && !s.magic;
      d.onUse = (s, mm) => { d.data.opened = true; const ids = Object.keys(MAGIC_ITEMS); s.magic = ids[Math.floor(Math.random() * ids.length)]; s.ai?.note(`Found ${MAGIC_ITEMS[s.magic].name}`, 'magic'); mm.events.emit('magicItem', { survivor: s, item: s.magic }); };
    }
  }
  speedMult() { if (this.state === 'fly') return 8 / this.k.def.speed; if (this.state === 'casting') return 0.3; return 1; }
  canAttack() { return this.state === 'idle'; }
  noContext() { return this.state === 'fly'; }
  moveFilter() { return this.state === 'fly' ? (s) => s.kind !== 'pallet' && s.kind !== 'window' : null; }
  update(dt, m, inp, locked) {
    this.tickCooldowns(dt);
    const k = this.k;
    if (inp.slot) this.sel = inp.slot - 1;
    if (inp.wheel) this.sel = (this.sel + (inp.wheel > 0 ? 1 : 3)) % 4;
    if (inp.cycleP) this.sel = (this.sel + 1) % 4;
    if (this.state === 'fly') {
      this.flyT -= dt;
      if (this.flyT <= 0 || (inp.rmbP && this.flyT < 4.7) || locked) this.land(m);
      return;
    }
    if (this.state === 'casting') { this.castT -= dt; if (this.castT <= 0) { this.state = 'idle'; this.castFn?.(); this.castFn = null; } return; }
    if (locked || k.carrying) return;
    if (inp.rmbP && this.state === 'idle') {
      const sp = this.spell; if (!this.ready(sp.id)) { m.fx.text(k.x, k.y - 40, `${sp.name} — ${Math.ceil(this.cd(sp.id))}s`, '#888'); return; }
      this.cast(m, sp);
    }
  }
  cast(m, sp) {
    const k = this.k; this.setCd(sp.id, sp.cd);
    m.sound('lich_cast', k.x, k.y, 40 * M, { spell: sp.id, dir: k.facing });
    m.events.emit(EV.POWER_USED, { killer: k, power: sp.id });
    m.fx.ring(k.x, k.y, 'rgba(170,120,255,0.9)', 2 * M, 0.5);
    if (sp.id === 'fly') { this.state = 'fly'; this.flyT = this.has('pewter_wings') ? 6 : 5; m.audio?.play('teleport'); return; }
    this.state = 'casting'; this.castT = sp.id === 'magehand' ? 0.5 : 0.35;
    if (sp.id === 'fotd') this.castFn = () => this.flightOfTheDamned(m);
    if (sp.id === 'sphere') this.castFn = () => this.dispel(m);
    if (sp.id === 'magehand') { const p = this.handTarget(m); this.castFn = () => this.mageHand(m, p); if (!p) { this.setCd('magehand', 2); } }
  }
  land(m) {
    this.state = 'idle'; this.k.status.add('cannotAttack', { duration: 2.75, source: 'fly' }); m.world.resolve(this.k, this.k.r);
    m.fx.burst(this.k.x, this.k.y, 14, '#a878ff', 80, 0.5, 2, 2);
  }
  flightOfTheDamned(m) {
    const k = this.k;
    const n = this.has('ornate_horn') ? 6 : 5;
    for (let i = 0; i < n; i++) {
      const a = k.facing + (i - (n - 1) / 2) * 0.16; const sp = 7.5 * M;
      m.spawnProjectile(new Projectile({ kind: 'damned', owner: k, x: k.x + Math.cos(a) * k.r, y: k.y + Math.sin(a) * k.r, vx: Math.cos(a) * sp, vy: Math.sin(a) * sp, r: 0.35 * M, range: 22 * M, passWalls: true, pierce: true, color: '#c8b0ff',
        filter: (s) => s.moveMode !== 'crouch' || !!s.vault,
        onHit: (p, s, mm) => { if (s.dodgeT && s.dodgeT > mm.time) return; const res = s.takeHit(mm, { source: 'damned' }); k.stats.hits++; k.stats.powerHits++; mm.events.emit(EV.KILLER_HIT, { killer: k, survivor: s, result: res, power: true }); mm.fx.hit(s, res); if (res === 'downed') killerInstinct(mm, s, 5); s.ai?.note('Hit by Flight of the Damned', 'fotd'); mm.addScore('brutality', 300, 'Flight of the Damned'); p.dead = true; } }));
    }
    m.audio?.play('scream');
  }
  handTarget(m) {
    const k = this.k; let best = null, bs = Infinity;
    for (const p of m.map.pallets) {
      if (p.state === 'broken') continue; const d = dist(p.x, p.y, k.x, k.y); if (d > 7 * M) continue;
      const ang = angleDiff(Math.atan2(p.y - k.y, p.x - k.x), k.facing); if (ang > 0.8) continue;
      const sc = d + ang * 4 * M; if (sc < bs) { bs = sc; best = p; }
    }
    return best;
  }
  mageHand(m, p) {
    if (!p) return;
    if (p.state === 'down') {
      p.state = 'up'; m.world.remove(p.solid); p.respectCounted = false; m.fx.text(p.x, p.y - 26, 'LIFTED', '#c8b0ff');
      m.events.emit('mageHand', { pallet: p, lifted: true });
    } else if (p.state === 'up') {
      p.blockedUntil = m.time + 4; m.fx.text(p.x, p.y - 26, 'HELD', '#c8b0ff');
      m.events.emit('mageHand', { pallet: p, lifted: false });
    }
    this.hand = { p, t: m.time };
    for (const s of m.survivors) if (s.magic === 'interloper' && !s.status.has('dispelled') && dist(s.x, s.y, p.x, p.y) < 24 * M) { s.status.add('haste', { duration: 4, value: 0.1, source: 'interloper' }); s.ai?.note('Interloper: Mage Hand on a pallet nearby', 'interloper'); }
  }
  dispel(m) { const k = this.k; this.sphere = { x: k.x, y: k.y, dir: k.facing, t: 0, r: 3 * M, hit: new Set(), len: 0 }; }
  tick(dt, m) {
    const sp = this.sphere; if (!sp) return;
    sp.t += dt; const v = (sp.t < 1 ? 9.75 : 6.5) * M; sp.x += Math.cos(sp.dir) * v * dt; sp.y += Math.sin(sp.dir) * v * dt; sp.len += v * dt;
    for (const s of m.survivors) if (s.standing && !sp.hit.has(s) && dist(s.x, s.y, sp.x, sp.y) < sp.r) { sp.hit.add(s); killerInstinct(m, s, 3, 'Dispelling Sphere'); s.status.add('dispelled', { duration: 45 }); s.ai?.note('Caught by the Dispelling Sphere', 'sphere'); }
    if (sp.len > 34 * M || sp.x < 0 || sp.y < 0 || sp.x > m.map.w || sp.y > m.map.h) this.sphere = null;
  }
  alpha() { return this.state === 'fly' ? 0.85 : 1; }
  drawOver(ctx, m) {
    const k = this.k;
    if (this.sphere) { const s = this.sphere; ctx.strokeStyle = 'rgba(170,120,255,0.55)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(s.x, s.y, s.r, 0, Math.PI * 2); ctx.stroke(); ctx.fillStyle = 'rgba(170,120,255,0.08)'; ctx.fill(); }
    if (this.state === 'fly') { ctx.strokeStyle = 'rgba(168,120,255,0.6)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(k.x, k.y, k.r + 6, 0, Math.PI * 2 * this.flyT / 5); ctx.stroke(); }
    if (this.spell.id === 'magehand' && this.state === 'idle' && this.ready('magehand')) { const p = this.handTarget(m); if (p) { ctx.strokeStyle = 'rgba(200,176,255,0.8)'; ctx.setLineDash([4, 4]); ctx.strokeRect(p.x - 18, p.y - 18, 36, 36); ctx.setLineDash([]); } }
    if (this.spell.id === 'fotd' && this.state === 'idle') { ctx.fillStyle = 'rgba(200,176,255,0.05)'; ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.arc(k.x, k.y, 22 * M, k.facing - 0.32, k.facing + 0.32); ctx.closePath(); ctx.fill(); }
  }
  drawGround(ctx, m) {
    for (const p of m.map.pallets) if ((p.blockedUntil || 0) > m.time) { ctx.strokeStyle = 'rgba(200,176,255,0.7)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(p.x, p.y, 16, 0, Math.PI * 2); ctx.stroke(); }
  }
  drawDevice(ctx, d) {
    if (d.kind !== 'magic_chest') return;
    ctx.fillStyle = d.data.opened ? '#3a3020' : '#6a4a20'; ctx.fillRect(d.x - 13, d.y - 9, 26, 18); ctx.fillStyle = '#c8a040'; ctx.fillRect(d.x - 13, d.y - 2, 26, 3); if (!d.data.opened) { ctx.fillStyle = '#a878ff'; ctx.beginPath(); ctx.arc(d.x, d.y, 3, 0, Math.PI * 2); ctx.fill(); }
  }
  drawWeapon(ctx, m) { const k = this.k; ctx.fillStyle = this.state === 'casting' ? '#c8b0ff' : '#6a5aa0'; ctx.beginPath(); ctx.arc(k.r + 6, 7, 5, 0, Math.PI * 2); ctx.fill(); return true; }
  drawBody(ctx) { ctx.fillStyle = '#d8d0b8'; ctx.beginPath(); ctx.arc(5, 0, 5.5, 0, Math.PI * 2); ctx.fill(); ctx.fillStyle = '#c03a3a'; ctx.beginPath(); ctx.arc(8, -2, 1.3, 0, Math.PI * 2); ctx.fill(); }
  hud() {
    return { title: 'Vile Darkness', value: this.spell.name.split(' ').map((w) => w[0]).join(''), meter: this.ready(this.spell.id) ? 1 : 1 - this.cd(this.spell.id) / this.spell.cd,
      state: this.state === 'fly' ? `Flying ${this.flyT.toFixed(1)}s — Q to land` : `${this.spell.name}${this.ready(this.spell.id) ? '' : ` · ${Math.ceil(this.cd(this.spell.id))}s`}`,
      spells: SPELLS.map((q) => ({ name: q.name, icon: q.icon, cd: this.cd(q.id) })), sel: this.sel,
      hints: [['Wheel / 1-4 / F', 'Select spell'], ['Q / RMB', 'Cast the selected spell']] };
  }
}

return { SPELLS, MAGIC_ITEMS, LichPower };
};
__defs["js/killers/skullmerchant.js"] = function(){
// THE SKULL MERCHANT — Eyes in the Sky (live design, adapted to top-down).
//  • Hold RMB (0.5 s) to deploy a DRONE at the aimed spot (≤ 10 m). Up to 6 drones; deploying
//    a 7th recycles the oldest. 7 s between deployments.
//  • Every drone has a 10 m detection zone swept by a scan line turning 105°/s. A survivor the
//    line crosses is DETECTED unless they are crouching or standing still (or the drone has no
//    line of sight to them).
//  • Detection: +1 LOCK-ON stack and the survivor shows on her radar (32 m) for 4 s.
//    At 3 stacks: healthy → injured, injured → Deep Wound; Broken 20 s and a CLAW TRAP (45 s
//    battery). A claw-trapped survivor detected again is Hindered 10% for 6 s and revealed.
//  • Survivors can HACK a drone (3 s): it is disabled for 45 s and their claw trap comes off.
//    A botched hack (failed skill check) gives a lock-on stack instead.
//  • E recalls the drone nearest your aim (1 s): Undetectable for 8 s.
const { M, dist, angleDiff, normAngle, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, Device } = __req("js/killers/framework.js");

const ZONE = 10 * M, SWEEP = 105 * Math.PI / 180, MAX_DRONES = 6, DEPLOY_CD = 7, RADAR = 32 * M;
class SkullMerchantPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'drones'; this.name = 'Eyes in the Sky'; this.state = 'idle'; this.deployT = 0; this.recallT = 0;
    this.maxDrones = this.has('ultrasonic_speaker') ? 7 : MAX_DRONES;
    this.aiProfile = { type: 'surveillance', label: 'Drones', counter: ['crouch or stand still when the scan line comes', 'hack drones near objectives (removes claw traps)', 'avoid stacking lock-ons', 'stay out of overlapping zones'] };
  }
  get drones() { return this._m ? this._m.devices.filter((d) => d.kind === 'drone' && d.owner === this.k) : []; }
  active(d, m) { return !d.dead && (d.data.disabledUntil || 0) <= m.time; }
  speedMult() { return this.state === 'deploying' ? 0.5 : this.state === 'recall' ? 0.4 : 1; }
  canAttack() { return this.state === 'idle'; }
  onInit(m) { this._m = m; }
  update(dt, m, inp, locked) {
    this._m = m; this.tickCooldowns(dt);
    const k = this.k;
    if (locked) { if (this.state !== 'idle') this.state = 'idle'; return; }
    if (this.state === 'idle') {
      if (inp.rmb && !k.carrying && this.cd('deploy') <= 0) { this.state = 'deploying'; this.deployT = 0; }
      else if (inp.eP && this.drones.length) {
        const ax = k.x + Math.cos(k.facing) * (inp.aimDist ?? 10 * M), ay = k.y + Math.sin(k.facing) * (inp.aimDist ?? 10 * M);
        const d = this.drones.filter((q) => dist(q.x, q.y, k.x, k.y) < 24 * M).sort((a, b) => dist(a.x, a.y, ax, ay) - dist(b.x, b.y, ax, ay))[0];
        if (d) { this.state = 'recall'; this.recallT = 1; this.recallD = d; }
      }
    }
    if (this.state === 'deploying') {
      this.deployT += dt;
      if (!inp.rmb) { this.state = 'idle'; return; }
      if (this.deployT >= 0.5) { this.deploy(m, inp); this.state = 'idle'; }
    }
    if (this.state === 'recall') {
      this.recallT -= dt;
      if (this.recallT <= 0) { m.removeDevice(this.recallD); this.state = 'idle'; k.status.add('undetectable', { duration: 8, source: 'drone_recall' }); m.fx.text(k.x, k.y - 40, 'UNDETECTABLE', '#9fd0a0'); m.events.emit(EV.POWER_USED, { killer: k, power: 'recall' }); }
    }
  }
  deploy(m, inp) {
    const k = this.k; const r = clamp(inp.aimDist ?? 6 * M, 1.5 * M, 10 * M);
    let x = k.x, y = k.y;
    for (let t = 0.5 * M; t <= r; t += 0.5 * M) { const nx = k.x + Math.cos(k.facing) * t, ny = k.y + Math.sin(k.facing) * t; if (m.world.pointBlocked(nx, ny, 0.3 * M)) break; x = nx; y = ny; }
    const ds = this.drones; if (ds.length >= this.maxDrones) m.removeDevice(ds[0]);
    const d = m.addDevice(new Device({ kind: 'drone', x, y, r: 0.45 * M, owner: k, label: 'Drone', useLabel: 'Hack the drone', useTime: 3, survivorUsable: true, seenRange: 22 * M, dangerRadius: ZONE, aiCost: 3,
      data: { a: k.facing, disabledUntil: 0, placedT: m.time, scanned: new Map() } }));
    d.usable = (s, mm) => this.active(d, mm || m);
    d.onUse = (s, mm) => this.hacked(mm, s, d);
    this.setCd('deploy', DEPLOY_CD);
    m.fx.burst(x, y, 10, '#8fd06a', 60, 0.5, 2, 2); m.audio?.play('ui');
    m.sound('killer_action', x, y, 16 * M, { what: 'drone' });
    m.events.emit(EV.POWER_USED, { killer: k, power: 'drone' });
  }
  hacked(m, s, d) {
    // AI hack quality: a botched hack (failed check) adds a stack and leaves the drone running
    if (!s.isPlayer && Math.random() < (s.ai ? 0.25 - 0.2 * (s.ai.params?.skill ?? 0.5) : 0.1)) { this.addStack(m, s, d, 'botched hack'); s.ai?.note('Botched the drone hack', 'hackfail'); return; }
    d.data.disabledUntil = m.time + 45; m.fx.sparks(d.x, d.y);
    if (s.status.has('clawTrap')) { s.status.remove('clawTrap'); s.ai?.note('Removed my claw trap by hacking a drone', 'clawoff'); m.warn(s, 'Claw trap removed'); }
    s.ai?.note('Hacked a drone (45 s)', 'hack'); m.notifyK(`${s.name} hacked a drone`, '#8fd06a');
  }
  // player hack checks: the controller fires a skill check halfway through; a miss = botched
  onPlayerCheck(m, s, d, res) { if (res === 'fail') { s.stopAction(m); this.addStack(m, s, d, 'botched hack'); } }
  addStack(m, s, d, why) {
    const cur = s.status.get('lockOn')?.value ?? 0; const v = cur + 1;
    if (v >= 3) {
      s.status.remove('lockOn');
      if (s.health === 'healthy') { s.lastHitBy = this.k; s.lastHitT = m.time; s.health = 'injured'; s.healProgress = 0; m.events.emit(EV.SURVIVOR_INJURED, { survivor: s, source: 'drone' }); m.fx.hit(s, 'injured'); }
      else if (s.health === 'injured') { s.status.add('deepWound', { duration: 20 }); s.mend = 0; }
      s.status.add('broken', { duration: 20, source: 'drone' });
      s.status.add('clawTrap', { duration: 45, source: 'drone' });
      m.addScore('deviousness', 300, 'Claw Trap'); this.k.stats.powerHits++;
      m.notifyK(`${s.name}: claw trap attached`, '#8fd06a'); s.ai?.note('3 lock-ons: claw trap!', 'claw'); m.warn(s, 'Claw trap! Hack a drone to remove it');
      m.events.emit('powerTriggered', { survivor: s, x: s.x, y: s.y, killer: this.k });
    } else { s.status.add('lockOn', { value: v, duration: 999, source: 'drone' }); const c = s.status.get('lockOn'); c.value = v; m.warn(s, `Lock-on ${v}/3`); }
  }
  // scanning runs every frame, also while the Skull Merchant is busy
  tick(dt, m) {
    this._m = m;
    for (const d of this.drones) {
      if (!this.active(d, m)) continue;
      const a0 = d.data.a; d.data.a = normAngle(a0 + SWEEP * dt);
      for (const s of m.survivors) {
        if (!s.standing || s.hiddenIn) continue;
        const r = dist(s.x, s.y, d.x, d.y); if (r > ZONE) continue;
        const b = Math.atan2(s.y - d.y, s.x - d.x);
        // did the line pass over the survivor this frame (plus a small width)?
        const width = Math.atan2(s.r, Math.max(r, 1));
        if (angleDiff(b, d.data.a) > SWEEP * dt + width) continue;
        const last = d.data.scanned.get(s) || -9; if (m.time - last < 1.2) continue;
        if (!m.world.losClear(d.x, d.y, s.x, s.y)) continue;
        d.data.scanned.set(s, m.time);
        const still = Math.hypot(s.vx || 0, s.vy || 0) < 0.3 * M;
        if (s.moveMode === 'crouch' || still) { s.ai?.note('The scan line passed — stayed undetected', 'scanok'); continue; }
        this.detect(m, s, d);
      }
    }
  }
  detect(m, s, d) {
    m.fx.ring(s.x, s.y, 'rgba(143,208,106,0.9)', 30, 0.5); m.audio?.play('ui');
    if (dist(s.x, s.y, this.k.x, this.k.y) < RADAR) m.reveal(s, 4, 'Radar');
    else m.notifyK(`Drone detected ${s.name}`, '#8fd06a');
    if (s.status.has('clawTrap')) { s.status.add('hindered', { duration: 6, value: 0.1, source: 'claw' }); m.reveal(s, 6, 'Claw Trap'); s.ai?.note('Detected with a claw trap: hindered', 'clawscan'); return; }
    m.warn(s, 'Detected by a drone'); s.ai?.note('Detected by a drone', 'scanned');
    this.addStack(m, s, d, 'scan');
  }
  drawDevice(ctx, d, m) {
    if (d.kind !== 'drone' || d.owner !== this.k) return;
    const on = this.active(d, m);
    if (on) {
      ctx.fillStyle = 'rgba(143,208,106,0.06)'; ctx.beginPath(); ctx.arc(d.x, d.y, ZONE, 0, Math.PI * 2); ctx.fill();
      ctx.strokeStyle = 'rgba(143,208,106,0.25)'; ctx.lineWidth = 1; ctx.stroke();
      const g = ctx.createLinearGradient(d.x, d.y, d.x + Math.cos(d.data.a) * ZONE, d.y + Math.sin(d.data.a) * ZONE);
      ctx.fillStyle = 'rgba(143,208,106,0.22)'; ctx.beginPath(); ctx.moveTo(d.x, d.y); ctx.arc(d.x, d.y, ZONE, d.data.a - 0.3, d.data.a); ctx.closePath(); ctx.fill();
      ctx.strokeStyle = 'rgba(190,255,140,0.9)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(d.x, d.y); ctx.lineTo(d.x + Math.cos(d.data.a) * ZONE, d.y + Math.sin(d.data.a) * ZONE); ctx.stroke(); void g;
    }
    ctx.fillStyle = '#2a3a2a'; ctx.beginPath(); ctx.arc(d.x, d.y, d.r + 2, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = on ? '#8fd06a' : '#7a3a3a'; ctx.beginPath(); ctx.arc(d.x, d.y, d.r * 0.55, 0, Math.PI * 2); ctx.fill();
    for (let i = 0; i < 4; i++) { const a = i * Math.PI / 2 + Math.PI / 4 + m.time * (on ? 20 : 0); ctx.strokeStyle = '#4a5a4a'; ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(d.x + Math.cos(a) * d.r, d.y + Math.sin(a) * d.r); ctx.lineTo(d.x + Math.cos(a) * (d.r + 6), d.y + Math.sin(a) * (d.r + 6)); ctx.stroke(); }
  }
  drawOver(ctx, m) {
    const k = this.k;
    if (this.state === 'deploying') { const r = clamp(k.input.aimDist ?? 6 * M, 1.5 * M, 10 * M); ctx.strokeStyle = 'rgba(143,208,106,0.6)'; ctx.setLineDash([4, 5]); ctx.beginPath(); ctx.arc(k.x + Math.cos(k.facing) * r, k.y + Math.sin(k.facing) * r, ZONE, 0, Math.PI * 2); ctx.stroke(); ctx.setLineDash([]); }
  }
  drawBody(ctx) { ctx.fillStyle = '#d8d0c0'; ctx.beginPath(); ctx.arc(5, 0, 6, 0, Math.PI * 2); ctx.fill(); ctx.fillStyle = '#2a2a2a'; ctx.fillRect(7, -4, 3, 8); }
  hud(m) {
    const n = this.drones.length, act = m ? this.drones.filter((d) => this.active(d, m)).length : n;
    const trapped = m ? m.survivors.filter((s) => s.status.has('clawTrap')).length : 0;
    return { title: 'Eyes in the Sky', value: `${act}/${n} drones`, meter: this.state === 'deploying' ? this.deployT / 0.5 : 1 - this.cd('deploy') / DEPLOY_CD,
      state: this.state === 'recall' ? 'Recalling…' : this.cd('deploy') > 0 ? `Deploy in ${Math.ceil(this.cd('deploy'))}s · ${trapped} claw-trapped` : `Ready · ${trapped} claw-trapped`,
      hints: [['Q / RMB', 'Hold: deploy a drone at the aimed spot (≤10 m)'], ['E', 'Recall the drone nearest your aim → Undetectable 8 s']] };
  }
}

return { SkullMerchantPower };
};
__defs["js/killers/blight.js"] = function(){
// THE BLIGHT — Blighted Corruption (adapted to top-down).
//  • 5 rush tokens; one token refills every 2 s while he isn't rushing (tokens refill one at a time).
//  • Press RMB to start a BLIGHTED RUSH (1 token): 9.2 m/s for up to 3 s, turning slowly.
//    Running into a survivor during a normal rush only bumps them (no damage) and ends it.
//  • RUSH SLAM: hitting a wall / obstacle head-on (≥ 45°) stops him. With a token left he has a
//    1.25 s window to press RMB again for the next rush. Glancing hits slide along the wall.
//  • LETHAL RUSH: any rush started after a slam. LMB during a lethal rush is the special attack:
//    a long, fast lunge that injures.
//  • Dropped pallets: a lethal rush slamming into one breaks it (costs 3 tokens if he has them).
//  • FATIGUE when the chain ends (no token, window missed, or after the special attack): 2.5 s,
//    slowed and unable to attack.
const { M, dist, normAngle, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, strike } = __req("js/killers/framework.js");

const RUSH = 9.2, RUSH_T = 3, TURN = 1.6, WINDOW = 1.25, FATIGUE = 2.5, TOKEN_T = 2;
class BlightPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'rush'; this.name = 'Blighted Corruption'; this.state = 'idle';
    this.maxTokens = this.has('compound_33') ? 6 : 5; this.tokens = this.maxTokens; this.tokenT = 0; this.rushT = 0; this.winT = 0; this.fatT = 0; this.lethal = false; this.slams = 0;
    this.turn = this.has('alchemists_ring') ? TURN * 1.25 : TURN;
    this.aiProfile = { type: 'rush', label: 'Blighted Rush', counter: ['side-step at the last moment (he turns slowly)', 'tall loops force him to slam', 'pre-drop pallets only if he has few tokens', 'punish fatigue'] };
  }
  speedMult() {
    const base = this.k.def.speed * M;
    if (this.state === 'rush') return RUSH * M / base;
    if (this.state === 'window') return 0.15;
    if (this.state === 'special') return RUSH * 0.9 * M / base;
    if (this.state === 'fatigue') return 0.45;
    return 1;
  }
  canAttack() { return this.state === 'idle'; }
  noContext() { return this.state === 'rush' || this.state === 'window' || this.state === 'special'; }
  overrideMove() { return this.state === 'rush' || this.state === 'special' ? { x: Math.cos(this.dir), y: Math.sin(this.dir) } : null; }
  update(dt, m, inp, locked) {
    this.tickCooldowns(dt);
    const k = this.k;
    if (this.state === 'idle' && this.tokens < this.maxTokens) { this.tokenT += dt; if (this.tokenT >= TOKEN_T) { this.tokenT = 0; this.tokens++; } }
    if (this.state === 'fatigue') { this.fatT -= dt; if (this.fatT <= 0) this.state = 'idle'; return; }
    if (locked) { if (this.state !== 'idle') this.fatigue(m); return; }
    switch (this.state) {
      case 'idle':
        if (inp.rmbP && this.tokens >= 1 && !k.carrying) this.startRush(m, false);
        break;
      case 'rush': {
        this.rushT -= dt; k.facing = this.dir;
        this.dir += clamp(normAngle(inp.aim - this.dir), -this.turn * dt, this.turn * dt);
        if (this.lethal && inp.lmbP) { this.startSpecial(m); break; }
        // bump survivors (normal rush) — a lethal rush lets him use the special attack instead
        for (const s of m.survivors) {
          if (!s.standing || dist(s.x, s.y, k.x, k.y) > k.r + s.r + 0.2 * M) continue;
          if (this.lethal) { this.startSpecial(m); return; }
          s.ai?.note('Bumped by the Blight', 'bump'); this.fatigue(m); return;
        }
        // dropped pallet ahead: a lethal rush smashes it, otherwise it's a slam
        const fx = k.x + Math.cos(this.dir) * 0.9 * M, fy = k.y + Math.sin(this.dir) * 0.9 * M;
        for (const p of m.map.pallets) if (p.state === 'down' && p.stunZone({ x: fx, y: fy })) {
          if (this.lethal && this.tokens >= 3) { this.tokens -= 3; m.breakPallet(p, k); m.fx.text(p.x, p.y - 30, 'SMASHED', '#c7a04a'); }
          this.slam(m); return;
        }
        if (this.rushT <= 0) this.fatigue(m);
        if (this.soundT === undefined || (this.soundT -= dt) <= 0) { this.soundT = 0.25; m.sound('killer_action', k.x, k.y, 30 * M, { what: 'rush', dir: this.dir }); }
        break;
      }
      case 'window':
        this.winT -= dt;
        if (inp.rmbP && this.tokens >= 1) { this.dir = inp.aim; this.startRush(m, true); break; }
        if (this.winT <= 0) this.fatigue(m);
        break;
      case 'special': {
        this.spT -= dt; k.facing = this.dir; this.dir += clamp(normAngle(inp.aim - this.dir), -this.turn * 1.5 * dt, this.turn * 1.5 * dt);
        for (const s of m.survivors) {
          if (!s.standing || dist(s.x, s.y, k.x + Math.cos(this.dir) * 0.6 * M, k.y + Math.sin(this.dir) * 0.6 * M) > k.r + s.r + 0.5 * M || !m.world.losClear(k.x, k.y, s.x, s.y)) continue;
          const r = strike(m, k, s, { source: 'rush', score: 'brutality', label: 'Lethal Rush' }); if (r && r !== 'dodged') { m.fx.shake(8); s.ai?.note('Hit by the Blight\'s lethal rush', 'lethal'); }
          this.fatigue(m, 0.5); return;
        }
        if (this.spT <= 0) this.fatigue(m);
        break;
      }
    }
  }
  startRush(m, chained) {
    const k = this.k; this.tokens--; this.tokenT = 0; this.state = 'rush'; this.rushT = RUSH_T; this.lethal = chained; this.lockFacing = true;
    if (!chained) { this.dir = k.facing; this.slams = 0; }
    m.audio?.play('swing'); m.events.emit(EV.POWER_USED, { killer: k, power: chained ? 'lethal_rush' : 'rush' });
    m.sound('power_windup', k.x, k.y, 30 * M, { what: 'rush', dir: this.dir });
  }
  startSpecial(m) { this.state = 'special'; this.spT = 0.45; m.fx.swing(this.k); m.audio?.play('swing'); }
  onCollide(m) {
    if (this.state !== 'rush' && this.state !== 'special') return;
    const k = this.k, sp = Math.hypot(k.vx, k.vy) || 0;
    const along = sp > 0 ? (k.vx * Math.cos(this.dir) + k.vy * Math.sin(this.dir)) / (RUSH * M) : 0;
    if (this.state === 'special') { this.fatigue(m); return; }
    if (along < 0.7) this.slam(m); // head-on (≥ 45°): slam; glancing hits slide along the wall
  }
  slam(m) {
    const k = this.k; this.slams++; m.fx.shake(6); m.audio?.play('clang'); m.fx.burst(k.x + Math.cos(this.dir) * k.r, k.y + Math.sin(this.dir) * k.r, 10, '#c7a04a', 80, 0.4, 2, 2);
    m.sound('killer_action', k.x, k.y, 26 * M, { what: 'slam' });
    if (this.tokens >= 1) { this.state = 'window'; this.winT = WINDOW; this.lockFacing = false; }
    else this.fatigue(m);
  }
  fatigue(m, extra = 0) { this.state = 'fatigue'; this.fatT = FATIGUE + extra; this.lockFacing = false; this.lethal = false; }
  drawOver(ctx, m) {
    const k = this.k;
    if (this.state === 'rush') { ctx.strokeStyle = this.lethal ? 'rgba(255,90,60,0.7)' : 'rgba(199,160,74,0.6)'; ctx.lineWidth = 3; ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.lineTo(k.x + Math.cos(this.dir) * 3 * M, k.y + Math.sin(this.dir) * 3 * M); ctx.stroke(); }
    if (this.state === 'window') { ctx.strokeStyle = 'rgba(255,200,90,0.8)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(k.x, k.y, k.r + 8, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * this.winT / WINDOW); ctx.stroke(); }
    if (this.state === 'fatigue') { ctx.fillStyle = 'rgba(199,160,74,0.5)'; ctx.font = '600 12px sans-serif'; ctx.textAlign = 'center'; ctx.fillText('fatigued', k.x, k.y - k.r - 10); }
  }
  drawBody(ctx) { ctx.fillStyle = '#c7a04a'; ctx.beginPath(); ctx.arc(6, 0, 5, 0, Math.PI * 2); ctx.fill(); ctx.fillStyle = '#5a4a2a'; ctx.fillRect(-4, -9, 3, 18); }
  hud() {
    const st = { idle: 'Ready', rush: this.lethal ? 'LETHAL RUSH — LMB to strike' : 'Rushing', window: `Slam! RMB to chain (${this.winT.toFixed(1)}s)`, special: 'Lethal strike', fatigue: `Fatigue ${this.fatT.toFixed(1)}s` }[this.state];
    return { title: 'Blighted Corruption', value: `${this.tokens}/${this.maxTokens}`, meter: this.state === 'fatigue' ? 1 - this.fatT / FATIGUE : this.tokens / this.maxTokens, state: st,
      hints: [['Q / RMB', 'Rush (1 token) · after a slam: chain a lethal rush'], ['Mouse', 'Steer (slow turn)'], ['LMB', 'Special attack during a lethal rush']] };
  }
}

return { BlightPower };
};
__defs["js/killers/nurse.js"] = function(){
// THE NURSE — Spencer's Last Breath (adapted to top-down).
//  • Slow walker (3.85 m/s), cannot vault windows.
//  • 2 blink tokens; each recharges in 3 s while she isn't blinking.
//  • Hold RMB to charge a BLINK (2 s): the distance grows from 1.5 m to 20 m. Release to
//    teleport straight ahead THROUGH walls and obstacles (not off the map).
//  • After a blink, a 1.5 s CHAIN window: hold RMB again for a second, shorter blink (up to
//    12 m, charges faster), or LMB to attack out of the blink.
//  • FATIGUE when the chain ends: 2 s after one blink, 2.5 s after two, +1 s if she attacked.
//    Slowed and unable to attack.
const { M, dist, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower } = __req("js/killers/framework.js");

const CHARGE = 2, MIN_D = 1.5, MAX_D = 20, CHAIN_MAX = 12, CHAIN_WIN = 1.5, TOKEN_T = 3;
class NursePower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'blink'; this.name = "Spencer's Last Breath"; this.state = 'idle';
    this.maxTokens = this.has('anxious_gasp') ? 3 : 2; this.tokens = this.maxTokens; this.tokenT = 0; this.charge = 0; this.blinks = 0; this.attacked = false; this.fatT = 0; this.winT = 0; this.fatigueT = 0;
    this.aiProfile = { type: 'blink', label: 'Blinks through walls', counter: ['change direction when her blink charge sound stops', 'double back behind tall walls', 'distance — she is slow while fatigued', 'pallets and windows do little'] };
  }
  noVault() { return true; }
  speedMult() {
    if (this.state === 'charging') return 0.35;
    if (this.state === 'chain') return 0.5;
    if (this.state === 'fatigue') return 0.3;
    return 1;
  }
  canAttack() { return this.state === 'idle' || this.state === 'chain'; }
  onAttack(m) { if (this.state === 'chain') { this.attacked = true; this.fatigueT = 0.45; } }
  get blinkDist() { const max = this.blinks ? CHAIN_MAX : MAX_D; return (MIN_D + (max - MIN_D) * clamp(this.charge, 0, 1)) * M; }
  update(dt, m, inp, locked) {
    this.tickCooldowns(dt);
    const k = this.k;
    if ((this.state === 'idle') && this.tokens < this.maxTokens) { this.tokenT += dt; if (this.tokenT >= TOKEN_T) { this.tokenT = 0; this.tokens++; } }
    if (this.state === 'fatigue') { this.fatT -= dt; if (this.fatT <= 0) this.state = 'idle'; return; }
    if (this.fatigueT > 0) { this.fatigueT -= dt; if (this.fatigueT <= 0 && this.state === 'chain') { this.fatigue(m); return; } }
    if (locked) { if (this.state === 'charging') { this.state = this.blinks ? 'chain' : 'idle'; } return; }
    switch (this.state) {
      case 'idle':
        if (inp.rmb && this.tokens >= 1 && !k.carrying) { this.state = 'charging'; this.charge = 0; this.blinks = 0; this.attacked = false; this.fatigueT = 0; }
        break;
      case 'charging':
        this.charge += dt / (this.blinks ? CHARGE * 0.6 : CHARGE);
        if (this.soundT === undefined || (this.soundT -= dt) <= 0) { this.soundT = 0.35; m.sound('blink_charge', k.x, k.y, 22 * M, { what: 'blink', charge: this.charge }); }
        if (!inp.rmb) this.blink(m);
        else if (this.charge >= 1.35) this.blink(m); // auto-release after holding at full
        break;
      case 'chain':
        this.winT -= dt;
        if (inp.rmb && this.tokens >= 1 && !this.attacked) { this.state = 'charging'; this.charge = 0; }
        else if (this.winT <= 0 && k.attack.state !== 'lunge') this.fatigue(m);
        break;
    }
  }
  blink(m) {
    const k = this.k, d = this.blinkDist; this.tokens--; this.tokenT = 0; this.blinks++;
    // straight ahead through everything; land on the furthest free point not past the map edge
    let tx = k.x, ty = k.y; const step = 0.25 * M;
    for (let t = step; t <= d; t += step) {
      const x = k.x + Math.cos(k.facing) * t, y = k.y + Math.sin(k.facing) * t;
      if (x < 1.5 * M || y < 1.5 * M || x > m.map.w - 1.5 * M || y > m.map.h - 1.5 * M) break;
      if (!m.world.pointBlocked(x, y, k.r)) { tx = x; ty = y; }
    }
    m.fx.burst(k.x, k.y, 18, '#c8d8e8', 100, 0.5, 2, 2);
    this.from = { x: k.x, y: k.y, t: m.time }; k.x = tx; k.y = ty;
    m.fx.burst(k.x, k.y, 22, '#c8d8e8', 140, 0.6, 2, 2); m.audio?.play('teleport');
    m.sound('killer_action', k.x, k.y, 24 * M, { what: 'blink' });
    m.events.emit(EV.POWER_USED, { killer: k, power: 'blink', n: this.blinks });
    this.state = 'chain'; this.winT = CHAIN_WIN; this.charge = 0;
  }
  fatigue(m) { this.state = 'fatigue'; this.fatT = (this.blinks >= 2 ? 2.5 : 2) + (this.attacked ? 1 : 0); this.blinks = 0; this.attacked = false; this.fatigueT = 0; }
  drawOver(ctx, m) {
    const k = this.k;
    if (this.state === 'charging') {
      const d = this.blinkDist, x = k.x + Math.cos(k.facing) * d, y = k.y + Math.sin(k.facing) * d;
      ctx.strokeStyle = 'rgba(200,216,232,0.5)'; ctx.setLineDash([4, 6]); ctx.lineWidth = 1.5; ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.lineTo(x, y); ctx.stroke(); ctx.setLineDash([]);
      ctx.fillStyle = 'rgba(200,216,232,0.35)'; ctx.beginPath(); ctx.arc(x, y, k.r, 0, Math.PI * 2); ctx.fill();
    }
    if (this.from && m.time - this.from.t < 0.6) { const a = 1 - (m.time - this.from.t) / 0.6; ctx.strokeStyle = `rgba(200,216,232,${a * 0.6})`; ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(this.from.x, this.from.y); ctx.lineTo(k.x, k.y); ctx.stroke(); }
    if (this.state === 'fatigue') { ctx.fillStyle = 'rgba(200,216,232,0.6)'; ctx.font = '600 12px sans-serif'; ctx.textAlign = 'center'; ctx.fillText('fatigued', k.x, k.y - k.r - 10); }
  }
  drawBody(ctx) { ctx.fillStyle = '#e8e0d0'; ctx.beginPath(); ctx.arc(5, 0, 6, 0, Math.PI * 2); ctx.fill(); ctx.fillStyle = '#5a2a2a'; ctx.fillRect(4, -2, 4, 4); }
  hud() {
    const st = { idle: this.tokens ? 'Ready' : 'Recharging', charging: `Blink ${(this.blinkDist / M).toFixed(1)} m`, chain: `Chain window ${this.winT.toFixed(1)}s — RMB blink / LMB attack`, fatigue: `Fatigue ${this.fatT.toFixed(1)}s` }[this.state];
    return { title: "Spencer's Last Breath", value: `${this.tokens}/${this.maxTokens}`, meter: this.state === 'charging' ? clamp(this.charge, 0, 1) : this.tokens / this.maxTokens, state: st,
      hints: [['Q / RMB', 'Hold to charge a blink (1.5–20 m, through walls), release to blink'], ['RMB again', 'Chain a second blink (≤12 m)'], ['LMB', 'Attack out of a blink']] };
  }
}

return { NursePower };
};
__defs["js/killers/unknown.js"] = function(){
// THE UNKNOWN — UVX (adapted to top-down).
//  • Hold RMB to charge a UVX shot (1 s, slowed), release to fire. The ball speeds up from
//    25 to 50 m/s, BOUNCES once off walls and bursts (2.25 m) on a survivor, on its second
//    wall hit or at the end of its 24 m flight.
//  • A survivor caught in the burst becomes WEAKENED; a Weakened survivor caught again takes
//    a health state. Weakened wears off by LOOKING at the Unknown (within 25 m, line of sight)
//    for 10 s in total.
//  • E leaves a HALLUCINATION where he stands (max 4; the oldest fades). Survivors DISPEL one
//    by looking at it from within 12 m for 4 s. Walking within 3 m of one undispelled is a
//    failure: Weakened and revealed for 3 s.
//  • F (cycle) TELEPORTS to the hallucination nearest your aim (1.5 s, 25 s cooldown).
const { M, dist, angleDiff, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, Device, Projectile, strike } = __req("js/killers/framework.js");

const BLAST = 2.25 * M, CHARGE = 1, RANGE = 24 * M, LOOK_R = 25 * M, HAL_MAX = 4, TP_CD = 25;
class UnknownPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'uvx'; this.name = 'UVX'; this.state = 'idle'; this.charge = 0; this.tpT = 0;
    this.aiProfile = { type: 'projectile', label: 'UVX & hallucinations', counter: ['side-step the UVX (it bounces off walls)', 'look at him to shake off Weakened', 'dispel hallucinations near objectives', 'don\'t stack up — the burst hits everyone near'] };
  }
  get hallucinations() { return this._m ? this._m.devices.filter((d) => d.kind === 'hallucination' && d.owner === this.k) : []; }
  speedMult() { return this.state === 'charging' ? 0.6 : this.state === 'teleporting' ? 0 : this.state === 'recover' ? 0.5 : 1; }
  canAttack() { return this.state === 'idle'; }
  onInit(m) { this._m = m; }
  update(dt, m, inp, locked) {
    this._m = m; this.tickCooldowns(dt);
    const k = this.k;
    if (this.state === 'teleporting') { this.tpT -= dt; if (this.tpT <= 0) this.finishTeleport(m); return; }
    if (this.state === 'recover') { this.recT -= dt; if (this.recT <= 0) this.state = 'idle'; }
    if (locked) { if (this.state === 'charging') this.state = 'idle'; return; }
    if (this.state === 'idle') {
      if (inp.rmb && !k.carrying && this.cd('uvx') <= 0) { this.state = 'charging'; this.charge = 0; m.sound('power_windup', k.x, k.y, 22 * M, { what: 'uvx', dir: k.facing }); }
      else if (inp.eP && !k.carrying && this.cd('hal') <= 0) this.placeHallucination(m, k.x, k.y);
      else if (inp.cycleP && !k.carrying && this.cd('tp') <= 0 && this.hallucinations.length) this.startTeleport(m, inp);
    }
    if (this.state === 'charging') {
      this.charge += dt / CHARGE;
      if (!inp.rmb) { if (this.charge >= 1) this.fire(m); else this.state = 'idle'; }
    }
  }
  fire(m) {
    const k = this.k; this.state = 'recover'; this.recT = 0.6; this.setCd('uvx', 1.5);
    const a = k.facing; this.launch(m, k.x + Math.cos(a) * k.r, k.y + Math.sin(a) * k.r, a, RANGE, 0);
    m.audio?.play('throw'); m.events.emit(EV.POWER_USED, { killer: k, power: 'uvx' });
  }
  launch(m, x, y, a, range, bounces) {
    const sp = bounces ? 50 * M : 25 * M;
    const p = m.spawnProjectile(new Projectile({
      kind: 'uvx', owner: this.k, x, y, vx: Math.cos(a) * sp, vy: Math.sin(a) * sp, r: 0.2 * M, range, color: '#b0ff80', data: { bounces },
      onHit: (pr, s, mm) => this.burst(mm, pr.x, pr.y),
      onEnd: (pr, mm) => this.burst(mm, pr.x, pr.y),
      onWall: (pr, mm) => {
        if (pr.data.bounces >= 1) { this.burst(mm, pr.x, pr.y); return; }
        // reflect off the face we hit
        const h = mm.world.firstHit(pr.x - pr.vx / 60, pr.y - pr.vy / 60, pr.x + pr.vx / 60, pr.y + pr.vy / 60, pr.r);
        let vx = pr.vx, vy = pr.vy;
        if (h) { const s = h.solid; const px = pr.x, py = pr.y; const side = px <= s.x + 1 || px >= s.x + s.w - 1; if (side) vx = -vx; else vy = -vy; } else { vx = -vx; vy = -vy; }
        const na = Math.atan2(vy, vx); this.launch(mm, pr.x + Math.cos(na) * 2, pr.y + Math.sin(na) * 2, na, Math.max(4 * M, range - pr.traveled), 1);
        mm.fx.sparks(pr.x, pr.y);
      },
    }));
    // accelerate 25 → 50 m/s over the flight
    const up = p.update.bind(p); p.update = (dt, mm) => { const v = Math.hypot(p.vx, p.vy), nv = Math.min(50 * M, v + 30 * M * dt); p.vx *= nv / v; p.vy *= nv / v; up(dt, mm); };
    return p;
  }
  burst(m, x, y) {
    m.fx.ring(x, y, 'rgba(176,255,128,0.9)', BLAST, 0.5); m.fx.burst(x, y, 24, '#b0ff80', 120, 0.6, 2, 3); m.audio?.play('clang');
    m.sound('killer_action', x, y, 20 * M, { what: 'uvx_burst' });
    for (const s of m.survivors) {
      if (!s.standing || dist(s.x, s.y, x, y) > BLAST + s.r || !m.world.losClear(x, y, s.x, s.y)) continue;
      if (s.status.has('weakened')) { const r = strike(m, this.k, s, { source: 'uvx', score: 'brutality', label: 'UVX' }); if (r && r !== 'dodged') { s.status.remove('weakened'); s.ai?.note('UVX burst while Weakened: hurt', 'uvxhit'); } }
      else { this.weaken(m, s, 'UVX burst'); }
    }
  }
  weaken(m, s, why) {
    s.status.add('weakened', { duration: 999, source: 'uvx' }); s._lookT = 0; this.k.stats.powerHits++; m.addScore('deviousness', 200, 'Weakened');
    s.ai?.note(`Weakened (${why})`, 'weak'); m.warn(s, 'Weakened — look at the Unknown to recover'); m.events.emit('powerTriggered', { survivor: s, x: s.x, y: s.y, killer: this.k });
  }
  placeHallucination(m, x, y) {
    const hs = this.hallucinations; if (hs.length >= HAL_MAX) m.removeDevice(hs[0]);
    m.addDevice(new Device({ kind: 'hallucination', x, y, r: 0.5 * M, owner: this.k, label: 'Hallucination', seenRange: 20 * M, dangerRadius: 3.5 * M, aiCost: 3, data: { placedT: m.time, gaze: new Map() } }));
    this.setCd('hal', 2); m.fx.burst(x, y, 12, '#9a8ab0', 60, 0.6, 2, 2);
    m.events.emit(EV.POWER_USED, { killer: this.k, power: 'hallucination' });
  }
  startTeleport(m, inp) {
    const k = this.k, ax = k.x + Math.cos(k.facing) * (inp.aimDist ?? 20 * M), ay = k.y + Math.sin(k.facing) * (inp.aimDist ?? 20 * M);
    const h = this.hallucinations.slice().sort((a, b) => dist(a.x, a.y, ax, ay) - dist(b.x, b.y, ax, ay))[0];
    this.tpTo = h; this.state = 'teleporting'; this.tpT = 1.5;
    m.sound('teleport_warn', h.x, h.y, 22 * M, { killer: k, what: 'unknown_tp' });
  }
  finishTeleport(m) {
    const k = this.k, h = this.tpTo; this.state = 'idle'; this.setCd('tp', TP_CD);
    if (!h || h.dead) return;
    m.fx.burst(k.x, k.y, 20, '#9a8ab0', 120, 0.5, 2, 2); k.x = h.x; k.y = h.y; m.world.resolve(k, k.r); m.removeDevice(h);
    m.fx.burst(k.x, k.y, 30, '#9a8ab0', 180, 0.6, 2, 3); m.audio?.play('teleport');
    m.events.emit(EV.POWER_USED, { killer: k, power: 'teleport' });
  }
  // gaze mechanics (always running): looking at him clears Weakened; looking at hallucinations dispels
  tick(dt, m) {
    const k = this.k;
    for (const s of m.survivors) {
      if (!s.standing) continue;
      const face = s.isPlayer && s.lookTarget ? Math.atan2(s.lookTarget.y - s.y, s.lookTarget.x - s.x) : s.facing;
      if (s.status.has('weakened')) {
        if (dist(s.x, s.y, k.x, k.y) < LOOK_R && angleDiff(face, Math.atan2(k.y - s.y, k.x - s.x)) < 0.6 && m.world.losClear(s.x, s.y, k.x, k.y)) {
          s._lookT = (s._lookT || 0) + dt; if (s._lookT >= 10) { s.status.remove('weakened'); s._lookT = 0; s.ai?.note('Stared down the Unknown — no longer Weakened', 'unweak'); m.warn(s, 'No longer Weakened'); }
        }
      }
      for (const h of this.hallucinations) {
        const d = dist(s.x, s.y, h.x, h.y); if (d > 12 * M) { h.data.gaze.delete(s); continue; }
        if (d < 3 * M && !h.dead) { // walked into it undispelled
          m.removeDevice(h); if (!s.status.has('weakened')) this.weaken(m, s, 'hallucination'); m.reveal(s, 3, 'Hallucination');
          m.notifyK(`${s.name} walked into a hallucination`, '#9a8ab0'); continue;
        }
        if (angleDiff(face, Math.atan2(h.y - s.y, h.x - s.x)) < 0.45 && m.world.losClear(s.x, s.y, h.x, h.y)) {
          const t = (h.data.gaze.get(s) || 0) + dt; h.data.gaze.set(s, t);
          if (t >= 4) { m.removeDevice(h); m.fx.burst(h.x, h.y, 14, '#d8d0ff', 80, 0.5, 2, 2); s.ai?.note('Dispelled a hallucination', 'dispel'); m.notifyK(`${s.name} dispelled a hallucination`, '#9a8ab0'); }
        }
      }
    }
  }
  drawDevice(ctx, d, m) {
    if (d.kind !== 'hallucination' || d.owner !== this.k) return;
    const t = m.time * 3 + d.x; ctx.globalAlpha = 0.55 + 0.15 * Math.sin(t);
    ctx.fillStyle = '#5a4a6a'; ctx.beginPath(); ctx.arc(d.x, d.y, 0.55 * M, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = '#b0a0d0'; ctx.beginPath(); ctx.arc(d.x + 5, d.y - 3, 4, 0, Math.PI * 2); ctx.fill(); ctx.globalAlpha = 1;
    const g = [...d.data.gaze.values()].reduce((a, b) => Math.max(a, b), 0);
    if (g > 0) { ctx.strokeStyle = '#d8d0ff'; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(d.x, d.y, 0.8 * M, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * g / 4); ctx.stroke(); }
  }
  drawOver(ctx, m) {
    const k = this.k;
    if (this.state === 'charging') { ctx.strokeStyle = 'rgba(176,255,128,0.5)'; ctx.setLineDash([5, 6]); ctx.lineWidth = 1.5; ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.lineTo(k.x + Math.cos(k.facing) * 10 * M, k.y + Math.sin(k.facing) * 10 * M); ctx.stroke(); ctx.setLineDash([]);
      ctx.strokeStyle = '#b0ff80'; ctx.lineWidth = 3; ctx.beginPath(); ctx.arc(k.x, k.y, k.r + 8, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * Math.min(1, this.charge)); ctx.stroke(); }
    if (this.state === 'teleporting' && this.tpTo) { ctx.strokeStyle = 'rgba(154,138,176,0.7)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.lineTo(this.tpTo.x, this.tpTo.y); ctx.stroke(); }
  }
  drawBody(ctx) { ctx.fillStyle = '#4a3a5a'; ctx.beginPath(); ctx.ellipse(4, 0, 9, 6, 0, 0, Math.PI * 2); ctx.fill(); ctx.fillStyle = '#e8e0f0'; ctx.beginPath(); ctx.arc(9, -2, 2, 0, Math.PI * 2); ctx.arc(9, 3, 1.6, 0, Math.PI * 2); ctx.fill(); }
  hud(m) {
    const weak = m ? m.survivors.filter((s) => s.standing && s.status.has('weakened')).length : 0;
    return { title: 'UVX', value: `${this.hallucinations.length}/${HAL_MAX} 👁`, meter: this.state === 'charging' ? Math.min(1, this.charge) : 1 - this.cd('tp') / TP_CD,
      state: this.state === 'teleporting' ? 'Teleporting…' : `${weak} Weakened · teleport ${this.cd('tp') > 0 ? Math.ceil(this.cd('tp')) + 's' : 'ready'}`,
      hints: [['Q / RMB', 'Hold 1 s, release: fire UVX (bounces once)'], ['E', 'Leave a hallucination here'], ['F', 'Teleport to the hallucination nearest your aim']] };
  }
}

return { UnknownPower };
};
__defs["js/killers/nemesis.js"] = function(){
// THE NEMESIS — T-Virus (adapted to top-down).
//  • Hold RMB (0.35 s) then release for a TENTACLE STRIKE in a straight line: 5 m at Mutation
//    Rate 1–2, 6.5 m at MR 3. From MR 2 the tentacle smashes dropped pallets in its path.
//  • A tentacle hit CONTAMINATES a survivor (+1 MR point); a hit on someone already Contaminated
//    takes a health state and removes the contamination. Basic attacks work as usual.
//    MR 2 at 1 point, MR 3 at 3 points. Killing a zombie with the tentacle: +0.5 point.
//  • Contaminated survivors cough (killer hears it). 4 VACCINE cases hold 2 vaccines each:
//    grab one (1 s), then inject (3 s) to cure.
//  • Two ZOMBIES shamble around (1 m/s) with a 14 m vision cone. A zombie that sees a survivor
//    lurches after them and swipes: injure + contaminate. Pallets stun zombies; a dead zombie
//    respawns somewhere else after 15 s.
const { M, dist, angleDiff, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, Device, Minion, strike } = __req("js/killers/framework.js");

class Zombie extends Minion {
  constructor(p, x, y) {
    super({ kind: 'zombie', owner: p.k, x, y, r: 0.38 * M, color: '#6a7a5a', label: 'zombie', threat: 0.6, visRange: 22 * M, speed: 1 * M });
    this.p = p; this.state = 'wander'; this.stunnable = true; this.stunTime = 3; this.repath = 0; this.swingT = 0; this.cd = 0;
  }
  onStun(m, s) { this.state = 'wander'; this.path = null; this.target = null; s?.ai?.note('Stunned a zombie', 'zstun'); }
  sees(m, s) {
    const d = dist(this.x, this.y, s.x, s.y); if (d > 14 * M) return false;
    if (d > 2 * M && angleDiff(this.facing, Math.atan2(s.y - this.y, s.x - this.x)) > 0.7) return false;
    if (s.moveMode === 'crouch' && m.map.isGrass(s.x, s.y) && d > 4 * M) return false;
    return m.world.losClear(this.x, this.y, s.x, s.y);
  }
  update(dt, m) {
    this.t += dt; this.cd = Math.max(0, this.cd - dt);
    if (this.state === 'wander') {
      if (!this.path || this.followPath(dt, m, 1 * M)) { const gs = m.map.gens.filter((q) => !q.done); const g = gs.length ? gs[Math.floor(Math.random() * gs.length)] : { x: this.x + (Math.random() - 0.5) * 20 * M, y: this.y + (Math.random() - 0.5) * 20 * M }; const a = Math.random() * 6.28; if (!this.pathTo(m, g.x + Math.cos(a) * 6 * M, g.y + Math.sin(a) * 6 * M)) this.path = [{ x: this.x, y: this.y }]; }
      for (const s of m.survivors) if (s.standing && this.sees(m, s)) { this.state = 'pursue'; this.target = s; this.lostT = 0; this.path = null; s.ai?.note('A zombie spotted me', 'zseen'); m.warn(s, 'A zombie has spotted you'); break; }
      return;
    }
    if (this.state === 'pursue') {
      const s = this.target; if (!s || !s.standing) { this.state = 'wander'; this.path = null; return; }
      if (!this.sees(m, s)) { this.lostT += dt; if (this.lostT > 4) { this.state = 'wander'; this.path = null; return; } } else this.lostT = 0;
      const d = dist(this.x, this.y, s.x, s.y);
      if (d < 1.4 * M && this.cd <= 0) { this.state = 'swing'; this.swingT = 0.8; this.facing = Math.atan2(s.y - this.y, s.x - this.x); m.sound('killer_action', this.x, this.y, 12 * M, { what: 'zombie' }); return; }
      this.repath -= dt; if (this.repath <= 0) { this.repath = 0.5; this.pathTo(m, s.x, s.y); }
      this.followPath(dt, m, 1.3 * M);
      return;
    }
    if (this.state === 'swing') {
      this.swingT -= dt; if (this.swingT > 0) return;
      this.state = 'pursue'; this.cd = 3; const s = this.target;
      if (s && s.standing && dist(this.x, this.y, s.x, s.y) < 1.8 * M && angleDiff(this.facing, Math.atan2(s.y - this.y, s.x - this.x)) < 1.0) {
        const r = strike(m, this.p.k, s, { source: 'zombie', label: 'Zombie' }); this.p.k.stats.hits--; // zombies don't count as the killer's basic hits
        if (r && r !== 'dodged') { this.p.contaminate(m, s, false); s.ai?.note('Hit by a zombie', 'zhit'); }
      }
    }
  }
}

class NemesisPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'tentacle'; this.name = 'T-Virus'; this.state = 'idle'; this.charge = 0; this.mrPts = 0; this.zombies = []; this.respawn = [];
    this.aiProfile = { type: 'whip', label: 'Tentacle strike & zombies', counter: ['keep more than 5 m (6.5 m at MR 3)', 'vaccinate when contaminated', 'avoid zombie sightlines and pallet-stun them', 'at MR 2+ pallets don\'t stop the tentacle'] };
  }
  get mr() { return this.mrPts >= 3 ? 3 : this.mrPts >= 1 ? 2 : 1; }
  get reach() { return (this.mr >= 3 ? 6.5 : 5) * M; }
  speedMult() { return this.state === 'charging' ? 0.6 : this.state === 'strike' ? 0.2 : this.state === 'recover' ? 0.6 : 1; }
  canAttack() { return this.state === 'idle'; }
  onInit(m) {
    this._m = m;
    const cand = m.map.openSpots(12, 10, m.map.gens), spots = [];
    while (spots.length < 4 && cand.length) { cand.sort((a, b) => Math.min(...spots.map((q) => dist(q.x, q.y, b.x, b.y)), 1e9) - Math.min(...spots.map((q) => dist(q.x, q.y, a.x, a.y)), 1e9)); spots.push(cand.shift()); }
    for (const p of spots) {
      const d = m.addDevice(new Device({ kind: 'vaccine', x: p.x, y: p.y, r: 0.6 * M, label: 'Vaccine case', useLabel: 'Take a vaccine', useTime: 1, survivorUsable: true, seenRange: 22 * M, data: { vaccines: 2 } }));
      d.usable = (s) => d.data.vaccines > 0 && !(s.vaccines > 0);
      d.onUse = (s, mm) => { d.data.vaccines--; s.vaccines = 1; s.ai?.note('Took a vaccine', 'vaxget'); mm.warn(s, 'Vaccine in hand — Q to inject'); };
    }
    for (let i = 0; i < 2; i++) this.spawnZombie(m);
  }
  spawnZombie(m) {
    const far = m.map.openSpots(6, 6, []).filter((p) => m.survivors.every((s) => dist(s.x, s.y, p.x, p.y) > 12 * M));
    const p = far[0] || m.map.openSpots(1, 4, [])[0]; if (!p) return;
    const z = m.addMinion(new Zombie(this, p.x, p.y)); this.zombies.push(z);
  }
  update(dt, m, inp, locked) {
    this._m = m; this.tickCooldowns(dt);
    const k = this.k;
    if (this.state === 'strike') { this.strikeT -= dt; if (this.strikeT <= 0) { this.resolveStrike(m); this.state = 'recover'; this.recT = 0.7; } return; }
    if (this.state === 'recover') { this.recT -= dt; if (this.recT <= 0) this.state = 'idle'; }
    if (locked) { if (this.state === 'charging') this.state = 'idle'; return; }
    if (this.state === 'idle' && inp.rmb && !k.carrying && this.cd('tentacle') <= 0) { this.state = 'charging'; this.charge = 0; m.sound('power_windup', k.x, k.y, 20 * M, { what: 'tentacle', dir: k.facing }); }
    if (this.state === 'charging') {
      this.charge += dt;
      if (!inp.rmb && this.charge >= 0.35) { this.state = 'strike'; this.strikeT = 0.15; this.dir = k.facing; m.audio?.play('swing'); m.events.emit(EV.POWER_USED, { killer: k, power: 'tentacle' }); }
      else if (!inp.rmb) this.state = 'idle';
    }
  }
  resolveStrike(m) {
    const k = this.k, a = this.dir, R = this.reach; this.setCd('tentacle', 1.2);
    let end = R;
    // walls stop the tentacle; at MR 2+ dropped pallets are smashed instead
    const h = m.world.firstHit(k.x, k.y, k.x + Math.cos(a) * R, k.y + Math.sin(a) * R, 0.15 * M, (q) => q.blocksLOS);
    if (h) end = h.t * R;
    for (let t = 0.3 * M; t < end; t += 0.3 * M) {
      const px = k.x + Math.cos(a) * t, py = k.y + Math.sin(a) * t;
      const pal = m.map.pallets.find((q) => q.state === 'down' && q.stunZone({ x: px, y: py }) && Math.abs((px - q.x) * q.nx + (py - q.y) * q.ny) < 0.3 * M);
      if (!pal) continue;
      if (this.mr >= 2) { m.breakPallet(pal, k); m.fx.text(pal.x, pal.y - 30, 'SMASHED', '#c05050'); }
      else end = t;
      break;
    }
    this.lastStrike = { x: k.x, y: k.y, a, len: end, t: m.time };
    // zombies on the line die (+0.5)
    for (const z of this.zombies) if (!z.dead && this.onLine(k, a, end, z, z.r)) { m.removeMinion(z); this.mrPts += 0.5; this.respawn.push(m.time + 15); m.fx.burst(z.x, z.y, 14, '#6a7a5a', 80, 0.5, 2, 2); }
    let best = null, bd = Infinity;
    for (const s of m.survivors) { if (!s.standing || !this.onLine(k, a, end, s, s.r)) continue; const d = dist(k.x, k.y, s.x, s.y); if (d < bd) { bd = d; best = s; } }
    if (!best) return;
    if (best.dodgeT && best.dodgeT > m.time) { m.fx.text(best.x, best.y - 30, 'DODGED', '#ffd27a'); return; }
    if (best.status.has('contaminated')) { const r = strike(m, k, best, { source: 'tentacle', score: 'brutality', label: 'Tentacle Strike' }); if (r && r !== 'dodged') { best.status.remove('contaminated'); best.ai?.note('Tentacle hit while contaminated', 'tent'); } }
    else { this.contaminate(m, best, true); best.ai?.note('Contaminated by the tentacle', 'contam'); }
  }
  onLine(k, a, len, e, r) {
    const dx = e.x - k.x, dy = e.y - k.y, along = dx * Math.cos(a) + dy * Math.sin(a); if (along < 0 || along > len + r) return false;
    return Math.abs(-dx * Math.sin(a) + dy * Math.cos(a)) < r + 0.45 * M;
  }
  contaminate(m, s, byTentacle) {
    if (s.status.has('contaminated')) return;
    s.status.add('contaminated', { duration: 999, source: 'tvirus' }); s._coughT = 2;
    if (byTentacle) { const before = this.mr; this.mrPts += 1; this.k.stats.powerHits++; if (this.mr > before) { m.fx.text(this.k.x, this.k.y - 50, `MUTATION RATE ${this.mr}`, '#c05050'); m.notifyK(`Mutation Rate ${this.mr}`, '#c05050'); } }
    m.addScore('deviousness', 200, 'Contamination'); m.warn(s, 'Contaminated — find a vaccine');
    m.events.emit('powerTriggered', { survivor: s, x: s.x, y: s.y, killer: this.k });
  }
  tick(dt, m) {
    this.zombies = this.zombies.filter((z) => !z.dead);
    this.respawn = this.respawn.filter((t) => { if (m.time >= t) { this.spawnZombie(m); return false; } return true; });
    for (const s of m.survivors) {
      if (!s.standing || !s.status.has('contaminated')) continue;
      s._coughT = (s._coughT || 0) - dt;
      if (s._coughT <= 0) { s._coughT = 3.5 + Math.random() * 2; m.addGrunt?.(s); m.sound('cough', s.x, s.y, 14 * M, { survivor: s }); }
    }
  }
  drawMinion(ctx, n, m) {
    if (n.kind !== 'zombie') return false;
    ctx.save(); ctx.translate(n.x, n.y); ctx.rotate(n.facing);
    ctx.fillStyle = 'rgba(0,0,0,0.4)'; ctx.beginPath(); ctx.arc(2, 3, 11, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = n.state === 'swing' ? '#9aa070' : '#5a6a4a'; ctx.beginPath(); ctx.arc(0, 0, 10, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = '#c8c0a0'; ctx.beginPath(); ctx.arc(6, 0, 4, 0, Math.PI * 2); ctx.fill();
    ctx.strokeStyle = '#5a6a4a'; ctx.lineWidth = 3; ctx.beginPath(); ctx.moveTo(2, -8); ctx.lineTo(12 + (n.state === 'swing' ? 6 : 0), -9); ctx.moveTo(2, 8); ctx.lineTo(12, 9); ctx.stroke();
    ctx.restore();
    // vision cone (faint)
    ctx.fillStyle = n.state === 'pursue' ? 'rgba(200,80,60,0.08)' : 'rgba(150,170,120,0.05)'; ctx.beginPath(); ctx.moveTo(n.x, n.y); ctx.arc(n.x, n.y, 14 * M, n.facing - 0.7, n.facing + 0.7); ctx.closePath(); ctx.fill();
    if (n.stunT > 0) { ctx.fillStyle = '#ffd27a'; ctx.beginPath(); ctx.arc(n.x, n.y - 14, 3, 0, Math.PI * 2); ctx.fill(); }
    return true;
  }
  drawDevice(ctx, d) {
    if (d.kind !== 'vaccine') return;
    ctx.fillStyle = '#3a4a5a'; ctx.fillRect(d.x - 13, d.y - 9, 26, 18); ctx.fillStyle = d.data.vaccines > 0 ? '#7fe0ff' : '#445'; ctx.fillRect(d.x - 3, d.y - 6, 6, 12); ctx.fillRect(d.x - 6, d.y - 2, 12, 4);
  }
  drawOver(ctx, m) {
    const k = this.k;
    if (this.state === 'charging') { ctx.strokeStyle = 'rgba(192,80,80,0.5)'; ctx.setLineDash([4, 5]); ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.lineTo(k.x + Math.cos(k.facing) * this.reach, k.y + Math.sin(k.facing) * this.reach); ctx.stroke(); ctx.setLineDash([]); }
    const L = this.lastStrike; if (L && m.time - L.t < 0.35) { ctx.strokeStyle = `rgba(160,40,60,${1 - (m.time - L.t) / 0.35})`; ctx.lineWidth = 6; ctx.lineCap = 'round'; ctx.beginPath(); ctx.moveTo(L.x, L.y); ctx.lineTo(L.x + Math.cos(L.a) * L.len, L.y + Math.sin(L.a) * L.len); ctx.stroke(); ctx.lineCap = 'butt'; }
  }
  drawBody(ctx) { ctx.fillStyle = '#3a3030'; ctx.beginPath(); ctx.arc(3, 0, 8, 0, Math.PI * 2); ctx.fill(); ctx.fillStyle = '#a04050'; ctx.fillRect(6, 4, 8, 3); }
  hud(m) {
    const c = m ? m.survivors.filter((s) => s.standing && s.status.has('contaminated')).length : 0;
    const next = this.mr === 1 ? 1 : this.mr === 2 ? 3 : 3;
    return { title: 'T-Virus', value: `MR ${this.mr}`, meter: this.mr >= 3 ? 1 : this.mrPts / next, state: `${c} contaminated · ${this.zombies.length} zombies · reach ${(this.reach / M).toFixed(1)} m`,
      hints: [['Q / RMB', 'Hold 0.35 s, release: Tentacle Strike'], ['MR 2+', 'Tentacle smashes dropped pallets']] };
  }
}

return { NemesisPower };
};
__defs["js/killers/ghoul.js"] = function(){
// THE GHOUL — Kagune Leap (adapted to top-down).
//  • 2 leap tokens (4 s to recharge each, while not leaping). Press RMB to KAGUNE LEAP straight
//    ahead (22 m/s, up to 14 m).
//  • Leap into a SURVIVOR: he LATCHES on (no damage, they are briefly slowed). A second leap that
//    lands on the latched survivor within 2 s is a GRAB ATTACK: a health state, Deep Wound and the
//    KAGUNE MARK (40 s).
//  • Leap into a WALL: he clings to it and has 1.5 s to leap again in a new direction.
//  • While any survivor carries the Kagune Mark he is ENRAGED: 3 tokens recharging every 2.5 s,
//    and a leap that lands on a marked survivor is an immediate grab attack.
//  • The mark ends when the 40 s run out, the survivor mends their Deep Wound, or goes down.
const { M, dist, normAngle, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { BasePower, strike } = __req("js/killers/framework.js");

const LEAP_V = 22, LEAP_D = 14, CHAIN_WIN = 1.5, LATCH_WIN = 2, MARK_T = 40;
class GhoulPower extends BasePower {
  constructor(killer, addons = []) {
    super(killer, addons);
    this.id = 'kagune'; this.name = 'Kagune Leap'; this.state = 'idle'; this.tokens = 2; this.tokenT = 0; this.latchedOn = null; this.winT = 0;
    this.aiProfile = { type: 'leap', label: 'Kagune Leap', counter: ['tight loops and corners (leaps are straight)', 'mend the Deep Wound to clear the Kagune Mark', 'break line after he latches', 'he is slow to recover after a missed leap'] };
  }
  get enraged() { return !!this._m?.survivors.some((s) => s.standing && s.status.has('kaguneMark')); }
  get maxTokens() { return this.enraged ? 3 : 2; }
  speedMult() {
    if (this.state === 'leap') return LEAP_V / this.k.def.speed;
    if (this.state === 'cling' || this.state === 'latch') return 0.1;
    if (this.state === 'recover') return 0.5;
    return 1;
  }
  canAttack() { return this.state === 'idle' || this.state === 'latch'; }
  noContext() { return this.state === 'leap' || this.state === 'cling'; }
  overrideMove() { return this.state === 'leap' ? { x: Math.cos(this.dir), y: Math.sin(this.dir) } : null; }
  onInit(m) { this._m = m; }
  update(dt, m, inp, locked) {
    this._m = m; this.tickCooldowns(dt);
    const k = this.k;
    if (this.state === 'idle' && this.tokens < this.maxTokens) { this.tokenT += dt; if (this.tokenT >= (this.enraged ? 2.5 : 4)) { this.tokenT = 0; this.tokens++; } }
    if (this.tokens > this.maxTokens) this.tokens = this.maxTokens;
    if (this.state === 'recover') { this.recT -= dt; if (this.recT <= 0) this.state = 'idle'; return; }
    if (locked) { if (this.state !== 'idle') { this.state = 'idle'; this.lockFacing = false; } return; }
    switch (this.state) {
      case 'idle': if (inp.rmbP && this.tokens >= 1 && !k.carrying) this.leap(m, k.facing); break;
      case 'leap': {
        this.leapLeft -= LEAP_V * M * dt; k.facing = this.dir;
        for (const s of m.survivors) {
          if (!s.standing || dist(s.x, s.y, k.x, k.y) > k.r + s.r + 0.3 * M || !m.world.losClear(k.x, k.y, s.x, s.y)) continue;
          this.landOn(m, s); return;
        }
        if (this.leapLeft <= 0) { this.state = 'recover'; this.recT = 0.9; this.lockFacing = false; }
        break;
      }
      case 'cling': case 'latch':
        this.winT -= dt;
        if (this.state === 'latch' && this.latchedOn) { const s = this.latchedOn; k.facing = Math.atan2(s.y - k.y, s.x - k.x); }
        if (inp.rmbP && this.tokens >= 1) { this.leap(m, inp.aim ?? k.facing); break; }
        if (this.winT <= 0) { this.state = 'recover'; this.recT = 0.6; this.latchedOn?.status.remove('latched'); this.latchedOn = null; }
        break;
    }
  }
  leap(m, dir) {
    const k = this.k; this.tokens--; this.tokenT = 0; this.state = 'leap'; this.dir = dir; this.leapLeft = LEAP_D * M; this.lockFacing = true;
    m.audio?.play('swing'); m.sound('power_windup', k.x, k.y, 24 * M, { what: 'leap', dir }); m.events.emit(EV.POWER_USED, { killer: k, power: 'leap' });
  }
  onCollide(m) {
    if (this.state !== 'leap') return;
    const k = this.k;
    const along = (k.vx * Math.cos(this.dir) + k.vy * Math.sin(this.dir)) / (LEAP_V * M);
    if (along > 0.75) return; // glancing: keep flying
    this.state = 'cling'; this.winT = CHAIN_WIN; this.lockFacing = false; this.latchedOn = null;
    m.fx.burst(k.x, k.y, 8, '#c02030', 60, 0.3, 2, 2); m.audio?.play('clang');
  }
  landOn(m, s) {
    const k = this.k;
    const grab = (this.latchedOn === s) || s.status.has('kaguneMark');
    if (grab) {
      this.lockFacing = false; this.state = 'recover'; this.recT = 1.2; this.latchedOn = null; s.status.remove('latched');
      const r = strike(m, k, s, { source: 'kagune', score: 'brutality', label: 'Grab Attack' });
      if (r && r !== 'dodged') {
        if (s.standing) { s.status.add('deepWound', { duration: 20 }); s.mend = 0; }
        if (s.standing && !s.status.has('kaguneMark')) { s.status.add('kaguneMark', { duration: MARK_T, source: 'ghoul' }); m.notifyK(`${s.name} is Kagune-Marked — ENRAGED`, '#c02030'); m.warn(s, 'Kagune Mark — mend to remove it'); }
        s.ai?.note('Grab attack by the Ghoul', 'kgrab'); m.fx.shake(8);
        m.events.emit('powerTriggered', { survivor: s, x: s.x, y: s.y, killer: k });
      }
      return;
    }
    // first contact: latch on (no damage)
    this.latchedOn = s; this.state = 'latch'; this.winT = LATCH_WIN; this.lockFacing = false; k.stats.powerHits++;
    s.status.add('latched', { duration: LATCH_WIN, source: 'ghoul' }); s.status.add('hindered', { duration: 0.8, value: 0.25, source: 'latch' });
    m.fx.ring(s.x, s.y, 'rgba(192,32,48,0.9)', 26, 0.4); s.ai?.note('The Ghoul latched onto me', 'latch'); m.warn(s, 'Latched — break away!');
  }
  drawOver(ctx, m) {
    const k = this.k;
    if (this.state === 'leap') { ctx.strokeStyle = 'rgba(192,32,48,0.7)'; ctx.lineWidth = 3; ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.lineTo(k.x - Math.cos(this.dir) * 2 * M, k.y - Math.sin(this.dir) * 2 * M); ctx.stroke(); }
    if (this.latchedOn && this.state === 'latch') { const s = this.latchedOn; ctx.strokeStyle = 'rgba(192,32,48,0.8)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(k.x, k.y); ctx.quadraticCurveTo((k.x + s.x) / 2 + 10, (k.y + s.y) / 2 - 10, s.x, s.y); ctx.stroke(); }
    if (this.state === 'cling' || this.state === 'latch') { ctx.strokeStyle = 'rgba(255,120,120,0.8)'; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(k.x, k.y, k.r + 8, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * Math.max(0, this.winT) / (this.state === 'latch' ? LATCH_WIN : CHAIN_WIN)); ctx.stroke(); }
    if (this.enraged) { ctx.strokeStyle = 'rgba(192,32,48,0.35)'; ctx.lineWidth = 4; ctx.beginPath(); ctx.arc(k.x, k.y, k.r + 3, 0, Math.PI * 2); ctx.stroke(); }
  }
  drawBody(ctx) { ctx.fillStyle = '#e8e8e8'; ctx.beginPath(); ctx.arc(5, 0, 6, 0, Math.PI * 2); ctx.fill(); ctx.strokeStyle = '#c02030'; ctx.lineWidth = 2; for (const a of [-2.4, -2.0, 2.0, 2.4]) { ctx.beginPath(); ctx.moveTo(-4, 0); ctx.lineTo(-4 + Math.cos(a) * 16, Math.sin(a) * 16); ctx.stroke(); } }
  hud(m) {
    const st = { idle: this.enraged ? 'ENRAGED' : 'Ready', leap: 'Leaping', cling: `Clinging — RMB to leap (${this.winT.toFixed(1)}s)`, latch: `Latched on ${this.latchedOn?.name} — RMB: grab attack`, recover: 'Recovering' }[this.state];
    return { title: 'Kagune Leap', value: `${this.tokens}/${this.maxTokens}`, meter: this.tokens / this.maxTokens, state: st,
      hints: [['Q / RMB', 'Kagune Leap (14 m)'], ['RMB after a latch', 'Grab attack: injure + Deep Wound + Kagune Mark'], ['Enraged', '3 tokens; marked survivors are grabbed at once']] };
  }
}

return { GhoulPower };
};
__defs["js/killers/index.js"] = function(){
// Killer registry. Data-driven: a new killer is a def + a power class (see framework.js).
// cost/unlocked are ready for a future currency system; all eight are free for now.
const { POWER_DATA } = __req("js/killers/powerData.js");
const { TrapperPower } = __req("js/killers/trapper.js");
const { HillbillyPower } = __req("js/killers/hillbilly.js");
const { WraithPower } = __req("js/killers/wraith.js");
const { GhostFacePower } = __req("js/killers/ghostface.js");
const { ClownPower } = __req("js/killers/clown.js");
const { ExecutionerPower } = __req("js/killers/executioner.js");
const { SingularityPower } = __req("js/killers/singularity.js");
const { HuntressPower } = __req("js/killers/huntress.js");
const { LegionPower } = __req("js/killers/legion.js");
const { HagPower } = __req("js/killers/hag.js");
const { CannibalPower } = __req("js/killers/cannibal.js");
const { DeathslingerPower } = __req("js/killers/deathslinger.js");
const { DredgePower } = __req("js/killers/dredge.js");
const { KnightPower } = __req("js/killers/knight.js");
const { CenobitePower } = __req("js/killers/cenobite.js");
const { MastermindPower } = __req("js/killers/mastermind.js");
const { HoundmasterPower } = __req("js/killers/houndmaster.js");
const { LichPower } = __req("js/killers/lich.js");
const { SkullMerchantPower } = __req("js/killers/skullmerchant.js");
const { BlightPower } = __req("js/killers/blight.js");
const { NursePower } = __req("js/killers/nurse.js");
const { UnknownPower } = __req("js/killers/unknown.js");
const { NemesisPower } = __req("js/killers/nemesis.js");
const { GhoulPower } = __req("js/killers/ghoul.js");

const KILLERS = {
  trapper: {
    id: 'trapper', name: 'The Trapper', short: 'Trapper', speed: 4.6, terror: 32, color: '#b8a48a', accent: '#d1b37a',
    power: 'Bear Trap', powerDesc: 'Set bear traps that snare survivors who step on them. Traps in tall grass are hard to spot.',
    Power: TrapperPower, cost: 0, unlocked: true, difficulty: 'Easy', signature: ['brutal_strength', 'agitation', 'unnerving'],
    addons: {
      trapper_bag: { name: 'Trapper Bag', desc: 'Carry one extra trap.' },
      fast_set: { name: 'Lengthened Jaws', desc: 'Set traps 30% faster.' },
      serrated_jaws: { name: 'Serrated Jaws', desc: 'Trapped survivors suffer Mangled and Hemorrhage.' },
      iron_coil: { name: 'Coil Spring', desc: 'Escaping a trap is harder.' },
    },
  },
  hillbilly: {
    id: 'hillbilly', name: 'The Hillbilly', short: 'Hillbilly', speed: 4.6, terror: 32, color: '#a87a5a', accent: '#e07a3a',
    power: 'Chainsaw', powerDesc: 'Rev the chainsaw to sprint at high speed. A chainsaw hit downs instantly; obstacles stop you hard.',
    Power: HillbillyPower, cost: 0, unlocked: true, difficulty: 'Hard', signature: ['enduring', 'lightborn', 'tinkerer'],
    addons: {
      primer_bulb: { name: 'Primer Bulb', desc: 'Rev 20% faster.' },
      heavy_clutch: { name: 'Heavy Clutch', desc: 'Sharper sprint turning.' },
      speed_limiter: { name: 'Speed Limiter', desc: 'Chainsaw heats up slower.' },
    },
  },
  wraith: {
    id: 'wraith', name: 'The Wraith', short: 'Wraith', speed: 4.6, terror: 32, color: '#8e9a9a', accent: '#9fd0d0',
    power: 'Wailing Bell', powerDesc: 'Cloak to move fast and unseen. The bell rings when you cloak and uncloak.',
    Power: WraithPower, cost: 0, unlocked: true, difficulty: 'Intermediate', signature: ['predator', 'bloodhound', 'shadowborn'],
    addons: {
      shadow_dance: { name: 'Shadow Dance', desc: 'Longer speed boost after uncloaking.' },
      bone_clapper: { name: 'Bone Clapper', desc: 'Bell range halved.' },
      blind_warrior: { name: 'Blind Warrior', desc: 'Cloak and uncloak 25% faster.' },
    },
  },
  ghostface: {
    id: 'ghostface', name: 'Ghost Face', short: 'Ghost Face', speed: 4.6, terror: 32, color: '#2b2b30', accent: '#e8e8ea',
    power: 'Night Shroud', powerDesc: 'Become undetectable and hard to spot. Hold the power to stalk survivors in view until they are Marked — then one hit downs them. Survivors who stare at you reveal you.',
    Power: GhostFacePower, cost: 0, unlocked: true, difficulty: 'Intermediate', signature: ['insidious', 'trail_of_torment', 'starstruck'],
    addons: {
      philly: { name: '"Philly"', desc: 'Marked lasts 50 s instead of 35 s.' },
      outdoor_rope: { name: 'Outdoor Rope', desc: 'Shroud recovers faster after a reveal.' },
      drop_leg_sheath: { name: 'Drop-Leg Sheath', desc: 'Stalk 25% faster.' },
    },
  },
  clown: {
    id: 'clown', name: 'The Clown', short: 'Clown', speed: 4.4, terror: 32, color: '#8a5a6a', accent: '#c28ee0',
    power: 'Afterpiece Tonic', powerDesc: 'Lob bottles of gas. Tonic slows and intoxicates survivors; antidote speeds up anyone inside, including you.',
    Power: ClownPower, cost: 0, unlocked: true, difficulty: 'Easy', signature: ['coulrophobia', 'pop', 'bamboozle'],
    addons: {
      fingerless_parade: { name: 'Fingerless Parade Gloves', desc: 'Carry 8 bottles.' },
      robin_feather: { name: 'Robin Feather', desc: 'Reload faster.' },
      redheads_pinkie: { name: "Redhead's Pinky Finger", desc: 'Bigger gas clouds.' },
      ether: { name: 'Ether 15 Vol%', desc: 'Stronger slow inside the gas.' },
    },
  },
  executioner: {
    id: 'executioner', name: 'The Executioner', short: 'Executioner', speed: 4.6, terror: 32, color: '#6a5444', accent: '#e2683c',
    power: 'Rites of Judgement', powerDesc: 'Enter Punishment mode to lay Torment Trails and fire a ground wave that passes through walls. Send Tormented downed survivors to a Cage.',
    Power: ExecutionerPower, cost: 0, unlocked: true, difficulty: 'Intermediate', signature: ['deathbound', 'forced_penance', 'trail_of_torment'],
    addons: {
      burning_man: { name: 'Burning Man Painting', desc: 'Faster wave wind-up.' },
      lead_ring: { name: 'Lead Ring', desc: 'Wave travels further.' },
      rust_shackles: { name: 'Rust-Coloured Egg', desc: 'Trails last 80 s.' },
    },
  },
  singularity: {
    id: 'singularity', name: 'The Singularity', short: 'Singularity', speed: 4.6, terror: 32, color: '#3c4a56', accent: '#5fe0e0',
    power: 'Quantum Instantiation', powerDesc: 'Place biopods that watch the map and Slipstream survivors. Teleport to a pod next to any Slipstreamed survivor. Survivors can print EMPs to wipe pods.',
    Power: SingularityPower, cost: 0, unlocked: true, difficulty: 'Hard', signature: ['genetic_limits', 'forced_hesitation', 'machine_learning'],
    addons: {
      cold_dirt: { name: 'Cold Dirt', desc: 'Carry 5 biopods.' },
      denied_requisition: { name: 'Denied Requisition Form', desc: 'Pods watch a wider area.' },
      nanomachine: { name: 'Nanomachine Gel', desc: 'Slipstream lasts 60 s.' },
      crew_manifest: { name: 'Crew Manifest', desc: 'Teleport recharges faster.' },
    },
  },
  huntress: {
    id: 'huntress', name: 'The Huntress', short: 'Huntress', speed: 4.4, terror: 40, color: '#7a6248', accent: '#cfd3d6',
    power: 'Hunting Hatchets', powerDesc: 'Wind up and throw hatchets across the map. Limited supply — reload at lockers.',
    Power: HuntressPower, cost: 0, unlocked: true, difficulty: 'Hard', signature: ['bbq', 'territorial', 'beast_of_prey'],
    addons: {
      leather_loop: { name: 'Leather Loop', desc: 'Carry 7 hatchets.' },
      shiny_pin: { name: 'Shiny Pin', desc: 'Faster full wind-up.' },
      flower_babushka: { name: 'Flower Babushka', desc: 'Hatchets fly further.' },
    },
  },
  legion: {
    id: 'legion', name: 'The Legion', short: 'Legion', speed: 4.6, terror: 32, color: '#5a5048', accent: '#d8e05a',
    power: 'Feral Frenzy', powerDesc: 'Enter a Frenzy: run faster, vault windows and dropped pallets, and slash survivors to inflict Deep Wound. Hits refill the gauge and reveal nearby survivors so you can chain. A miss ends in Fatigue.',
    Power: LegionPower, cost: 0, unlocked: true, difficulty: 'Easy', signature: ['discordance', 'mad_grit', 'iron_maiden'],
    addons: {
      stab_wounds: { name: 'Stab Wounds Study', desc: 'Frenzy speed 5.4 m/s.' },
      mural_sketch: { name: 'Mural Sketch', desc: 'Frenzy lasts longer.' },
      never_sleep: { name: 'Never-Sleep Pills', desc: 'Gauge recharges faster.' },
      cold_dirt: { name: 'Cold Dirt', desc: 'Shorter Fatigue.' },
      filthy_blade: { name: 'Filthy Blade', desc: 'Deep Wound timer 16 s.' },
    },
  },
  hag: {
    id: 'hag', name: 'The Hag', short: 'Hag', speed: 4.4, terror: 32, color: '#4a5a3a', accent: '#a9c27a',
    power: 'Blackened Catalyst', powerDesc: 'Draw Phantasm Traps on the ground. A survivor who walks over one triggers it (crouching avoids it) — teleport to it with E if you are within 40 m.',
    Power: HagPower, cost: 0, unlocked: true, difficulty: 'Hard', signature: ['third_seal', 'devour_hope', 'ruin'],
    addons: {
      rusty_shackles: { name: 'Rusty Shackles', desc: '12 traps.' },
      bloodied_mud: { name: 'Bloodied Mud', desc: 'Draw traps faster.' },
      mint_rag: { name: 'Mint Rag', desc: 'Teleport range 50 m.' },
      dried_cicada: { name: 'Dried Cicada', desc: 'Longer teleport window.' },
      scarred_hand: { name: 'Scarred Hand', desc: 'Traps are harder to spot.' },
    },
  },
  cannibal: {
    id: 'cannibal', name: 'The Cannibal', short: 'Cannibal', speed: 4.6, terror: 32, color: '#7a6048', accent: '#d06a2a',
    power: "Bubba's Chainsaw", powerDesc: 'Rev and sweep the chainsaw in a wide arc in front of you — downs instantly, can down several survivors. Three charges. Sawing into walls or dropped pallets throws you into a Tantrum.',
    Power: CannibalPower, cost: 0, unlocked: true, difficulty: 'Easy', signature: ['knock_out', 'bbq', 'franklins_demise'],
    addons: {
      award_winning_chili: { name: 'Award-Winning Chili', desc: '4 chainsaw charges.' },
      primer_bulb: { name: 'Primer Bulb', desc: 'Rev faster.' },
      long_guide_bar: { name: 'Long Guide Bar', desc: 'Wider sweep reach.' },
      knife_scratches: { name: 'Knife Scratches', desc: 'Charges recover faster.' },
      the_beast_marks: { name: "The Beast's Marks", desc: 'Shorter Tantrums.' },
    },
  },
  deathslinger: {
    id: 'deathslinger', name: 'The Deathslinger', short: 'Deathslinger', speed: 4.4, terror: 24, color: '#6a5a40', accent: '#b9a37a',
    power: 'The Redeemer', powerDesc: 'Aim down the sights and fire a harpoon. A speared survivor is reeled in until the chain snaps on an obstacle (Deep Wound). Reload after every shot.',
    Power: DeathslingerPower, cost: 0, unlocked: true, difficulty: 'Hard', signature: ['dead_mans_switch', 'retribution', 'gearhead'],
    addons: {
      rusted_spike: { name: 'Rusted Spike', desc: 'Range 20 m.' },
      speed_loader: { name: 'Speed Loader', desc: 'Reload in 1.5 s.' },
      chewing_tobacco: { name: 'Chewing Tobacco', desc: 'Reel for longer before the chain snaps.' },
    },
  },
  dredge: {
    id: 'dredge', name: 'The Dredge', short: 'Dredge', speed: 4.6, terror: 32, color: '#2a2834', accent: '#9a8fe0',
    power: 'Reign of Darkness', powerDesc: 'Enter the Gloaming and teleport into any locker on the map, or snap back to your Remnant. Build up Nightfall: 60 s of darkness where survivors can barely see.',
    Power: DredgePower, cost: 0, unlocked: true, difficulty: 'Intermediate', signature: ['dissolution', 'darkness_revealed', 'septic_touch'],
    addons: {
      boat_key: { name: 'Boat Key', desc: '4 teleport tokens.' },
      malthinkers_skull: { name: "Malthinker's Skull", desc: 'Nightfall lasts 70 s.' },
    },
  },
  knight: {
    id: 'knight', name: 'The Knight', short: 'Knight', speed: 4.6, terror: 32, color: '#5a5a64', accent: '#e6c878',
    power: 'Guardia Compagnia', powerDesc: 'Draw a patrol path and summon a Guard — Carnifex, Assassin or Jailer. Guards patrol, spot survivors and hunt them down, breaking pallets on the way.',
    Power: KnightPower, cost: 0, unlocked: true, difficulty: 'Intermediate', signature: ['nowhere_to_hide', 'face_the_darkness', 'hubris'],
    addons: {
      broken_hilt: { name: 'Broken Hilt', desc: 'Longer patrol paths.' },
      ironworks_tongs: { name: 'Ironworks Tongs', desc: 'Power recharges faster.' },
    },
  },
  cenobite: {
    id: 'cenobite', name: 'The Cenobite', short: 'Cenobite', speed: 4.6, terror: 32, color: '#3a3438', accent: '#e0b65a',
    power: 'Summons of Pain', powerDesc: 'Shoot steerable Possessed Chains (also from Gateways) that bind survivors. The Lament Configuration starts a Chain Hunt if nobody solves it.',
    Power: CenobitePower, cost: 0, unlocked: true, difficulty: 'Hard', signature: ['deadlock', 'plaything', 'gift_of_pain'],
    addons: {
      impaling_wire: { name: 'Impaling Wire', desc: 'Chain Hunt charges in 80 s.' },
      chatterer_tooth: { name: "Chatterer's Tooth", desc: 'Chain cooldown 4 s.' },
    },
  },
  mastermind: {
    id: 'mastermind', name: 'The Mastermind', short: 'Mastermind', speed: 4.6, terror: 32, color: '#2a2a2e', accent: '#7fae3a',
    power: 'Virulent Bound', powerDesc: 'Charge and bound forward; chain a second, longer bound. Grab survivors, carry them and slam them into walls. Bounds spread the Uroboros infection.',
    Power: MastermindPower, cost: 0, unlocked: true, difficulty: 'Intermediate', signature: ['superior_anatomy', 'awakened_awareness', 'terminus'],
    addons: { cell_divider: { name: 'Cell Divider', desc: 'Charge bounds faster.' } },
  },
  houndmaster: {
    id: 'houndmaster', name: 'The Houndmaster', short: 'Houndmaster', speed: 4.6, terror: 32, color: '#6a4a3a', accent: '#c8a878',
    power: 'Hunting', powerDesc: 'Command your dog: Chase Command sends it charging down a plotted line; Search Command sends it to sniff out an area. Snug drags caught survivors back to you.',
    Power: HoundmasterPower, cost: 0, unlocked: true, difficulty: 'Intermediate', signature: ['all_shaking_thunder', 'no_quarter', 'jagged_compass'],
    addons: {
      training_whistle: { name: 'Training Whistle', desc: 'Chase Command reaches 24 m.' },
      rope_leash: { name: 'Rope Leash', desc: 'Drag lasts 9 s.' },
    },
  },
  lich: {
    id: 'lich', name: 'The Lich', short: 'Lich', speed: 4.6, terror: 32, color: '#3a3044', accent: '#c8b0ff',
    power: 'Vile Darkness', powerDesc: 'Four spells on the wheel: Flight of the Damned, Fly, Mage Hand and Dispelling Sphere. Survivors loot Magic Items that each counter one spell.',
    Power: LichPower, cost: 0, unlocked: true, difficulty: 'Hard', signature: ['dark_arrogance', 'languid_touch', 'weave_attunement'],
    addons: {
      ornate_horn: { name: 'Ornate Horn', desc: 'One more damned entity.' },
      pewter_wings: { name: 'Pewter Wings', desc: 'Fly for 6 s.' },
    },
  },
  skull_merchant: {
    id: 'skull_merchant', name: 'The Skull Merchant', short: 'Skull Merchant', speed: 4.6, terror: 32, color: '#c8c0b0', accent: '#8fd06a',
    power: 'Eyes in the Sky', powerDesc: 'Deploy drones whose rotating scan lines detect moving survivors. Three lock-ons inflict a claw trap; claw-trapped survivors are tracked and hindered. Survivors can hack drones.',
    Power: SkullMerchantPower, cost: 0, unlocked: true, difficulty: 'Intermediate', signature: ['game_afoot', 'leverage', 'thwack'],
    addons: { ultrasonic_speaker: { name: 'Ultrasonic Speaker', desc: 'One more drone.' } },
  },
  blight: {
    id: 'blight', name: 'The Blight', short: 'Blight', speed: 4.6, terror: 32, color: '#5a4a3a', accent: '#c7a04a',
    power: 'Blighted Corruption', powerDesc: 'Rush at 9.2 m/s with 5 tokens. Slamming into walls lets you chain another rush; chained rushes are lethal and allow a special attack. Fatigue when the chain ends.',
    Power: BlightPower, cost: 0, unlocked: true, difficulty: 'Very Hard', signature: ['dragons_grip', 'blood_favour', 'undying'],
    addons: { compound_33: { name: 'Compound Thirty-Three', desc: 'One more rush token.' }, alchemists_ring: { name: "Alchemist's Ring", desc: 'Turn 25% tighter while rushing.' } },
  },
  nurse: {
    id: 'nurse', name: 'The Nurse', short: 'Nurse', speed: 3.85, terror: 32, color: '#c8c0b8', accent: '#c8d8e8',
    power: "Spencer's Last Breath", powerDesc: 'Charge and blink up to 20 m through walls, chain a second blink, attack out of a blink. Slow walker, cannot vault, fatigued after blinking.',
    Power: NursePower, cost: 0, unlocked: true, difficulty: 'Very Hard', signature: ['nurses_calling', 'stridor', 'thanatophobia'],
    addons: { anxious_gasp: { name: 'Anxious Gasp', desc: 'One more blink token.' } },
  },
  unknown: {
    id: 'unknown', name: 'The Unknown', short: 'Unknown', speed: 4.6, terror: 32, color: '#4a3a5a', accent: '#b0ff80',
    power: 'UVX', powerDesc: 'Fire bouncing UVX bursts that Weaken survivors (and hurt the Weakened). Leave hallucinations and teleport to them. Survivors stare to recover and dispel.',
    Power: UnknownPower, cost: 0, unlocked: true, difficulty: 'Intermediate', signature: ['unforeseen', 'undone', 'unbound'],
    addons: {},
  },
  nemesis: {
    id: 'nemesis', name: 'The Nemesis', short: 'Nemesis', speed: 4.6, terror: 32, color: '#3a3030', accent: '#c05050',
    power: 'T-Virus', powerDesc: 'Tentacle Strike contaminates (and hurts the contaminated); Mutation Rate grows its reach and lets it smash pallets. Two zombies roam the map. Survivors cure with vaccines.',
    Power: NemesisPower, cost: 0, unlocked: true, difficulty: 'Intermediate', signature: ['lethal_pursuer', 'lethal_power', 'eruption'],
    addons: {},
  },
  ghoul: {
    id: 'ghoul', name: 'The Ghoul', short: 'Ghoul', speed: 4.6, terror: 32, color: '#3a3a3e', accent: '#c02030',
    power: 'Kagune Leap', powerDesc: 'Leap 14 m: latch onto a survivor, then leap again for a grab attack (injury, Deep Wound, Kagune Mark). Cling to walls to redirect. Enraged while anyone is marked.',
    Power: GhoulPower, cost: 0, unlocked: true, difficulty: 'Intermediate', signature: ['forever_entwined', 'none_are_free', 'haywire'],
    addons: {},
  },
};
for (const k of Object.values(KILLERS)) k.powerData = POWER_DATA[k.id];
const KILLER_LIST = Object.values(KILLERS);

return { KILLERS, KILLER_LIST };
};
__defs["js/perks/killerPerks.js"] = function(){
// Killer perks the player picks in the loadout (up to 4).
const { EV } = __req("js/core/events.js");
const { M, dist } = __req("js/core/util.js");

const KILLER_PERKS = {
  bbq: { id: 'bbq', side: 'killer', name: 'Barbecue & Chili', icon: '♨', rarity: 'very rare',
    description: 'After hooking a survivor, every survivor farther than 40 m from the hook is revealed for 4 s.',
    triggers: { [EV.SURVIVOR_HOOKED]: (p, d, m) => { let any = false; for (const s of m.survivors) if (s.standing && dist(s.x, s.y, d.hook.x, d.hook.y) > 40 * M) { m.reveal(s, 4, 'BBQ'); any = true; } if (any) m.perks.proc(p, 'Barbecue & Chili'); } } },
  ruin: { id: 'ruin', side: 'killer', name: 'Hex: Ruin', icon: '☠', rarity: 'very rare', hex: true,
    description: 'Generators not being repaired regress automatically while the hex totem stands.',
    init: (p, m) => { const t = m.lightHex('ruin'); p.data.totem = t; },
    update: (p, m, dt) => { const t = p.data.totem; if (!t || t.cleansed || t.hex !== 'ruin') { p.active = false; return; } p.active = true;
      for (const g of m.map.gens) if (!g.done && g.repairers.size === 0 && g.charges > 0) { g.charges = Math.max(0, g.charges - dt * 0.5); g.ruin = true; } else g.ruin = false; } },
  pop: { id: 'pop', side: 'killer', name: 'Pop Goes the Weasel', icon: '✷', rarity: 'rare',
    description: 'After hooking, your next generator kick within 45 s removes an extra 20% progress.',
    triggers: { [EV.SURVIVOR_HOOKED]: (p, d, m) => { p.data.until = m.time + 45; p.active = true; },
      [EV.GENERATOR_DAMAGED]: (p, d, m) => { if (p.data.until > m.time) { d.gen.charges = Math.max(0, d.gen.charges - 90 * 0.2); p.data.until = 0; p.active = false; m.perks.proc(p, 'Pop Goes the Weasel'); } } },
    update: (p, m) => { if (p.data.until <= m.time) p.active = false; } },
  sloppy: { id: 'sloppy', side: 'killer', name: 'Sloppy Butcher', icon: '✂', rarity: 'common',
    description: 'Basic attacks inflict Mangled and Hemorrhage for 90 s.',
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { if (!d.power && d.result === 'injured') { d.survivor.status.add('mangled', { duration: 90 }); d.survivor.status.add('hemorrhage', { duration: 90 }); } } } },
  discordance: { id: 'discordance', side: 'killer', name: 'Discordance', icon: '⚇', rarity: 'uncommon',
    description: 'Generators being repaired by 2 or more survivors are revealed.',
    update: (p, m) => { for (const g of m.map.gens) { if (!g.done && g.repairers.size >= 2 && !(g._disc > m.time)) { g._disc = m.time + 8; m.reveal(g, 6, 'Discordance'); m.perks.proc(p, 'Discordance'); } } } },
  brutal_strength: { id: 'brutal_strength', side: 'killer', name: 'Brutal Strength', icon: '✊', rarity: 'common',
    description: 'Break pallets and damage generators 20% faster.' },
  enduring: { id: 'enduring', side: 'killer', name: 'Enduring', icon: '⛉', rarity: 'uncommon', description: 'Recover from pallet stuns 50% faster.' },
  bamboozle: { id: 'bamboozle', side: 'killer', name: 'Bamboozle', icon: '⊘', rarity: 'rare',
    description: 'Vault windows 15% faster; windows you vault are blocked for survivors for 8 s.' },
  nurses_calling: { id: 'nurses_calling', side: 'killer', name: 'A Nurse\'s Calling', icon: '✚', rarity: 'rare',
    description: 'Survivors healing within 28 m are revealed.',
    update: (p, m) => { for (const s of m.survivors) { if (s.action && (s.action.type === 'heal' || s.action.type === 'healSelf') && dist(s.x, s.y, p.owner.x, p.owner.y) < 28 * M) m.reveal(s, 0.3, 'Nurse\'s Calling', true); } } },
  lethal_pursuer: { id: 'lethal_pursuer', side: 'killer', name: 'Lethal Pursuer', icon: '◬', rarity: 'rare',
    description: 'At the start of the trial, every survivor is revealed for 9 s.',
    triggers: { matchStart: (p, d, m) => { for (const s of m.survivors) m.reveal(s, 9, 'Lethal Pursuer'); m.perks.proc(p, 'Lethal Pursuer'); } } },
  corrupt: { id: 'corrupt', side: 'killer', name: 'Corrupt Intervention', icon: '⛬', rarity: 'rare',
    description: 'The three generators farthest from you are blocked for the first 80 s.',
    triggers: { matchStart: (p, d, m) => { const k = p.owner; const gs = [...m.map.gens].sort((a, b) => dist(b.x, b.y, k.x, k.y) - dist(a.x, a.y, k.x, k.y)).slice(0, 3); for (const g of gs) g.blockedUntil = m.time + 80; m.perks.proc(p, 'Corrupt Intervention'); } } },
  noed: { id: 'noed', side: 'killer', name: 'Hex: No One Escapes Death', icon: '☩', rarity: 'very rare', hex: true,
    description: 'When the exit gates are powered, a dull totem becomes a hex: all survivors are Exposed until it is cleansed.',
    triggers: { [EV.EXIT_GATES_POWERED]: (p, d, m) => { const t = m.lightHex('noed'); if (t) { p.data.totem = t; p.active = true; m.perks.proc(p, 'No One Escapes Death'); } } },
    update: (p, m) => { const t = p.data.totem; if (!t) return; const on = !t.cleansed && t.hex === 'noed'; p.active = on;
      for (const s of m.survivors) { if (on) s.status.add('exposed', { duration: 0.5, source: 'noed' }); } } },
  iron_grasp: { id: 'iron_grasp', side: 'killer', name: 'Iron Grasp', icon: '✋', rarity: 'uncommon', description: 'Survivors take 25% longer to wiggle free.' },
  agitation: { id: 'agitation', side: 'killer', name: 'Agitation', icon: '⇈', rarity: 'uncommon', description: 'Move 18% faster while carrying a survivor.' },
  thanatophobia: { id: 'thanatophobia', side: 'killer', name: 'Thanatophobia', icon: '⚕', rarity: 'uncommon',
    description: 'Survivors repair, heal and cleanse 4% slower for each injured, dying or hooked survivor (max 16%).',
    survivorActionMult: (p, sv, type, m) => { const n = m.survivors.filter((s) => s.health === 'injured' || s.health === 'dying' || s.health === 'hooked').length; return ['repair', 'heal', 'healSelf', 'cleanse'].includes(type) ? 1 - Math.min(4, n) * 0.04 : 1; } },
  whispers: { id: 'whispers', side: 'killer', name: 'Whispers', icon: '≋', rarity: 'common', description: 'Your HUD lights up when a survivor is within 48 m.' },
};
// ---------------------------------------------------------------------------------
// Expansion pool. Hooks the engine asks killer perks for:
//   terrorMult(p,m)            terror radius multiplier
//   skillCheckMod(p,s,g,m)     -> { fail:+x, great:-x, failExtra:+fraction }
//   survivorHealMult(p,h,t,m)  healing speed multiplier for survivors
//   wiggleMult(p,m)            carry wiggle time multiplier
//   scratchLife / bloodLife / gruntRate   tracking multipliers
//   killerActionMult(p,type,m) killerCooldownMult(p,kind,m)  survivorActionMult(...)
//   flashImmune                lightborn
const GC = 90;
const explode = (g, frac, m, why) => { if (g.done) return; g.charges = Math.max(0, g.charges - GC * frac); g.regressing = g.charges > 0; m.fx.sparks(g.x, g.y); m.audio?.play('genfail'); m.loudNoise(g.x, g.y, 'explode');
  for (const s of [...g.repairers]) { s.stopAction(m); m.addGrunt(s); s.ai?.note(`${why}: generator exploded`, 'explode'); } };
const inTR = (s, m, k = m.killer) => dist(s.x, s.y, k.x, k.y) < k.terrorRadius;
const undetect = (m, dur, src, k = m.killer) => k.status.add('undetectable', { duration: dur, source: src });
Object.assign(KILLER_PERKS, {
  // ---- generator pressure ----
  eruption: { id: 'eruption', side: 'killer', cat: 'gen', name: 'Eruption', icon: '⇯', rarity: 'rare', cooldown: 30,
    description: 'Kicked generators are highlighted. Downing a survivor explodes them (-10%) and Incapacitates everyone repairing them for 20 s.',
    triggers: { [EV.GENERATOR_DAMAGED]: (p, d) => { (p.data.gens ||= new Set()).add(d.gen); },
      [EV.SURVIVOR_DOWNED]: (p, d, m) => { if (p.cooldownT > 0 || !p.data.gens?.size) return; for (const g of p.data.gens) { for (const s of g.repairers) s.status.add('incapacitated', { duration: 20 }); explode(g, 0.1, m, 'Eruption'); } p.data.gens.clear(); p.cooldownT = 30; m.perks.proc(p, 'Eruption'); } } },
  grim_embrace: { id: 'grim_embrace', side: 'killer', cat: 'gen', name: 'Grim Embrace', icon: '⛓', rarity: 'rare',
    description: 'The first time each survivor is hooked, all generators are blocked for 12 s. On the 4th token, they are blocked for 40 s.',
    triggers: { [EV.SURVIVOR_HOOKED]: (p, d, m) => { (p.data.seen ||= new Set()); if (p.data.seen.has(d.survivor)) return; p.data.seen.add(d.survivor); p.tokens++; const t = p.tokens >= 4 ? 40 : 12; for (const g of m.map.gens) if (!g.done) g.blockedUntil = Math.max(g.blockedUntil, m.time + t); m.perks.proc(p, 'Grim Embrace'); } } },
  jolt: { id: 'jolt', side: 'killer', cat: 'gen', name: 'Surge', icon: 'ϟ', rarity: 'uncommon', cooldown: 40,
    description: 'Downing a survivor with a basic attack explodes every generator within 32 m (-8%).',
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { if (d.power || d.result !== 'downed' || p.cooldownT > 0) return; let n = 0; for (const g of m.map.gens) if (!g.done && g.charges > 0 && dist(g.x, g.y, p.owner.x, p.owner.y) < 32 * M) { explode(g, 0.08, m, 'Surge'); n++; } if (n) { p.cooldownT = 40; m.perks.proc(p, 'Surge'); } } } },
  overcharge: { id: 'overcharge', side: 'killer', cat: 'gen', name: 'Overcharge', icon: '⚡', rarity: 'uncommon',
    description: 'The next skill check on a generator you kicked is much harder; failing it regresses an extra 4%.',
    triggers: { [EV.GENERATOR_DAMAGED]: (p, d) => { d.gen.overcharged = true; } },
    skillCheckMod: (p, s, g) => { if (!g.overcharged) return null; g.overcharged = false; return { fail: 0.35, failExtra: 0.04 }; } },
  deadlock: { id: 'deadlock', side: 'killer', cat: 'gen', name: 'Deadlock', icon: '⊠', rarity: 'rare',
    description: 'When a generator is completed, the generator with the most progress is blocked for 25 s.',
    triggers: { [EV.GENERATOR_REPAIRED]: (p, d, m) => { const g = m.map.gens.filter((q) => !q.done).sort((a, b) => b.charges - a.charges)[0]; if (g) { g.blockedUntil = Math.max(g.blockedUntil, m.time + 25); m.reveal(g, 25, 'Deadlock', true); m.perks.proc(p, 'Deadlock'); } } } },
  pain_resonance: { id: 'pain_resonance', side: 'killer', cat: 'gen', name: 'Scourge: Pain Resonance', icon: '♒', rarity: 'very rare',
    description: 'First 4 unique hooks: the generator with the most progress explodes (-15%) and everyone on it screams and is revealed.',
    triggers: { [EV.SURVIVOR_HOOKED]: (p, d, m) => { (p.data.seen ||= new Set()); if (p.data.seen.has(d.survivor) || p.data.seen.size >= 4) return; p.data.seen.add(d.survivor); const g = m.map.gens.filter((q) => !q.done && q.charges > 0).sort((a, b) => b.charges - a.charges)[0]; if (!g) return; for (const s of g.repairers) m.reveal(s, 3, 'Pain Resonance'); explode(g, 0.15, m, 'Pain Resonance'); m.perks.proc(p, 'Pain Resonance'); } } },
  unnerving: { id: 'unnerving', side: 'killer', cat: 'gen', name: 'Unnerving Presence', icon: '◍', rarity: 'common',
    description: 'Survivors in your terror radius fail skill checks more often.', skillCheckMod: (p, s, g, m) => (inTR(s, m, p.owner) ? { fail: 0.08, great: -0.1 } : null) },
  machine_learning: { id: 'machine_learning', side: 'killer', cat: 'gen', name: 'Machine Learning', icon: '⌬', rarity: 'rare',
    description: 'Kick a generator to compromise it. When it is completed you gain 10% haste and Undetectable for 30 s.',
    triggers: { [EV.GENERATOR_DAMAGED]: (p, d) => { if (!p.data.gen || p.data.gen.done) p.data.gen = d.gen; },
      [EV.GENERATOR_REPAIRED]: (p, d, m) => { if (d.gen === p.data.gen) { p.data.gen = null; p.owner.status.add('haste', { duration: 30, value: 0.1, source: 'ml' }); undetect(m, 30, 'ml', p.owner); m.perks.proc(p, 'Machine Learning'); } } } },
  fire_up: { id: 'fire_up', side: 'killer', cat: 'endgame', name: 'Fire Up', icon: '🜂', rarity: 'uncommon',
    description: 'Each completed generator makes you 4% faster at breaking, kicking, vaulting and picking up.',
    killerActionMult: (p, type, m) => (['kick', 'break', 'vault', 'pickup', 'hook'].includes(type) ? 1 - 0.04 * m.gensDone : 1) },
  // ---- chase ----
  spirit_fury: { id: 'spirit_fury', side: 'killer', cat: 'chase', name: 'Spirit Fury', icon: '♆', rarity: 'rare',
    description: 'After breaking 2 pallets, the next pallet that stuns you is instantly destroyed and the stun is shorter.',
    triggers: { [EV.PALLET_DESTROYED]: (p) => { if (!p.data.ready) { p.tokens++; if (p.tokens >= 2) { p.data.ready = true; p.active = true; } } } } },
  play_with_food: { id: 'play_with_food', side: 'killer', cat: 'chase', name: 'Play with Your Food', icon: '☊', rarity: 'uncommon',
    description: 'Each chase that ends without a hit gives a token (max 3): +3% speed each. A basic attack spends a token.',
    triggers: { [EV.CHASE_END]: (p, d, m) => { if (!p.data.hit) { p.tokens = Math.min(3, p.tokens + 1); m.perks.proc(p, 'Play with Your Food'); } p.data.hit = false; },
      [EV.KILLER_HIT]: (p, d) => { p.data.hit = true; if (!d.power && p.tokens > 0) p.tokens--; }, [EV.KILLER_MISS]: (p) => { if (p.tokens > 0) p.tokens--; } },
    update: (p, m) => { if (p.tokens > 0) p.owner.status.add('haste', { duration: 0.3, value: 0.03 * p.tokens, source: 'pwyf' }); } },
  save_the_best: { id: 'save_the_best', side: 'killer', cat: 'chase', name: 'Save the Best for Last', icon: '✦', rarity: 'rare',
    description: 'Each hit gives a token (max 8): 5% faster attack recovery per token. Downing someone costs 3 tokens.',
    triggers: { [EV.KILLER_HIT]: (p, d) => { if (d.result === 'downed') p.tokens = Math.max(0, p.tokens - 3); else p.tokens = Math.min(8, p.tokens + 1); } },
    killerCooldownMult: (p, kind) => (kind === 'hit' ? 1 - 0.05 * p.tokens : 1) },
  coulrophobia: { id: 'coulrophobia', side: 'killer', cat: 'chase', name: 'Coulrophobia', icon: '☺', rarity: 'uncommon',
    description: 'Survivors in your terror radius heal 50% slower.', survivorHealMult: (p, h, t, m) => (inTR(h, m, p.owner) ? 0.5 : 1) },
  unrelenting: { id: 'unrelenting', side: 'killer', cat: 'chase', name: 'Unrelenting', icon: '↺', rarity: 'common',
    description: 'Recover from missed attacks 30% faster.', killerCooldownMult: (p, kind) => (kind === 'miss' ? 0.7 : 1) },
  starstruck: { id: 'starstruck', side: 'killer', cat: 'hook', name: 'Starstruck', icon: '✩', rarity: 'rare', cooldown: 60,
    description: 'While carrying, survivors in your terror radius become Exposed for 26 s.',
    update: (p, m) => { const k = p.owner; if (!k.carrying || p.cooldownT > 0) { if (p.data.on && !k.carrying) { p.data.on = false; p.cooldownT = 60; } return; } p.data.on = true; for (const s of m.survivors) if (s !== k.carrying && s.standing && inTR(s, m, p.owner)) s.status.add('exposed', { duration: 26, source: 'starstruck' }); } },
  forced_penance: { id: 'forced_penance', side: 'killer', cat: 'chase', name: 'Forced Penance', icon: '⚖', rarity: 'uncommon',
    description: 'Survivors who take a protection hit (hit within 10 s of rescuing or healing someone) become Broken for 80 s.',
    triggers: { [EV.SURVIVOR_UNHOOKED]: (p, d, m) => { if (d.rescuer) d.rescuer._protT = m.time; }, [EV.SURVIVOR_HEALED]: (p, d, m) => { if (d.healer && d.healer !== d.survivor) d.healer._protT = m.time; },
      [EV.KILLER_HIT]: (p, d, m) => { if (m.time - (d.survivor._protT ?? -99) < 10) { d.survivor.status.add('broken', { duration: 80 }); m.perks.proc(p, 'Forced Penance'); } } } },
  genetic_limits: { id: 'genetic_limits', side: 'killer', cat: 'chase', name: 'Genetic Limits', icon: '⧖', rarity: 'uncommon',
    description: 'Survivors who finish healing become Exhausted for 30 s.', triggers: { [EV.SURVIVOR_HEALED]: (p, d) => { d.survivor.status.add('exhausted', { duration: 30, source: 'genetic' }); } } },
  forced_hesitation: { id: 'forced_hesitation', side: 'killer', cat: 'chase', name: 'Forced Hesitation', icon: '⧗', rarity: 'uncommon', cooldown: 30,
    description: 'When you down a survivor, others within 16 m are Hindered by 20% for 4 s.',
    triggers: { [EV.SURVIVOR_DOWNED]: (p, d, m) => { if (p.cooldownT > 0) return; let n = 0; for (const s of m.survivors) if (s !== d.survivor && s.standing && dist(s.x, s.y, d.survivor.x, d.survivor.y) < 16 * M) { s.status.add('hindered', { duration: 4, value: 0.2, source: 'fh' }); n++; } if (n) { p.cooldownT = 30; m.perks.proc(p, 'Forced Hesitation'); } } } },
  lightborn: { id: 'lightborn', side: 'killer', cat: 'chase', name: 'Lightborn', icon: '☼', rarity: 'uncommon', flashImmune: true, description: 'You are immune to flashlight blinds.' },
  // ---- tracking ----
  tinkerer: { id: 'tinkerer', side: 'killer', cat: 'tracking', name: 'Tinkerer', icon: '⚒', rarity: 'rare',
    description: 'When a generator reaches 70%, you are alerted and become Undetectable for 16 s.',
    update: (p, m) => { for (const g of m.map.gens) if (!g.done && !g._tink && g.charges >= GC * 0.7) { g._tink = true; m.reveal(g, 4, 'Tinkerer'); undetect(m, 16, 'tinkerer', p.owner); m.notifyK(`Tinkerer: ${g.name} is at 70%`, '#e0b65a'); m.perks.proc(p, 'Tinkerer'); } } },
  bitter_murmur: { id: 'bitter_murmur', side: 'killer', cat: 'tracking', name: 'Bitter Murmur', icon: '☊', rarity: 'common',
    description: 'When a generator is completed, survivors within 16 m of it are revealed for 5 s. The last one reveals everyone.',
    triggers: { [EV.GENERATOR_REPAIRED]: (p, d, m) => { const last = m.gensDone >= m.gensNeeded; for (const s of m.survivors) if (s.standing && (last || dist(s.x, s.y, d.gen.x, d.gen.y) < 16 * M)) m.reveal(s, 5, 'Bitter Murmur'); m.perks.proc(p, 'Bitter Murmur'); } } },
  stridor: { id: 'stridor', side: 'killer', cat: 'tracking', name: 'Stridor', icon: '∰', rarity: 'uncommon', gruntRate: 1.8, description: 'Injured survivors grunt in pain far more often.' },
  predator: { id: 'predator', side: 'killer', cat: 'tracking', name: 'Predator', icon: '⋰', rarity: 'common', scratchLife: 1.5, description: 'Scratch marks last 50% longer.' },
  bloodhound: { id: 'bloodhound', side: 'killer', cat: 'tracking', name: 'Bloodhound', icon: '⁘', rarity: 'common', bloodLife: 2, description: 'Pools of blood last twice as long and are brighter.' },
  shadowborn: { id: 'shadowborn', side: 'killer', cat: 'tracking', name: 'Shadowborn', icon: '◐', rarity: 'common', description: 'Your field of view in the fog is 25% wider.' },
  infectious_fright: { id: 'infectious_fright', side: 'killer', cat: 'tracking', name: 'Infectious Fright', icon: '❢', rarity: 'rare',
    description: 'When you down a survivor, everyone else in your terror radius screams and is revealed for 4 s.',
    triggers: { [EV.SURVIVOR_DOWNED]: (p, d, m) => { let n = 0; for (const s of m.survivors) if (s !== d.survivor && s.standing && inTR(s, m, p.owner)) { m.reveal(s, 4, 'Infectious Fright'); m.addGrunt(s); s.stopAction(m); n++; } if (n) { m.audio?.play('scream'); m.perks.proc(p, 'Infectious Fright'); } } } },
  gearhead: { id: 'gearhead', side: 'killer', cat: 'tracking', name: 'Gearhead', icon: '⚙', rarity: 'uncommon',
    description: 'For 30 s after you hit someone, good skill checks reveal the generator for 4 s.',
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { p.data.until = m.time + 30; p.active = true; }, [EV.SKILL_CHECK_SUCCESS]: (p, d, m) => { if (p.data.until > m.time && !d.great) { m.reveal(d.gen, 4, 'Gearhead'); m.perks.proc(p, 'Gearhead'); } } },
    update: (p, m) => { p.active = p.data.until > m.time; } },
  deathbound: { id: 'deathbound', side: 'killer', cat: 'tracking', name: 'Deathbound', icon: '⚰', rarity: 'uncommon',
    description: 'A survivor who heals someone at least 32 m from you screams, is revealed and becomes Oblivious for 60 s.',
    triggers: { [EV.SURVIVOR_HEALED]: (p, d, m) => { const h = d.healer; if (!h || h === d.survivor || dist(h.x, h.y, p.owner.x, p.owner.y) < 32 * M) return; m.reveal(h, 4, 'Deathbound'); m.addGrunt(h); h.status.add('oblivious', { duration: 60, source: 'deathbound' }); m.perks.proc(p, 'Deathbound'); } } },
  territorial: { id: 'territorial', side: 'killer', cat: 'tracking', name: 'Territorial Imperative', icon: '⌂', rarity: 'uncommon', cooldown: 30,
    description: 'When a survivor comes within 14 m of a hooked survivor while you are 32 m away, they are revealed for 5 s.',
    update: (p, m) => { if (p.cooldownT > 0) return; const k = p.owner; for (const h of m.survivors) { if (h.health !== 'hooked') continue; if (dist(h.x, h.y, k.x, k.y) < 32 * M) continue;
      for (const s of m.survivors) if (s.standing && dist(s.x, s.y, h.x, h.y) < 14 * M) { m.reveal(s, 5, 'Territorial'); p.cooldownT = 30; m.perks.proc(p, 'Territorial Imperative'); return; } } } },
  // ---- hook pressure ----
  make_your_choice: { id: 'make_your_choice', side: 'killer', cat: 'hook', name: 'Make Your Choice', icon: '⚔', rarity: 'rare', cooldown: 60,
    description: 'A survivor who unhooks someone while you are 32 m away becomes Exposed for 60 s.',
    triggers: { [EV.SURVIVOR_UNHOOKED]: (p, d, m) => { if (!d.rescuer || p.cooldownT > 0 || dist(d.survivor.x, d.survivor.y, p.owner.x, p.owner.y) < 32 * M) return; d.rescuer.status.add('exposed', { duration: 60, source: 'myc' }); m.reveal(d.rescuer, 3, 'Make Your Choice'); p.cooldownT = 60; m.perks.proc(p, 'Make Your Choice'); } } },
  // ---- stealth ----
  insidious: { id: 'insidious', side: 'killer', cat: 'stealth', name: 'Insidious', icon: '◌', rarity: 'uncommon',
    description: 'Standing still for 3 s makes you Undetectable until you move.',
    update: (p, m, dt) => { const k = p.owner; if ((k.lastSpeed || 0) < 4) { p.data.t = (p.data.t || 0) + dt; if (p.data.t > 3) undetect(m, 0.25, 'insidious', p.owner); } else p.data.t = 0; } },
  trail_of_torment: { id: 'trail_of_torment', side: 'killer', cat: 'stealth', name: 'Trail of Torment', icon: '⟿', rarity: 'rare', cooldown: 80,
    description: 'After kicking a generator, you are Undetectable until it stops regressing (max 30 s).',
    triggers: { [EV.GENERATOR_DAMAGED]: (p, d, m) => { if (p.cooldownT > 0) return; p.data.gen = d.gen; p.data.until = m.time + 30; p.cooldownT = 80; m.perks.proc(p, 'Trail of Torment'); } },
    update: (p, m) => { const g = p.data.gen; if (!g) return; if (!g.regressing || g.done || m.time > p.data.until) { p.data.gen = null; return; } undetect(m, 0.25, 'tot', p.owner); m.reveal(g, 0.3, 'Trail of Torment', true); } },
  beast_of_prey: { id: 'beast_of_prey', side: 'killer', cat: 'stealth', name: 'Beast of Prey', icon: '♞', rarity: 'uncommon', cooldown: 30,
    description: 'When Bloodlust kicks in, you are Undetectable for 30 s.',
    update: (p, m) => { const b = p.owner.bloodlust; if (b > (p.data.b || 0) && p.cooldownT <= 0) { undetect(m, 30, 'bop', p.owner); p.cooldownT = 30; m.perks.proc(p, 'Beast of Prey'); } p.data.b = b; } },
  // ---- endgame ----
  no_way_out: { id: 'no_way_out', side: 'killer', cat: 'endgame', name: 'No Way Out', icon: '⛌', rarity: 'rare',
    description: 'Hooking each survivor for the first time gives a token. When the gates are powered, they are blocked for 12 s + 6 s per token.',
    triggers: { [EV.SURVIVOR_HOOKED]: (p, d) => { (p.data.seen ||= new Set()).add(d.survivor); p.tokens = p.data.seen.size; },
      [EV.EXIT_GATES_POWERED]: (p, d, m) => { const t = 12 + 6 * p.tokens; for (const g of m.map.gates) g.blockedUntil = m.time + t; m.notifyK(`No Way Out: gates blocked for ${t} s`, '#e05a5a'); m.perks.proc(p, 'No Way Out'); } } },
  remember_me: { id: 'remember_me', side: 'killer', cat: 'endgame', name: 'Remember Me', icon: '☍', rarity: 'uncommon', description: 'Exit gates take 30% longer to open.',
    survivorActionMult: (p, sv, type) => (type === 'openGate' ? 0.77 : 1) },
  blood_warden: { id: 'blood_warden', side: 'killer', cat: 'endgame', name: 'Blood Warden', icon: '⛬', rarity: 'rare', charges: 1,
    description: 'Once an exit gate is open, hooking a survivor seals the exits for 60 s.',
    triggers: { [EV.SURVIVOR_HOOKED]: (p, d, m) => { if (p.charges > 0 && m.map.gates.some((g) => g.open)) { p.charges--; m.gatesSealedUntil = m.time + 60; m.notifyK('Blood Warden: exits sealed for 60 s', '#e05a5a'); m.perks.proc(p, 'Blood Warden'); } } } },
  rancor: { id: 'rancor', side: 'killer', cat: 'endgame', name: 'Rancor', icon: '☄', rarity: 'rare',
    description: 'Each completed generator reveals every survivor for 3 s. When the gates are powered, the healthiest survivor is Exposed for 60 s.',
    triggers: { [EV.GENERATOR_REPAIRED]: (p, d, m) => { for (const s of m.survivors) if (s.standing) m.reveal(s, 3, 'Rancor'); },
      [EV.EXIT_GATES_POWERED]: (p, d, m) => { const s = m.survivors.filter((q) => q.standing).sort((a, b) => (a.health === 'healthy' ? -1 : 1) - (b.health === 'healthy' ? -1 : 1))[0]; if (s) { s.status.add('exposed', { duration: 60, source: 'rancor' }); m.reveal(s, 5, 'Rancor'); m.perks.proc(p, 'Rancor'); } } } },
});
// ---------------------------------------------------------------------------------
// Roster expansion: signature perks of the ten newer killers plus power-synergy, status,
// healing, exhaustion and map-control perks. They react to POWER_USED, hits, hooks etc.,
// so they work for any killer — none assumes a basic-attack-only playstyle.
const TRAVEL = new Set(['teleport', 'gloaming', 'remnant', 'fly', 'search', 'bound', 'guard']);
const gaObs = (p) => { let best = null, bt = 0; for (const [s, t] of p.data.t || []) if (s.standing && t > bt) { bt = t; best = s; } return best; };
const reveal = (m, s, t, label) => { if (s && s.alive && s.standing !== false) m.reveal(s, t, label); };
Object.assign(KILLER_PERKS, {
  // --- Legion ---
  mad_grit: { id: 'mad_grit', side: 'killer', cat: 'hook', name: 'Mad Grit', icon: '⚔', rarity: 'rare',
    description: 'While carrying a survivor, missed attacks have no cooldown and hitting a rescuer pauses the wiggle for 4 s.',
    killerCooldownMult: (p, kind, m) => (p.owner.carrying && kind === 'miss' ? 0.05 : 1),
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { const c = p.owner.carrying; if (c) { c.wiggle = Math.max(0, c.wiggle - 4 / 16); m.perks.proc(p, 'Mad Grit'); } } } },
  iron_maiden: { id: 'iron_maiden', side: 'killer', cat: 'tracking', name: 'Iron Maiden', icon: '⛓', rarity: 'uncommon',
    description: 'Survivors who finish using a chest, locker, supply case, printer or the Lament Configuration scream, are revealed for 4 s and Exposed for 15 s.',
    triggers: { [EV.CHEST_OPENED]: (p, d, m) => ironMaiden(p, d.survivor, m), deviceUsed: (p, d, m) => ironMaiden(p, d.survivor, m) } },
  // --- Hag ---
  third_seal: { id: 'third_seal', side: 'killer', cat: 'tracking', name: 'Hex: The Third Seal', icon: '⛬', rarity: 'rare', hex: true,
    description: 'While the hex stands, the last 4 survivors you hit are Blinded: they lose every aura and teammate callout.',
    init: (p, m) => { p.data.totem = m.lightHex('third_seal'); },
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { const t = p.data.totem; if (!t || t.cleansed) return; (p.data.list ||= []).push(d.survivor); if (p.data.list.length > 4) p.data.list.shift(); m.perks.proc(p, 'Third Seal'); } },
    update: (p, m) => { const t = p.data.totem; p.active = !!t && !t.cleansed && t.hex === 'third_seal'; if (p.active) for (const s of p.data.list || []) if (s.alive) s.status.add('blindness', { duration: 0.5, source: 'third_seal' }); } },
  devour_hope: { id: 'devour_hope', side: 'killer', cat: 'hook', name: 'Hex: Devour Hope', icon: '☽', rarity: 'very rare', hex: true,
    description: 'Each unhook while you are 24 m away gives a token. 2 tokens: 5% haste after hooks. 3 tokens: everyone is Exposed. 5 tokens: your next hook kills.',
    init: (p, m) => { p.data.totem = m.lightHex('devour_hope'); },
    triggers: { [EV.SURVIVOR_UNHOOKED]: (p, d, m) => { const t = p.data.totem; if (!t || t.cleansed || !d.rescuer) return; if (dist(p.owner.x, p.owner.y, d.survivor.x, d.survivor.y) > 24 * M) { p.tokens = Math.min(5, p.tokens + 1); m.perks.proc(p, `Devour Hope ${p.tokens}`); if (p.tokens >= 2) p.owner.status.add('haste', { duration: 10, value: 0.05, source: 'devour' }); } },
      [EV.SURVIVOR_HOOKED]: (p, d, m) => { const t = p.data.totem; if (t && !t.cleansed && p.tokens >= 5 && d.survivor.health === 'hooked') { m.killSurvivor(d.survivor, 'hook'); m.notifyK('Devour Hope: the Entity takes them', '#e05a5a'); } } },
    update: (p, m) => { const t = p.data.totem; p.active = !!t && !t.cleansed && t.hex === 'devour_hope'; if (p.active && p.tokens >= 3) for (const s of m.survivors) if (s.standing) s.status.add('exposed', { duration: 0.5, source: 'devour' }); } },
  // --- Cannibal ---
  knock_out: { id: 'knock_out', side: 'killer', cat: 'hook', name: 'Knock Out', icon: '✊', rarity: 'uncommon',
    description: 'A survivor you down with a basic attack is hidden for 15 s: teammates farther than 16 m cannot locate them and they cannot be heard.',
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { if (!d.power && d.result === 'downed') { d.survivor.knockOutUntil = m.time + 15; m.perks.proc(p, 'Knock Out'); } } } },
  franklins_demise: { id: 'franklins_demise', side: 'killer', cat: 'chase', name: "Franklin's Demise", icon: '⚒', rarity: 'common',
    description: 'Basic attacks knock the item out of a survivor\'s hands; it loses all charges.',
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { const s = d.survivor; if (!d.power && s.item && s.item.charges > 0) { s.ai?.note(`Lost the ${s.item.name}`, 'franklin'); s.item.charges = 0; m.perks.proc(p, "Franklin's Demise"); } } } },
  // --- Deathslinger ---
  dead_mans_switch: { id: 'dead_mans_switch', side: 'killer', cat: 'gen', name: "Dead Man's Switch", icon: '⏚', rarity: 'rare',
    description: 'For 30 s after you hook someone, a survivor who stops repairing a generator blocks it for 15 s.',
    triggers: { [EV.SURVIVOR_HOOKED]: (p, d, m) => { p.data.until = m.time + 30; }, survivorActionStop: (p, d, m) => { if (d.type === 'repair' && !d.target.done && (p.data.until || 0) > m.time) { d.target.blockedUntil = Math.max(d.target.blockedUntil, m.time + 15); m.reveal(d.target, 15, "Dead Man's Switch", true); m.perks.proc(p, "Dead Man's Switch"); } } },
    update: (p, m) => { p.active = (p.data.until || 0) > m.time; } },
  retribution: { id: 'retribution', side: 'killer', cat: 'tracking', name: 'Retribution', icon: '⚡', rarity: 'uncommon',
    description: 'A survivor who cleanses a totem is Oblivious for 35 s. Cleansing a hex reveals everyone for 10 s.',
    triggers: { [EV.TOTEM_CLEANSED]: (p, d, m) => { d.survivor.status.add('oblivious', { duration: 35, source: 'retribution' }); if (d.hex) for (const s of m.survivors) reveal(m, s, 10, 'Retribution'); m.perks.proc(p, 'Retribution'); } } },
  // --- Dredge ---
  dissolution: { id: 'dissolution', side: 'killer', cat: 'chase', name: 'Dissolution', icon: '≋', rarity: 'rare', cooldown: 40,
    description: 'For 15 s after you injure a survivor, a pallet they drop near you breaks instantly.',
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { if (d.result === 'injured') { d.survivor._dissolveUntil = m.time + 15; } },
      [EV.PALLET_DROPPED]: (p, d, m) => { const s = d.survivor; if (p.cooldownT > 0 || !s || (s._dissolveUntil || 0) < m.time || dist(s.x, s.y, p.owner.x, p.owner.y) > 12 * M) return; p.cooldownT = 40; p.owner.stunT = Math.min(p.owner.stunT, 0.2); m.breakPallet(d.pallet); m.fx.text(d.pallet.x, d.pallet.y - 30, 'DISSOLUTION', '#e05a5a'); m.perks.proc(p, 'Dissolution'); } } },
  darkness_revealed: { id: 'darkness_revealed', side: 'killer', cat: 'tracking', name: 'Darkness Revealed', icon: '◉', rarity: 'uncommon', cooldown: 30,
    description: 'When you travel with your power (teleport, locker, flight, bound…), survivors within 8 m of any locker, case or printer are revealed for 5 s.',
    triggers: { [EV.POWER_USED]: (p, d, m) => { if (p.cooldownT > 0 || !TRAVEL.has(d.power)) return; const devs = m.devices; let n = 0;
      for (const s of m.survivors) if (s.standing && devs.some((q) => dist(q.x, q.y, s.x, s.y) < 8 * M)) { reveal(m, s, 5, 'Darkness Revealed'); n++; } if (n) { p.cooldownT = 30; m.perks.proc(p, 'Darkness Revealed'); } } } },
  septic_touch: { id: 'septic_touch', side: 'killer', cat: 'healing', name: 'Septic Touch', icon: '☣', rarity: 'uncommon',
    description: 'Survivors healing inside your terror radius are Blinded and Exhausted.',
    update: (p, m) => { for (const s of m.survivors) { const a = s.action?.type; if ((a === 'heal' || a === 'healSelf' || a === 'mend') && inTR(s, m, p.owner)) { s.status.add('blindness', { duration: 6, source: 'septic' }); s.status.add('exhausted', { duration: 6, source: 'septic' }); } } } },
  // --- Knight ---
  nowhere_to_hide: { id: 'nowhere_to_hide', side: 'killer', cat: 'tracking', name: 'Nowhere to Hide', icon: '⚑', rarity: 'rare',
    description: 'Kicking a generator (or having a guard damage one) reveals survivors within 24 m of it for 5 s.',
    triggers: { [EV.GENERATOR_DAMAGED]: (p, d, m) => { let n = 0; for (const s of m.survivors) if (s.standing && dist(s.x, s.y, d.gen.x, d.gen.y) < 24 * M) { reveal(m, s, 5, 'Nowhere to Hide'); n++; } if (n) m.perks.proc(p, 'Nowhere to Hide'); } } },
  face_the_darkness: { id: 'face_the_darkness', side: 'killer', cat: 'tracking', name: 'Face the Darkness', icon: '☾', rarity: 'uncommon', cooldown: 40,
    description: 'After you injure someone, every survivor not in a chase screams and is revealed for 2 s.',
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { if (p.cooldownT > 0 || d.result !== 'injured') return; p.cooldownT = 40; for (const s of m.survivors) if (s !== d.survivor && s.standing && m.chaseTarget !== s) { reveal(m, s, 2, 'Face the Darkness'); m.addGrunt(s); } m.audio?.play('scream'); m.perks.proc(p, 'Face the Darkness'); } } },
  hubris: { id: 'hubris', side: 'killer', cat: 'chase', name: 'Hubris', icon: '♛', rarity: 'rare', cooldown: 30,
    description: 'The survivor who stuns you with a pallet becomes Exposed for 20 s.',
    triggers: { [EV.KILLER_STUNNED]: (p, d, m) => { if (d.kind !== 'pallet' || p.cooldownT > 0 || !m.lastStunner) return; m.lastStunner.status.add('exposed', { duration: 20, source: 'hubris' }); reveal(m, m.lastStunner, 3, 'Hubris'); p.cooldownT = 30; m.perks.proc(p, 'Hubris'); } } },
  // --- Cenobite ---
  plaything: { id: 'plaything', side: 'killer', cat: 'hook', name: 'Plaything', icon: '▣', rarity: 'rare',
    description: 'A survivor hooked for the first time is Oblivious (no heartbeat, no warnings) for 90 s.',
    triggers: { [EV.SURVIVOR_HOOKED]: (p, d, m) => { if (d.survivor.hookCount === 1) { d.survivor.status.add('oblivious', { duration: 90 + 60, source: 'plaything' }); m.perks.proc(p, 'Plaything'); } } } },
  gift_of_pain: { id: 'gift_of_pain', side: 'killer', cat: 'healing', name: 'Gift of Pain', icon: '✚', rarity: 'uncommon',
    description: 'A survivor hooked for the first time is Mangled until healed; when healed they repair and heal 16% slower for 60 s.',
    triggers: { [EV.SURVIVOR_HOOKED]: (p, d) => { if (d.survivor.hookCount === 1) d.survivor._gop = true; },
      [EV.SURVIVOR_UNHOOKED]: (p, d) => { if (d.survivor._gop) d.survivor.status.add('mangled', { duration: 999, source: 'gop' }); },
      [EV.SURVIVOR_HEALED]: (p, d, m) => { const s = d.survivor; if (s._gop && s.health === 'healthy') { s._gop = false; s._gopUntil = m.time + 60; m.perks.proc(p, 'Gift of Pain'); } } },
    survivorActionMult: (p, sv, type, m) => ((sv._gopUntil || 0) > m.time && ['repair', 'heal', 'healSelf'].includes(type) ? 0.84 : 1) },
  // --- Mastermind ---
  superior_anatomy: { id: 'superior_anatomy', side: 'killer', cat: 'chase', name: 'Superior Anatomy', icon: '⤴', rarity: 'uncommon', cooldown: 30,
    description: 'When a survivor fast-vaults within 8 m of you, your next vault within 4 s is 40% faster.',
    triggers: { [EV.VAULT_PERFORMED]: (p, d, m) => { if (d.who === p.owner) return; if (p.cooldownT <= 0 && dist(d.who.x, d.who.y, p.owner.x, p.owner.y) < 8 * M) { p.data.until = m.time + 4; p.cooldownT = 30; m.perks.proc(p, 'Superior Anatomy'); } } },
    killerActionMult: (p, type, m) => (type === 'vault' && (p.data.until || 0) > m.time ? 0.6 : 1) },
  awakened_awareness: { id: 'awakened_awareness', side: 'killer', cat: 'hook', name: 'Awakened Awareness', icon: '◎', rarity: 'uncommon',
    description: 'While carrying a survivor, you see survivors within 16 m.',
    update: (p, m) => { if (!p.owner.carrying) return; for (const s of m.survivors) if (s.standing && dist(s.x, s.y, p.owner.x, p.owner.y) < 16 * M) m.reveal(s, 0.3, 'Awakened Awareness', true); } },
  terminus: { id: 'terminus', side: 'killer', cat: 'endgame', name: 'Terminus', icon: '⛌', rarity: 'rare',
    description: 'Once the gates are powered, injured, dying or hooked survivors are Broken and Hindered by 2% until a gate opens (and 20 s after).',
    update: (p, m) => { if (!m.gatesPowered) return; const open = m.map.gates.some((g) => g.open); if (open && !p.data.openT) p.data.openT = m.time; if (p.data.openT && m.time - p.data.openT > 20) return;
      p.active = true; for (const s of m.survivors) if (['injured', 'dying', 'hooked'].includes(s.health)) { s.status.add('broken', { duration: 0.5, source: 'terminus' }); s.status.add('hindered', { duration: 0.5, value: 0.02, source: 'terminus' }); } } },
  // --- Houndmaster ---
  all_shaking_thunder: { id: 'all_shaking_thunder', side: 'killer', cat: 'chase', name: 'All-Shaking Thunder', icon: '⚡', rarity: 'uncommon', cooldown: 10,
    description: 'After you vault a window or a dropped pallet, your next lunge within 4 s reaches 30% further.',
    triggers: { [EV.VAULT_PERFORMED]: (p, d, m) => { if (d.who === p.owner && p.cooldownT <= 0) { p.data.until = m.time + 4; p.cooldownT = 10; m.perks.proc(p, 'All-Shaking Thunder'); } } },
    lungeMult: (p, m) => ((p.data.until || 0) > m.time ? 1.3 : 1) },
  no_quarter: { id: 'no_quarter', side: 'killer', cat: 'healing', name: 'No Quarter', icon: '✕', rarity: 'rare',
    description: 'Survivors who recover from the dying state on their own, or are picked up within 10 s of going down, become Exposed for 20 s.',
    triggers: { [EV.SURVIVOR_DOWNED]: (p, d, m) => { d.survivor._downAt = m.time; }, [EV.SURVIVOR_HEALED]: (p, d, m) => { const s = d.survivor; if (s.health === 'injured' && (d.healer === s || m.time - (s._downAt || -99) < 10)) { s.status.add('exposed', { duration: 20, source: 'nq' }); m.perks.proc(p, 'No Quarter'); } } } },
  jagged_compass: { id: 'jagged_compass', side: 'killer', cat: 'tracking', name: 'Jagged Compass', icon: '✣', rarity: 'uncommon', cooldown: 45,
    description: 'When a generator is completed, you sense the direction of the survivor closest to it for 6 s.',
    triggers: { [EV.GENERATOR_REPAIRED]: (p, d, m) => { if (p.cooldownT > 0) return; const s = m.survivors.filter((q) => q.standing).sort((a, b) => dist(a.x, a.y, d.gen.x, d.gen.y) - dist(b.x, b.y, d.gen.x, d.gen.y))[0]; if (s) { reveal(m, s, 6, 'Jagged Compass'); p.cooldownT = 45; m.perks.proc(p, 'Jagged Compass'); } } } },
  // --- Lich ---
  dark_arrogance: { id: 'dark_arrogance', side: 'killer', cat: 'chase', name: 'Dark Arrogance', icon: '♚', rarity: 'rare',
    description: 'Vault 20% faster, but stuns and blinds last 25% longer.', killerActionMult: (p, type) => (type === 'vault' ? 0.8 : 1), stunMult: 1.25 },
  languid_touch: { id: 'languid_touch', side: 'killer', cat: 'status', name: 'Languid Touch', icon: '⧖', rarity: 'uncommon', cooldown: 30,
    description: 'When a survivor triggers one of your power\'s traps, devices or creatures, survivors within 20 m become Exhausted for 10 s.',
    triggers: { powerTriggered: (p, d, m) => { if (p.cooldownT > 0 || !d.x) return; let n = 0; for (const s of m.survivors) if (s.standing && dist(s.x, s.y, d.x, d.y) < 20 * M) { s.status.add('exhausted', { duration: 10, source: 'languid' }); n++; } if (n) { p.cooldownT = 30; m.perks.proc(p, 'Languid Touch'); } } } },
  weave_attunement: { id: 'weave_attunement', side: 'killer', cat: 'tracking', name: 'Weave Attunement', icon: '✧', rarity: 'uncommon',
    description: 'Hitting a survivor makes them drop their item\'s magic: you see survivors who carry items within 16 m of where a hit landed for 4 s.',
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { let n = 0; for (const s of m.survivors) if (s !== d.survivor && s.standing && s.item && dist(s.x, s.y, d.survivor.x, d.survivor.y) < 16 * M) { reveal(m, s, 4, 'Weave Attunement'); n++; } if (n) m.perks.proc(p, 'Weave Attunement'); } } },
  // --- Skull Merchant ---
  game_afoot: { id: 'game_afoot', side: 'killer', cat: 'chase', name: 'Game Afoot', icon: '♞', rarity: 'uncommon',
    description: 'The survivor you have chased the longest is your obsession: breaking a pallet or damaging a generator while chasing them gives 5% Haste for 8 s.',
    update: (p, m, dt) => { const k = p.owner; if (k.chaseTarget) { p.data.t = p.data.t || new Map(); p.data.t.set(k.chaseTarget, (p.data.t.get(k.chaseTarget) || 0) + dt); } },
    triggers: { [EV.PALLET_DESTROYED]: (p, d, m) => { const k = p.owner, ob = gaObs(p); if (k.chaseTarget && k.chaseTarget === ob) { k.status.add('haste', { duration: 8, value: 0.05, source: 'game_afoot' }); m.perks.proc(p, 'Game Afoot'); } },
      [EV.GENERATOR_DAMAGED]: (p, d, m) => { const k = p.owner, ob = gaObs(p); if (ob && dist(ob.x, ob.y, k.x, k.y) < 24 * M) { k.status.add('haste', { duration: 8, value: 0.05, source: 'game_afoot' }); m.perks.proc(p, 'Game Afoot'); } } } },
  leverage: { id: 'leverage', side: 'killer', cat: 'healing', name: 'Leverage', icon: '⚖', rarity: 'uncommon',
    description: 'After each hook, all survivors heal 6% slower (up to 30%) for 30 s.',
    triggers: { [EV.SURVIVOR_HOOKED]: (p, d, m) => { p.data.stacks = Math.min(5, (p.data.stacks || 0) + 1); p.data.until = m.time + 30; m.perks.proc(p, 'Leverage'); } },
    update: (p, m) => { p.active = (p.data.until || 0) > m.time; if (!p.active) return; for (const s of m.survivors) if (s.action?.type === 'heal' || s.action?.type === 'healSelf') s.status.add('actionSpeed', { duration: 0.2, value: -0.06 * p.data.stacks, source: 'leverage' }); } },
  thwack: { id: 'thwack', side: 'killer', cat: 'tracking', name: 'THWACK!', icon: '✹', rarity: 'uncommon', tokens: 0,
    description: 'Breaking a pallet or wall makes survivors within 16 m scream and reveals them for 4 s (once per pallet break, 30 s cooldown).', cooldown: 30,
    triggers: { [EV.PALLET_DESTROYED]: (p, d, m) => { if (p.cooldownT > 0 || !d.pallet) return; let n = 0; for (const s of m.survivors) if (s.standing && dist(s.x, s.y, d.pallet.x, d.pallet.y) < 16 * M) { reveal(m, s, 4, 'THWACK!'); m.addGrunt(s); n++; } if (n) { p.cooldownT = 30; m.perks.proc(p, 'THWACK!'); } } } },
  // --- Blight ---
  dragons_grip: { id: 'dragons_grip', side: 'killer', cat: 'gen', name: "Dragon's Grip", icon: '🐉', rarity: 'rare', cooldown: 60,
    description: 'After you damage a generator, the next survivor to touch it within 30 s screams, is revealed for 4 s and becomes Exposed for 60 s.',
    triggers: { [EV.GENERATOR_DAMAGED]: (p, d, m) => { if (p.cooldownT > 0) return; p.data.gen = d.gen; p.data.until = m.time + 30; p.active = true; },
      [EV.GENERATOR_START]: (p, d, m) => { if (p.data.gen === d.gen && (p.data.until || 0) > m.time) { const s = d.survivor; reveal(m, s, 4, "Dragon's Grip"); m.addGrunt(s); s.status.add('exposed', { duration: 60, source: 'dragons_grip' }); p.data.gen = null; p.active = false; p.cooldownT = 60; m.perks.proc(p, "Dragon's Grip"); } } } },
  blood_favour: { id: 'blood_favour', side: 'killer', cat: 'chase', name: 'Hex: Blood Favour', icon: '♅', rarity: 'rare', hex: true, cooldown: 15,
    description: 'While the hex stands, hitting a survivor blocks every pallet within 16 m of them for 15 s.',
    init: (p, m) => { p.data.totem = m.lightHex('blood_favour'); },
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { const t = p.data.totem; if (!t || t.cleansed || t.hex !== 'blood_favour' || p.cooldownT > 0) return; let n = 0; for (const q of m.map.pallets) if (q.state === 'up' && dist(q.x, q.y, d.survivor.x, d.survivor.y) < 16 * M) { q.blockedUntil = m.time + 15; n++; } if (n) { p.cooldownT = 15; m.perks.proc(p, 'Hex: Blood Favour'); } } },
    update: (p) => { const t = p.data.totem; p.active = !!(t && !t.cleansed && t.hex === 'blood_favour'); } },
  undying: { id: 'undying', side: 'killer', cat: 'tracking', name: 'Hex: Undying', icon: '♾', rarity: 'very rare', hex: true,
    description: 'Survivors within 2 m of a dull totem are revealed. When another hex is cleansed, Undying takes its place once.',
    init: (p, m) => { p.data.totem = m.lightHex('undying'); },
    triggers: { [EV.TOTEM_CLEANSED]: (p, d, m) => { const t = p.data.totem; if (!t || t.cleansed || t.hex !== 'undying' || !d.hex || d.hex === 'undying') return;
      const owner = p.owner.perks.find((q) => q.id === d.hex); t.hex = d.hex; if (owner) owner.data.totem = t; p.data.totem = null; p.active = false; m.perks.proc(p, `Hex: Undying → ${d.hex}`); } },
    update: (p, m) => { const t = p.data.totem; p.active = !!(t && !t.cleansed && t.hex === 'undying'); if (!p.active) return; for (const tt of m.map.totems) if (!tt.hex && !tt.cleansed) for (const s of m.survivors) if (s.standing && dist(s.x, s.y, tt.x, tt.y) < 2 * M) reveal(m, s, 0.3, 'Hex: Undying'); } },
  // --- Unknown ---
  unforeseen: { id: 'unforeseen', side: 'killer', cat: 'stealth', name: 'Unforeseen', icon: '◍', rarity: 'uncommon', cooldown: 20,
    description: 'When a generator regresses after your kick, your terror radius moves to that generator and you are Undetectable for 10 s.',
    triggers: { [EV.GENERATOR_DAMAGED]: (p, d, m) => { if (p.cooldownT > 0 || d.killer !== p.owner) return; undetect(m, 10, 'unforeseen', p.owner); p.cooldownT = 20; m.perks.proc(p, 'Unforeseen'); } } },
  undone: { id: 'undone', side: 'killer', cat: 'gen', name: 'Undone', icon: '⊘', rarity: 'rare', tokens: 0,
    description: 'Each failed skill check on a generator gives a token (max 8). Kicking a generator spends them: −1% progress per token and it starts regressing.',
    triggers: { [EV.SKILL_CHECK_FAILURE]: (p, d, m) => { if (d.gen) p.tokens = Math.min(8, (p.tokens || 0) + 1); },
      [EV.GENERATOR_DAMAGED]: (p, d, m) => { if (d.killer !== p.owner || !p.tokens) return; d.gen.charges = Math.max(0, d.gen.charges - GC * 0.01 * p.tokens); p.tokens = 0; m.perks.proc(p, 'Undone'); } } },
  unbound: { id: 'unbound', side: 'killer', cat: 'chase', name: 'Unbound', icon: '⇶', rarity: 'uncommon', cooldown: 25,
    description: 'Vaulting a window during a chase gives you 7% Haste for 10 s.',
    triggers: { [EV.VAULT_PERFORMED]: (p, d, m) => { if (d.who !== p.owner || !p.owner.chaseTarget || p.cooldownT > 0) return; p.owner.status.add('haste', { duration: 10, value: 0.07, source: 'unbound' }); p.cooldownT = 25; m.perks.proc(p, 'Unbound'); } } },
  // --- Ghoul ---
  forever_entwined: { id: 'forever_entwined', side: 'killer', cat: 'chase', name: 'Forever Entwined', icon: '∞', rarity: 'uncommon', tokens: 0,
    description: 'Each hook gives a token (max 5): +2% vault and pallet-break speed per token.',
    triggers: { [EV.SURVIVOR_HOOKED]: (p, d, m) => { if (d.killer && d.killer !== p.owner) return; p.tokens = Math.min(5, (p.tokens || 0) + 1); m.perks.proc(p, 'Forever Entwined'); } },
    killerActionMult: (p, type) => (type === 'vault' || type === 'break' ? 1 - 0.02 * (p.tokens || 0) : 1) },
  none_are_free: { id: 'none_are_free', side: 'killer', cat: 'hook', name: 'None Are Free', icon: '⛓', rarity: 'rare', tokens: 0,
    description: 'Each unhook gives a token (max 3). When a survivor is unhooked, survivors who repair a generator within 32 m of the hook have their auras revealed for 2 s per token.',
    triggers: { [EV.SURVIVOR_UNHOOKED]: (p, d, m) => { p.tokens = Math.min(3, (p.tokens || 0) + 1); let n = 0; for (const s of m.survivors) if (s.action?.type === 'repair' && dist(s.x, s.y, d.survivor.x, d.survivor.y) < 32 * M) { reveal(m, s, 2 * p.tokens, 'None Are Free'); n++; } if (n) m.perks.proc(p, 'None Are Free'); } } },
  haywire: { id: 'haywire', side: 'killer', cat: 'gen', name: 'Haywire', icon: '⚡', rarity: 'uncommon', cooldown: 40,
    description: 'Hitting a survivor makes the generator with the most progress lose 8% and regress.',
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { if (p.cooldownT > 0 || (d.killer && d.killer !== p.owner)) return; const g = m.map.gens.filter((q) => !q.done).sort((a, b) => b.charges - a.charges)[0]; if (!g) return; g.charges = Math.max(0, g.charges - GC * 0.08); g.regressing = true; p.cooldownT = 40; m.fx.sparks(g.x, g.y); m.perks.proc(p, 'Haywire'); } } },
  // --- general: power synergy / map control / status ---
  hex_pentimento: { id: 'hex_pentimento', side: 'killer', cat: 'gen', name: 'Hex: Crowd Control', icon: '⊞', rarity: 'uncommon', hex: true,
    description: 'While the hex stands, windows a survivor fast-vaults are blocked for 12 s.',
    init: (p, m) => { p.data.totem = m.lightHex('crowd'); },
    triggers: { [EV.VAULT_PERFORMED]: (p, d, m) => { const t = p.data.totem; if (!t || t.cleansed || d.who === p.owner || d.obj.kind !== 'window') return; d.obj.blockedUntil = Math.max(d.obj.blockedUntil, m.time + 12); m.perks.proc(p, 'Crowd Control'); } } },
  surveillance: { id: 'surveillance', side: 'killer', cat: 'map', name: 'Surveillance', icon: '⌖', rarity: 'uncommon',
    description: 'Regressing generators are highlighted; when one starts being repaired again it flashes for 3 s.',
    update: (p, m) => { for (const g of m.map.gens) { if (g.done) continue; if (g.regressing) g._surv = true; else if (g._surv && g.repairers.size) { g._surv = false; m.reveal(g, 3, 'Surveillance'); m.perks.proc(p, 'Surveillance'); } } } },
  scourge_floods: { id: 'scourge_floods', side: 'killer', cat: 'gen', name: 'Scourge: Floods of Rage', icon: '♒', rarity: 'rare',
    description: 'When you hook a survivor, everyone else is revealed for 5 s.',
    triggers: { [EV.SURVIVOR_HOOKED]: (p, d, m) => { for (const s of m.survivors) if (s !== d.survivor) reveal(m, s, 5, 'Floods of Rage'); m.perks.proc(p, 'Floods of Rage'); } } },
  call_of_brine: { id: 'call_of_brine', side: 'killer', cat: 'gen', name: 'Call of Brine', icon: '≈', rarity: 'uncommon',
    description: 'A generator you kick regresses 75% faster for 60 s and flashes whenever a skill check succeeds on it.',
    triggers: { [EV.GENERATOR_DAMAGED]: (p, d, m) => { d.gen._brine = m.time + 60; }, [EV.SKILL_CHECK_SUCCESS]: (p, d, m) => { if ((d.gen._brine || 0) > m.time) m.reveal(d.gen, 2, 'Call of Brine', true); } },
    update: (p, m, dt) => { for (const g of m.map.gens) if (!g.done && g.regressing && (g._brine || 0) > m.time) g.charges = Math.max(0, g.charges - dt * 0.25 * 0.75); } },
  lethal_power: { id: 'lethal_power', side: 'killer', cat: 'power', name: 'Hysteria', icon: '✶', rarity: 'uncommon', cooldown: 30,
    description: 'When your power injures a healthy survivor, every other injured survivor becomes Oblivious for 30 s.',
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { if (!d.power || d.result !== 'injured' || p.cooldownT > 0) return; for (const s of m.survivors) if (s !== d.survivor && s.health === 'injured') s.status.add('oblivious', { duration: 30, source: 'hysteria' }); p.cooldownT = 30; m.perks.proc(p, 'Hysteria'); } } },
  power_surge: { id: 'power_surge', side: 'killer', cat: 'power', name: 'Unbound Instinct', icon: '⇶', rarity: 'rare', cooldown: 20,
    description: 'Each time your power hits a survivor you gain 7% haste for 5 s.',
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { if (d.power && p.cooldownT <= 0) { p.owner.status.add('haste', { duration: 5, value: 0.07, source: 'unbound' }); p.cooldownT = 20; m.perks.proc(p, 'Unbound Instinct'); } } } },
  trickster_toll: { id: 'trickster_toll', side: 'killer', cat: 'power', name: 'Toll of the Trap', icon: '⚙', rarity: 'uncommon',
    description: 'Survivors caught by your traps, guards, chains, dog or harpoon scream and are Hindered by 5% for 6 s.',
    triggers: { powerTriggered: (p, d, m) => { const s = d.survivor; if (!s) return; s.status.add('hindered', { duration: 6, value: 0.05, source: 'toll' }); m.addGrunt(s); m.perks.proc(p, 'Toll of the Trap'); } } },
  claustrophobia: { id: 'claustrophobia', side: 'killer', cat: 'status', name: 'Claustrophobia', icon: '▤', rarity: 'uncommon',
    description: 'Exhaustion perks recover 30% slower while survivors are inside your terror radius.',
    update: (p, m, dt) => { for (const s of m.survivors) { const e = s.status.get('exhausted'); if (e && inTR(s, m, p.owner)) e.expiration += dt * 0.3; } } },
});
function ironMaiden(p, s, m) { if (!s || !s.alive) return; reveal(m, s, 4, 'Iron Maiden'); s.status.add('exposed', { duration: 15, source: 'iron_maiden' }); m.addGrunt(s); m.perks.proc(p, 'Iron Maiden'); }
const KCAT = { bbq: 'tracking', ruin: 'gen', pop: 'gen', sloppy: 'chase', discordance: 'tracking', brutal_strength: 'chase', enduring: 'chase', bamboozle: 'chase', nurses_calling: 'tracking', lethal_pursuer: 'tracking',
  corrupt: 'gen', noed: 'endgame', iron_grasp: 'hook', agitation: 'hook', thanatophobia: 'gen', whispers: 'tracking' };
for (const [k, c] of Object.entries(KCAT)) KILLER_PERKS[k].cat = c;
KILLER_PERKS.iron_grasp.wiggleMult = () => 1.25;
KILLER_PERKS.brutal_strength.killerActionMult = (p, type) => (type === 'kick' || type === 'break' ? 0.8 : 1);
KILLER_PERKS.bamboozle.killerActionMult = (p, type) => (type === 'vault' ? 0.85 : 1);
KILLER_PERKS.whispers.terrorMult = undefined;
const KILLER_PERK_CATEGORIES = { gen: 'Generator pressure', chase: 'Chase', tracking: 'Tracking', hook: 'Hook pressure', stealth: 'Stealth', endgame: 'Endgame', healing: 'Healing denial', status: 'Status effects', power: 'Power synergy', map: 'Map control' };
const KILLER_PERK_IDS = Object.keys(KILLER_PERKS);

return { KILLER_PERKS, KILLER_PERK_CATEGORIES, KILLER_PERK_IDS };
};
__defs["js/perks/survivorPerks.js"] = function(){
// Survivor perk pool. Survivors get random loadouts from this pool.
// AI-facing flags (ai: {...}) tell the bots how to value / use each perk.
const { EV } = __req("js/core/events.js");
const { M, dist } = __req("js/core/util.js");

const exhaust = (sv, t = 40) => sv.status.add('exhausted', { duration: t, source: 'perk' });
const notExhausted = (sv) => !sv.status.has('exhausted');

const SURVIVOR_PERKS = {
  sprint_burst: { id: 'sprint_burst', side: 'survivor', name: 'Sprint Burst', icon: '»', rarity: 'rare', tier: 'strong',
    description: 'Breaking into a run grants 150% speed for 3 s. Causes Exhausted for 40 s.',
    onStartRun: (p, sv, m) => { if (notExhausted(sv) && (sv.isPlayer || sv.ai?.wantsExhaustionPerk(p))) { sv.status.add('haste', { duration: 3, value: 0.5, source: 'sprint_burst' }); exhaust(sv); m.perks.proc(p, 'Sprint Burst'); } } },
  lithe: { id: 'lithe', side: 'survivor', name: 'Lithe', icon: '↯', rarity: 'uncommon', tier: 'strong',
    description: 'After a fast vault, gain 150% speed for 3 s. Causes Exhausted for 40 s.',
    onFastVault: (p, sv, m) => { if (notExhausted(sv) && (sv.isPlayer || sv.ai?.inChase)) { sv.status.add('haste', { duration: 3, value: 0.5, source: 'lithe' }); exhaust(sv); m.perks.proc(p, 'Lithe'); } } },
  dead_hard: { id: 'dead_hard', side: 'survivor', name: 'Dead Hard', icon: '⛨', rarity: 'rare', tier: 'strong',
    description: 'While injured, press to dodge: 0.5 s of invulnerability to one hit. Causes Exhausted for 40 s.',
    activate: (p, sv, m) => { if (!notExhausted(sv) || sv.health !== 'injured') return false; sv.dodgeT = m.time + 0.5; exhaust(sv); m.perks.proc(p, 'Dead Hard'); return true; } },
  adrenaline: { id: 'adrenaline', side: 'survivor', name: 'Adrenaline', icon: '⚡', rarity: 'very rare', tier: 'strong',
    description: 'When the exit gates are powered, heal one health state and gain 150% speed for 5 s.',
    triggers: { [EV.EXIT_GATES_POWERED]: (p, d, m) => { const sv = p.owner; if (!sv.standing && sv.health !== 'dying') return;
      if (sv.health === 'injured') { sv.health = 'healthy'; sv.status.remove('deepWound'); } else if (sv.health === 'dying') sv.health = 'injured';
      sv.status.add('haste', { duration: 5, value: 0.5, source: 'adrenaline' }); m.perks.proc(p, 'Adrenaline'); } } },
  self_care: { id: 'self_care', side: 'survivor', name: 'Self-Care', icon: '✚', rarity: 'common', tier: 'basic',
    description: 'Heal yourself without a med-kit at 50% speed.' },
  borrowed_time: { id: 'borrowed_time', side: 'survivor', name: 'Borrowed Time', icon: '⏳', rarity: 'rare', tier: 'strong',
    description: 'Survivors you unhook get Endurance for 15 s.',
    triggers: { [EV.SURVIVOR_UNHOOKED]: (p, d, m) => { if (d.rescuer === p.owner) { d.survivor.status.add('endurance', { duration: 15, source: 'bt' }); m.perks.proc(p, 'Borrowed Time'); } } } },
  bond: { id: 'bond', side: 'survivor', name: 'Bond', icon: '⚭', rarity: 'uncommon', tier: 'basic',
    description: 'See teammates within 36 m. Bots coordinate better.', ai: { teammateSense: 36 } },
  kindred: { id: 'kindred', side: 'survivor', name: 'Kindred', icon: '◎', rarity: 'uncommon', tier: 'strong',
    description: 'While you are hooked, teammates see the killer when within 16 m of your hook.', ai: { kindred: 16 } },
  resilience: { id: 'resilience', side: 'survivor', name: 'Resilience', icon: '✦', rarity: 'common', tier: 'basic',
    description: 'While injured, repair, heal and vault 9% faster.' },
  prove_thyself: { id: 'prove_thyself', side: 'survivor', name: 'Prove Thyself', icon: '⚙', rarity: 'uncommon', tier: 'strong',
    description: 'Repair 15% faster for each other survivor on your generator.' },
  iron_will: { id: 'iron_will', side: 'survivor', name: 'Iron Will', icon: '◐', rarity: 'rare', tier: 'strong',
    description: 'Grunts of pain are silenced while injured.' },
  urban_evasion: { id: 'urban_evasion', side: 'survivor', name: 'Urban Evasion', icon: '⌄', rarity: 'common', tier: 'basic',
    description: 'Crouch-walking speed nearly doubled. Bots hide more effectively.' },
  spine_chill: { id: 'spine_chill', side: 'survivor', name: 'Spine Chill', icon: '❄', rarity: 'uncommon', tier: 'strong',
    description: 'Warned when the killer is within 36 m and facing you — even when undetectable.', ai: { spineChill: 36 } },
  decisive_strike: { id: 'decisive_strike', side: 'survivor', name: 'Decisive Strike', icon: '✖', rarity: 'very rare', tier: 'strong',
    description: 'For 40 s after being unhooked, escape the killer\'s grasp with a skill check, stunning them for 4 s.' },
  unbreakable: { id: 'unbreakable', side: 'survivor', name: 'Unbreakable', icon: '⬆', rarity: 'rare', tier: 'strong',
    description: 'Once per trial, fully recover from the dying state.' },
  deliverance: { id: 'deliverance', side: 'survivor', name: 'Deliverance', icon: '⛓', rarity: 'rare', tier: 'strong',
    description: 'After a safe unhook, your next first-stage self-unhook succeeds.',
    triggers: { [EV.SURVIVOR_UNHOOKED]: (p, d, m) => { if (d.rescuer === p.owner) { const k = m.nearestKiller(d.survivor.x, d.survivor.y); if (dist(k.x, k.y, d.survivor.x, d.survivor.y) > 24 * M) { p.tokens = 1; m.perks.proc(p, 'Deliverance ready'); } } } } },
  small_game: { id: 'small_game', side: 'survivor', name: 'Small Game', icon: '⚠', rarity: 'uncommon', tier: 'basic',
    description: 'Warned of traps and totems within 8 m ahead.', ai: { smallGame: 8 } },
  calm_spirit: { id: 'calm_spirit', side: 'survivor', name: 'Calm Spirit', icon: '☾', rarity: 'common', tier: 'basic',
    description: 'You do not scream when hit or downed.' },
  windows_of_opportunity: { id: 'windows_of_opportunity', side: 'survivor', name: 'Windows of Opportunity', icon: '⊞', rarity: 'uncommon', tier: 'strong',
    description: 'Know the state of pallets and windows within 24 m. Bots plan chases better.', ai: { windows: 24 } },
  dark_sense: { id: 'dark_sense', side: 'survivor', name: 'Dark Sense', icon: '◉', rarity: 'common', tier: 'basic',
    description: 'Each time a generator is completed, see the killer for 5 s.',
    triggers: { [EV.GENERATOR_REPAIRED]: (p, d, m) => { if (p.owner.alive) { m.revealKillerTo(p.owner, 5, 'Dark Sense'); m.perks.proc(p, 'Dark Sense'); } } } },
  botany: { id: 'botany', side: 'survivor', name: 'Botany Knowledge', icon: '❦', rarity: 'common', tier: 'basic',
    description: 'Heal others 33% faster.', healMult: () => 1.33 },
  well_make_it: { id: 'well_make_it', side: 'survivor', name: 'We\'ll Make It', icon: '❤', rarity: 'uncommon', tier: 'strong',
    description: 'After unhooking someone, heal others 100% faster for 90 s.',
    triggers: { [EV.SURVIVOR_UNHOOKED]: (p, d, m) => { if (d.rescuer === p.owner) p.data.until = m.time + 90; } },
    healMult: (p, h, t, m) => (p.data.until > m.time ? 2 : 1) },
};

// ---------------------------------------------------------------------------------
// Expansion pool. `cat` drives weighted random builds; `ai` tells the perk brain how
// to value a perk (bias multipliers on goals) or when to fire an active one (use).
const tok = (p, n = 1, max = 4) => { p.tokens = Math.min(max, (p.tokens || 0) + n); };
Object.assign(SURVIVOR_PERKS, {
  // ---- Generator / objective ----
  stake_out: { id: 'stake_out', side: 'survivor', cat: 'objective', name: 'Stake Out', icon: '◔', rarity: 'uncommon', tier: 'strong', weight: 2,
    description: 'Gain a token every 15 s near the killer while not in a chase (max 4). Tokens turn good skill checks into great ones.',
    update: (p, m, dt) => { const s = p.owner; if (!s.standing || m.inChase(s)) return; if (m.killerDist(s.x, s.y) < 32 * M) { p.data.t = (p.data.t || 0) + dt; if (p.data.t > 15) { p.data.t = 0; tok(p); } } },
    onSkillCheck: (p) => { if (p.tokens > 0) { p.tokens--; return 'great'; } return null; }, ai: { bias: { repair: 1.05 } } },
  hyperfocus: { id: 'hyperfocus', side: 'survivor', cat: 'objective', name: 'Hyperfocus', icon: '◎', rarity: 'rare', tier: 'strong', weight: 2,
    description: 'Each great skill check in a row adds 4% repair speed (max 6). A missed check resets it.',
    triggers: { [EV.SKILL_CHECK_SUCCESS]: (p, d) => { if (d.survivor !== p.owner) return; if (d.great) { tok(p, 1, 6); p.owner.status.remove('repairSpeed', 'hyperfocus'); p.owner.status.add('repairSpeed', { value: 0.04 * p.tokens, source: 'hyperfocus', duration: 60 }); } },
      [EV.SKILL_CHECK_FAILURE]: (p, d) => { if (d.survivor === p.owner) { p.tokens = 0; p.owner.status.remove('repairSpeed', 'hyperfocus'); } } }, ai: { bias: { repair: 1.08 } } },
  technician: { id: 'technician', side: 'survivor', cat: 'objective', name: 'Technician', icon: '⚙', rarity: 'common', tier: 'basic', weight: 3,
    description: 'Failed skill checks make no loud noise and cost only 5% progress.', ai: { bias: { repair: 1.03 } } },
  deja_vu: { id: 'deja_vu', side: 'survivor', cat: 'objective', name: 'Déjà Vu', icon: '⁂', rarity: 'common', tier: 'basic', weight: 3,
    description: 'See the generators most likely to form a 3-gen. Bots break clusters first.', ai: { bias: { cluster: 1.6 } } },
  repressed_alliance: { id: 'repressed_alliance', side: 'survivor', cat: 'objective', name: 'Repressed Alliance', icon: '⛔', rarity: 'rare', tier: 'strong', weight: 2, charges: 1,
    description: 'After 80 s of repairs, press to block your generator for 30 s.',
    update: (p, m, dt) => { if (p.owner.action?.type === 'repair') p.data.rep = (p.data.rep || 0) + dt; },
    activate: (p, s, m) => { const g = s.action?.type === 'repair' ? s.action.target : p.data.lastGen; if (!g || (p.data.rep || 0) < 80 || p.charges <= 0) return false; g.blockedUntil = m.time + 30; p.charges--; m.notifyK(`Generator ${g.name} was blocked by the Entity`, '#9fd0d0'); m.perks.proc(p, 'Repressed Alliance'); return true; },
    ai: { use: { when: 'leaveGen', value: (p, ai) => { const g = ai.sv.action?.type === 'repair' ? ai.sv.action.target : null; return g && g.progress > 0.45 && ai.threat > 0.5 && (p.data.rep || 0) >= 80 ? 1 : 0; } } } },
  plunderers_instinct: { id: 'plunderers_instinct', side: 'survivor', cat: 'objective', name: "Plunderer's Instinct", icon: '⚿', rarity: 'common', tier: 'basic', weight: 2,
    description: 'Search chests 25% faster and know where unopened chests are.', actionMult: (p, s, type) => (type === 'searchChest' ? 1.25 : 1), ai: { bias: { chest: 2.2 } } },
  // ---- Chase ----
  quick_and_quiet: { id: 'quick_and_quiet', side: 'survivor', cat: 'chase', name: 'Quick & Quiet', icon: '∿', rarity: 'uncommon', tier: 'strong', weight: 2,
    description: 'Every 20 s, a fast vault or pallet drop makes no loud noise.' },
  dance_with_me: { id: 'dance_with_me', side: 'survivor', cat: 'chase', name: 'Dance With Me', icon: '♪', rarity: 'uncommon', tier: 'basic', weight: 2,
    description: 'After a fast vault, leave no scratch marks for 3 s.', onFastVault: (p, s, m) => { s.noScratchUntil = m.time + 3; } },
  lightweight: { id: 'lightweight', side: 'survivor', cat: 'stealth', name: 'Lightweight', icon: '❍', rarity: 'common', tier: 'basic', weight: 3,
    description: 'Your scratch marks fade much faster.' },
  vigil: { id: 'vigil', side: 'survivor', cat: 'chase', name: 'Vigil', icon: '◓', rarity: 'common', tier: 'basic', weight: 2,
    description: 'Recover from Exhausted 25% faster.', update: (p, m, dt) => { const e = p.owner.status.get('exhausted'); if (e) e.expiration -= dt * 0.25; } },
  overcome: { id: 'overcome', side: 'survivor', cat: 'chase', name: 'Overcome', icon: '➹', rarity: 'rare', tier: 'strong', weight: 2, exhaustion: true,
    description: 'When injured, gain 150% speed for 3 s. Causes Exhausted for 60 s.',
    triggers: { [EV.SURVIVOR_INJURED]: (p, d, m) => { const s = p.owner; if (d.survivor === s && notExhausted(s) && s.health === 'injured') { s.status.add('haste', { duration: 3, value: 0.5, source: 'overcome' }); exhaust(s, 60); m.perks.proc(p, 'Overcome'); s.ai?.note('Overcome burst after the hit', 'overcome'); } } } },
  background_player: { id: 'background_player', side: 'survivor', cat: 'rescue', name: 'Background Player', icon: '⇶', rarity: 'uncommon', tier: 'strong', weight: 2, exhaustion: true,
    description: 'When a teammate is picked up within 32 m, gain 150% speed for 5 s. Causes Exhausted for 60 s.',
    triggers: { [EV.SURVIVOR_PICKED_UP]: (p, d, m) => { const s = p.owner; if (d.survivor !== s && s.standing && notExhausted(s) && m.killerDist(s.x, s.y) < 32 * M && (s.isPlayer || s.ai?.perkBrain.wantsBurst('save'))) { s.status.add('haste', { duration: 5, value: 0.5, source: 'bgp' }); exhaust(s, 60); m.perks.proc(p, 'Background Player'); s.ai?.note('Background Player — sprinting to the carry', 'bgp'); } } },
    ai: { bias: { followCarry: 1.7, protect: 1.2 } } },
  smash_hit: { id: 'smash_hit', side: 'survivor', cat: 'chase', name: 'Smash Hit', icon: '✸', rarity: 'uncommon', tier: 'strong', weight: 2, exhaustion: true,
    description: 'After a pallet stun, gain 150% speed for 4 s. Causes Exhausted for 30 s.',
    triggers: { [EV.KILLER_STUNNED]: (p, d, m) => { const s = p.owner; if (d.kind === 'pallet' && m.lastStunner === s && notExhausted(s)) { s.status.add('haste', { duration: 4, value: 0.5, source: 'smash' }); exhaust(s, 30); m.perks.proc(p, 'Smash Hit'); } } },
    ai: { bias: { stunHunt: 1.5 } } },
  // ---- Killer information / stealth ----
  alert: { id: 'alert', side: 'survivor', cat: 'stealth', name: 'Alert', icon: '!', rarity: 'common', tier: 'basic', weight: 3,
    description: 'When the killer breaks a pallet or damages a generator, see them for 4 s.',
    triggers: { [EV.PALLET_DESTROYED]: (p, d, m) => { if (p.owner.alive) m.revealKillerTo(p.owner, 4, 'Alert'); }, [EV.GENERATOR_DAMAGED]: (p, d, m) => { if (p.owner.alive) m.revealKillerTo(p.owner, 4, 'Alert'); } } },
  premonition: { id: 'premonition', side: 'survivor', cat: 'stealth', name: 'Premonition', icon: '◭', rarity: 'common', tier: 'basic', weight: 2,
    description: 'Warned (every 30 s) when the killer is within 36 m in the direction you face.',
    update: (p, m, dt) => { const s = p.owner, k = m.nearestKiller(p.owner.x, p.owner.y); if (!s.standing || p.cooldownT > 0) return; const d = dist(k.x, k.y, s.x, s.y); const a = Math.atan2(k.y - s.y, k.x - s.x); let df = Math.abs(a - s.facing); if (df > Math.PI) df = Math.PI * 2 - df;
      if (d < 36 * M && df < 0.8) { p.cooldownT = 30; m.perks.proc(p, 'Premonition'); if (!s.ai) { m.warn(s, 'Premonition: the killer is ahead', a); return; } const B = s.ai.beliefFor(k); if (!B.observing) { const r = Math.min(d, 25 * M); B.x = s.x + Math.cos(a) * r; B.y = s.y + Math.sin(a) * r; B.vx = B.vy = 0; B.t = m.time; B.conf = Math.max(B.conf, 0.55); B.dirKnown = true; } s.ai.note('Premonition: killer ahead', 'premon'); } } },
  lucky_break: { id: 'lucky_break', side: 'survivor', cat: 'stealth', name: 'Lucky Break', icon: '✤', rarity: 'uncommon', tier: 'basic', weight: 2,
    description: 'While injured you leave no blood for up to 180 s.', update: (p, m, dt) => { if (p.owner.health === 'injured') p.data.used = (p.data.used || 0) + dt; }, ai: { bias: { hide: 0.1 } } },
  detectives_hunch: { id: 'detectives_hunch', side: 'survivor', cat: 'objective', name: "Detective's Hunch", icon: '⌕', rarity: 'common', tier: 'basic', weight: 2,
    description: 'When a generator is finished, learn every totem and chest within 64 m.',
    triggers: { [EV.GENERATOR_REPAIRED]: (p, d, m) => { const s = p.owner; if (!s.ai || !s.alive) return; for (const t of m.map.totems) if (dist(t.x, t.y, s.x, s.y) < 64 * M) s.ai.memory.totems.set(t.id, { totem: t, t: m.time }); } } },
  // ---- Healing ----
  empathy: { id: 'empathy', side: 'survivor', cat: 'healing', name: 'Empathy', icon: '♥', rarity: 'common', tier: 'basic', weight: 3,
    description: 'Know where injured and dying teammates are. Bots heal teammates more.', ai: { bias: { heal: 1.3 } } },
  solidarity: { id: 'solidarity', side: 'survivor', cat: 'healing', name: 'Solidarity', icon: '⊕', rarity: 'uncommon', tier: 'strong', weight: 2,
    description: 'While injured, healing a teammate also heals you by 50% of the progress.', ai: { dynBias: (p, ai) => (ai.sv.health === 'injured' ? { heal: 1.6 } : null) } },
  desperate_measures: { id: 'desperate_measures', side: 'survivor', cat: 'healing', name: 'Desperate Measures', icon: '⚕', rarity: 'uncommon', tier: 'strong', weight: 2,
    description: 'Heal and unhook 10% faster for each injured, hooked or dying teammate (max 40%).',
    healMult: (p, h, t, m) => 1 + 0.1 * Math.min(4, m.survivors.filter((o) => o !== h && ['injured', 'hooked', 'dying'].includes(o.health)).length),
    actionMult: (p, s, type, m) => (type === 'unhook' ? 1 + 0.1 * Math.min(4, m.survivors.filter((o) => o !== s && ['injured', 'hooked', 'dying'].includes(o.health)).length) : 1) },
  resurgence: { id: 'resurgence', side: 'survivor', cat: 'healing', name: 'Resurgence', icon: '↻', rarity: 'common', tier: 'basic', weight: 2,
    description: 'When you are unhooked, start with 50% healing progress.', triggers: { [EV.SURVIVOR_UNHOOKED]: (p, d) => { if (d.survivor === p.owner) p.owner.healProgress = 0.5; } } },
  circle_of_healing: { id: 'circle_of_healing', side: 'survivor', cat: 'healing', name: 'Circle of Healing', icon: '◌', rarity: 'rare', tier: 'strong', weight: 1,
    description: 'Every survivor can heal themselves without a med-kit (at 50% speed).' },
  inner_strength: { id: 'inner_strength', side: 'survivor', cat: 'healing', name: 'Inner Strength', icon: '✧', rarity: 'uncommon', tier: 'basic', weight: 2,
    description: 'Cleansing a totem heals you instantly if injured.',
    triggers: { [EV.TOTEM_CLEANSED]: (p, d, m) => { if (d.survivor === p.owner && p.owner.health === 'injured') { m.healed(p.owner, p.owner); m.perks.proc(p, 'Inner Strength'); } } },
    ai: { dynBias: (p, ai) => (ai.sv.health === 'injured' ? { cleanse: 3 } : null) } },
  // ---- Rescue ----
  breakout: { id: 'breakout', side: 'survivor', cat: 'rescue', name: 'Breakout', icon: '⇱', rarity: 'uncommon', tier: 'strong', weight: 2,
    description: 'Within 6 m of a killer carrying a teammate, that teammate wiggles 20% faster.', ai: { bias: { followCarry: 1.9 } } },
  babysitter: { id: 'babysitter', side: 'survivor', cat: 'rescue', name: 'Babysitter', icon: '☂', rarity: 'common', tier: 'basic', weight: 2,
    description: 'Teammates you unhook get a 4 s burst of speed; the killer sees you for 4 s.',
    triggers: { [EV.SURVIVOR_UNHOOKED]: (p, d, m) => { if (d.rescuer === p.owner) { d.survivor.status.add('haste', { duration: 4, value: 0.5, source: 'babysit' }); m.reveal(p.owner, 4, 'Babysitter'); } } }, ai: { bias: { rescue: 1.1 } } },
  for_the_people: { id: 'for_the_people', side: 'survivor', cat: 'rescue', name: 'For the People', icon: '✚', rarity: 'rare', tier: 'strong', weight: 1, charges: 1,
    description: 'Once per trial, instantly heal a dying teammate you are healing to injured. You become Broken for 80 s.',
    activate: (p, s, m) => { const t = s.action?.type === 'heal' ? s.action.target : null; if (!t || t.health !== 'dying' || p.charges <= 0) return false; m.healed(t, s); s.stopAction(m); s.status.add('broken', { duration: 80 }); p.charges--; m.perks.proc(p, 'For the People'); s.ai?.note('For the People — instant pick-up', 'ftp'); return true; },
    ai: { use: { value: (p, ai) => { const t = ai.sv.action?.type === 'heal' ? ai.sv.action.target : null; return t && t.health === 'dying' && ai.killerDistAt(t.x, t.y) < 26 ? 1 : 0; } } } },
  reassurance: { id: 'reassurance', side: 'survivor', cat: 'rescue', name: 'Reassurance', icon: '✋', rarity: 'uncommon', tier: 'strong', weight: 2, cooldown: 60,
    description: 'Pause a hooked teammate\'s hook timer for 30 s from up to 12 m away (60 s cooldown).',
    activate: (p, s, m) => { if (p.cooldownT > 0) return false; const t = m.survivors.find((o) => o.health === 'hooked' && dist(o.x, o.y, s.x, s.y) < 12 * M); if (!t) return false; t.hookPauseUntil = m.time + 30; p.cooldownT = 60; m.perks.proc(p, 'Reassurance'); s.ai?.note(`Reassured ${t.name} on the hook`, 'reassure'); return true; },
    ai: { use: { value: (p, ai) => { const m = ai.m; const t = m.survivors.find((o) => o.health === 'hooked' && dist(o.x, o.y, ai.sv.x, ai.sv.y) < 12 * M); if (!t) return 0; const left = m.hookStageTime - t.hookTimer; return left < 22 && ai.killerDistAt(t.x, t.y) < 16 ? 1 : 0; } } } },
  off_the_record: { id: 'off_the_record', side: 'survivor', cat: 'rescue', name: 'Off the Record', icon: '⊝', rarity: 'rare', tier: 'strong', weight: 2,
    description: 'After being unhooked, gain Endurance for 80 s. It ends early if you repair or heal.',
    triggers: { [EV.SURVIVOR_UNHOOKED]: (p, d) => { if (d.survivor === p.owner) p.owner.status.add('endurance', { duration: 80, source: 'otr' }); } },
    update: (p) => { const a = p.owner.action?.type; if (a === 'repair' || a === 'heal' || a === 'healSelf') p.owner.status.remove('endurance', 'otr'); } },
  slippery_meat: { id: 'slippery_meat', side: 'survivor', cat: 'rescue', name: 'Slippery Meat', icon: '≈', rarity: 'common', tier: 'basic', weight: 2,
    description: 'Self-unhook attempts are three times as likely to succeed.' },
  // ---- Endgame ----
  hope: { id: 'hope', side: 'survivor', cat: 'endgame', name: 'Hope', icon: '☀', rarity: 'common', tier: 'basic', weight: 3,
    description: 'When the exit gates are powered, gain 7% speed for 120 s.', triggers: { [EV.EXIT_GATES_POWERED]: (p) => p.owner.status.add('haste', { duration: 120, value: 0.07, source: 'hope' }) } },
  wake_up: { id: 'wake_up', side: 'survivor', cat: 'endgame', name: 'Wake Up!', icon: '⚑', rarity: 'uncommon', tier: 'basic', weight: 2,
    description: 'Open exit gates 15% faster and always know where they are.', actionMult: (p, s, type) => (type === 'openGate' ? 1.15 : 1), ai: { bias: { gate: 1.2 } } },
  left_behind: { id: 'left_behind', side: 'survivor', cat: 'endgame', name: 'Left Behind', icon: '⇲', rarity: 'common', tier: 'basic', weight: 2,
    description: 'As the last survivor, you know where the hatch is.', update: (p, m) => { if (p.owner.ai && m.aliveCount() === 1 && m.hatch.state !== 'hidden') p.owner.ai.memory.hatchSeen = true; } },
  no_one_left_behind: { id: 'no_one_left_behind', side: 'survivor', cat: 'endgame', name: 'No One Left Behind', icon: '⇄', rarity: 'uncommon', tier: 'basic', weight: 2,
    description: 'After the gates are powered, heal and unhook 50% faster.',
    healMult: (p, h, t, m) => (m.gatesPowered ? 1.5 : 1), actionMult: (p, s, type, m) => (m.gatesPowered && type === 'unhook' ? 1.5 : 1), ai: { dynBias: (p, ai) => (ai.m.gatesPowered ? { rescue: 1.3, heal: 1.3 } : null) } },
});
// ---------------------------------------------------------------------------------
// Roster expansion (18 killers): more chase/gen/heal/stealth/rescue/endgame perks and a
// handful of anti-power perks. Anti-power perks soften a killer's power, never cancel it.
Object.assign(SURVIVOR_PERKS, {
  distortion: { id: 'distortion', side: 'survivor', cat: 'stealth', name: 'Distortion', icon: '◌', rarity: 'rare', tier: 'strong', weight: 2, charges: 3,
    description: 'Three times per trial, when the killer would see your aura, it is hidden instead and your scratch marks vanish for 10 s.', ai: { bias: { hide: 0.1 } } },
  vigilant_eye: { id: 'vigilant_eye', side: 'survivor', cat: 'anti', name: 'Vigilant Eye', icon: '◉', rarity: 'uncommon', tier: 'strong', weight: 2,
    description: 'You notice killer power objects — traps, Phantasms, pods, gas, trails, guards, lockers — within 12 m even without line of sight.', ai: { bias: { cleanse: 1.05 } } },
  steadfast: { id: 'steadfast', side: 'survivor', cat: 'anti', name: 'Steadfast', icon: '⛉', rarity: 'uncommon', tier: 'strong', weight: 2,
    description: 'Power debuffs on you (Hindered, Intoxicated, Tormented, Slipstream, Infection) wear off 35% faster.',
    update: (p, m, dt) => { const sv = p.owner; for (const id of ['hindered', 'intoxicated', 'tormented', 'slipstream']) { const st = sv.status.get(id); if (st && st.expiration < 1e6) st.expiration -= dt * 0.35; } if (sv.infection) sv.infection = Math.max(0, sv.infection - dt * 0.28); } },
  fogwise: { id: 'fogwise', side: 'survivor', cat: 'objective', name: 'Fogwise', icon: '◎', rarity: 'uncommon', tier: 'basic', weight: 2,
    description: 'Great skill checks on generators show you the killer for 6 s.',
    triggers: { [EV.SKILL_CHECK_SUCCESS]: (p, d, m) => { if (d.survivor === p.owner && d.great) { m.revealKillerTo(p.owner, 6, 'Fogwise'); m.perks.proc(p, 'Fogwise'); } } } },
  leader: { id: 'leader', side: 'survivor', cat: 'objective', name: 'Leader', icon: '⚑', rarity: 'uncommon', tier: 'strong', weight: 2,
    description: 'Teammates within 8 m of you repair, heal, unhook and open gates 12% faster.', teamActionMult: 1.12, ai: { bias: { joinGen: 1.25 } } },
  situational_awareness: { id: 'situational_awareness', side: 'survivor', cat: 'objective', name: 'Situational Awareness', icon: '⊛', rarity: 'common', tier: 'basic', weight: 2,
    description: 'Repairing next to a teammate is 10% faster.', actionMult: (p, sv, type) => (type === 'repair' && sv.action?.target?.repairers?.size > 1 ? 1.1 : 1), ai: { bias: { joinGen: 1.2 } } },
  overzealous: { id: 'overzealous', side: 'survivor', cat: 'objective', name: 'Overzealous', icon: '✶', rarity: 'uncommon', tier: 'basic', weight: 2,
    description: 'Cleansing a totem makes you repair 8% faster (16% after a hex) until you are injured.',
    triggers: { [EV.TOTEM_CLEANSED]: (p, d) => { if (d.survivor === p.owner) p.owner.status.add('repairSpeed', { value: d.hex ? 0.16 : 0.08, source: 'overzealous', duration: 9999 }); },
      [EV.SURVIVOR_INJURED]: (p, d) => { if (d.survivor === p.owner) p.owner.status.remove('repairSpeed', 'overzealous'); } }, ai: { bias: { cleanse: 2.2 } } },
  parental_guidance: { id: 'parental_guidance', side: 'survivor', cat: 'chase', name: 'Parental Guidance', icon: '☂', rarity: 'common', tier: 'basic', weight: 2,
    description: 'After stunning the killer, you leave no scratch marks, blood or grunts for 7 s.',
    triggers: { [EV.KILLER_STUNNED]: (p, d, m) => { if (m.lastStunner === p.owner) { p.owner.noScratchUntil = m.time + 7; p.owner.silentUntil = m.time + 7; m.perks.proc(p, 'Parental Guidance'); } } } },
  self_preservation: { id: 'self_preservation', side: 'survivor', cat: 'stealth', name: 'Self-Preservation', icon: '⇲', rarity: 'common', tier: 'basic', weight: 2,
    description: 'When a teammate is hit within 16 m of you, you leave no scratch marks for 10 s.',
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { const s = p.owner; if (d.survivor !== s && s.standing && dist(s.x, s.y, d.survivor.x, d.survivor.y) < 16 * M) s.noScratchUntil = m.time + 10; } } },
  reactive_healing: { id: 'reactive_healing', side: 'survivor', cat: 'healing', name: 'Reactive Healing', icon: '♥', rarity: 'uncommon', tier: 'basic', weight: 2,
    description: 'When a teammate within 32 m is injured while you are injured, you gain 50% of your missing healing progress.',
    triggers: { [EV.SURVIVOR_INJURED]: (p, d, m) => { const s = p.owner; if (d.survivor !== s && s.health === 'injured' && dist(s.x, s.y, d.survivor.x, d.survivor.y) < 32 * M) { s.healProgress += (1 - s.healProgress) * 0.5; m.perks.proc(p, 'Reactive Healing'); } } } },
  bite_the_bullet: { id: 'bite_the_bullet', side: 'survivor', cat: 'healing', name: 'Bite the Bullet', icon: '◆', rarity: 'common', tier: 'basic', weight: 2,
    description: 'You and the teammate you heal make no grunts while healing, and you heal 6% faster.', healMult: () => 1.06, ai: { bias: { heal: 1.1 } } },
  power_of_two: { id: 'power_of_two', side: 'survivor', cat: 'healing', name: 'Teamwork: Power of Two', icon: '⚭', rarity: 'uncommon', tier: 'basic', weight: 2,
    description: 'After healing a teammate, you both gain 5% haste while within 16 m of each other (until either is injured).',
    triggers: { [EV.SURVIVOR_HEALED]: (p, d, m) => { if (d.healer === p.owner && d.survivor !== p.owner) { p.data.buddy = d.survivor; m.perks.proc(p, 'Power of Two'); } } },
    update: (p, m) => { const b = p.data.buddy, s = p.owner; if (!b) return; if (s.health !== 'healthy' || b.health !== 'healthy') { p.data.buddy = null; return; } if (dist(s.x, s.y, b.x, b.y) < 16 * M) { s.status.add('haste', { duration: 0.3, value: 0.05, source: 'p2' }); b.status.add('haste', { duration: 0.3, value: 0.05, source: 'p2' }); } }, ai: { bias: { heal: 1.15 } } },
  boil_over: { id: 'boil_over', side: 'survivor', cat: 'rescue', name: 'Boil Over', icon: '♨', rarity: 'uncommon', tier: 'basic', weight: 2,
    description: 'You wiggle free from the killer\'s grasp 25% faster.' },
  inner_focus: { id: 'inner_focus', side: 'survivor', cat: 'stealth', name: 'Inner Focus', icon: '◬', rarity: 'uncommon', tier: 'basic', weight: 2,
    description: 'When a teammate is hit, you see the killer for 3 s.',
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { if (d.survivor !== p.owner && p.owner.standing) m.revealKillerTo(p.owner, 3, 'Inner Focus'); } } },
  champion_of_light: { id: 'champion_of_light', side: 'survivor', cat: 'anti', name: 'Champion of Light', icon: '☀', rarity: 'rare', tier: 'strong', weight: 1,
    description: 'Your flashlight blinds 50% faster. Bots with this perk look for flashlight saves.', ai: { bias: { protect: 1.2, chest: 1.4 } } },
  low_profile: { id: 'low_profile', side: 'survivor', cat: 'endgame', name: 'Low Profile', icon: '▁', rarity: 'uncommon', tier: 'basic', weight: 2,
    description: 'As the last survivor standing, you leave no scratch marks or blood.' },
  sole_survivor: { id: 'sole_survivor', side: 'survivor', cat: 'endgame', name: 'Sole Survivor', icon: '☥', rarity: 'rare', tier: 'basic', weight: 2,
    description: 'Each dead teammate lets you hide your aura from the killer once more. As the last survivor you repair 75% faster.',
    triggers: { [EV.SURVIVOR_KILLED]: (p, d) => { if (d.survivor !== p.owner) p.tokens++; } },
    actionMult: (p, sv, type, m) => (type === 'repair' && m.aliveCount() === 1 ? 1.75 : 1) },
  wiretap_eye: { id: 'wiretap_eye', side: 'survivor', cat: 'anti', name: 'Ward Breaker', icon: '⊘', rarity: 'uncommon', tier: 'basic', weight: 2,
    description: 'You disarm killer traps, Phantasms and locker locks 40% faster, and chests 20% faster.',
    actionMult: (p, sv, type) => (type === 'disarm' || type === 'device' || type === 'wipe' ? 1.4 : type === 'searchChest' ? 1.2 : 1), ai: { bias: { chest: 1.2 } } },
  dodge_instinct: { id: 'dodge_instinct', side: 'survivor', cat: 'anti', name: 'Danger Sense', icon: '⚠', rarity: 'uncommon', tier: 'strong', weight: 2,
    description: 'You sense incoming projectiles, bounds and creatures a moment earlier (better dodges against ranged powers), but are Exhausted for 20 s after a successful dodge.',
    triggers: { dodged: (p, d, m) => { if (d.survivor === p.owner) { p.owner.status.add('exhausted', { duration: 20, source: 'danger' }); m.perks.proc(p, 'Danger Sense'); } } } },
});
// Categories for the original perks
const CAT = { sprint_burst: 'chase', lithe: 'chase', dead_hard: 'chase', adrenaline: 'endgame', self_care: 'healing', borrowed_time: 'rescue', bond: 'rescue', kindred: 'rescue', resilience: 'objective', prove_thyself: 'objective',
  iron_will: 'stealth', urban_evasion: 'stealth', spine_chill: 'stealth', decisive_strike: 'rescue', unbreakable: 'healing', deliverance: 'rescue', small_game: 'stealth', calm_spirit: 'stealth', windows_of_opportunity: 'chase',
  dark_sense: 'stealth', botany: 'healing', well_make_it: 'rescue' };
for (const [k, c] of Object.entries(CAT)) { SURVIVOR_PERKS[k].cat = c; SURVIVOR_PERKS[k].weight ??= SURVIVOR_PERKS[k].tier === 'strong' ? 3 : 2; }
for (const k of ['sprint_burst', 'lithe', 'dead_hard']) SURVIVOR_PERKS[k].exhaustion = true;
// AI usage hints for the original perks
SURVIVOR_PERKS.self_care.ai = { bias: { healSelf: 1.1 } };
SURVIVOR_PERKS.botany.ai = { bias: { heal: 1.25 } };
SURVIVOR_PERKS.well_make_it.ai = { bias: { rescue: 1.15 }, dynBias: (p, ai) => (p.data.until > ai.m.time ? { heal: 1.8 } : null) };
SURVIVOR_PERKS.borrowed_time.ai = { bias: { rescue: 1.15 } };
SURVIVOR_PERKS.prove_thyself.ai = { bias: { joinGen: 1.35 } };
SURVIVOR_PERKS.urban_evasion.ai = { bias: { hide: 0.25 } };
SURVIVOR_PERKS.iron_will.ai = { bias: { hide: 0.2 } };
SURVIVOR_PERKS.resilience.ai = { dynBias: (p, ai) => (ai.sv.health === 'injured' ? { healSelf: 0.7, repair: 1.1 } : null) };
SURVIVOR_PERKS.adrenaline.ai = { dynBias: (p, ai) => (ai.m.gensDone >= ai.m.gensNeeded - 1 ? { heal: 0.6, healSelf: 0.5 } : null) };
SURVIVOR_PERKS.deliverance.ai = { bias: { rescue: 1.2 } };
SURVIVOR_PERKS.dead_hard.ai = { exhaustion: 'dodge' };
SURVIVOR_PERKS.sprint_burst.ai = { exhaustion: 'burst' };

const SURVIVOR_PERK_IDS = Object.keys(SURVIVOR_PERKS);
const STRONG_SURVIVOR_PERKS = SURVIVOR_PERK_IDS.filter((k) => SURVIVOR_PERKS[k].tier === 'strong');
const BASIC_SURVIVOR_PERKS = SURVIVOR_PERK_IDS.filter((k) => SURVIVOR_PERKS[k].tier === 'basic');
const PERK_CATEGORIES = ['objective', 'chase', 'healing', 'stealth', 'rescue', 'endgame', 'anti'];
// Meta builds used in late story levels.
const META_BUILDS = [
  ['dead_hard', 'borrowed_time', 'sprint_burst', 'adrenaline'],
  ['lithe', 'decisive_strike', 'kindred', 'prove_thyself'],
  ['sprint_burst', 'iron_will', 'spine_chill', 'windows_of_opportunity'],
  ['dead_hard', 'unbreakable', 'deliverance', 'adrenaline'],
  ['lithe', 'borrowed_time', 'well_make_it', 'kindred'],
  ['overcome', 'off_the_record', 'hyperfocus', 'stake_out'],
  ['dead_hard', 'circle_of_healing', 'reassurance', 'deja_vu'],
];
// Themed builds for scenarios: each is a weighted category mix plus must-have perks.
const BUILD_THEMES = {
  chase: { cats: { chase: 4, stealth: 1, healing: 1, rescue: 1 }, must: ['dead_hard', 'lithe', 'sprint_burst', 'overcome', 'windows_of_opportunity', 'quick_and_quiet', 'smash_hit'] },
  objective: { cats: { objective: 5, stealth: 1, endgame: 1 }, must: ['prove_thyself', 'hyperfocus', 'stake_out', 'deja_vu', 'technician', 'resilience', 'repressed_alliance'] },
  stealth: { cats: { stealth: 5, objective: 1 }, must: ['urban_evasion', 'iron_will', 'lightweight', 'spine_chill', 'calm_spirit', 'lucky_break', 'premonition'] },
  rescue: { cats: { rescue: 5, healing: 2 }, must: ['borrowed_time', 'kindred', 'reassurance', 'babysitter', 'breakout', 'deliverance', 'off_the_record'] },
  healing: { cats: { healing: 5, rescue: 1 }, must: ['botany', 'self_care', 'empathy', 'solidarity', 'circle_of_healing', 'desperate_measures', 'well_make_it'] },
  counter: { cats: { anti: 5, stealth: 2, chase: 1 }, must: ['vigilant_eye', 'steadfast', 'dodge_instinct', 'wiretap_eye', 'champion_of_light', 'distortion', 'spine_chill'] },
  awareness: { cats: { stealth: 3, chase: 2 }, must: ['spine_chill', 'alert', 'premonition', 'dark_sense', 'kindred', 'windows_of_opportunity', 'small_game'] },
  endgame: { cats: { endgame: 4, rescue: 2 }, must: ['adrenaline', 'hope', 'wake_up', 'no_one_left_behind', 'left_behind', 'borrowed_time', 'decisive_strike'] },
};

function weightedTake(rng, pool, wf) {
  const tot = pool.reduce((a, k) => a + wf(k), 0); let r = rng() * tot;
  for (let i = 0; i < pool.length; i++) { r -= wf(pool[i]); if (r <= 0) return pool.splice(i, 1)[0]; }
  return pool.pop();
}
// Weighted random loadout: reasonable builds are common (one exhaustion perk at most,
// usually a spread of categories), unusual ones still possible.
function randomSurvivorPerks(rng, mode = 'random', i = 0) {
  if (mode === 'none') return [];
  if (mode === 'meta') return [...META_BUILDS[(i + Math.floor(rng() * META_BUILDS.length)) % META_BUILDS.length]];
  const theme = BUILD_THEMES[mode];
  if (theme) {
    const must = [...theme.must]; const out = [];
    while (out.length < 2 && must.length) { const k = must.splice(Math.floor(rng() * must.length), 1)[0]; if (SURVIVOR_PERKS[k].exhaustion && out.some((q) => SURVIVOR_PERKS[q].exhaustion)) continue; out.push(k); }
    const pool = SURVIVOR_PERK_IDS.filter((k) => !out.includes(k));
    while (out.length < 4 && pool.length) {
      const k = weightedTake(rng, pool, (q) => (theme.cats[SURVIVOR_PERKS[q].cat] || 0.3) * (SURVIVOR_PERKS[q].weight || 2));
      if (SURVIVOR_PERKS[k].exhaustion && out.some((q) => SURVIVOR_PERKS[q].exhaustion)) continue;
      out.push(k);
    }
    return out;
  }
  const pool = mode === 'strong' ? [...STRONG_SURVIVOR_PERKS] : mode === 'basic' ? [...BASIC_SURVIVOR_PERKS] : [...SURVIVOR_PERK_IDS];
  const out = [];
  while (out.length < 4 && pool.length) {
    // repeated categories get less likely, so builds spread out (but stacking is still possible)
    const k = weightedTake(rng, pool, (q) => { const d = SURVIVOR_PERKS[q]; const same = out.filter((o) => SURVIVOR_PERKS[o].cat === d.cat).length; return (d.weight || 2) * Math.pow(0.45, same); });
    if (SURVIVOR_PERKS[k].exhaustion && out.some((q) => SURVIVOR_PERKS[q].exhaustion)) continue;
    out.push(k);
  }
  return out;
}

return { SURVIVOR_PERKS, SURVIVOR_PERK_IDS, STRONG_SURVIVOR_PERKS, BASIC_SURVIVOR_PERKS, PERK_CATEGORIES, META_BUILDS, BUILD_THEMES, randomSurvivorPerks };
};
__defs["js/items/items.js"] = function(){
// Survivor items and item add-ons (data-driven). Charges are seconds of use.
const ITEMS = {
  medkit: { id: 'medkit', name: 'Med-Kit', icon: '✚', charges: 24, desc: 'Self-heal (16 s of charges). Heals others 25% faster.', healSelf: true, healOthersMult: 1.25 },
  toolbox: { id: 'toolbox', name: 'Toolbox', icon: '⚒', charges: 32, desc: 'Repair generators 50% faster while charges last.', repairMult: 1.5 },
  flashlight: { id: 'flashlight', name: 'Flashlight', icon: '☀', charges: 10, desc: 'Blind the killer; shining during a carry forces a drop.', blind: true },
  key: { id: 'key', name: 'Key', icon: '⚷', charges: 1, desc: 'Open the closed hatch when two or fewer survivors remain.', hatch: true },
};
const ITEM_ADDONS = {
  gauze: { id: 'gauze', item: 'medkit', name: 'Gauze Roll', desc: '+8 charges', apply: (it) => { it.charges += 8; } },
  syringe: { id: 'syringe', item: 'medkit', name: 'Anti-Haemorrhagic Syringe', desc: 'Self-heals 30% faster', apply: (it) => { it.healSpeed = 1.3; } },
  wire: { id: 'wire', item: 'toolbox', name: 'Wire Spool', desc: '+10 charges', apply: (it) => { it.charges += 10; } },
  socket: { id: 'socket', item: 'toolbox', name: 'Socket Swivels', desc: 'Repair bonus 65%', apply: (it) => { it.repairMult = 1.65; } },
  battery: { id: 'battery', item: 'flashlight', name: 'Battery', desc: '+5 charges', apply: (it) => { it.charges += 5; } },
  lens: { id: 'lens', item: 'flashlight', name: 'Wide Lens', desc: 'Wider beam', apply: (it) => { it.beam = 1.4; } },
  amber: { id: 'amber', item: 'key', name: 'Blood Amber', desc: 'Open hatch faster', apply: (it) => { it.hatchSpeed = 1.5; } },
};
function makeItem(id, addons = []) {
  if (!id || !ITEMS[id]) return null;
  const d = ITEMS[id];
  const it = { id, name: d.name, icon: d.icon, charges: d.charges, maxCharges: d.charges, repairMult: d.repairMult || 1, healSpeed: 1, beam: 1, hatchSpeed: 1, addons: [] };
  for (const a of addons) { const ad = ITEM_ADDONS[a]; if (ad && ad.item === id) { ad.apply(it); it.addons.push(a); } }
  it.maxCharges = it.charges;
  return it;
}
const ITEM_POOL = ['medkit', 'medkit', 'toolbox', 'toolbox', 'flashlight', 'key', null, null];

return { ITEMS, ITEM_ADDONS, makeItem, ITEM_POOL };
};
__defs["js/maps/maps.js"] = function(){
// Data-driven map definitions. Sizes are in meters (1 m = 32 px). Every map is far
// larger than the viewport; the generator lays out tiles differently per seed.
const MAPS = {
  rotwood: {
    id: 'rotwood', name: 'Rotwood Forest', blurb: 'Dense pines, a ruined chapel and plenty of rock cover.',
    size: [104, 104], main: 'main_temple', tiles: 15, fillers: 30, grassPatches: 22, gens: 7, hooks: 12,
    weights: { jungle_gym: 4, jungle_gym_b: 3, shack: 1, t_wall: 3, l_wall: 3, long_wall: 2, short_wall: 2, debris: 3 },
    fillerWeights: { rocks: 3, trees: 6, grass: 2 },
    theme: { ground: '#18201a', ground2: '#1d261e', speck: '#2a3628', wall: '#5e625a', wallTop: '#7a7f74', wood: '#5c4630', woodTop: '#77603f', rock: '#4b4f4b', rockTop: '#646a64', tree: '#16251a', treeTop: '#24402a', grass: '#2c4a2a', border: '#0c100c', fog: [150, 175, 160] },
  },
  ashfarm: {
    id: 'ashfarm', name: 'Ashen Farm', blurb: 'An open farmstead. Tall corn hides survivors; tiles are spread out.',
    size: [116, 92], main: 'main_house', tiles: 13, fillers: 18, grassPatches: 34, gens: 7, hooks: 12,
    weights: { jungle_gym: 3, jungle_gym_b: 2, shack: 1, t_wall: 2, l_wall: 3, long_wall: 3, short_wall: 2, debris: 2 },
    fillerWeights: { rocks: 2, trees: 2, grass: 6 },
    theme: { ground: '#221d15', ground2: '#2a2319', speck: '#3a301f', wall: '#6b6155', wallTop: '#857a6b', wood: '#614629', woodTop: '#80603a', rock: '#4f4a43', rockTop: '#6a645b', tree: '#2a2617', treeTop: '#3e3a20', grass: '#5a5426', border: '#100d08', fog: [190, 170, 140] },
  },
  ironworks: {
    id: 'ironworks', name: 'Ironworks Yard', blurb: 'Industrial yard: strong loops, few hiding spots, a huge foundry.',
    size: [100, 100], main: 'main_factory', tiles: 18, fillers: 16, grassPatches: 8, gens: 7, hooks: 12,
    weights: { jungle_gym: 5, jungle_gym_b: 4, shack: 1, t_wall: 3, l_wall: 2, long_wall: 3, short_wall: 3, debris: 1 },
    fillerWeights: { rocks: 5, trees: 1, grass: 1 },
    theme: { ground: '#1a1b1e', ground2: '#202226', speck: '#2c2f33', wall: '#5d6168', wallTop: '#7c818a', wood: '#5a4733', woodTop: '#755d42', rock: '#45484d', rockTop: '#60646b', tree: '#1c241c', treeTop: '#2a3a2a', grass: '#33402e', border: '#0b0b0d', fog: [160, 165, 180] },
  },
  hollowmarsh: {
    id: 'hollowmarsh', name: 'Hollow Marsh', blurb: 'Foggy wetland with weak filler and long dead zones. Hard for survivors.',
    size: [112, 112], main: 'main_temple', tiles: 11, fillers: 26, grassPatches: 30, gens: 7, hooks: 13,
    weights: { jungle_gym: 2, jungle_gym_b: 2, shack: 1, t_wall: 3, l_wall: 3, long_wall: 2, short_wall: 4, debris: 5 },
    fillerWeights: { rocks: 2, trees: 5, grass: 5 },
    theme: { ground: '#16201f', ground2: '#1a2625', speck: '#243231', wall: '#565e5c', wallTop: '#717b78', wood: '#4d4130', woodTop: '#685840', rock: '#434a48', rockTop: '#5c6461', tree: '#142320', treeTop: '#1f3a33', grass: '#2b4a40', border: '#091010', fog: [140, 170, 165] },
  },
  // ---------------- expansion maps: each has its own layout style ----------------
  asylum: {
    id: 'asylum', name: 'Sunken Asylum', blurb: 'A walled asylum of linked wards: doorways, short sightlines and pallets in the doors. Ranged killers struggle; stealth and mobility shine.',
    size: [110, 96], layout: 'compound', compound: { cols: 4, rows: 3, room: 15, roomH: 13 }, main: null, tiles: 7, fillers: 14, grassPatches: 6, gens: 7, hooks: 13,
    weights: { t_wall: 2, l_wall: 2, short_wall: 2, debris: 2, ward_pillars: 3, jungle_gym: 1 },
    fillerWeights: { rocks: 2, trees: 3, grass: 1 },
    theme: { ground: '#1b1a1d', ground2: '#211f24', speck: '#2d2a31', wall: '#5a5560', wallTop: '#77717e', wood: '#4f4237', woodTop: '#685748', rock: '#47444b', rockTop: '#605c66', tree: '#1a1f1c', treeTop: '#27322b', grass: '#2f3a30', metal: '#3f4a52', metalTop: '#5a6872', water: '#16232b', border: '#0b0a0c', fog: [150, 145, 165] },
  },
  docks: {
    id: 'docks', name: 'Saltmere Docks', blurb: 'Harbour strips split by water channels: bridges are chokepoints, but you can see — and throw — across the water. Container stacks make strong loops.',
    size: [120, 100], layout: 'channels', channels: { count: 2, width: 4 }, main: 'main_warehouse', mainAt: [0.5, 0.5], tiles: 14, fillers: 10, grassPatches: 4, gens: 7, hooks: 13,
    weights: { container_yard: 4, container_row: 3, dock_crane: 3, l_wall: 1, short_wall: 2, debris: 1 },
    fillerWeights: { rocks: 2, trees: 1, grass: 1 },
    theme: { ground: '#1a1d20', ground2: '#1f2327', speck: '#2b3136', wall: '#59616a', wallTop: '#77808a', wood: '#5a4a39', woodTop: '#776350', rock: '#444a50', rockTop: '#5e666e', tree: '#1a221e', treeTop: '#26342c', grass: '#2e3c34', metal: '#3c5566', metalTop: '#55798f', water: '#0f2a36', border: '#0a0c0e', fog: [150, 170, 185] },
  },
  quarry: {
    id: 'quarry', name: 'Ember Quarry', blurb: 'A tar pit sinks the centre of the map; every route bends around it. Rock ridges with pallet gaps, long open lanes across the pit.',
    size: [112, 104], layout: 'pit', pit: { radius: 11, shelves: 4 }, main: 'main_office', mainAt: [0.22, 0.24], tiles: 12, fillers: 18, grassPatches: 6, gens: 7, hooks: 12,
    weights: { ridge_gap: 4, ridge_window: 3, l_wall: 2, debris: 3, t_wall: 1, jungle_gym_b: 1 },
    fillerWeights: { rocks: 6, trees: 1, grass: 1 },
    theme: { ground: '#221a16', ground2: '#291f1a', speck: '#3a2a22', wall: '#6a5a50', wallTop: '#86756a', wood: '#5c4430', woodTop: '#7a5c40', rock: '#5a4a42', rockTop: '#7a665a', tree: '#2a2018', treeTop: '#3a2e22', grass: '#4a3a24', metal: '#4a4440', metalTop: '#665e58', water: '#1a0f0c', waterGlow: 'rgba(220,90,30,0.22)', border: '#100b08', fog: [190, 150, 130] },
  },
  grove: {
    id: 'grove', name: 'Hallowed Grove', blurb: 'A cathedral in a clearing, ringed by deep woods. Thick tree cover hides survivors, but the woods hold few real loops.',
    size: [116, 116], layout: 'clearing', forest: { count: 190, clear: 19 }, main: 'main_cathedral', tiles: 10, fillers: 6, grassPatches: 30, gens: 7, hooks: 13,
    weights: { hedge_loop: 4, jungle_gym: 2, l_wall: 2, debris: 2, short_wall: 2, t_wall: 1 },
    fillerWeights: { rocks: 1, trees: 4, grass: 3 },
    theme: { ground: '#141c16', ground2: '#18221a', speck: '#22301f', wall: '#5b6158', wallTop: '#787f74', wood: '#4c3c2a', woodTop: '#66523a', rock: '#454a45', rockTop: '#5e655e', tree: '#122016', treeTop: '#1d3a24', grass: '#284a2c', metal: '#3f4a44', metalTop: '#5a6860', water: '#10201c', border: '#080c09', fog: [135, 165, 140] },
  },
  lodge: {
    id: 'lodge', name: 'Frostpeak Lodge', blurb: 'A snowbound ski lodge with cabins, fence runs and pallet boxes. Plenty of strong tiles, but wide snowfields between them.',
    size: [108, 100], main: 'main_lodge', tiles: 15, fillers: 14, grassPatches: 4, gens: 7, hooks: 12,
    weights: { cabin: 3, fence_run: 3, double_pallet: 3, jungle_gym: 2, l_wall: 2, short_wall: 2, debris: 1 },
    fillerWeights: { rocks: 3, trees: 4, grass: 0 },
    theme: { ground: '#c9ced4', ground2: '#bfc5cc', speck: '#d8dde2', wall: '#5a5f66', wallTop: '#7b828b', wood: '#5a4232', woodTop: '#7a5a44', rock: '#6b7178', rockTop: '#8b9199', tree: '#1c2a24', treeTop: '#2e4a3c', grass: '#8a9488', metal: '#4a5866', metalTop: '#64788a', water: '#7f99ad', border: '#2a2e33', fog: [215, 225, 235], snow: true },
  },
};
const MAP_LIST = Object.values(MAPS);

return { MAPS, MAP_LIST };
};
__defs["js/ai/personality.js"] = function(){
// Hidden personality traits (0..1). Archetypes are data; each bot rolls variance on top,
// so two bots at the same difficulty still play differently.
const { clamp, gauss, pick } = __req("js/core/util.js");

const TRAITS = ['bravery', 'selfishness', 'teamwork', 'riskTolerance', 'loopSkill', 'stealthSkill', 'objectiveFocus',
  'rescuePriority', 'healingPriority', 'killerAwareness', 'palletDiscipline', 'vaultSkill', 'predictionSkill'];

const ARCHETYPES = {
  altruist: { label: 'Altruist', bravery: 0.75, selfishness: 0.1, teamwork: 0.9, riskTolerance: 0.6, loopSkill: 0.55, stealthSkill: 0.45, objectiveFocus: 0.5, rescuePriority: 0.95, healingPriority: 0.75, killerAwareness: 0.6, palletDiscipline: 0.55, vaultSkill: 0.55, predictionSkill: 0.55 },
  genrusher: { label: 'Gen Rusher', bravery: 0.4, selfishness: 0.55, teamwork: 0.55, riskTolerance: 0.45, loopSkill: 0.5, stealthSkill: 0.6, objectiveFocus: 0.95, rescuePriority: 0.45, healingPriority: 0.2, killerAwareness: 0.7, palletDiscipline: 0.6, vaultSkill: 0.5, predictionSkill: 0.6 },
  looper: { label: 'Looper', bravery: 0.9, selfishness: 0.3, teamwork: 0.65, riskTolerance: 0.8, loopSkill: 0.95, stealthSkill: 0.25, objectiveFocus: 0.55, rescuePriority: 0.6, healingPriority: 0.35, killerAwareness: 0.8, palletDiscipline: 0.85, vaultSkill: 0.95, predictionSkill: 0.85 },
  stealth: { label: 'Stealthy', bravery: 0.25, selfishness: 0.6, teamwork: 0.45, riskTolerance: 0.25, loopSkill: 0.4, stealthSkill: 0.95, objectiveFocus: 0.7, rescuePriority: 0.35, healingPriority: 0.6, killerAwareness: 0.85, palletDiscipline: 0.5, vaultSkill: 0.45, predictionSkill: 0.6 },
  medic: { label: 'Medic', bravery: 0.5, selfishness: 0.15, teamwork: 0.85, riskTolerance: 0.4, loopSkill: 0.5, stealthSkill: 0.55, objectiveFocus: 0.5, rescuePriority: 0.7, healingPriority: 0.95, killerAwareness: 0.6, palletDiscipline: 0.6, vaultSkill: 0.5, predictionSkill: 0.5 },
  wildcard: { label: 'Wildcard', bravery: 0.6, selfishness: 0.4, teamwork: 0.5, riskTolerance: 0.7, loopSkill: 0.6, stealthSkill: 0.4, objectiveFocus: 0.55, rescuePriority: 0.55, healingPriority: 0.45, killerAwareness: 0.5, palletDiscipline: 0.4, vaultSkill: 0.6, predictionSkill: 0.5 },
};

function rollPersonality(rng, archetype = null) {
  const key = archetype || pick(rng, Object.keys(ARCHETYPES));
  const base = ARCHETYPES[key];
  const p = { archetype: key, label: base.label };
  const spread = key === 'wildcard' ? 0.3 : 0.12;
  for (const t of TRAITS) p[t] = clamp(base[t] + gauss(rng) * spread, 0.02, 0.98);
  return p;
}

const SURVIVOR_ROSTER = [
  { name: 'Mara', color: '#d98c6a' }, { name: 'Desmond', color: '#6aa7d9' }, { name: 'Ivy', color: '#8fd97a' }, { name: 'Tomas', color: '#d9c86a' },
  { name: 'June', color: '#c58ad9' }, { name: 'Rook', color: '#6ad9c4' }, { name: 'Elena', color: '#e0a0b8' }, { name: 'Bram', color: '#b0b8c8' },
];

return { TRAITS, ARCHETYPES, rollPersonality, SURVIVOR_ROSTER };
};
__defs["js/modes/classes.js"] = function(){
// 2v8 classes. In 2v8 survivors and killers don't bring perks or items: each picks a class
// with a fixed kit. Classes are perk-shaped definitions so they run through the central perk /
// status / event systems like everything else (and work for the player and for bots alike).
const { M, dist } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");

const SURVIVOR_CLASSES = {
  escapist: { id: 'class_escapist', name: 'Escapist', icon: '➹', blurb: 'Runner. After being hit: +50% Haste for 3 s more. Gains 10% Haste for 5 s whenever a chase starts (60 s cooldown).' },
  guide:    { id: 'class_guide', name: 'Guide', icon: '⚑', blurb: 'Rescuer. Frees caged teammates 50% faster and picks up dying teammates 30% faster.' },
  medic:    { id: 'class_medic', name: 'Medic', icon: '✚', blurb: 'Healer. Heals 40% faster and is the only class that can heal themselves (at half speed).' },
  scout:    { id: 'class_scout', name: 'Scout', icon: '◉', blurb: 'Lookout. When a killer comes within 24 m, the whole team sees that killer for 5 s (30 s cooldown).' },
};
const KILLER_CLASSES = {
  brute:      { id: 'class_brute', name: 'Brute', icon: '⚒', blurb: 'Breaks pallets and kicks generators 25% faster; stuns last 20% less.' },
  enforcer:   { id: 'class_enforcer', name: 'Enforcer', icon: '⚔', blurb: 'Each hit gives 5% Haste for 6 s.' },
  fearmonger: { id: 'class_fearmonger', name: 'Fearmonger', icon: '☠', blurb: 'Terror radius +20%; survivors inside it repair 10% slower.' },
  shadow:     { id: 'class_shadow', name: 'Shadow', icon: '☾', blurb: 'Undetectable for 12 s after caging a survivor; vaults 15% faster.' },
};

const CLASS_PERKS = {
  class_escapist: { id: 'class_escapist', side: 'survivor', cat: 'class', name: 'Escapist', icon: '➹', description: SURVIVOR_CLASSES.escapist.blurb, cooldown: 60,
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { if (d.survivor === p.owner && d.survivor.standing) { p.owner.status.add('haste', { duration: 3, value: 0.5, source: 'escapist' }); m.perks.proc(p, 'Escapist'); } },
      [EV.CHASE_START]: (p, d, m) => { if (d.survivor === p.owner && p.cooldownT <= 0) { p.owner.status.add('haste', { duration: 5, value: 0.1, source: 'escapist_run' }); p.cooldownT = 60; m.perks.proc(p, 'Escapist'); } } } },
  class_guide: { id: 'class_guide', side: 'survivor', cat: 'class', name: 'Guide', icon: '⚑', description: SURVIVOR_CLASSES.guide.blurb,
    actionMult: (p, sv, type) => (type === 'unhook' ? 1.5 : 1),
    healMult: (p, healer, target) => (target.health === 'dying' ? 1.3 : 1) },
  class_medic: { id: 'class_medic', side: 'survivor', cat: 'class', name: 'Medic', icon: '✚', description: SURVIVOR_CLASSES.medic.blurb,
    healMult: (p, healer, target) => (healer === target ? 1 : 1.4) },
  class_scout: { id: 'class_scout', side: 'survivor', cat: 'class', name: 'Scout', icon: '◉', description: SURVIVOR_CLASSES.scout.blurb, cooldown: 30,
    update: (p, m) => { const s = p.owner; if (!s.standing || p.cooldownT > 0) return; for (const k of m.killers) if (dist(k.x, k.y, s.x, s.y) < 24 * M) { for (const o of m.survivors) if (o.alive) m.revealKillerTo(o, 5, 'Scout', k); p.cooldownT = 30; m.perks.proc(p, 'Scout'); s.ai?.note(`Scouted the ${k.def.short}`, 'scout'); m.bb.report?.(k.x, k.y, 'scout', k); break; } } },
  class_brute: { id: 'class_brute', side: 'killer', cat: 'class', name: 'Brute', icon: '⚒', description: KILLER_CLASSES.brute.blurb,
    killerActionMult: (p, type) => (type === 'break' || type === 'kick' ? 0.75 : 1), stunMult: 0.8 },
  class_enforcer: { id: 'class_enforcer', side: 'killer', cat: 'class', name: 'Enforcer', icon: '⚔', description: KILLER_CLASSES.enforcer.blurb,
    triggers: { [EV.KILLER_HIT]: (p, d, m) => { if (d.killer !== p.owner) return; p.owner.status.add('haste', { duration: 6, value: 0.05, source: 'enforcer' }); m.perks.proc(p, 'Enforcer'); } } },
  class_fearmonger: { id: 'class_fearmonger', side: 'killer', cat: 'class', name: 'Fearmonger', icon: '☠', description: KILLER_CLASSES.fearmonger.blurb, terrorMult: 1.2,
    update: (p, m) => { const k = p.owner; for (const s of m.survivors) if (s.action?.type === 'repair' && dist(s.x, s.y, k.x, k.y) < k.terrorRadius) s.status.add('repairSpeed', { duration: 0.2, value: -0.1, source: 'fearmonger' }); } },
  class_shadow: { id: 'class_shadow', side: 'killer', cat: 'class', name: 'Shadow', icon: '☾', description: KILLER_CLASSES.shadow.blurb,
    killerActionMult: (p, type) => (type === 'vault' ? 0.85 : 1),
    triggers: { [EV.SURVIVOR_HOOKED]: (p, d, m) => { if (d.killer !== p.owner) return; p.owner.status.add('undetectable', { duration: 12, source: 'shadow' }); m.perks.proc(p, 'Shadow'); } } },
};

return { SURVIVOR_CLASSES, KILLER_CLASSES, CLASS_PERKS };
};
__defs["js/modes/twoVeight.js"] = function(){
// 2v8 mode: two killers against eight survivors (rules after the live DBD 2v8 event).
//  • a larger version of the chosen map: 13 generators, 8 to power the 3 exit gates
//  • hooking sends the survivor to a CAGE somewhere else on the map (60 s per hook stage);
//    teammates free them there
//  • no perks or items: every player picks a CLASS with a fixed kit (modes/classes.js)
//  • no self-healing (the Medic class excepted), no hatch
//  • REVEALED: survivors are revealed to both killers when a chase starts on them, when they
//    are near a generator a killer kicks (20 m) and when they finish a generator
// Two configurations: the player is Killer A (bot Killer B, 8 bot survivors), or the player
// is one of the survivors (7 bot teammates, two killer bots).
const { M, dist } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { SURVIVOR_CLASSES, KILLER_CLASSES } = __req("js/modes/classes.js");

const TWO_V_EIGHT = {
  generators: 13, gensRequired: 8, gates: 3, survivors: 8, cageStageTime: 90, mapScale: 1.4,
  repairMult: 1.25, revealChase: 5, revealKick: 20, revealKickTime: 4, revealGenDone: 4, allowPerks: false, hatch: false,
};
const SV_CLASS_ORDER = ['escapist', 'guide', 'medic', 'scout'];
const K_CLASS_ORDER = ['brute', 'enforcer', 'fearmonger', 'shadow'];

// Build a Match config for 2v8.
//  o = { role: 'killer'|'survivor'|'spectate', killerA, killerB, killerAClass, killerBClass,
//        survivorClass (player), mapId, survivorDifficulty, teammateDifficulty, killerDifficulty,
//        killerBDifficulty, allowPerks, seed }
function config2v8(o = {}) {
  const role = o.role || 'killer';
  const kA = o.killerA || 'trapper', kB = o.killerB || 'wraith';
  const kAc = KILLER_CLASSES[o.killerAClass] ? o.killerAClass : K_CLASS_ORDER[0];
  const kBc = KILLER_CLASSES[o.killerBClass] ? o.killerBClass : K_CLASS_ORDER[(K_CLASS_ORDER.indexOf(kAc) + 1) % 4];
  const perks = o.allowPerks ?? TWO_V_EIGHT.allowPerks;
  const survivors = [];
  for (let i = 0; i < TWO_V_EIGHT.survivors; i++) {
    const player = i === 0 && role === 'survivor';
    const cls = player && SURVIVOR_CLASSES[o.survivorClass] ? o.survivorClass : SV_CLASS_ORDER[(i + (player ? 0 : 1)) % 4];
    const diff = player ? 5 : role === 'survivor' ? (o.teammateDifficulty ?? 5) : (o.survivorDifficulty ?? 5);
    survivors.push({ difficulty: diff, perks: [SURVIVOR_CLASSES[cls].id, ...(perks && o.survivorPerks ? o.survivorPerks.slice(0, 2) : [])], item: perks ? undefined : null, cls });
  }
  return {
    mode: '2v8', playerRole: role, mapId: o.mapId || 'rotwood', seed: o.seed ?? null,
    killers: [
      { killerId: kA, perks: [KILLER_CLASSES[kAc].id, ...(perks && o.killerPerks ? o.killerPerks.slice(0, 2) : [])], addons: [], difficulty: o.killerDifficulty ?? 5, cls: kAc },
      { killerId: kB, perks: [KILLER_CLASSES[kBc].id], addons: [], difficulty: o.killerBDifficulty ?? o.killerDifficulty ?? 5, cls: kBc },
    ],
    killerDifficulty: o.killerDifficulty ?? 5, killerId: kA,
    survivors, survivorCount: TWO_V_EIGHT.survivors, difficulty: o.survivorDifficulty ?? 5,
    gensOnMap: TWO_V_EIGHT.generators, gensRequired: TWO_V_EIGHT.gensRequired, gateCount: TWO_V_EIGHT.gates,
    perkMode: 'none', itemMode: perks ? 'random' : 'none',
    modifiers: { noHatch: !TWO_V_EIGHT.hatch, ...(o.modifiers || {}) },
  };
}

// A larger copy of a map definition for 2v8.
function scaleMapDef(def) {
  const s = TWO_V_EIGHT.mapScale;
  return { ...def, size: [Math.round(def.size[0] * s), Math.round(def.size[1] * s)], tiles: Math.round(def.tiles * s * s), fillers: Math.round(def.fillers * s * s),
    grassPatches: Math.round((def.grassPatches || 0) * s * s), hooks: Math.round(def.hooks * 1.7), gens: TWO_V_EIGHT.generators };
}

// Match-side rules (called from the Match constructor when cfg.mode === '2v8').
function install2v8(m) {
  m.hookStageTime = m.cfg.modifiers.hardcoreHooks ? 40 : TWO_V_EIGHT.cageStageTime;
  m.is2v8 = true; m.repairMult = TWO_V_EIGHT.repairMult;
  // second survivor group spawns elsewhere; the two killers start apart
  const extra = m.map.openSpots(8, 8, [...m.map.gens]).filter((p) => m.killers.every((k) => dist(k.x, k.y, p.x, p.y) > 30 * M));
  const base = extra[0] || m.map.spawn.survivors[0];
  m.survivors.forEach((s, i) => { if (i >= 4) { const a = (i - 4) * Math.PI / 2; s.x = base.x + Math.cos(a) * 1.2 * M; s.y = base.y + Math.sin(a) * 1.2 * M; m.world.resolve(s, s.r); } });
  // Revealed: chase start, kicked generator (20 m), finished generator
  const R = TWO_V_EIGHT;
  m.events.on(EV.CHASE_START, (d) => { if (d.survivor) m.reveal(d.survivor, R.revealChase, 'Revealed'); });
  m.events.on(EV.GENERATOR_DAMAGED, (d) => { for (const s of m.survivors) if (s.standing && dist(s.x, s.y, d.gen.x, d.gen.y) < R.revealKick * M) m.reveal(s, R.revealKickTime, 'Revealed'); });
  m.events.on(EV.GENERATOR_REPAIRED, (d) => { for (const s of m.survivors) if (s.standing && dist(s.x, s.y, d.gen.x, d.gen.y) < 3 * M) m.reveal(s, R.revealGenDone, 'Revealed'); });
  // remember each player's class for the HUD / results
  m.cfg.killers?.forEach((c, i) => { if (m.killers[i]) m.killers[i].cls = c.cls; });
  m.cfg.survivors?.forEach((c, i) => { if (m.survivors[i]) m.survivors[i].cls = c.cls; });
}

// Cage instead of hook: the survivor is sent to a free cage far from both killers.
function pickCage(m, from) {
  const free = m.map.hooks.filter((h) => !h.survivor && !h.broken);
  const far = free.filter((h) => m.killers.every((k) => dist(h.x, h.y, k.x, k.y) > 24 * M));
  const pool = far.length ? far : free;
  return pool.length ? pool[Math.floor(m.rng() * pool.length)] : from;
}


return { TWO_V_EIGHT, config2v8, scaleMapDef, install2v8, pickCage, SURVIVOR_CLASSES, KILLER_CLASSES };
};
__defs["js/core/fx.js"] = function(){
// Game-feel container: particles, floating text, screen shake, flashes. Pure data;
// the renderer draws it. Works headless for tests.
const { M } = __req("js/core/util.js");

class FX {
  constructor() { this.particles = []; this.texts = []; this.shakeAmt = 0; this.flash = 0; this.flashColor = '255,255,255'; this.hitStop = 0; this.rings = []; }
  update(dt) {
    for (const p of this.particles) { p.t += dt; p.x += p.vx * dt; p.y += p.vy * dt; p.vx *= 1 - dt * p.drag; p.vy *= 1 - dt * p.drag; }
    this.particles = this.particles.filter((p) => p.t < p.life);
    for (const t of this.texts) { t.t += dt; t.y -= 18 * dt; }
    this.texts = this.texts.filter((t) => t.t < t.life);
    for (const r of this.rings) r.t += dt; this.rings = this.rings.filter((r) => r.t < r.life);
    this.shakeAmt = Math.max(0, this.shakeAmt - dt * 40);
    this.flash = Math.max(0, this.flash - dt * 2.5);
  }
  burst(x, y, n, color, speed = 120, life = 0.7, size = 3, drag = 3) {
    if (this.particles.length > 900) return;
    for (let i = 0; i < n; i++) { const a = Math.random() * Math.PI * 2, s = speed * (0.3 + Math.random() * 0.7); this.particles.push({ x, y, vx: Math.cos(a) * s, vy: Math.sin(a) * s, t: 0, life: life * (0.6 + Math.random() * 0.6), color, size: size * (0.6 + Math.random() * 0.8), drag }); }
  }
  ring(x, y, color, r = 60, life = 0.6) { this.rings.push({ x, y, color, r, life, t: 0 }); }
  text(x, y, s, color = '#fff', life = 1.3) { this.texts.push({ x, y, s, color, t: 0, life }); }
  shake(a) { this.shakeAmt = Math.max(this.shakeAmt, a); }
  swing(k) { this.ring(k.x + Math.cos(k.facing) * 30, k.y + Math.sin(k.facing) * 30, 'rgba(220,220,220,0.5)', 26, 0.25); }
  hit(s, res) { this.burst(s.x, s.y, res === 'downed' ? 34 : 20, '#a3101e', 190, 0.9, 3.5); this.shake(res === 'downed' ? 9 : 6); this.hitStop = 0.06; this.flash = 0.25; this.flashColor = '160,10,20'; }
  sparks(x, y) { this.burst(x, y, 26, '#ffcf6a', 260, 0.5, 2.2, 5); }
  splinters(x, y) { this.burst(x, y, 30, '#9a7448', 200, 0.8, 3, 4); }
  gen(x, y) { this.burst(x, y, 40, '#ffe7a3', 160, 1.2, 2.5, 2); this.ring(x, y, 'rgba(255,220,140,0.8)', 3 * M, 1.2); }
}

return { FX };
};
__defs["js/core/collision.js"] = function(){
// Spatial partitioning (uniform bucket grid) for collision + line-of-sight queries.
// Solid = { x, y, w, h, blocksMove, blocksLOS, kind, ref }
class SpatialGrid {
  constructor(w, h, cell = 128) {
    this.cell = cell; this.cols = Math.ceil(w / cell); this.rows = Math.ceil(h / cell);
    this.buckets = Array.from({ length: this.cols * this.rows }, () => []);
    this.stamp = 0;
  }
  _range(x0, y0, x1, y1) {
    const c = this.cell;
    return [Math.max(0, Math.floor(x0 / c)), Math.max(0, Math.floor(y0 / c)), Math.min(this.cols - 1, Math.floor(x1 / c)), Math.min(this.rows - 1, Math.floor(y1 / c))];
  }
  insert(s) {
    const [a, b, cc, d] = this._range(s.x, s.y, s.x + s.w, s.y + s.h);
    s._stamp = 0;
    for (let j = b; j <= d; j++) for (let i = a; i <= cc; i++) this.buckets[j * this.cols + i].push(s);
  }
  remove(s) {
    const [a, b, cc, d] = this._range(s.x, s.y, s.x + s.w, s.y + s.h);
    for (let j = b; j <= d; j++) for (let i = a; i <= cc; i++) {
      const bk = this.buckets[j * this.cols + i]; const k = bk.indexOf(s); if (k >= 0) bk.splice(k, 1);
    }
  }
  query(x0, y0, x1, y1, out = []) {
    const [a, b, cc, d] = this._range(Math.min(x0, x1), Math.min(y0, y1), Math.max(x0, x1), Math.max(y0, y1));
    const st = ++this.stamp;
    for (let j = b; j <= d; j++) for (let i = a; i <= cc; i++) {
      for (const s of this.buckets[j * this.cols + i]) { if (s._stamp !== st) { s._stamp = st; out.push(s); } }
    }
    return out;
  }
}

// Segment vs AABB (slab). Returns entry t in [0,1] or -1.
function segRect(ax, ay, bx, by, r, pad = 0) {
  const dx = bx - ax, dy = by - ay;
  let t0 = 0, t1 = 1;
  const minX = r.x - pad, maxX = r.x + r.w + pad, minY = r.y - pad, maxY = r.y + r.h + pad;
  if (Math.abs(dx) < 1e-9) { if (ax < minX || ax > maxX) return -1; }
  else {
    let ta = (minX - ax) / dx, tb = (maxX - ax) / dx; if (ta > tb) [ta, tb] = [tb, ta];
    t0 = Math.max(t0, ta); t1 = Math.min(t1, tb); if (t0 > t1) return -1;
  }
  if (Math.abs(dy) < 1e-9) { if (ay < minY || ay > maxY) return -1; }
  else {
    let ta = (minY - ay) / dy, tb = (maxY - ay) / dy; if (ta > tb) [ta, tb] = [tb, ta];
    t0 = Math.max(t0, ta); t1 = Math.min(t1, tb); if (t0 > t1) return -1;
  }
  return t0;
}

class World {
  constructor(w, h) { this.w = w; this.h = h; this.grid = new SpatialGrid(w, h, 128); this._q = []; }
  add(s) { this.grid.insert(s); return s; }
  remove(s) { this.grid.remove(s); }

  losClear(ax, ay, bx, by, ignore = null) {
    this._q.length = 0;
    const list = this.grid.query(ax, ay, bx, by, this._q);
    for (const s of list) {
      if (!s.blocksLOS || s === ignore) continue;
      if (segRect(ax, ay, bx, by, s) >= 0) return false;
    }
    return true;
  }
  // Is a straight circle sweep free of movement blockers? (used for path smoothing / dash checks)
  sweepClear(ax, ay, bx, by, r, filter = null) {
    this._q.length = 0;
    const list = this.grid.query(ax - r, ay - r, bx + r, by + r, this._q);
    for (const s of list) {
      if (!s.blocksMove) continue;
      if (filter && !filter(s)) continue;
      if (segRect(ax, ay, bx, by, s, r) >= 0) return false;
    }
    return true;
  }
  firstHit(ax, ay, bx, by, r, filter = null) {
    this._q.length = 0;
    const list = this.grid.query(ax - r, ay - r, bx + r, by + r, this._q);
    let best = 2, hit = null;
    for (const s of list) {
      if (!s.blocksMove || (filter && !filter(s))) continue;
      const t = segRect(ax, ay, bx, by, s, r);
      if (t >= 0 && t < best) { best = t; hit = s; }
    }
    return hit ? { t: best, solid: hit } : null;
  }
  // Move a circle with collision resolution. filter(solid) -> true if it should block this mover.
  move(e, dx, dy, r, filter = null) {
    const steps = Math.max(1, Math.ceil(Math.hypot(dx, dy) / (r * 0.5)));
    const sx = dx / steps, sy = dy / steps;
    let collided = false;
    for (let i = 0; i < steps; i++) {
      e.x += sx; e.y += sy;
      if (this.resolve(e, r, filter)) collided = true;
    }
    e.x = Math.max(r, Math.min(this.w - r, e.x));
    e.y = Math.max(r, Math.min(this.h - r, e.y));
    return collided;
  }
  resolve(e, r, filter) {
    this._q.length = 0;
    const list = this.grid.query(e.x - r, e.y - r, e.x + r, e.y + r, this._q);
    let hit = false;
    for (const s of list) {
      if (!s.blocksMove || (filter && !filter(s))) continue;
      const cx = Math.max(s.x, Math.min(e.x, s.x + s.w));
      const cy = Math.max(s.y, Math.min(e.y, s.y + s.h));
      let dx = e.x - cx, dy = e.y - cy;
      const d2 = dx * dx + dy * dy;
      if (d2 >= r * r) continue;
      hit = true;
      if (d2 > 1e-6) {
        const d = Math.sqrt(d2); const push = r - d;
        e.x += (dx / d) * push; e.y += (dy / d) * push;
      } else {
        // center inside rect: push out along shortest axis
        const l = e.x - s.x, rr = s.x + s.w - e.x, t = e.y - s.y, b = s.y + s.h - e.y;
        const m = Math.min(l, rr, t, b);
        if (m === l) e.x = s.x - r; else if (m === rr) e.x = s.x + s.w + r; else if (m === t) e.y = s.y - r; else e.y = s.y + s.h + r;
      }
    }
    return hit;
  }
  pointBlocked(x, y, r = 0, filter = null) {
    this._q.length = 0;
    const list = this.grid.query(x - r, y - r, x + r, y + r, this._q);
    for (const s of list) {
      if (!s.blocksMove || (filter && !filter(s))) continue;
      const cx = Math.max(s.x, Math.min(x, s.x + s.w)), cy = Math.max(s.y, Math.min(y, s.y + s.h));
      if ((x - cx) ** 2 + (y - cy) ** 2 < r * r || (r === 0 && x >= s.x && x <= s.x + s.w && y >= s.y && y <= s.y + s.h)) return true;
    }
    return false;
  }
}

return { SpatialGrid, segRect, World };
};
__defs["js/maps/templates.js"] = function(){
// Chase-tile templates authored as 1m-per-character grids.
//  '#' wall   'W' window (vaultable low wall)   'P' pallet (stands in a gap)
//  'r' rock   'T' tree   'g' tall grass   'L' shack/building wall (same as # but drawn as wood)
//  'C' shipping container / metal crate (solid)   'w' water / tar pit (blocks movement, NOT sight)
//  '.' open ground
// Each template carries chase metadata the AI reasons about.
const TEMPLATES = {
  jungle_gym: {
    type: 'jungle_gym', label: 'Jungle Gym', safety: 0.75, weight: 4,
    rows: [
      '..............',
      '.#####W####...',
      '.#............',
      '.#............',
      '.P......#.....',
      '.P......#.....',
      '.#......#.....',
      '.#......####W#',
      '.####.........',
      '..............',
    ],
  },
  jungle_gym_b: {
    type: 'jungle_gym', label: 'Jungle Gym', safety: 0.7, weight: 3,
    rows: [
      '.............',
      '.####PP####..',
      '......#......',
      '......#......',
      '.#....#....#.',
      '.W....W....#.',
      '.#.........#.',
      '.##########..',
      '.............',
    ],
  },
  shack: {
    type: 'shack', label: 'Shack', safety: 0.85, weight: 2, unique: true,
    rows: [
      '..............',
      '..LLLLLLLLLL..',
      '..L........L..',
      '..L........W..',
      '..L..LL....L..',
      '..W........L..',
      '..L........L..',
      '..LLLL..LLLL..',
      '..............',
      '....##PP##....',
      '..............',
    ],
  },
  t_wall: {
    type: 't_wall', label: 'T-Wall', safety: 0.55, weight: 3,
    rows: [
      '...........',
      '.####PP###.',
      '.....#.....',
      '.....#.....',
      '.....#.....',
      '.....#.....',
      '...........',
    ],
  },
  l_wall: {
    type: 'l_wall', label: 'L-Wall', safety: 0.5, weight: 3,
    rows: [
      '.........',
      '.#####...',
      '.#.......',
      '.#.......',
      '.#....##.',
      '.P.......',
      '.P.......',
      '.#.......',
      '.........',
    ],
  },
  long_wall: {
    type: 'long_wall', label: 'Long Wall', safety: 0.6, weight: 2,
    rows: [
      '..................',
      '..gg..............',
      '.#######W########.',
      '..............gg..',
      '..................',
    ],
  },
  short_wall: {
    type: 'short_wall', label: 'Short Wall', safety: 0.3, weight: 2,
    rows: [
      '.........',
      '.###PP##.',
      '.........',
    ],
  },
  debris: {
    type: 'debris', label: 'Debris Pallet', safety: 0.25, weight: 3,
    rows: [
      '..........',
      '..rr......',
      '..rrPPrr..',
      '......rr..',
      '..........',
    ],
  },
  rocks: {
    type: 'filler', label: 'Rocks', safety: 0.15, weight: 3, filler: true,
    rows: [
      '.......',
      '..rr...',
      '.rrr.r.',
      '..r..rr',
      '.......',
    ],
  },
  trees: {
    type: 'filler', label: 'Trees', safety: 0.12, weight: 3, filler: true,
    rows: [
      '........',
      '.T...T..',
      '...g....',
      '..ggT.T.',
      '.T..g...',
      '........',
    ],
  },
  grass: {
    type: 'grass', label: 'Tall Grass', safety: 0, weight: 2, filler: true,
    rows: [
      '.ggggg.',
      'ggggggg',
      'gggTggg',
      'ggggggg',
      '.ggg...',
    ],
  },
  // Main buildings (one per map, themed)
  main_house: {
    type: 'main', label: 'Farmhouse', safety: 0.9, weight: 0, main: true,
    rows: [
      '......................',
      '.LLLLLLLLL..LLLLLLLLL.',
      '.L..................L.',
      '.L..................L.',
      '.L...LLLLLWLLLLL....L.',
      '.L...L.........L....W.',
      '.W...L.........L....L.',
      '.L...P.........P....L.',
      '.L...P.........P....L.',
      '.L...L.........L....L.',
      '.L...LLLLL..LLLL....L.',
      '.L..................L.',
      '.LLLLLLLLLLWWLLLLLLLL.',
      '......................',
    ],
  },
  main_temple: {
    type: 'main', label: 'Ruined Chapel', safety: 0.85, weight: 0, main: true,
    rows: [
      '....................',
      '..rr.....rr.....rr..',
      '..rr.###W##.....rr..',
      '.....#.....#........',
      '.....#.....P.....rr.',
      '.rr..W.....P.....rr.',
      '.rr..#.....#........',
      '.....###PP##........',
      '..rr.........rr.....',
      '..rr.........rr.....',
      '....................',
    ],
  },
  main_factory: {
    type: 'main', label: 'Foundry Hall', safety: 0.88, weight: 0, main: true,
    rows: [
      '........................',
      '.######W######..#######.',
      '.#....................#.',
      '.#...##...........##..W.',
      '.#...##....###....##..#.',
      '.P.........#.#........#.',
      '.P.........#.#........P.',
      '.#...##....#W#....##..P.',
      '.#...##...........##..#.',
      '.#....................#.',
      '.###########..##W######.',
      '........................',
    ],
  },

  // ---------------- expansion tiles (new maps) ----------------
  container_yard: {
    type: 'containers', label: 'Container Stack', safety: 0.62, weight: 3,
    rows: [
      '..............',
      '.CCCCPPCCCCC..',
      '.C..........C.',
      '.C...CC.....C.',
      '.C...CC.....C.',
      '.CCCC..CWWCCC.',
      '..............',
    ],
  },
  container_row: {
    type: 'containers', label: 'Container Row', safety: 0.35, weight: 2,
    rows: [
      '...............',
      '.CCCCC..CCCCCC.',
      '.CCCCC..CCCCCC.',
      '...............',
    ],
  },
  cabin: {
    type: 'shack', label: 'Cabin', safety: 0.78, weight: 2,
    rows: [
      '.............',
      '..LLLLWLLL...',
      '..L......L...',
      '..W......L...',
      '..L......W...',
      '..LLL..LLL...',
      '.............',
      '...rrPPrr....',
      '.............',
    ],
  },
  fence_run: {
    type: 'long_wall', label: 'Fence Run', safety: 0.58, weight: 2,
    rows: [
      '.....................',
      '.######W#######W####.',
      '.....................',
    ],
  },
  double_pallet: {
    type: 'jungle_gym', label: 'Pallet Box', safety: 0.7, weight: 2,
    rows: [
      '...........',
      '.####PP###.',
      '.#.......#.',
      '.#.......#.',
      '.###PP####.',
      '...........',
    ],
  },
  ridge_gap: {
    type: 'ridge', label: 'Rock Ridge', safety: 0.45, weight: 3,
    rows: [
      '.................',
      '.rrrrrrPPrrrrrr..',
      '.rr..........rr..',
      '.................',
    ],
  },
  ridge_window: {
    type: 'ridge', label: 'Ridge Cut', safety: 0.5, weight: 2,
    rows: [
      '..............',
      '.rrrr.........',
      '.rrrr#W#rrrrr.',
      '.........rrrr.',
      '..............',
    ],
  },
  ward_pillars: {
    type: 'room', label: 'Pillared Ward', safety: 0.55, weight: 2,
    rows: [
      '...........',
      '.##.....##.',
      '.##..P..##.',
      '.....P.....',
      '.##.....##.',
      '.##.....##.',
      '...........',
    ],
  },
  hedge_loop: {
    type: 'jungle_gym', label: 'Hedge Loop', safety: 0.66, weight: 3,
    rows: [
      '.............',
      '.TTTT.TTTTT..',
      '.T.........T.',
      '.P....TT...T.',
      '.P....TT...W.',
      '.T.........T.',
      '.TTTTTT.TTT..',
      '.............',
    ],
  },
  dock_crane: {
    type: 'containers', label: 'Crane Base', safety: 0.5, weight: 2,
    rows: [
      '..........',
      '.##....##.',
      '.##....##.',
      '....PP....',
      '.##....##.',
      '.##....##.',
      '..........',
    ],
  },
  // expansion main buildings
  main_lodge: {
    type: 'main', label: 'Mountain Lodge', safety: 0.88, weight: 0, main: true,
    rows: [
      '......................',
      '.LLLLLLLWLLLLLLLLLLLL.',
      '.L.........L........L.',
      '.L.........L........W.',
      '.L...LL....P........L.',
      '.W...LL....P...LLL..L.',
      '.L.........L...L....L.',
      '.LLLLL..LLLLLLLL..LLL.',
      '......................',
      '..rr..rPPr...rr.......',
      '......................',
    ],
  },
  main_cathedral: {
    type: 'main', label: 'Grove Cathedral', safety: 0.86, weight: 0, main: true,
    rows: [
      '...................',
      '.#######WW#######..',
      '.#..............#..',
      '.#..##......##..#..',
      '.#..##......##..#..',
      '.P..............P..',
      '.P..............P..',
      '.#..##......##..#..',
      '.#..##......##..#..',
      '.#..............#..',
      '.######....######..',
      '...................',
    ],
  },
  main_warehouse: {
    type: 'main', label: 'Dock Warehouse', safety: 0.87, weight: 0, main: true,
    rows: [
      '........................',
      '.######W#######..######.',
      '.#......................',
      '.#..CCC....CCC....CCC.#.',
      '.#..CCC....CCC....CCC.#.',
      '.P....................W.',
      '.P....................#.',
      '.#..CCC....CCC....CCC.#.',
      '.#..CCC....CCC....CCC.#.',
      '.#....................#.',
      '.###########..#####W###.',
      '........................',
    ],
  },
  main_office: {
    type: 'main', label: 'Quarry Office', safety: 0.84, weight: 0, main: true,
    rows: [
      '..................',
      '.LLLLLLLWLLLLLL...',
      '.L............L...',
      '.L...rrrr.....W...',
      '.W...rrrr.....L...',
      '.L............L...',
      '.LLLLL..LLLLPPL...',
      '..................',
    ],
  },
};

// Rotate a char grid by k*90 degrees clockwise.
function rotateRows(rows, k) {
  let g = rows.map((r) => r.split(''));
  const w = Math.max(...rows.map((r) => r.length));
  g = g.map((r) => { while (r.length < w) r.push('.'); return r; });
  for (let t = 0; t < (k % 4 + 4) % 4; t++) {
    const H = g.length, W = g[0].length;
    const n = Array.from({ length: W }, () => Array(H).fill('.'));
    for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) n[x][H - 1 - y] = g[y][x];
    g = n;
  }
  return g;
}

const SOLID = { '#': 'wall', L: 'wood', r: 'rock', T: 'tree', C: 'metal', w: 'water' };

// Parse a (rotated) grid into world pieces in meter coordinates relative to origin (ox, oy).
function parseTemplate(grid, ox, oy) {
  const H = grid.length, W = grid[0].length;
  const at = (x, y) => (x < 0 || y < 0 || x >= W || y >= H ? '.' : grid[y][x]);
  const isWallish = (c) => c === '#' || c === 'L' || c === 'W' || c === 'r' || c === 'T' || c === 'C';
  const solids = [], windows = [], pallets = [], grass = [];
  // Solid rects: merge horizontal runs per kind, then merge vertically identical runs.
  for (const ch of Object.keys(SOLID)) {
    const runs = [];
    for (let y = 0; y < H; y++) {
      let x = 0;
      while (x < W) {
        if (grid[y][x] === ch) { let x2 = x; while (x2 + 1 < W && grid[y][x2 + 1] === ch) x2++; runs.push({ x, y, w: x2 - x + 1, h: 1 }); x = x2 + 1; }
        else x++;
      }
    }
    // vertical merge
    const merged = [];
    for (const r of runs) {
      const m = merged.find((q) => q.x === r.x && q.w === r.w && q.y + q.h === r.y);
      if (m) m.h++; else merged.push({ ...r });
    }
    // trees/rocks: single cells are fine
    for (const r of merged) solids.push({ x: ox + r.x, y: oy + r.y, w: r.w, h: r.h, kind: SOLID[ch] });
  }
  const seen = new Set();
  const group = (ch, x, y) => {
    // contiguous run horizontal or vertical
    const cells = [[x, y]]; seen.add(x + ',' + y);
    let horiz = at(x + 1, y) === ch, vert = at(x, y + 1) === ch;
    if (horiz) { let k = x + 1; while (at(k, y) === ch) { cells.push([k, y]); seen.add(k + ',' + y); k++; } }
    else if (vert) { let k = y + 1; while (at(x, k) === ch) { cells.push([x, k]); seen.add(x + ',' + k); k++; } }
    return { cells, horiz, vert };
  };
  for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) {
    const c = grid[y][x];
    if (c === 'g') grass.push({ x: ox + x + 0.5, y: oy + y + 0.5 });
    if ((c === 'W' || c === 'P') && !seen.has(x + ',' + y)) {
      const g = group(c, x, y);
      const x0 = Math.min(...g.cells.map((q) => q[0])), x1 = Math.max(...g.cells.map((q) => q[0]));
      const y0 = Math.min(...g.cells.map((q) => q[1])), y1 = Math.max(...g.cells.map((q) => q[1]));
      // orientation: does the obstacle line run along x? (walls on left/right ends)
      let alongX;
      if (g.horiz) alongX = true; else if (g.vert) alongX = false;
      else alongX = isWallish(at(x0 - 1, y0)) || isWallish(at(x1 + 1, y0));
      const piece = { cx: ox + (x0 + x1 + 1) / 2, cy: oy + (y0 + y1 + 1) / 2, len: alongX ? x1 - x0 + 1 : y1 - y0 + 1, alongX };
      (c === 'W' ? windows : pallets).push(piece);
    }
  }
  return { solids, windows, pallets, grass, w: W, h: H };
}

return { TEMPLATES, rotateRows, parseTemplate };
};
__defs["js/ai/navigation.js"] = function(){
// Grid navigation (1 m cells) with special vault/pallet links, A* pathing and local
// Dijkstra distance fields used by the chase AI for loop reasoning.
const { M } = __req("js/core/util.js");

class Heap { // min-heap of (key, val)
  constructor(n = 4096) { this.k = new Float32Array(n); this.v = new Int32Array(n); this.n = 0; }
  clear() { this.n = 0; }
  push(key, val) {
    if (this.n >= this.k.length) { const k = new Float32Array(this.k.length * 2); k.set(this.k); this.k = k; const v = new Int32Array(this.v.length * 2); v.set(this.v); this.v = v; }
    let i = this.n++; this.k[i] = key; this.v[i] = val;
    while (i > 0) { const p = (i - 1) >> 1; if (this.k[p] <= this.k[i]) break; this._sw(i, p); i = p; }
  }
  pop() {
    const v = this.v[0]; this.n--;
    if (this.n > 0) { this.k[0] = this.k[this.n]; this.v[0] = this.v[this.n]; let i = 0;
      for (;;) { const l = 2 * i + 1, r = l + 1; let m = i;
        if (l < this.n && this.k[l] < this.k[m]) m = l; if (r < this.n && this.k[r] < this.k[m]) m = r;
        if (m === i) break; this._sw(i, m); i = m; } }
    return v;
  }
  _sw(a, b) { const k = this.k[a]; this.k[a] = this.k[b]; this.k[b] = k; const v = this.v[a]; this.v[a] = this.v[b]; this.v[b] = v; }
}

const SQ2 = Math.SQRT2;
const DIRS = [[1, 0, 1], [-1, 0, 1], [0, 1, 1], [0, -1, 1], [1, 1, SQ2], [1, -1, SQ2], [-1, 1, SQ2], [-1, -1, SQ2]];

class NavGrid {
  constructor(widthPx, heightPx) {
    this.cols = Math.ceil(widthPx / M); this.rows = Math.ceil(heightPx / M);
    const n = this.cols * this.rows;
    this.blocked = new Uint8Array(n);
    this.pallet = new Int32Array(n).fill(-1); // index into pallets when cell is a pallet gap
    this.grass = new Uint8Array(n);
    this.deadZone = new Float32Array(n);      // 0 = covered by tiles, 1 = pure dead zone
    this.tileOf = new Int16Array(n).fill(-1);
    this.links = []; this.linksAt = new Map();
    this.pallets = [];
    // A* scratch
    this.g = new Float32Array(n); this.par = new Int32Array(n); this.seen = new Uint32Array(n); this.closed = new Uint32Array(n);
    this.parLink = new Int32Array(n); this.stamp = 1; this.heap = new Heap();
    this.searches = 0;
  }
  idx(cx, cy) { return cy * this.cols + cx; }
  cellOf(x, y) { return [Math.max(0, Math.min(this.cols - 1, Math.floor(x / M))), Math.max(0, Math.min(this.rows - 1, Math.floor(y / M)))]; }
  idxAt(x, y) { const [c, r] = this.cellOf(x, y); return this.idx(c, r); }
  center(i) { return { x: (i % this.cols + 0.5) * M, y: (Math.floor(i / this.cols) + 0.5) * M }; }
  isBlocked(i) {
    if (this.blocked[i]) return true;
    const p = this.pallet[i]; return p >= 0 && this.pallets[p].state === 'down';
  }

  build(solids, pallets, windows, inflate = 0.45 * M) {
    this.pallets = pallets;
    for (let r = 0; r < this.rows; r++) for (let c = 0; c < this.cols; c++) {
      const x = (c + 0.5) * M, y = (r + 0.5) * M;
      for (const s of solids) {
        if (!s.blocksMove || s.kind === 'pallet') continue;
        if (x > s.x - inflate && x < s.x + s.w + inflate && y > s.y - inflate && y < s.y + s.h + inflate) { this.blocked[this.idx(c, r)] = 1; break; }
      }
    }
    pallets.forEach((p, k) => {
      const pad = 0.45 * M;
      for (let r = Math.floor((p.rect.y - pad) / M); r <= Math.floor((p.rect.y + p.rect.h + pad) / M); r++)
        for (let c = Math.floor((p.rect.x - pad) / M); c <= Math.floor((p.rect.x + p.rect.w + pad) / M); c++) {
          if (c < 0 || r < 0 || c >= this.cols || r >= this.rows) continue;
          const x = (c + 0.5) * M, y = (r + 0.5) * M;
          if (x > p.rect.x - pad && x < p.rect.x + p.rect.w + pad && y > p.rect.y - pad && y < p.rect.y + p.rect.h + pad) {
            const i = this.idx(c, r); if (!this.blocked[i]) this.pallet[i] = k;
          }
        }
      this._addLink(p, 'pallet');
    });
    for (const w of windows) this._addLink(w, 'window');
  }
  _addLink(obj, kind) {
    const [A, B] = obj.sides(1.25 * M);
    let a = this.nearestOpen(A.x, A.y, 2, true), b = this.nearestOpen(B.x, B.y, 2, true);
    if (a < 0 || b < 0 || a === b) return;
    const link = { a, b, kind, ref: obj, id: this.links.length };
    this.links.push(link);
    for (const c of [a, b]) { if (!this.linksAt.has(c)) this.linksAt.set(c, []); this.linksAt.get(c).push(link); }
    obj.navLink = link;
  }
  nearestOpen(x, y, maxR = 6, ignorePallets = false) {
    const [c0, r0] = this.cellOf(x, y);
    let best = -1, bd = 1e9;
    for (let rad = 0; rad <= maxR; rad++) {
      for (let dy = -rad; dy <= rad; dy++) for (let dx = -rad; dx <= rad; dx++) {
        if (Math.max(Math.abs(dx), Math.abs(dy)) !== rad) continue;
        const c = c0 + dx, r = r0 + dy; if (c < 0 || r < 0 || c >= this.cols || r >= this.rows) continue;
        const i = this.idx(c, r);
        if (this.blocked[i] || (!ignorePallets && this.isBlocked(i))) continue;
        if (ignorePallets && this.pallet[i] >= 0) continue;
        const cc = this.center(i); const d = (cc.x - x) ** 2 + (cc.y - y) ** 2;
        if (d < bd) { bd = d; best = i; }
      }
      if (best >= 0) return best;
    }
    return best;
  }

  // A* from world point to world point. agent: { linkCost(link)->seconds-equivalent meters or Infinity, cellCost(i)->extra }
  findPath(sx, sy, tx, ty, agent = {}, maxNodes = 12000) {
    this.searches++;
    const s = this.nearestOpen(sx, sy), t = this.nearestOpen(tx, ty);
    if (s < 0 || t < 0) return null;
    const st = ++this.stamp; const h = this.heap; h.clear();
    const cols = this.cols;
    const tc = t % cols, tr = (t / cols) | 0;
    const heur = (i) => { const dx = Math.abs(i % cols - tc), dy = Math.abs(((i / cols) | 0) - tr); return (dx + dy) + (SQ2 - 2) * Math.min(dx, dy); };
    this.g[s] = 0; this.seen[s] = st; this.par[s] = -1; this.parLink[s] = -1; h.push(heur(s), s);
    let expanded = 0, found = false;
    const cellCost = agent.cellCost, linkCost = agent.linkCost;
    while (h.n) {
      const cur = h.pop();
      if (this.closed[cur] === st) continue; this.closed[cur] = st;
      if (cur === t) { found = true; break; }
      if (++expanded > maxNodes) break;
      const cx = cur % cols, cy = (cur / cols) | 0;
      for (const [dx, dy, w] of DIRS) {
        const nx = cx + dx, ny = cy + dy; if (nx < 0 || ny < 0 || nx >= cols || ny >= this.rows) continue;
        const ni = ny * cols + nx; if (this.isBlocked(ni)) continue;
        if (dx && dy && (this.isBlocked(cy * cols + nx) || this.isBlocked(ny * cols + cx))) continue;
        const ng = this.g[cur] + w + (cellCost ? cellCost(ni) : 0);
        if (this.seen[ni] !== st || ng < this.g[ni]) { this.seen[ni] = st; this.g[ni] = ng; this.par[ni] = cur; this.parLink[ni] = -1; h.push(ng + heur(ni), ni); }
      }
      const ls = this.linksAt.get(cur);
      if (ls && linkCost) for (const L of ls) {
        const c = linkCost(L); if (!isFinite(c)) continue;
        const ni = L.a === cur ? L.b : L.a;
        const ng = this.g[cur] + c;
        if (this.seen[ni] !== st || ng < this.g[ni]) { this.seen[ni] = st; this.g[ni] = ng; this.par[ni] = cur; this.parLink[ni] = L.id; h.push(ng + heur(ni), ni); }
      }
    }
    if (!found) return null;
    const cells = []; let c = t;
    while (c >= 0) { cells.push({ i: c, link: this.parLink[c] }); c = this.par[c]; }
    cells.reverse();
    const pts = cells.map((q, k) => {
      const p = this.center(q.i);
      // a link index on node k means we arrived at k by crossing that link from k-1
      return { x: p.x, y: p.y, viaLink: q.link >= 0 ? this.links[q.link] : null };
    });
    pts[pts.length - 1] = { x: tx, y: ty, viaLink: pts[pts.length - 1].viaLink };
    return { pts, cost: this.g[t] };
  }

  // String-pull a path using world sweeps; never smooths across link crossings.
  smooth(world, path, r) {
    if (!path || path.length < 3) return path;
    const out = [path[0]]; let i = 0;
    while (i < path.length - 1) {
      let j = path.length - 1;
      // don't skip a link: find first link index after i
      for (let k = i + 1; k < path.length; k++) if (path[k].viaLink) { j = Math.min(j, k - 1 > i ? k - 1 : k); break; }
      while (j > i + 1 && !world.sweepClear(path[i].x, path[i].y, path[j].x, path[j].y, r, (s) => s.kind !== 'gate' || !s.ref.open)) j--;
      out.push(path[j]); i = j;
    }
    return out;
  }

  // Local Dijkstra distance field (meters) from a source, within a square window.
  // Returns { ox, oy, size, d: Float32Array } where d is Infinity for unreachable.
  field(sx, sy, half, linkCost, cellCost = null, maxCost = 80) {
    const [c0, r0] = this.cellOf(sx, sy);
    const size = half * 2 + 1, ox = c0 - half, oy = r0 - half;
    const d = new Float32Array(size * size).fill(Infinity);
    const at = (x, y) => { const [c, r] = this.cellOf(x, y); const lc = c - ox, lr = r - oy; return lc < 0 || lr < 0 || lc >= size || lr >= size ? Infinity : d[lr * size + lc]; };
    const h = new Heap(1024);
    const src = this.nearestOpen(sx, sy, 3);
    if (src < 0) return { ox, oy, size, d, at };
    const lid = (i) => { const c = i % this.cols - ox, r = ((i / this.cols) | 0) - oy; return c < 0 || r < 0 || c >= size || r >= size ? -1 : r * size + c; };
    const sL = lid(src); if (sL < 0) return { ox, oy, size, d, at };
    d[sL] = 0; h.push(0, src);
    const cols = this.cols;
    while (h.n) {
      const kv = h.k[0]; const cur = h.pop(); const cl = lid(cur);
      if (kv > d[cl]) continue; if (kv > maxCost) break;
      const cx = cur % cols, cy = (cur / cols) | 0;
      for (const [dx, dy, w] of DIRS) {
        const nx = cx + dx, ny = cy + dy; if (nx < 0 || ny < 0 || nx >= cols || ny >= this.rows) continue;
        const ni = ny * cols + nx; const nl = lid(ni); if (nl < 0 || this.isBlocked(ni)) continue;
        if (dx && dy && (this.isBlocked(cy * cols + nx) || this.isBlocked(ny * cols + cx))) continue;
        const nd = kv + w + (cellCost ? cellCost(ni) : 0);
        if (nd < d[nl]) { d[nl] = nd; h.push(nd, ni); }
      }
      const ls = this.linksAt.get(cur);
      if (ls && linkCost) for (const L of ls) {
        const c = linkCost(L); if (!isFinite(c)) continue;
        const ni = L.a === cur ? L.b : L.a; const nl = lid(ni); if (nl < 0) continue;
        const nd = kv + c; if (nd < d[nl]) { d[nl] = nd; h.push(nd, ni); }
      }
    }
    return { ox, oy, size, d, at };
  }
}

return { NavGrid };
};
__defs["js/maps/map.js"] = function(){
// GameMap: procedural layout from a data definition + chase-tile metadata (loop graph),
// dead-zone analysis, spatial collision world and navigation grid.
const { M, mulberry32, rr, ri, pick, dist, clamp, smoothstep } = __req("js/core/util.js");
const { World } = __req("js/core/collision.js");
const { TEMPLATES, rotateRows, parseTemplate } = __req("js/maps/templates.js");
const { NavGrid } = __req("js/ai/navigation.js");
const { Window, Pallet, Generator, Hook, Chest, Totem, ExitGate, Hatch } = __req("js/entities/objects.js");

const GEN_NAMES = 'ABCDEFGHIJKL'.split('');

function weightedPick(rng, weights) {
  const entries = Object.entries(weights).filter(([, w]) => w > 0);
  const tot = entries.reduce((a, [, w]) => a + w, 0); let r = rng() * tot;
  for (const [k, w] of entries) { r -= w; if (r <= 0) return k; }
  return entries[0][0];
}

class GameMap {
  constructor(def, opts = {}) {
    this.def = def; this.name = def.name; this.theme = def.theme;
    this.seed = opts.seed ?? Math.floor(Math.random() * 1e9);
    this.rng = mulberry32(this.seed);
    this.wM = def.size[0]; this.hM = def.size[1];
    this.w = this.wM * M; this.h = this.hM * M;
    this.genCount = opts.gens ?? def.gens; this.hookCount = opts.hooks ?? def.hooks; this.gateCount = opts.gates ?? 2;
    this.solids = []; this.windows = []; this.pallets = []; this.gens = []; this.hooks = []; this.chests = []; this.totems = [];
    this.gates = []; this.tiles = []; this.grassCells = []; this.decor = [];
    this.world = new World(this.w, this.h);
    this.occ = new Uint8Array(this.wM * this.hM); // meter occupancy for placement
    if (opts.custom) opts.custom(this); else this.generate();
    this.finalize();
  }

  occupied(x0, y0, w, h) {
    for (let y = Math.floor(y0); y < Math.ceil(y0 + h); y++) for (let x = Math.floor(x0); x < Math.ceil(x0 + w); x++) {
      if (x < 0 || y < 0 || x >= this.wM || y >= this.hM) return true;
      if (this.occ[y * this.wM + x]) return true;
    }
    return false;
  }
  mark(x0, y0, w, h, v = 1) {
    for (let y = Math.floor(y0); y < Math.ceil(y0 + h); y++) for (let x = Math.floor(x0); x < Math.ceil(x0 + w); x++)
      if (x >= 0 && y >= 0 && x < this.wM && y < this.hM) this.occ[y * this.wM + x] = v;
  }

  // Place a template with origin (meters). Returns tile record.
  placeTemplate(key, ox, oy, rot) {
    const tpl = TEMPLATES[key];
    const grid = rotateRows(tpl.rows, rot);
    const p = parseTemplate(grid, ox, oy);
    const tile = {
      id: this.tiles.length, key, type: tpl.type, label: tpl.label, baseSafety: tpl.safety, filler: !!tpl.filler, main: !!tpl.main,
      x: (ox + p.w / 2) * M, y: (oy + p.h / 2) * M, rect: { x: ox * M, y: oy * M, w: p.w * M, h: p.h * M },
      radius: Math.hypot(p.w, p.h) / 2 * M, windows: [], pallets: [], solids: [], connected: [],
    };
    for (const s of p.solids) {
      const solid = { x: s.x * M, y: s.y * M, w: s.w * M, h: s.h * M, kind: s.kind, blocksMove: true, blocksLOS: s.kind !== 'water', tile: tile.id };
      this.solids.push(solid); tile.solids.push(solid);
    }
    for (const w of p.windows) { const o = new Window(w); o.tile = tile.id; this.windows.push(o); tile.windows.push(o); }
    for (const pl of p.pallets) { const o = new Pallet(pl); o.tile = tile.id; this.pallets.push(o); tile.pallets.push(o); }
    for (const g of p.grass) this.grassCells.push(g);
    this.tiles.push(tile);
    return tile;
  }

  tryPlace(key, margin, attempts = 60, near = null) {
    const tpl = TEMPLATES[key]; const rng = this.rng;
    for (let a = 0; a < attempts; a++) {
      const rot = ri(rng, 0, 3);
      const g = rotateRows(tpl.rows, rot); const w = g[0].length, h = g.length;
      let ox, oy;
      if (near) { ox = Math.round(near.x - w / 2 + rr(rng, -4, 4)); oy = Math.round(near.y - h / 2 + rr(rng, -4, 4)); }
      else { ox = ri(rng, 5, this.wM - w - 5); oy = ri(rng, 5, this.hM - h - 5); }
      if (ox < 4 || oy < 4 || ox + w > this.wM - 4 || oy + h > this.hM - 4) continue;
      if (this.occupied(ox - margin, oy - margin, w + margin * 2, h + margin * 2)) continue;
      this.mark(ox - 1, oy - 1, w + 2, h + 2);
      return this.placeTemplate(key, ox, oy, rot);
    }
    return null;
  }

  generate() {
    const d = this.def, rng = this.rng;
    const B = 3; // border thickness meters
    this.border = B;
    this.mark(0, 0, this.wM, B + 1); this.mark(0, this.hM - B - 1, this.wM, B + 1); this.mark(0, 0, B + 1, this.hM); this.mark(this.wM - B - 1, 0, B + 1, this.hM);
    // Exit gates on opposite sides
    const horizontalPair = rng() < 0.5;
    const sides = this.gateCount >= 2 ? (horizontalPair ? ['n', 's'] : ['w', 'e']) : [pick(rng, ['n', 's', 'e', 'w'])];
    if (this.gateCount >= 3) sides.push(horizontalPair ? pick(rng, ['w', 'e']) : pick(rng, ['n', 's']));
    const gaps = { n: [], s: [], e: [], w: [] };
    sides.forEach((side, k) => {
      const along = side === 'n' || side === 's' ? this.wM : this.hM;
      const c = Math.round(rr(rng, along * 0.25, along * 0.75));
      gaps[side].push([c - 2, c + 2]);
      const gw = 4;
      let door, exit, sw;
      if (side === 'n') { door = { x: (c - 2) * M, y: (B - 0.5) * M, w: gw * M, h: 0.5 * M }; exit = { x: (c - 2) * M, y: 0, w: gw * M, h: (B - 0.5) * M }; sw = { x: (c + 3.5) * M, y: (B + 1.8) * M }; this.mark(c - 4, B, 9, 4); }
      if (side === 's') { door = { x: (c - 2) * M, y: (this.hM - B) * M, w: gw * M, h: 0.5 * M }; exit = { x: (c - 2) * M, y: (this.hM - B + 0.5) * M, w: gw * M, h: (B - 0.5) * M }; sw = { x: (c + 3.5) * M, y: (this.hM - B - 1.8) * M }; this.mark(c - 4, this.hM - B - 4, 9, 4); }
      if (side === 'w') { door = { x: (B - 0.5) * M, y: (c - 2) * M, w: 0.5 * M, h: gw * M }; exit = { x: 0, y: (c - 2) * M, w: (B - 0.5) * M, h: gw * M }; sw = { x: (B + 1.8) * M, y: (c + 3.5) * M }; this.mark(B, c - 4, 4, 9); }
      if (side === 'e') { door = { x: (this.wM - B) * M, y: (c - 2) * M, w: 0.5 * M, h: gw * M }; exit = { x: (this.wM - B + 0.5) * M, y: (c - 2) * M, w: (B - 0.5) * M, h: gw * M }; sw = { x: (this.wM - B - 1.8) * M, y: (c + 3.5) * M }; this.mark(this.wM - B - 4, c - 4, 4, 9); }
      this.gates.push(new ExitGate({ x: sw.x, y: sw.y, side, doorRect: door, exitRect: exit, name: k === 0 ? 'Gate I' : k === 1 ? 'Gate II' : 'Gate III' }));
    });
    // Border walls with gaps
    const seg = (side, a, b) => {
      if (b <= a) return;
      const r = side === 'n' ? { x: a * M, y: 0, w: (b - a) * M, h: B * M } : side === 's' ? { x: a * M, y: (this.hM - B) * M, w: (b - a) * M, h: B * M }
        : side === 'w' ? { x: 0, y: a * M, w: B * M, h: (b - a) * M } : { x: (this.wM - B) * M, y: a * M, w: B * M, h: (b - a) * M };
      this.solids.push({ ...r, kind: 'border', blocksMove: true, blocksLOS: true });
    };
    for (const side of ['n', 's', 'e', 'w']) {
      const along = side === 'n' || side === 's' ? this.wM : this.hM;
      let start = side === 'n' || side === 's' ? 0 : B; const end = side === 'n' || side === 's' ? along : along - B;
      const gs = gaps[side].sort((a, b) => a[0] - b[0]);
      for (const [g0, g1] of gs) { seg(side, start, g0); start = g1; }
      seg(side, start, end);
    }
    // Map-specific large-scale structure (each new map has its own layout style)
    if (d.layout === 'compound') this.buildCompound(d.compound);
    if (d.layout === 'channels') this.buildChannels(d.channels);
    if (d.layout === 'pit') this.buildPit(d.pit);
    // Main building near the center (or where the layout asks)
    if (d.main) this.tryPlace(d.main, 3, 80, d.mainAt ? { x: this.wM * d.mainAt[0], y: this.hM * d.mainAt[1] } : { x: this.wM / 2 + rr(rng, -8, 8), y: this.hM / 2 + rr(rng, -8, 8) });
    // Chase tiles
    let placed = 0, shack = false;
    for (let i = 0; i < d.tiles * 4 && placed < d.tiles; i++) {
      let key = weightedPick(rng, d.weights);
      if (TEMPLATES[key].unique && shack) continue;
      if (this.tryPlace(key, 4, 25)) { placed++; if (key === 'shack') shack = true; }
    }
    // Filler LOS blockers / grass
    let f = 0;
    for (let i = 0; i < d.fillers * 4 && f < d.fillers; i++) if (this.tryPlace(weightedPick(rng, d.fillerWeights), 2, 15)) f++;
    // Dense woodland outside a central clearing (Hallowed Grove)
    if (d.layout === 'clearing') this.buildForest(d.forest);
    // Loose grass patches (cornfields / ferns)
    for (let i = 0; i < d.grassPatches; i++) {
      const cx = rr(rng, 6, this.wM - 6), cy = rr(rng, 6, this.hM - 6), rad = rr(rng, 2, 4.5);
      for (let y = Math.floor(cy - rad); y <= cy + rad; y++) for (let x = Math.floor(cx - rad); x <= cx + rad; x++) {
        if ((x + 0.5 - cx) ** 2 + (y + 0.5 - cy) ** 2 < rad * rad && rng() < 0.85) this.grassCells.push({ x: x + 0.5, y: y + 0.5 });
      }
    }
  }

  // Register a hand-built structure as one or more chase tiles the AI can reason about.
  addTileRecord(meta, rect, windows, pallets) {
    const tile = { id: this.tiles.length, key: meta.key, type: meta.type, label: meta.label, baseSafety: meta.safety, filler: false, main: !!meta.main,
      x: (rect.x + rect.w / 2) * M, y: (rect.y + rect.h / 2) * M, rect: { x: rect.x * M, y: rect.y * M, w: rect.w * M, h: rect.h * M },
      radius: Math.hypot(rect.w, rect.h) / 2 * M, windows, pallets, solids: [], connected: [] };
    for (const o of [...windows, ...pallets]) o.tile = tile.id;
    this.tiles.push(tile); return tile;
  }
  // Parse a char grid (template syntax) at (ox, oy) without registering a tile.
  placeRows(grid, ox, oy, kindLOS = true) {
    const p = parseTemplate(grid, ox, oy); const out = { windows: [], pallets: [], solids: [] };
    for (const q of p.solids) { const solid = { x: q.x * M, y: q.y * M, w: q.w * M, h: q.h * M, kind: q.kind, blocksMove: true, blocksLOS: q.kind !== 'water' && kindLOS }; this.solids.push(solid); out.solids.push(solid); }
    for (const w of p.windows) { const o = new Window(w); this.windows.push(o); out.windows.push(o); }
    for (const pl of p.pallets) { const o = new Pallet(pl); this.pallets.push(o); out.pallets.push(o); }
    for (const g of p.grass) this.grassCells.push(g);
    return out;
  }
  // SUNKEN ASYLUM: a walled compound split into wards. Walls between wards have doorways;
  // some doorways hold pallets and some walls have windows, so every ward is a small loop
  // and the compound is a maze of short sightlines.
  buildCompound(c = {}) {
    const rng = this.rng; const cols = c.cols || 4, rows = c.rows || 3, rw = c.room || 15, rh = c.roomH || 13;
    const W = cols * rw + 1, H = rows * rh + 1;
    const ox = Math.round((this.wM - W) / 2), oy = Math.round((this.hM - H) / 2);
    const g = Array.from({ length: H }, () => Array(W).fill('.'));
    for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) if (x % rw === 0 || y % rh === 0) g[y][x] = '#';
    // carve each wall segment: a doorway (sometimes with a pallet), sometimes a window
    const carve = (cells, outer) => {
      const n = cells.length; const door = ri(rng, 3, n - 6);
      const pal = !outer && rng() < 0.45;
      for (let k = 0; k < (pal ? 2 : 3); k++) { const [x, y] = cells[door + k]; g[y][x] = pal ? 'P' : '.'; }
      if (rng() < (outer ? 0.35 : 0.5)) { const wpos = door > n / 2 ? ri(rng, 1, Math.max(1, door - 4)) : ri(rng, Math.min(n - 3, door + 5), n - 3); for (let k = 0; k < 2; k++) { const [x, y] = cells[wpos + k] || []; if (x !== undefined && g[y][x] === '#') g[y][x] = 'W'; } }
    };
    for (let ry = 0; ry <= rows; ry++) for (let cx = 0; cx < cols; cx++) { const y = ry * rh; const cells = []; for (let x = cx * rw + 1; x < (cx + 1) * rw; x++) cells.push([x, y]); const outer = ry === 0 || ry === rows; if (!outer || rng() < 0.6) carve(cells, outer); }
    for (let rx = 0; rx <= cols; rx++) for (let cy = 0; cy < rows; cy++) { const x = rx * rw; const cells = []; for (let y = cy * rh + 1; y < (cy + 1) * rh; y++) cells.push([x, y]); const outer = rx === 0 || rx === cols; if (!outer || rng() < 0.6) carve(cells, outer); }
    // a pillar or bed-row in some wards gives a loop inside the room
    for (let cy = 0; cy < rows; cy++) for (let cx = 0; cx < cols; cx++) {
      if (rng() < 0.55) { const px = cx * rw + ri(rng, 4, rw - 6), py = cy * rh + ri(rng, 4, rh - 6); const kind = rng() < 0.5 ? '#' : 'C'; for (let y = py; y < py + 2; y++) for (let x = px; x < px + 3; x++) g[y][x] = kind; }
    }
    const placed = this.placeRows(g, ox, oy);
    this.mark(ox - 2, oy - 2, W + 4, H + 4);
    // each ward becomes a chase tile with the pallets/windows in its own walls
    for (let cy = 0; cy < rows; cy++) for (let cx = 0; cx < cols; cx++) {
      const r = { x: ox + cx * rw, y: oy + cy * rh, w: rw + 1, h: rh + 1 };
      const inR = (o) => o.x / M >= r.x - 0.5 && o.x / M <= r.x + r.w + 0.5 && o.y / M >= r.y - 0.5 && o.y / M <= r.y + r.h + 0.5 && o.tile === undefined;
      const ws = placed.windows.filter(inR), ps = placed.pallets.filter(inR);
      this.addTileRecord({ key: 'ward', type: 'room', label: 'Ward', safety: 0.5 + 0.08 * Math.min(3, ws.length + ps.length), main: cx === Math.floor(cols / 2) && cy === Math.floor(rows / 2) }, r, ws, ps);
    }
    this.compound = { x: ox * M, y: oy * M, w: W * M, h: H * M };
  }
  // SALTMERE DOCKS: water channels cut the map into strips; bridges are the only crossings,
  // but you can see (and throw) across the water.
  buildChannels(c = {}) {
    const rng = this.rng, B = this.border; const n = c.count || 2, width = c.width || 4;
    for (let k = 1; k <= n; k++) {
      const y0 = Math.round(this.hM * k / (n + 1) - width / 2 + rr(rng, -3, 3));
      let x = B; const bridges = [];
      for (let bx = ri(rng, 10, 18); bx < this.wM - 10; bx += ri(rng, 20, 30)) bridges.push(bx);
      for (const bx of [...bridges, this.wM - B]) {
        if (bx - x > 0) { this.solids.push({ x: x * M, y: y0 * M, w: (bx - x) * M, h: width * M, kind: 'water', blocksMove: true, blocksLOS: false }); this.mark(x, y0 - 1, bx - x, width + 2); }
        x = bx + 4;
      }
      this.channels = (this.channels || []).concat({ y: y0 * M, h: width * M, bridges: bridges.map((b) => (b + 2) * M) });
    }
  }
  // EMBER QUARRY: a sunken tar pit in the middle (blocks movement, open sightlines) ringed
  // by rock shelves, so most routes bend around the centre.
  buildPit(c = {}) {
    const rng = this.rng; const r = c.radius || 11; const cx = this.wM / 2 + rr(rng, -6, 6), cy = this.hM / 2 + rr(rng, -6, 6);
    for (let y = Math.floor(cy - r); y <= cy + r; y++) {
      let x0 = null;
      for (let x = Math.floor(cx - r * 1.3); x <= cx + r * 1.3 + 1; x++) {
        const inside = ((x + 0.5 - cx) / 1.3) ** 2 + (y + 0.5 - cy) ** 2 < r * r * (0.85 + 0.15 * Math.sin(x * 0.7 + y));
        if (inside && x0 === null) x0 = x;
        if ((!inside || x > cx + r * 1.3) && x0 !== null) { this.solids.push({ x: x0 * M, y: y * M, w: (x - x0) * M, h: M, kind: 'water', blocksMove: true, blocksLOS: false }); x0 = null; }
      }
    }
    this.mark(cx - r * 1.3 - 2, cy - r - 2, r * 2.6 + 4, r * 2 + 4);
    // rock shelves around the rim with a few pallet gaps
    for (let k = 0; k < (c.shelves || 4); k++) {
      const a = k * Math.PI * 2 / (c.shelves || 4) + rr(rng, -0.3, 0.3); const rx = Math.round(cx + Math.cos(a) * (r * 1.3 + 4)) - 7, ry = Math.round(cy + Math.sin(a) * (r + 4)) - 2;
      if (!this.occupied(rx, ry, 17, 4)) { this.mark(rx - 1, ry - 1, 19, 6); this.placeTemplate('ridge_gap', rx, ry, Math.abs(Math.sin(a)) > 0.7 ? 0 : 1); }
    }
    this.pit = { x: cx * M, y: cy * M, r: r * M };
  }
  // HALLOWED GROVE: thick woods everywhere except a clearing around the cathedral.
  buildForest(f = {}) {
    const rng = this.rng; const cx = this.wM / 2, cy = this.hM / 2, clear = f.clear || 20;
    let n = 0;
    for (let t = 0; t < (f.count || 160) * 6 && n < (f.count || 160); t++) {
      const x = ri(rng, 6, this.wM - 7), y = ri(rng, 6, this.hM - 7);
      if (Math.hypot(x - cx, y - cy) < clear) continue;
      if (this.occupied(x - 1, y - 1, 3, 3)) continue;
      this.mark(x, y, 1, 1); this.solids.push({ x: x * M, y: y * M, w: M, h: M, kind: 'tree', blocksMove: true, blocksLOS: true }); n++;
    }
  }

  // Put solids in world, build nav, then place gens/hooks/etc. on open ground.
  finalize() {
    const rng = this.rng;
    for (const s of this.solids) this.world.add(s);
    for (const w of this.windows) this.world.add(w.solid);
    for (const g of this.gates) this.world.add(g.solid);
    this.nav = new NavGrid(this.w, this.h);
    this.nav.build([...this.solids, ...this.windows.map((w) => w.solid), ...this.gates.map((g) => g.solid)], this.pallets, this.windows);
    for (const g of this.grassCells) {
      const i = this.nav.idxAt(g.x * M, g.y * M); this.nav.grass[i] = 1;
    }
    const isClear = (xm, ym, r) => {
      for (let y = Math.floor(ym - r); y <= ym + r; y++) for (let x = Math.floor(xm - r); x <= xm + r; x++) {
        if (x < 0 || y < 0 || x >= this.wM || y >= this.hM) return false;
        const i = y * this.nav.cols + x; if (this.nav.blocked[i] || this.nav.pallet[i] >= 0) return false;
      }
      return true;
    };
    const taken = [];
    const farPick = (count, minSpace, clearR, prefer = null, tries = 600) => {
      const out = [];
      for (let n = 0; n < count; n++) {
        let best = null, bestScore = -1;
        for (let t = 0; t < 60; t++) {
          let xm, ym;
          if (prefer && rng() < 0.7 && this.tiles.length) {
            const tl = pick(rng, this.tiles); const a = rng() * Math.PI * 2; const rad = tl.radius / M + rr(rng, 1.5, 4);
            xm = Math.round(tl.x / M + Math.cos(a) * rad) + 0.5; ym = Math.round(tl.y / M + Math.sin(a) * rad) + 0.5;
          } else { xm = Math.round(rr(rng, 6, this.wM - 6)) + 0.5; ym = Math.round(rr(rng, 6, this.hM - 6)) + 0.5; }
          if (!isClear(xm, ym, clearR)) continue;
          let md = 1e9; for (const p of [...out, ...taken]) md = Math.min(md, dist(xm, ym, p.x, p.y));
          if (md < minSpace) continue;
          const score = Math.min(md, 60) * rr(rng, 0.75, 1);
          if (score > bestScore) { bestScore = score; best = { x: xm, y: ym }; }
        }
        if (best) out.push(best); else if (tries-- > 0) n--;
        if (tries <= 0) break;
      }
      return out;
    };
    if (!this.customObjects) {
      // Generators: spread out, usually near structures.
      const gp = farPick(this.genCount, 20, 2, true);
      gp.forEach((p, k) => { const g = new Generator(p.x * M, p.y * M, GEN_NAMES[k]); this.gens.push(g); taken.push(p); });
      const hp = farPick(this.hookCount, 13, 1, false);
      hp.forEach((p) => { this.hooks.push(new Hook(p.x * M, p.y * M)); taken.push(p); });
      farPick(4, 18, 1, true).forEach((p) => { this.chests.push(new Chest(p.x * M, p.y * M)); taken.push(p); });
      farPick(5, 16, 1, true).forEach((p) => { this.totems.push(new Totem(p.x * M, p.y * M)); taken.push(p); });
      const hatchP = farPick(1, 10, 1, false)[0] || { x: this.wM / 2, y: this.hM / 2 };
      this.hatch = new Hatch(hatchP.x * M, hatchP.y * M);
      // Spawns: survivors together, killer far away.
      const sp = farPick(1, 14, 2, false)[0];
      let kp = null, best = -1;
      for (let t = 0; t < 80; t++) { const xm = Math.round(rr(rng, 8, this.wM - 8)) + 0.5, ym = Math.round(rr(rng, 8, this.hM - 8)) + 0.5; if (!isClear(xm, ym, 1)) continue; const d = dist(xm, ym, sp.x, sp.y); if (d > best) { best = d; kp = { x: xm, y: ym }; } }
      this.spawn = { killer: { x: kp.x * M, y: kp.y * M }, survivors: [] };
      for (let k = 0; k < 4; k++) { const i = this.nav.nearestOpen((sp.x + (k % 2) * 2 - 1) * M, (sp.y + Math.floor(k / 2) * 2 - 1) * M, 5); this.spawn.survivors.push(this.nav.center(i)); }
      this.trapSpawns = farPick(6, 10, 1, true).map((p) => ({ x: p.x * M, y: p.y * M }));
    }
    for (const g of this.gens) this.world.add(g.solid);
    // Re-block nav cells under generators
    for (const g of this.gens) for (const [x, y] of [[0, 0], [1, 0], [-1, 0], [0, 1], [0, -1]]) { const i = this.nav.idxAt(g.x + x * M, g.y + y * M); this.nav.blocked[i] = 1; }
    this.analyzeTiles();
  }

  // Loop graph + dead zones: the metadata the chase AI reasons about.
  analyzeTiles() {
    const nav = this.nav;
    for (const t of this.tiles) {
      t.connected = this.tiles.filter((o) => o !== t && dist(o.x, o.y, t.x, t.y) < 34 * M).map((o) => o.id);
      // cells inside tile footprint
      const r = t.rect;
      for (let y = Math.floor(r.y / M) - 1; y <= Math.ceil((r.y + r.h) / M); y++) for (let x = Math.floor(r.x / M) - 1; x <= Math.ceil((r.x + r.w) / M); x++)
        if (x >= 0 && y >= 0 && x < nav.cols && y < nav.rows) nav.tileOf[y * nav.cols + x] = t.id;
    }
    // Dead zone value per cell: distance to nearest loopable structure.
    const loopTiles = this.tiles.filter((t) => t.baseSafety > 0.2);
    for (let i = 0; i < nav.cols * nav.rows; i++) {
      const c = nav.center(i); let best = 1e9;
      for (const t of loopTiles) {
        const dx = Math.max(t.rect.x - c.x, 0, c.x - (t.rect.x + t.rect.w)), dy = Math.max(t.rect.y - c.y, 0, c.y - (t.rect.y + t.rect.h));
        best = Math.min(best, Math.hypot(dx, dy));
      }
      nav.deadZone[i] = smoothstep(5 * M, 16 * M, best);
    }
    this.refreshTileSafety();
  }

  // Live tile strength: base safety scaled by remaining resources.
  refreshTileSafety(now = 0) {
    for (const t of this.tiles) {
      const up = t.pallets.filter((p) => p.state === 'up').length, down = t.pallets.filter((p) => p.state === 'down').length;
      const wins = t.windows.filter((w) => w.blockedUntil <= now).length;
      const res = up * 0.35 + down * 0.25 + wins * 0.3;
      t.resources = { up, down, wins };
      t.safety = clamp(t.baseSafety * (0.35 + Math.min(res, 1.2) * 0.6), 0, 1);
      if (t.main) t.safety = Math.max(t.safety, 0.45);
    }
  }

  // Open ground points spread apart (used by killer powers for lockers, printers…)
  openSpots(count, minSpace = 14, avoid = []) {
    const out = []; const rng = this.rng; const nav = this.nav;
    for (let t = 0; t < count * 80 && out.length < count; t++) {
      const xm = Math.floor(rr(rng, 6, this.wM - 6)) + 0.5, ym = Math.floor(rr(rng, 6, this.hM - 6)) + 0.5;
      let ok = true;
      for (let y = Math.floor(ym - 1); y <= ym + 1 && ok; y++) for (let x = Math.floor(xm - 1); x <= xm + 1; x++) { const i = y * nav.cols + x; if (nav.blocked[i] || nav.pallet[i] >= 0) { ok = false; break; } }
      if (!ok) continue;
      const p = { x: xm * M, y: ym * M };
      if ([...out, ...avoid].some((q) => dist(q.x, q.y, p.x, p.y) < minSpace * M)) continue;
      out.push(p);
    }
    return out;
  }
  tileAt(x, y) { const i = this.nav.idxAt(x, y); const t = this.nav.tileOf[i]; return t >= 0 ? this.tiles[t] : null; }
  isGrass(x, y) { return this.nav.grass[this.nav.idxAt(x, y)] === 1; }
}

return { GameMap };
};
__defs["js/core/status.js"] = function(){
// Reusable status-effect system. Every status is data; perks/killers only call add().
// Modifiers are aggregated: speed (multiplicative from percent), action speed, flags.
const STATUS_DEFS = {
  haste:       { name: 'Haste', kind: 'buff', stack: 'max', mod: (s) => ({ speedPct: s.value ?? 0.5 }) },
  hindered:    { name: 'Hindered', kind: 'debuff', stack: 'max', mod: (s) => ({ speedPct: -(s.value ?? 0.2) }) },
  slow:        { name: 'Slow', kind: 'debuff', stack: 'max', mod: (s) => ({ speedPct: -(s.value ?? 0.25) }) },
  exhausted:   { name: 'Exhausted', kind: 'debuff', stack: 'max', mod: () => ({ exhausted: true }) },
  blindness:   { name: 'Blindness', kind: 'debuff', stack: 'max', mod: () => ({ blind: true }) },
  broken:      { name: 'Broken', kind: 'debuff', stack: 'max', mod: () => ({ broken: true }) },
  deepWound:   { name: 'Deep Wound', kind: 'debuff', stack: 'max', mod: () => ({ deepWound: true }) },
  hemorrhage:  { name: 'Hemorrhage', kind: 'debuff', stack: 'max', mod: () => ({ hemorrhage: true }) },
  mangled:     { name: 'Mangled', kind: 'debuff', stack: 'max', mod: () => ({ healPct: -0.2 }) },
  oblivious:   { name: 'Oblivious', kind: 'debuff', stack: 'max', mod: () => ({ oblivious: true }) },
  undetectable:{ name: 'Undetectable', kind: 'buff', stack: 'max', mod: () => ({ undetectable: true }) },
  exposed:     { name: 'Exposed', kind: 'debuff', stack: 'max', mod: () => ({ exposed: true }) },
  endurance:   { name: 'Endurance', kind: 'buff', stack: 'max', mod: () => ({ endurance: true }) },
  stunned:     { name: 'Stunned', kind: 'debuff', stack: 'max', mod: () => ({ stunned: true, speedPct: -1 }) },
  blinded:     { name: 'Blinded', kind: 'debuff', stack: 'max', mod: () => ({ flashBlind: true }) },
  intoxicated: { name: 'Intoxicated', kind: 'debuff', stack: 'max', mod: (s) => ({ speedPct: -(s.value ?? 0.2), intoxicated: true }) },
  tormented:   { name: 'Tormented', kind: 'debuff', stack: 'max', mod: () => ({ tormented: true }) },
  slipstream:  { name: 'Slipstreamed', kind: 'debuff', stack: 'max', mod: () => ({ slipstream: true }) },
  marked:      { name: 'Marked', kind: 'debuff', stack: 'max', mod: () => ({ exposed: true, marked: true }) },
  incapacitated: { name: 'Incapacitated', kind: 'debuff', stack: 'max', mod: () => ({ incapacitated: true }) },
  // expansion: killer-specific states, all through the central framework
  infected:    { name: 'Uroboros Infection', kind: 'debuff', stack: 'max', mod: (s) => ((s.value ?? 0) >= 100 ? { speedPct: -0.04, critInfected: true, infected: true } : { infected: true }) },
  chained:     { name: 'Chained', kind: 'debuff', stack: 'max', mod: (s) => ({ speedPct: [0, -0.435, -0.576, -0.718][Math.min(3, s.value ?? 3)], incapacitated: true, chained: s.value ?? 3 }) },
  grabbed:     { name: 'Grabbed', kind: 'debuff', stack: 'max', mod: () => ({ incapacitated: true, grabbed: true }) },
  nightfall:   { name: 'Nightfall', kind: 'debuff', stack: 'max', mod: () => ({ sightMult: 0.22, nightfall: true }) },
  dispelled:   { name: 'Dispelled', kind: 'debuff', stack: 'max', mod: () => ({ dispelled: true }) },
  deepWoundFrenzy: { name: 'Deep Wound', kind: 'debuff', stack: 'max', mod: () => ({}) },
  // Skull Merchant / Unknown / Nemesis / Ghoul
  lockOn:      { name: 'Lock-On', kind: 'debuff', stack: 'max', mod: (s) => ({ lockOn: s.value ?? 1 }) },
  clawTrap:    { name: 'Claw Trap', kind: 'debuff', stack: 'max', mod: () => ({ clawTrap: true }) },
  weakened:    { name: 'Weakened', kind: 'debuff', stack: 'max', mod: () => ({ weakened: true }) },
  contaminated:{ name: 'Contaminated', kind: 'debuff', stack: 'max', mod: () => ({ contaminated: true }) },
  kaguneMark:  { name: 'Kagune Mark', kind: 'debuff', stack: 'max', mod: () => ({ kaguneMark: true }) },
  latched:     { name: 'Latched', kind: 'debuff', stack: 'max', mod: () => ({ latched: true }) },
  cannotAttack:{ name: 'Cannot Attack', kind: 'debuff', stack: 'max', mod: () => ({ cannotAttack: true }) },
  actionSpeed: { name: 'Action Speed', kind: 'buff', stack: 'sum', mod: (s) => ({ actionPct: s.value ?? 0.1 }) },
  repairSpeed: { name: 'Repair Speed', kind: 'buff', stack: 'sum', mod: (s) => ({ repairPct: s.value ?? 0.1 }) },
};

class StatusManager {
  constructor(owner) { this.owner = owner; this.list = []; }
  // add({id, duration, value, source, stacks}) — duration Infinity = until removed
  add(id, opts = {}) {
    const def = STATUS_DEFS[id]; if (!def) { console.warn('unknown status', id); return; }
    const existing = this.list.find((s) => s.id === id && (def.stack === 'max' || s.source === opts.source));
    const duration = opts.duration ?? Infinity;
    if (existing) {
      existing.expiration = Math.max(existing.expiration, duration);
      existing.duration = Math.max(existing.duration, duration);
      if (opts.value !== undefined) existing.value = def.stack === 'max' ? Math.max(existing.value ?? 0, opts.value) : opts.value;
      existing.stacks = Math.min((existing.stacks || 1) + (opts.stacks ? 1 : 0), 8);
      return existing;
    }
    const s = { id, duration, expiration: duration, value: opts.value, stacks: opts.stacks || 1, source: opts.source || null, modifiers: null };
    this.list.push(s);
    return s;
  }
  remove(id, source) { this.list = this.list.filter((s) => !(s.id === id && (source === undefined || s.source === source))); }
  has(id) { return this.list.some((s) => s.id === id); }
  get(id) { return this.list.find((s) => s.id === id); }
  timeLeft(id) { const s = this.get(id); return s ? s.expiration : 0; }
  update(dt) {
    for (const s of this.list) s.expiration -= dt;
    this.list = this.list.filter((s) => s.expiration > 0);
  }
  // Aggregate: speed haste uses max, hindered max, combined additively like DBD.
  mods() {
    const out = { speedPct: 0, actionPct: 0, repairPct: 0, healPct: 0 };
    let haste = 0, slow = 0;
    for (const s of this.list) {
      const m = STATUS_DEFS[s.id].mod(s);
      for (const k in m) {
        if (k === 'speedPct') { if (m[k] > 0) haste = Math.max(haste, m[k]); else slow = Math.min(slow, m[k]); }
        else if (k === 'sightMult') out[k] = Math.min(out[k] ?? 1, m[k]);
        else if (typeof m[k] === 'number') out[k] += m[k];
        else out[k] = out[k] || m[k];
      }
    }
    out.speedPct = haste + slow;
    return out;
  }
}

return { STATUS_DEFS, StatusManager };
};
__defs["js/entities/killer.js"] = function(){
// Killer entity (player controlled): movement, lunge attack, interactions, carrying,
// stuns, bloodlust. Powers plug in via killers/*.js.
const { M, clamp, dist, normAngle, angleDiff, uid } = __req("js/core/util.js");
const { StatusManager } = __req("js/core/status.js");
const { EV } = __req("js/core/events.js");

const KILLER_ACTIONS = {
  kick: { dur: 1.8, label: 'Damaging generator' },
  break: { dur: 2.6, label: 'Breaking pallet' },
  vault: { dur: 1.7, label: 'Vaulting' },
  pickup: { dur: 1.5, label: 'Picking up' },
  hook: { dur: 1.2, label: 'Hooking' },
  closeHatch: { dur: 1.5, label: 'Closing hatch' },
};

class Killer {
  constructor(def, PowerClass, { perks = [], addons = [] } = {}) {
    this.id = uid('k'); this.kind = 'killer'; this.def = def; this.name = def.name;
    this.x = 0; this.y = 0; this.r = 0.56 * M; this.facing = 0; this.vx = 0; this.vy = 0;
    this.status = new StatusManager(this);
    this.perks = perks; this.addons = addons;
    this.carrying = null; this.action = null; this.stunT = 0; this.stunKind = null;
    this.attack = { state: 'ready', t: 0, held: 0, dir: 0, hitSomeone: false };
    this.bloodlust = 0; this.chaseT = 0;
    this.input = { mx: 0, my: 0, aim: 0, lmb: false, lmbP: false, rmb: false, rmbP: false, rmbR: false, e: false, eP: false, space: false, spaceP: false };
    this.power = new PowerClass(this, addons);
    this.terrorRadius = def.terror * M;
    this.baseTerror = this.terrorRadius; this.blindT = 0; this.lastSpeed = 0;
    this.stats = { hits: 0, downs: 0, hooks: 0, kicks: 0, palletsBroken: 0, stuns: 0, missed: 0, powerHits: 0 };
  }
  hasPerk(id) { return this.perks.some((p) => p.id === id); }
  get busy() { return !!this.action || this.stunT > 0; }

  baseSpeed(m) {
    const mods = this.status.mods();
    let s = this.def.speed * M;
    if (this.carrying) s = 3.68 * M * (this.hasPerk('agitation') ? 1.18 : 1);
    s *= this.power.speedMult(m);
    if (this.attack.state === 'lunge') s *= 1.55 * (m ? m.perks.killerProduct('lungeMult', this) : 1);
    else if (this.attack.state === 'hit') s *= this.attack.t < 1.2 ? 0.3 : 0.7;
    else if (this.attack.state === 'miss') s *= 0.55;
    s += this.bloodlust * 0.2 * M;
    return Math.max(0, s * (1 + mods.speedPct));
  }

  update(dt, m) {
    this.status.update(dt);
    this.blindT = Math.max(0, this.blindT - dt);
    const inp = this.input;
    if (this.stunT > 0) { this.stunT -= dt; this.vx = this.vy = 0; if (this.stunT <= 0) this.stunKind = null; this.power.update(dt, m, inp, true); this.clearPresses(); return; }
    if (inp.aim !== null && !this.power.lockFacing) this.facing = inp.aim;
    // Timed action
    if (this.action) { this.updateAction(dt, m); this.power.update(dt, m, inp, true); this.clearPresses(); return; }
    // Attack state machine
    const at = this.attack;
    if (at.state === 'lunge') {
      at.t += dt;
      const hit = this.tryHit(m);
      if (hit) { at.state = 'hit'; at.t = 0; }
      else if ((!inp.lmb && at.t > 0.15) || at.t > 0.5) { at.state = 'miss'; at.t = 0; this.stats.missed++; m.events.emit(EV.KILLER_MISS, { killer: this }); }
    } else if (at.state === 'hit') { at.t += dt; if (at.t > 2.7 * m.perks.killerCooldownMult('hit', this)) at.state = 'ready'; }
    else if (at.state === 'miss') { at.t += dt; if (at.t > 1.5 * m.perks.killerCooldownMult('miss', this)) at.state = 'ready'; }
    if (inp.lmbP && at.state === 'ready' && !this.carrying && this.power.canAttack(m) && !this.status.has('cannotAttack')) { at.state = 'lunge'; at.t = 0; at.dir = this.facing; m.fx.swing(this); this.power.onAttack?.(m); }

    // Space: context interaction
    if (inp.spaceP && at.state !== 'lunge') { const c = m.killerContext(this); if (c) this.startAction(c.type, c.target, m); }
    // Drop carried survivor (RMB/Q while carrying)
    if (this.carrying && inp.rmbP) { m.dropCarried('drop', this); }

    this.power.update(dt, m, inp, false);
    // Movement
    let mx = inp.mx, my = inp.my;
    if (this.power.overrideMove) { const o = this.power.overrideMove(dt, m); if (o) { mx = o.x; my = o.y; } }
    if (at.state === 'lunge') { const a = this.facing; const l = Math.hypot(mx, my); mx = Math.cos(a) * Math.max(0.6, l); my = Math.sin(a) * Math.max(0.6, l); }
    const l = Math.hypot(mx, my);
    if (l > 0.01) {
      const sp = this.baseSpeed(m); const ox = this.x, oy = this.y;
      const pf = this.power.moveFilter?.(m);
      const collided = m.world.move(this, (mx / l) * sp * dt, (my / l) * sp * dt, this.r, (s) => !(s.kind === 'gate' && s.ref.open) && (!pf || pf(s)));
      m.separateKiller(this);
      this.vx = (this.x - ox) / dt; this.vy = (this.y - oy) / dt;
      if (collided && this.power.onCollide) this.power.onCollide(m);
    } else { this.vx = this.vy = 0; }
    this.lastSpeed = Math.hypot(this.vx, this.vy);
    m.checkKillerTraps?.(this);
    this.clearPresses();
  }
  clearPresses() { const i = this.input; i.lmbP = i.rmbP = i.rmbR = i.eP = i.spaceP = false; i.wheel = 0; i.slot = 0; i.cycleP = false; }

  tryHit(m) {
    const reach = this.r + 1.0 * M + (this.attack.t > 0.25 ? 0.35 * M : 0);
    let best = null, bd = 1e9;
    for (const s of m.survivors) {
      if (!s.standing) continue;
      const d = dist(this.x, this.y, s.x, s.y); if (d > reach + s.r) continue;
      const ang = Math.atan2(s.y - this.y, s.x - this.x);
      if (angleDiff(ang, this.facing) > 0.85 && d > this.r + s.r + 4) continue;
      if (!m.world.losClear(this.x, this.y, s.x, s.y)) continue;
      if (s.vault && s.vault.t < s.vault.dur * 0.15) continue;
      if (d < bd) { bd = d; best = s; }
    }
    if (!best) return false;
    // Dead Hard style dodge: active i-frames
    if (best.dodgeT && best.dodgeT > m.time) { m.fx.text(best.x, best.y - 30, 'DODGED', '#ffd27a'); m.events.emit(EV.KILLER_MISS, { killer: this, dodged: best }); return false; }
    best.lastHitBy = this; const res = best.takeHit(m, { source: 'basic' });
    this.stats.hits++; this.attack.hitSomeone = true; this.bloodlust = 0; this.chaseT = 0;
    m.events.emit(EV.KILLER_HIT, { killer: this, survivor: best, result: res });
    m.fx.hit(best, res);
    return true;
  }

  startAction(type, target, m) {
    // 'custom' actions come from killer powers: target = { dur, label, done(m), tick?(m, a) }
    const def = type === 'custom' ? { dur: target.dur, label: target.label } : KILLER_ACTIONS[type]; if (!def) return;
    let dur = def.dur * (type === 'custom' ? 1 : m.perks.killerActionMult(type, this) * (this.power.actionMult?.(type, target, m) ?? 1));
    this.action = { type, target, t: 0, dur, label: def.label, from: { x: this.x, y: this.y } };
    if (type === 'vault') { const side = target.sideOf(this); this.action.to = target.sides(1.2 * M)[1 - side]; }
    if (type === 'pickup') m.events.emit('pickupStart', { killer: this, survivor: target });
    this.attack.state = 'ready';
  }
  updateAction(dt, m) {
    const a = this.action; a.t += dt;
    if (a.type === 'vault') { const k = clamp(a.t / a.dur, 0, 1); this.x = a.from.x + (a.to.x - a.from.x) * k; this.y = a.from.y + (a.to.y - a.from.y) * k; }
    if (a.type === 'pickup' && (a.target.health !== 'dying')) { this.action = null; return; }
    if (a.type === 'custom' && a.target.tick && a.target.tick(m, a) === false) { this.action = null; return; }
    if (a.t >= a.dur) {
      this.action = null;
      switch (a.type) {
        case 'kick': m.kickGenerator(a.target, this); break;
        case 'break': m.breakPallet(a.target, this); break;
        case 'vault': m.world.resolve(this, this.r); m.events.emit(EV.VAULT_PERFORMED, { who: this, obj: a.target }); if (this.hasPerk('bamboozle')) a.target.blockedUntil = Math.max(a.target.blockedUntil, m.time + 8); break;
        case 'pickup': m.pickUp(a.target, this); break;
        case 'hook': m.hookSurvivor(a.target, false, this); break;
        case 'closeHatch': m.closeHatch(); break;
        case 'custom': a.target.done(m); break;
      }
    }
  }
  stun(m, t, kind) {
    if (this.hasPerk('enduring') && kind === 'pallet') t *= 0.5;
    t *= m.perks.killerProduct('stunMult', this);
    this.stunT = Math.max(this.stunT, t); this.stunKind = kind; this.action = null; this.attack.state = 'ready';
    this.bloodlust = 0; this.stats.stuns++;
    if (this.carrying) m.dropCarried('stun', this);
    this.power.onStun?.(m);
    m.events.emit(EV.KILLER_STUNNED, { killer: this, kind });
  }
}

return { KILLER_ACTIONS, Killer };
};
__defs["js/entities/survivor.js"] = function(){
// Survivor entity: physical state, health states, timed actions. The AI only sets
// intents (moveTo / moveMode / startAction); the rules live here and in the match.
const { M, clamp, dist, uid, normAngle } = __req("js/core/util.js");
const { StatusManager } = __req("js/core/status.js");
const { EV } = __req("js/core/events.js");
const { GEN_CHARGES, PALLET_DROP_TIME } = __req("js/entities/objects.js");
const { makeItem, ITEMS } = __req("js/items/items.js");

const SPEED = { run: 4.0 * M, walk: 2.26 * M, crouch: 1.13 * M, crawl: 0.7 * M };
const BLEED_OUT = 240;
const HOOK_STAGE_TIME = 60;

const ACTION_DEF = {
  repair:      { range: 2.1, label: 'Repairing' },
  heal:        { range: 1.9, label: 'Healing', dur: 16 },
  healSelf:    { range: 0, label: 'Self-healing', dur: 16 },
  unhook:      { range: 1.9, label: 'Unhooking', dur: 1.0 },
  dropPallet:  { range: 2.3, label: 'Pallet', dur: PALLET_DROP_TIME },
  openGate:    { range: 1.8, label: 'Opening gate', dur: 20 },
  searchChest: { range: 1.6, label: 'Searching', dur: 10 },
  cleanse:     { range: 1.6, label: 'Cleansing', dur: 14 },
  disarm:      { range: 1.5, label: 'Disarming', dur: 3 },
  freeTrapped: { range: 1.6, label: 'Freeing', dur: 2.5 },
  escapeTrap:  { range: 0, label: 'Escaping trap', dur: 1.2 },
  recover:     { range: 0, label: 'Recovering', dur: 32 },
  mend:        { range: 0, label: 'Mending', dur: 8 },
  openHatch:   { range: 1.6, label: 'Opening hatch', dur: 6 },
  flashlight:  { range: 7, label: 'Flashlight', dur: 99 },
  unhookSelf:  { range: 0, label: 'Unhook attempt', dur: 2.5 },
  device:      { range: 1.8, label: 'Using' },          // killer-power devices (printers, pods…)
  useEmp:      { range: 0, label: 'EMP', dur: 2.5 },
  breakChain:  { range: 0, label: 'Breaking chain', dur: 1.0 },   // Cenobite
  solveBox:    { range: 0, label: 'Solving the box', dur: 6 },     // Cenobite Lament Configuration
  spray:       { range: 0, label: 'First Aid Spray', dur: 1.5 },    // Mastermind
  vaccinate:   { range: 0, label: 'Injecting vaccine', dur: 3 },    // Nemesis
};

class Survivor {
  constructor({ name, color, difficulty, personality, perks = [], item = null, itemAddons = [] }) {
    this.id = uid('sv'); this.kind = 'survivor';
    this.name = name; this.color = color; this.difficulty = difficulty; this.personality = personality;
    this.x = 0; this.y = 0; this.r = 0.44 * M; this.facing = 0; this.vx = 0; this.vy = 0;
    this.health = 'healthy'; // healthy | injured | dying | carried | hooked | dead | escaped
    this.status = new StatusManager(this);
    this.hookStage = 0; this.hookTimer = 0; this.hook = null; this.hookCount = 0; this.unhookAttempts = 0; this.hookedAt = -999; this.unhookedAt = -999;
    this.bleed = BLEED_OUT; this.recovery = 0; this.healProgress = 0; this.healers = new Set(); this.mend = 0;
    this.wiggle = 0; this.trap = null; this.trapAttempts = 0;
    this.item = makeItem(item, itemAddons);
    this.perks = perks; // perk instances, filled by perk system
    this.action = null; this.vault = null; this.moveTo = null; this.moveMode = 'run';
    this.scratchT = 0; this.bloodT = 0; this.gruntT = 2;
    this.lastHitT = -999; this.downedAt = -999; this.carriedSince = 0;
    this.stats = { gens: 0, heals: 0, unhooks: 0, pallets: 0, stuns: 0, vaults: 0, chaseTime: 0, hits: 0, escapes: 0, totems: 0, chests: 0 };
    this.outcome = null; this.ai = null;
    this.unbreakableUsed = false; this.dsReady = false; this.lookTarget = null; this.emp = false; this.stalk = 0;
  }
  get alive() { return this.health !== 'dead' && this.health !== 'escaped'; }
  get standing() { return this.health === 'healthy' || this.health === 'injured'; }
  get busy() { return !!this.action || !!this.vault; }
  hasPerk(id) { return this.perks.some((p) => p.id === id); }
  perk(id) { return this.perks.find((p) => p.id === id); }

  speed() {
    const mods = this.status.mods();
    if (this.trap) return 0;
    let base = this.health === 'dying' ? SPEED.crawl : SPEED[this.moveMode] || SPEED.run;
    if (this.moveMode === 'crouch' && this.hasPerk('urban_evasion')) base *= 1.9;
    return Math.max(0, base * (1 + mods.speedPct));
  }

  // ---- actions --------------------------------------------------------
  canStart(type, target, m) {
    const def = ACTION_DEF[type]; if (!def) return false;
    if (!this.standing && !(this.health === 'dying' && type === 'recover')) return false;
    if (this.vault) return false;
    if (this.trap && type !== 'escapeTrap') return false;
    if (this.control && !this.control.allow?.includes(type)) return false;
    if (this.status.has('incapacitated') || this.status.has('chained') || this.status.has('grabbed')) { if (!['breakChain', 'mend', 'dropPallet', 'recover', 'escapeTrap', 'spray'].includes(type)) return false; }
    if (def.range && target && dist(this.x, this.y, target.x, target.y) > def.range * M + (type === 'repair' ? 0.6 * M : 0)) return false;
    switch (type) {
      case 'repair': return !target.done && target.blockedUntil <= m.time && target.repairers.size < 4 && !this.status.has('incapacitated');
      case 'heal': return target !== this && (target.health === 'injured' || target.health === 'dying') && !target.status.has('broken');
      case 'healSelf': if (m.cfg.mode === '2v8' && !this.hasPerk('class_medic')) return false; // 2v8: no self-healing (Medic excepted)
        return this.health === 'injured' && !this.status.has('broken') && ((this.item?.id === 'medkit' && this.item.charges > 0) || this.hasPerk('self_care') || this.hasPerk('class_medic') || m.perks.anySurvivorHas('circle_of_healing'));
      case 'unhook': return target.health === 'hooked' && target.hook;
      case 'dropPallet': return target.state === 'up' && !target.dropping && !((target.blockedUntil || 0) > m.time);
      case 'openGate': return target.powered && !target.open && !((this.gateBlockUntil || 0) > m.time) && !((target.blockedUntil || 0) > m.time) && (!target.user || target.user === this);
      case 'searchChest': return !target.opened && (!target.user || target.user === this);
      case 'cleanse': return !target.cleansed && (!target.user || target.user === this);
      case 'disarm': return target.state === 'set';
      case 'freeTrapped': return !!target.trap && target !== this;
      case 'escapeTrap': return !!this.trap;
      case 'recover': return this.health === 'dying';
      case 'mend': return this.status.has('deepWound');
      case 'openHatch': return m.hatch.state === 'closed' && this.item?.id === 'key' && m.aliveCount() <= 2;
      case 'flashlight': return this.item?.id === 'flashlight' && this.item.charges > 0;
      case 'unhookSelf': return false;
      case 'device': return !target.dead && target.usable(this, m);
      case 'useEmp': return !!this.emp;
      case 'breakChain': return this.status.has('chained');
      case 'solveBox': return !!this.box;
      case 'spray': return (this.sprays || 0) > 0 && this.status.has('infected');
      case 'vaccinate': return (this.vaccines || 0) > 0 && this.status.has('contaminated');
    }
    return true;
  }
  startAction(type, target, m) {
    if (this.action && this.action.type === type && this.action.target === target) return true;
    if (!this.canStart(type, target, m)) return false;
    this.stopAction(m);
    this.action = { type, target, t: 0, dur: ACTION_DEF[type].dur || 0, label: ACTION_DEF[type].label };
    this.moveTo = null;
    if (target && type !== 'flashlight') this.facing = Math.atan2(target.y - this.y, target.x - this.x);
    if (type === 'repair') { target.repairers.add(this); target.regressing = false; this.scT = 3 + Math.random() * 6; m.events.emit(EV.GENERATOR_START, { gen: target, survivor: this }); }
    if (type === 'heal') { target.healers.add(this); m.events.emit('healStart', { healer: this, target }); }
    if (type === 'healSelf') this.healers.add(this);
    if (type === 'openGate' || type === 'searchChest' || type === 'cleanse') target.user = this;
    if (type === 'dropPallet') { target.dropping = true; }
    return true;
  }
  stopAction(m) {
    const a = this.action; if (!a) return;
    m?.events?.emit('survivorActionStop', { survivor: this, type: a.type, target: a.target, done: a.type === 'repair' ? !!a.target.done : undefined });
    if (a.type === 'repair') a.target.repairers.delete(this);
    if (a.type === 'heal') a.target.healers.delete(this);
    if (a.type === 'healSelf') this.healers.delete(this);
    if ((a.type === 'openGate' || a.type === 'searchChest' || a.type === 'cleanse') && a.target.user === this) a.target.user = null;
    if (a.type === 'dropPallet' && a.t < a.dur) a.target.dropping = false;
    this.action = null;
  }

  startVault(obj, quality, m) {
    if (this.vault || !this.standing || this.trap) return false;
    if (obj.kind === 'window' && obj.blockedUntil > m.time) return false;
    if (obj.kind === 'pallet' && obj.state !== 'down') return false;
    this.stopAction(m);
    const side = obj.sideOf(this);
    const sides = obj.sides(1.05 * M);
    const from = { x: this.x, y: this.y }; const to = sides[1 - side];
    // fast vault needs running momentum; slower vaults otherwise
    let dur = quality === 'fast' ? 0.5 : quality === 'medium' ? 0.9 : 1.2;
    const mods = this.status.mods(); if (this.hasPerk('resilience') && this.health === 'injured') dur *= 0.91;
    if (mods.actionPct) dur /= 1 + mods.actionPct * 0.5;
    this.vault = { obj, from, to, t: 0, dur, quality };
    this.facing = Math.atan2(to.y - from.y, to.x - from.x);
    m.onVault(this, obj, quality);
    return true;
  }

  takeHit(m, { instadown = false, source = 'basic' } = {}) {
    if (!this.standing) return false;
    this.stopAction(m); this.vault = null; this.lastHitT = m.time; this.stats.hits++;
    const mods = this.status.mods();
    if (mods.endurance && !instadown || (mods.endurance && instadown)) {
      this.status.remove('endurance');
      this.status.add('deepWound', { duration: 20 }); this.mend = 0;
      if (this.health === 'healthy') this.health = 'injured';
      this.status.add('haste', { duration: 1.8, value: 0.5, source: 'hit' });
      m.events.emit(EV.SURVIVOR_INJURED, { survivor: this, endurance: true });
      return 'endurance';
    }
    if (this.health === 'healthy' && !instadown && !mods.exposed) {
      this.health = 'injured'; this.healProgress = 0;
      this.status.add('haste', { duration: 1.8, value: 0.5, source: 'hit' });
      m.events.emit(EV.SURVIVOR_INJURED, { survivor: this, source });
      return 'injured';
    }
    this.goDown(m, source);
    return 'downed';
  }
  goDown(m, source) {
    this.stopAction(m); this.vault = null;
    if (this.trap) { this.trap.victim = null; this.trap.state = 'unset'; this.trap = null; }
    this.health = 'dying'; this.healProgress = 0; this.recovery = 0; this.downedAt = m.time;
    this.status.remove('deepWound'); this.status.remove('endurance'); this.status.remove('kaguneMark'); this.status.remove('latched');
    const by = this.lastHitBy && m.time - this.lastHitT < 1 ? this.lastHitBy : null;
    m.events.emit(EV.SURVIVOR_DOWNED, { survivor: this, source, killer: by });
  }

  // ---- update ---------------------------------------------------------
  update(dt, m) {
    this.status.update(dt);
    if (!this.alive || this.health === 'hooked' || this.health === 'carried') { this.vx = this.vy = 0; return; }
    const mods = this.status.mods();
    if (this.health === 'dying') {
      this.bleed -= dt * (m.collapse ? 2 : 1);
      if (this.bleed <= 0) { m.killSurvivor(this, 'bled out'); return; }
    }
    // Deep wound timer → goes down if not mended
    if (mods.deepWound && this.status.timeLeft('deepWound') < dt * 1.5 && this.health === 'injured') {
      this.status.remove('deepWound'); this.goDown(m, 'deepwound');
    }
    if (this.control) {
      const c = this.control, ox = this.x, oy = this.y;
      if (this.action && !c.allow?.includes(this.action.type)) this.stopAction(m);
      if (this.action) this.updateAction(dt, m, mods);
      if (c.update(dt, m, this) === false || !this.standing) { this.control = null; this.status.remove('grabbed'); }
      this.vx = (this.x - ox) / dt; this.vy = (this.y - oy) / dt;
      return;
    }
    if (this.vault) { this.updateVault(dt, m); return; }
    if (this.action) { this.updateAction(dt, m, mods); }
    if (this.action && this.action.type !== 'flashlight') { this.vx = this.vy = 0; return; }
    // movement
    let moved = 0;
    if (!this.isPlayer && this.lookUntil && this.lookUntil < m.time) { this.lookTarget = null; this.lookUntil = 0; }
    if (this.moveTo && !this.trap) {
      const dx = this.moveTo.x - this.x, dy = this.moveTo.y - this.y; const d = Math.hypot(dx, dy);
      if (d > 2) {
        const sp = this.speed(); const step = Math.min(d, sp * dt);
        const ox = this.x, oy = this.y;
        m.world.move(this, (dx / d) * step, (dy / d) * step, this.r, (s) => s.kind !== 'gate' || !s.ref.open);
        this.vx = (this.x - ox) / dt; this.vy = (this.y - oy) / dt; moved = Math.hypot(this.vx, this.vy);
        if (!this.action || this.action.type !== 'flashlight') {
          // survivors can deliberately look at something (e.g. to reveal Ghost Face) while moving
          const want = this.lookTarget ? Math.atan2(this.lookTarget.y - this.y, this.lookTarget.x - this.x) : Math.atan2(dy, dx);
          this.facing += normAngle(want - this.facing) * Math.min(1, dt * 14);
        }
      } else { this.vx = this.vy = 0; }
    } else { this.vx = this.vy = 0; if (this.lookTarget && !this.action) this.facing += normAngle(Math.atan2(this.lookTarget.y - this.y, this.lookTarget.x - this.x) - this.facing) * Math.min(1, dt * 12); }
    // Scratch marks while running; blood while injured; grunts of pain.
    if (this.health !== 'dying' && moved > SPEED.walk * 1.15) {
      this.scratchT -= dt; if (this.scratchT <= 0) { this.scratchT = 0.22; m.addScratch(this); }
      if (!this._wasRunning) { this._wasRunning = true; m.perks.survivorStartRun(this); }
    } else this._wasRunning = false;
    if (this.health === 'injured' || this.health === 'dying') {
      this.bloodT -= dt * (mods.hemorrhage ? 2.5 : 1); if (this.bloodT <= 0) { this.bloodT = 0.9 + Math.random() * 0.6; m.addBlood(this); }
      this.gruntT -= dt;
      const quietHeal = (this.action?.type === 'healSelf' && this.hasPerk('bite_the_bullet')) || [...this.healers].some((h) => h.hasPerk?.('bite_the_bullet')) || (this.silentUntil || 0) > m.time;
      if (this.gruntT <= 0 && quietHeal) this.gruntT = 1;
      if (this.gruntT <= 0) { this.gruntT = (2.5 + Math.random() * 2.5) / m.perks.killerProduct('gruntRate'); if (!this.hasPerk('iron_will') || this.health === 'dying') m.addGrunt(this); }
    }
  }

  updateVault(dt, m) {
    const v = this.vault; v.t += dt;
    const k = clamp(v.t / v.dur, 0, 1);
    this.x = v.from.x + (v.to.x - v.from.x) * k; this.y = v.from.y + (v.to.y - v.from.y) * k;
    this.vx = (v.to.x - v.from.x) / v.dur; this.vy = (v.to.y - v.from.y) / v.dur;
    if (k >= 1) { this.vault = null; m.world.resolve(this, this.r, (s) => s.kind !== 'gate' || !s.ref.open); if (v.quality === 'fast') m.perks.survivorFastVault(this); }
  }

  updateAction(dt, m, mods) {
    const a = this.action, tg = a.target;
    const def = ACTION_DEF[a.type];
    if (tg && def.range && dist(this.x, this.y, tg.x, tg.y) > def.range * M + 0.8 * M) { this.stopAction(m); return; }
    let speed = 1 + (mods.actionPct || 0);
    if (this.hasPerk('resilience') && this.health === 'injured') speed += 0.09;
    speed *= m.perks.actionMult(this, a.type);
    a.t += dt;
    switch (a.type) {
      case 'repair': {
        if (tg.done || tg.blockedUntil > m.time) { this.stopAction(m); return; }
        const n = tg.repairers.size; const eff = [1, 1, 0.9, 0.8, 0.7][n] || 0.7;
        let rate = eff * speed * (1 + (mods.repairPct || 0)) * (m.repairMult || 1);
        if (this.item?.id === 'toolbox' && this.item.charges > 0) { rate *= this.item.repairMult; this.item.charges -= dt; }
        if (this.hasPerk('prove_thyself') && n > 1) rate *= 1 + 0.15 * (n - 1) / n * 1.0;
        tg.charges += rate * dt; tg.lastRepairT = m.time; tg.regressing = false;
        this.stats.gens += rate * dt / GEN_CHARGES;
        // skill checks
        this.scT -= dt;
        if (this.scT <= 0) { this.scT = 4 + Math.random() * 9; m.skillCheck(this, tg); }
        if (tg.charges >= GEN_CHARGES) m.completeGenerator(tg);
        break;
      }
      case 'heal': {
        if (!(tg.health === 'injured' || tg.health === 'dying') || tg.status.has('broken')) { this.stopAction(m); return; }
        let rate = speed / 16 * (1 + (tg.status.mods().healPct || 0));
        if (this.item?.id === 'medkit' && this.item.charges > 0) { rate *= 1.25; this.item.charges -= dt * 0.5; }
        rate *= m.perks.healMult(this, tg);
        const n = tg.healers.size; if (n > 1) rate *= [1, 1, 0.85, 0.75, 0.65][n];
        tg.healProgress += rate * dt;
        if (this.isPlayer) { this.scT = (this.scT ?? 6) - dt; if (this.scT <= 0) { this.scT = 5 + Math.random() * 7; m.startPlayerCheck(this, 'heal', tg); } }
        if (this.health === 'injured' && this.hasPerk('solidarity')) { this.healProgress += rate * dt * 0.5; if (this.healProgress >= 1) m.healed(this, this); }
        if (tg.healProgress >= 1) { m.healed(tg, this); this.stopAction(m); }
        break;
      }
      case 'healSelf': {
        if (this.health !== 'injured') { this.stopAction(m); return; }
        let rate = speed / 16 * (1 + (mods.healPct || 0));
        if (this.item?.id === 'medkit' && this.item.charges > 0) { rate *= this.item.healSpeed; this.item.charges -= dt; }
        else rate *= 0.5; // Self-Care
        this.healProgress += rate * dt;
        if (this.healProgress >= 1) { m.healed(this, this); this.stopAction(m); }
        break;
      }
      case 'unhook': if (a.t >= a.dur) { m.unhook(tg, this); this.stopAction(m); } break;
      case 'dropPallet': if (a.t >= a.dur) { const p = tg; this.action = null; m.dropPallet(p, this); } break;
      case 'openGate': if (a.t * speed >= a.dur) { m.openGate(tg, this); this.stopAction(m); } break;
      case 'searchChest': if (a.t * speed >= a.dur) { m.openChest(tg, this); this.stopAction(m); } break;
      case 'cleanse': if (a.t * speed >= a.dur) { m.cleanseTotem(tg, this); this.stopAction(m); } break;
      case 'disarm': if (tg.state !== 'set') { this.stopAction(m); return; } if (a.t * speed >= a.dur) { m.disarmTrap(tg, this); this.stopAction(m); } break;
      case 'freeTrapped': if (!tg.trap) { this.stopAction(m); return; } if (a.t >= a.dur) { m.releaseTrap(tg, this); this.stopAction(m); } break;
      case 'escapeTrap': if (!this.trap) { this.stopAction(m); return; } if (a.t >= a.dur) { this.action = null; m.trapEscapeAttempt(this); } break;
      case 'recover': {
        const cap = this.hasPerk('unbreakable') && !this.unbreakableUsed ? 1 : 0.95;
        this.recovery = Math.min(cap, this.recovery + dt / 32 * (this.hasPerk('unbreakable') && !this.unbreakableUsed ? 1.35 : 1));
        if (this.recovery >= 1) { this.unbreakableUsed = true; this.health = 'injured'; this.recovery = 0; this.stopAction(m); m.events.emit(EV.SURVIVOR_HEALED, { survivor: this, healer: this, fromDying: true }); }
        break;
      }
      case 'mend': if (!this.status.has('deepWound')) { this.stopAction(m); return; } if (a.t >= a.dur) { this.status.remove('deepWound'); if (this.status.has('kaguneMark')) { this.status.remove('kaguneMark'); m.events.emit('kaguneMarkCleared', { survivor: this }); } this.stopAction(m); } break;
      case 'openHatch': if (a.t * this.item.hatchSpeed >= a.dur) { m.openHatchWithKey(this); this.stopAction(m); } break;
      case 'device': if (tg.dead) { this.stopAction(m); return; } if (!tg.manualComplete && a.t * speed >= tg.useTime) { this.stopAction(m); tg.onUse(this, m); m.events.emit('deviceUsed', { survivor: this, device: tg }); } break;
      case 'breakChain': if (a.t >= a.dur) { const c = this.status.get('chained'); if (c) { c.value = (c.value ?? 3) - 1; if (c.value <= 0) { this.status.remove('chained'); m.events.emit('chainsBroken', { survivor: this }); } } this.stopAction(m); if (this.status.has('chained')) this.startAction('breakChain', null, m); } break;
      case 'solveBox': if (a.t * speed >= a.dur) { this.stopAction(m); m.killerWith('solveBox').power.solveBox?.(this, m); } break;
      case 'spray': if (a.t >= a.dur) { this.stopAction(m); this.sprays--; this.status.remove('infected'); m.killerWith('onSpray').power.onSpray?.(this, m); } break;
      case 'vaccinate': if (a.t >= a.dur) { this.stopAction(m); this.vaccines--; this.status.remove('contaminated'); this.ai?.note('Vaccinated — no longer contaminated', 'vax'); m.warn(this, 'Vaccinated'); } break;
      case 'useEmp': this.status.add('hindered', { duration: 0.15, value: 0.1, source: 'emp' }); if (a.t >= a.dur) { this.stopAction(m); m.killerWith('useEmp').power.useEmp?.(this, m); this.emp = false; } break;
      case 'flashlight': {
        if (!this.item || this.item.charges <= 0) { this.stopAction(m); return; }
        this.item.charges -= dt; m.flashlightTick(this, dt);
        if (a.t > 2.2) this.stopAction(m);
        break;
      }
    }
  }
}


return { SPEED, BLEED_OUT, HOOK_STAGE_TIME, Survivor, ACTION_DEF, ITEMS };
};
__defs["js/perks/perkSystem.js"] = function(){
// Event-driven perk system. A perk def: { id, name, description, icon, rarity, side,
// effects, triggers: { eventName: fn(inst, data, m) }, init, update, cooldown, charges }.
// The match asks the system for multipliers; perks react to bus events.
const { SURVIVOR_PERKS } = __req("js/perks/survivorPerks.js");
const { KILLER_PERKS } = __req("js/perks/killerPerks.js");
const { CLASS_PERKS } = __req("js/modes/classes.js");

const ALL_PERKS = { ...SURVIVOR_PERKS, ...KILLER_PERKS, ...CLASS_PERKS };

function makePerk(id, owner) {
  const def = ALL_PERKS[id]; if (!def) return null;
  return { id, def, owner, cooldownT: 0, charges: def.charges ?? null, tokens: 0, active: false, data: {}, lastProc: -999 };
}

class PerkSystem {
  constructor(m) { this.m = m; this.instances = []; this.unsubs = []; }
  register(owner, ids) {
    const list = [];
    for (const id of ids) {
      const inst = makePerk(id, owner); if (!inst) continue;
      list.push(inst); this.instances.push(inst);
      const trig = inst.def.triggers || {};
      // A killer perk reacts only to its own killer's actions (2v8 has two killers).
      const own = owner.kind === 'killer';
      for (const ev in trig) this.unsubs.push(this.m.events.on(ev, (d) => { if (own && d && d.killer && d.killer !== owner) return; try { trig[ev](inst, d, this.m); } catch (e) { console.error('perk', id, e); } }));
    }
    owner.perks = list;
    return list;
  }
  init() { for (const p of this.instances) p.def.init?.(p, this.m); }
  update(dt) {
    for (const p of this.instances) { if (p.cooldownT > 0) p.cooldownT -= dt; p.def.update?.(p, this.m, dt); }
    for (const k of this.m.killers) if (k.baseTerror) k.terrorRadius = k.baseTerror * this.killerProduct('terrorMult', k) * (k.terrorScale ?? 1);
  }
  // product of a numeric killer-perk property or function(p, m)
  // k given: only that killer's perks (its own stun/lunge/terror); omitted: every killer's perks
  // (effects on the world, like scratch-mark lifetime).
  killerPerks(k = null) { return k ? k.perks : this.m.killers.flatMap((q) => q.perks); }
  killerProduct(key, k = null) { let v = 1; for (const p of this.killerPerks(k)) { const f = p.def[key]; if (typeof f === 'function') v *= f(p, this.m); else if (typeof f === 'number') v *= f; } return v; }
  anySurvivorHas(id) { return this.m.survivors.some((s) => s.alive && s.hasPerk(id)); }
  killerHas(key, k = null) { return this.killerPerks(k).some((p) => p.def[key]); }
  // Skill check odds: survivor token perks first (stake out), then killer modifiers.
  skillCheckMods(sv, g) {
    const out = { fail: 0, great: 0, failExtra: 0, forceGreat: false };
    for (const p of sv.perks) if (p.def.onSkillCheck?.(p, this.m, g) === 'great') out.forceGreat = true;
    for (const p of this.killerPerks()) { const r = p.def.skillCheckMod?.(p, sv, g, this.m); if (r) { out.fail += r.fail || 0; out.great += r.great || 0; out.failExtra += r.failExtra || 0; } }
    return out;
  }
  proc(inst, label) { inst.lastProc = this.m.time; this.m.events.emit('perkProc', { perk: inst, label }); }
  // multipliers / hooks used by the rules
  actionMult(sv, type) {
    let k = 1;
    for (const p of this.killerPerks()) if (p.def.survivorActionMult) k *= p.def.survivorActionMult(p, sv, type, this.m);
    for (const p of sv.perks) if (p.def.actionMult) k *= p.def.actionMult(p, sv, type, this.m);
    if (['repair', 'heal', 'healSelf', 'unhook', 'openGate'].includes(type)) for (const o of this.m.survivors) if (o !== sv && o.standing) for (const p of o.perks) if (p.def.teamActionMult && Math.hypot(o.x - sv.x, o.y - sv.y) < 8 * 32) k *= p.def.teamActionMult;
    return k;
  }
  healMult(healer, target) {
    let k = 1; for (const p of healer.perks) if (p.def.healMult) k *= p.def.healMult(p, healer, target, this.m);
    for (const p of this.killerPerks()) if (p.def.survivorHealMult) k *= p.def.survivorHealMult(p, healer, target, this.m);
    return k;
  }
  killerActionMult(type, killer = this.m.killer) { let k = 1; for (const p of killer.perks) if (p.def.killerActionMult) k *= p.def.killerActionMult(p, type, this.m); return k; }
  killerCooldownMult(kind, killer = this.m.killer) { let k = 1; for (const p of killer.perks) if (p.def.killerCooldownMult) k *= p.def.killerCooldownMult(p, kind, this.m); return k; }
  survivorStartRun(sv) { for (const p of sv.perks) p.def.onStartRun?.(p, sv, this.m); }
  survivorFastVault(sv) { for (const p of sv.perks) p.def.onFastVault?.(p, sv, this.m); }
  activate(sv, id) { const p = sv.perks.find((q) => q.id === id); if (p && p.def.activate) return p.def.activate(p, sv, this.m); return false; }
  destroy() { for (const u of this.unsubs) u(); }
}

return { ALL_PERKS, makePerk, PerkSystem };
};
__defs["js/ai/difficulty.js"] = function(){
// Difficulty 1–10 maps to *how the bot thinks*, never to raw speed or extra information.
// Every value is data so tuning never touches AI code.
const { lerp, clamp } = __req("js/core/util.js");

const SPEC = {
  reaction:        [1.15, 0.12],  // s before a newly-visible killer is acted on
  tacticalHz:      [4, 10],       // chase/tactical decisions per second
  strategicEvery:  [1.4, 0.5],    // s between utility re-evaluations
  temperature:     [20, 2.5],     // softmax temperature on utility scores (randomness)
  mistakeRate:     [0.22, 0.0],   // chance to take a poor option on purpose
  memoryTau:       [4, 15],       // s for killer-belief confidence to decay by e
  predictLead:     [0.0, 0.9],    // s of killer motion extrapolation
  awareness:       [0.35, 1.0],   // shimmer / sound interpretation quality
  fieldHalf:       [16, 24],      // local chase field radius in cells (meters)
  horizon:         [3.0, 7.0],    // chase lookahead (m)
  loopIQ:          [0.05, 1.0],   // weight on loop margin vs naive running away
  fastVault:       [0.45, 0.98],  // chance a vault is a fast vault
  throwDist:       [5.8, 3.7],    // m: killer distance that prompts a pallet drop
  greed:           [0.0, 0.85],   // willingness to keep a pallet up while safe
  palletWaste:     [0.3, 0.0],    // chance to drop with no real gain
  dodge:           [0.08, 0.92],  // Dead Hard / chainsaw sidestep timing quality
  mindgameRead:    [0.0, 0.9],
  deadZoneAware:   [0.0, 1.0],
  tileGraph:       [0.1, 1.0],
  stealth:         [0.2, 1.0],
  scGreat:         [0.08, 0.72],
  scFail:          [0.3, 0.01],
  clusterAware:    [0.0, 1.0],
  comms:           [0.25, 1.0],
  leaveAhead:      [1.0, 4.2],    // s of predicted killer ETA that makes a bot leave a gen
  threatSense:     [0.45, 1.0],
  protect:         [0.0, 1.0],
  hookWisdom:      [0.0, 1.0],
  pathNoise:       [2.6, 0.0],    // m of waypoint error
  healWisdom:      [0.0, 1.0],
  lineAware:       [0.15, 1.0],   // chainsaw line-of-fire awareness
  trapSense:       [0.35, 1.0],   // trap spotting radius multiplier
  leadAway:        [0.0, 1.0],    // drag the killer away from teammates' gens
  zoneSense:       [0.2, 1.0],    // how much known danger zones (gas, trails, pods) shape routes
  perkIQ:          [0.15, 1.0],   // how well perks are used (timing, saving charges)
};

function aiParams(d) {
  d = clamp(d, 1, 10);
  const t = (d - 1) / 9;
  const out = { difficulty: d };
  for (const k in SPEC) out[k] = lerp(SPEC[k][0], SPEC[k][1], k === 'mistakeRate' || k === 'palletWaste' ? Math.pow(t, 0.7) : t);
  out.tacticalHz = Math.round(out.tacticalHz);
  out.fieldHalf = Math.round(out.fieldHalf);
  // Discrete capability gates (low levels simply do not use some techniques)
  out.usesMindgames = d >= 6; out.preDrops = d >= 6; out.usesGreed = d >= 7; out.leavesEarly = d >= 4;
  out.protectionHits = d >= 8; out.flashlightSaves = d >= 6; out.genStrategy = d >= 6; out.waitsForCamper = d >= 6;
  return out;
}

const DIFFICULTY_LABELS = ['', 'Fresh Meat', 'Beginner', 'Beginner+', 'Average', 'Average+', 'Good', 'Good+', 'Strong', 'Very Strong', 'Elite'];

return { aiParams, DIFFICULTY_LABELS };
};
__defs["js/ai/killerPrediction.js"] = function(){
// Per-survivor perception and belief about ONE killer (2v8 gives each survivor a belief per
// killer). Survivors never read a killer's true position unless they could perceive it.
const { M, dist, clamp, gauss, angleDiff, normAngle } = __req("js/core/util.js");

const INFO = { VISIBLE: 'VISIBLE', RECENT: 'RECENTLY_SEEN', HEARD: 'HEARD', PREDICTED: 'PREDICTED', UNKNOWN: 'UNKNOWN' };

class KillerBelief {
  constructor(ai, k) {
    this.ai = ai; this.k = k;
    this.x = 0; this.y = 0; this.vx = 0; this.vy = 0; this.facing = 0; this.t = -999; this.conf = 0; this.state = INFO.UNKNOWN;
    this.rawVisible = false; this.noticeAt = 0; this.observing = false; this.heardDist = null; this.heartbeat = 0;
    this.lastHeading = null; this.headingT = 0; this.mindgameT = -999; this.standT = -999;
    this.revvingT = -999; this.sprintT = -999; this.bellT = -999; this.bellKind = null; this.cloakSeenT = -999;
    this.lastSoundT = 0; this.lastSighting = null; this.dirKnown = false; this.hbHist = []; this.spineChill = false; this.approach = 0; this._prevD = null;
    this.powerState = null; this.attacking = false; this.carrying = false; this.actionType = null;
  }
  get age() { return this.ai.m.time - this.t; }
  pos(lead = 0) {
    const a = Math.min(this.age, 2.5) + lead;
    const k = this.observing ? lead : a;
    return { x: this.x + this.vx * k * 0.85, y: this.y + this.vy * k * 0.85 };
  }
  known(minConf = 0.15) { return this.conf >= minConf; }
  // m/s the heartbeat distance is shrinking (distance-only cue, no direction)
  hbClosing() { const h = this.hbHist; if (h.length < 4) return 0; const a = h[0], b = h[h.length - 1]; return (a.d - b.d) / Math.max(0.3, b.t - a.t) / M; }

  observe(k, m, how = INFO.VISIBLE, conf = 1) {
    this.x = k.x; this.y = k.y; this.vx = k.vx; this.vy = k.vy; this.facing = k.facing; this.t = m.time; this.conf = conf; this.state = how; this.dirKnown = true;
    this.powerState = k.power.state ?? null; this.attacking = k.attack.state === 'lunge'; this.carrying = !!k.carrying; this.actionType = k.action?.type ?? null;
  }

  update(dt, m) {
    const ai = this.ai, s = ai.sv, k = this.k, P = ai.params, pers = ai.pers;
    const d = dist(s.x, s.y, k.x, k.y);
    this.observing = false;
    // --- vision ----------------------------------------------------------
    let visible = false;
    const sight = 46 * M * (s.status.mods().sightMult ?? 1);
    if (d < sight && !k.hidden && m.world.losClear(s.x, s.y, k.x, k.y)) {
      const pw = k.power;
      if (pw.id === 'cloak' && pw.state === 'cloaked') {
        const shimmer = (4 + 10 * P.awareness * (0.6 + 0.4 * pers.killerAwareness)) * M * (k.lastSpeed > 2 * M ? 1 : 0.6);
        visible = d < shimmer; if (visible) this.cloakSeenT = m.time;
      } else {
        // some powers (Ghost Face's shroud) shrink how far away a survivor can pick the killer out
        const vis = pw.visibilityTo?.(s, m) ?? 1;
        visible = (vis >= 1 && d < sight) || d < sight * vis * (0.65 + 0.35 * P.awareness) * (0.8 + 0.4 * pers.killerAwareness);
      }
      if (k.blindHidden) visible = false;
    }
    if (visible) {
      if (!this.rawVisible) {
        // Reaction delay — longer if the killer appears behind the survivor.
        const behind = angleDiff(Math.atan2(k.y - s.y, k.x - s.x), s.facing) > 1.9 ? 0.35 : 0;
        this.noticeAt = m.time + P.reaction * (1.25 - 0.45 * pers.killerAwareness) + behind * (1 - P.awareness);
        this.rawVisible = true;
      }
      if (m.time >= this.noticeAt) {
        this.detectMindgame(k, m, d);
        this.observe(k, m); this.observing = true;
        if (this.lastSighting === null || m.time - this.lastSighting > 0.5) { m.bb.report(s, k.x, k.y, k.vx, k.vy, k); this.lastSighting = m.time; }
      }
    } else this.rawVisible = false;

    // --- perk / aura information -----------------------------------------
    if (!this.observing) {
      if (m.killerRevealedTo(s, k)) { this.observe(k, m, INFO.VISIBLE, 0.95); this.observing = true; }
      else {
        // Kindred: a hooked teammate with Kindred shows the killer near the hook
        for (const o of m.survivors) if (o !== s && o.health === 'hooked' && o.hasPerk('kindred') && !s.status.has('blindness') && o.hook && dist(k.x, k.y, o.hook.x, o.hook.y) < 16 * M) { this.observe(k, m, INFO.VISIBLE, 0.9); this.observing = true; break; }
      }
    }
    // --- decay ------------------------------------------------------------
    if (!this.observing) {
      this.conf *= Math.exp(-dt / (P.memoryTau * (0.7 + 0.6 * pers.killerAwareness)));
      this.state = this.conf < 0.12 ? INFO.UNKNOWN : this.age < 2 ? INFO.RECENT : INFO.PREDICTED;
    }
    // --- terror radius (heartbeat): distance without direction -----------
    const mods = k.status.mods();
    this.heartbeat = 0; this.heardDist = null;
    if (!mods.undetectable && !s.status.has('oblivious') && d < k.terrorRadius) {
      this.heartbeat = 1 - d / k.terrorRadius;
      this.heardDist = d * (1 + gauss() * 0.12 * (1 - P.awareness));
      this.hbHist.push({ t: m.time, d: this.heardDist }); while (this.hbHist.length && m.time - this.hbHist[0].t > 2) this.hbHist.shift();
      if (!this.observing) {
        const p = this.pos();
        let dir;
        if (this.conf > 0.15) dir = Math.atan2(p.y - s.y, p.x - s.x);
        else if (m.bb.sightingOf(k) && Math.random() < P.comms) { const sg = m.bb.sightingOf(k); dir = Math.atan2(sg.y - s.y, sg.x - s.x); }
        else dir = this._hbDir ?? (this._hbDir = Math.random() * Math.PI * 2);
        const hd = this.heardDist;
        this.x = s.x + Math.cos(dir) * hd; this.y = s.y + Math.sin(dir) * hd; if (this.conf < 0.12) { this.vx = this.vy = 0; }
        this.dirKnown = this.dirKnown && this.age < 4;
        this.t = m.time; this.conf = Math.max(this.conf, 0.3 + 0.25 * P.threatSense); this.state = INFO.HEARD;
      }
    } else { this._hbDir = null; this.hbHist.length = 0; }
    // --- world sounds -------------------------------------------------------
    for (const snd of m.sounds) {
      if (snd.t <= this.lastSoundT || (snd.src && snd.src !== k)) continue;
      const sd = dist(s.x, s.y, snd.x, snd.y); if (sd > snd.radius) continue;
      const err = (1 - P.awareness) * 0.25 * sd;
      const ex = snd.x + gauss() * err, ey = snd.y + gauss() * err;
      if (snd.type === 'chainsaw_rev' || snd.type === 'chainsaw_sprint') {
        this.revvingT = m.time; if (snd.type === 'chainsaw_sprint') this.sprintT = m.time;
        if (!this.observing) { this.x = ex; this.y = ey; this.t = m.time; this.conf = Math.max(this.conf, 0.7); this.state = INFO.HEARD; this.dirKnown = true; }
        m.bb.model.chainsawUses += 0.02;
      } else if (snd.type === 'wraith_bell') {
        this.bellT = m.time; this.bellKind = snd.data.kind;
        if (!this.observing) { this.x = ex; this.y = ey; this.vx = this.vy = 0; this.t = m.time; this.conf = Math.max(this.conf, 0.75); this.state = INFO.HEARD; this.dirKnown = true; }
        m.bb.model.cloaks += 0.5;
      } else if (snd.type === 'teleport_warn') {
        // the Singularity is about to appear at this pod
        this.x = snd.x; this.y = snd.y; this.vx = this.vy = 0; this.t = m.time; this.conf = Math.max(this.conf, 0.85); this.state = INFO.HEARD; this.dirKnown = true; this.teleportT = m.time;
        if (sd < 16 * M) ai.note('Heard a biopod hum — teleport incoming', 'tpwarn');
      } else if (snd.type === 'power_warn') {
        // generic "the killer is about to appear here" cue (Dredge locker, Hag teleport, Cenobite box teleport…)
        this.x = snd.x; this.y = snd.y; this.vx = this.vy = 0; this.t = m.time; this.conf = Math.max(this.conf, 0.8); this.state = INFO.HEARD; this.dirKnown = true; this.teleportT = m.time; this.warnWhat = snd.data.what;
        if (sd < 16 * M) ai.note(snd.data.note || 'Heard the killer about to appear nearby', 'pwarn');
      } else if (snd.type === 'lich_cast') {
        // the team learns which spell went on cooldown
        (m.bb.lichCasts ||= {})[snd.data.spell] = m.time; this.lastSpell = snd.data.spell;
        if (!this.observing) { this.x = ex; this.y = ey; this.vx = this.vy = 0; this.t = m.time; this.conf = Math.max(this.conf, 0.7); this.state = INFO.HEARD; this.dirKnown = true; }
        if (sd < 30 * M && ai.params.difficulty >= 6) ai.note(`Heard the Lich cast ${({ fotd: 'Flight of the Damned', fly: 'Fly', magehand: 'Mage Hand', sphere: 'Dispelling Sphere' })[snd.data.spell]}`, 'lich-' + snd.data.spell);
      } else if (snd.type === 'dog_lullaby') {
        if (sd < 32 * M) { ai.note('Heard the dog\'s lullaby — it is searching nearby', 'lullaby'); this.lullabyT = m.time; }
      } else if (snd.type === 'blink_charge') {
        // the Nurse's charge: you hear where she is and that a blink is coming
        if (!this.observing) { this.x = ex; this.y = ey; this.vx = this.vy = 0; this.t = m.time; this.conf = Math.max(this.conf, 0.75); this.state = INFO.HEARD; this.dirKnown = true; }
        this.windupT = m.time; this.windupWhat = 'blink';
      } else if (snd.type === 'power_windup') {
        if (!this.observing) { this.x = ex; this.y = ey; this.vx = this.vy = 0; this.t = m.time; this.conf = Math.max(this.conf, 0.7); this.state = INFO.HEARD; this.dirKnown = true; }
        this.windupT = m.time; this.windupWhat = snd.data.what;
      } else if (snd.type === 'killer_action') {
        if (!this.observing) { this.x = ex; this.y = ey; this.vx = this.vy = 0; this.t = m.time; this.conf = Math.max(this.conf, 0.7); this.state = INFO.HEARD; this.dirKnown = true; }
      }
    }
    this.lastSoundT = m.time;
    // --- team callouts -------------------------------------------------------
    const sg = m.bb.sightingOf(k);
    if (!this.observing && sg && sg.by !== s && sg.t > this.t + 0.3 && Math.random() < P.comms * (0.6 + 0.4 * ai.pers.teamwork)) {
      const age = m.time - sg.t; const c = 0.8 * Math.exp(-age / P.memoryTau);
      if (c > this.conf) { this.x = sg.x + sg.vx * Math.min(age, 1.5); this.y = sg.y + sg.vy * Math.min(age, 1.5); this.vx = sg.vx; this.vy = sg.vy; this.t = sg.t; this.conf = c; this.state = INFO.RECENT; this.dirKnown = true; }
    }
    // --- Spine Chill: killer near and facing me, even undetectable --------
    this.spineChill = false;
    if (s.hasPerk('spine_chill') && d < 36 * M && angleDiff(k.facing, Math.atan2(s.y - k.y, s.x - k.x)) < 0.8 && m.world.losClear(k.x, k.y, s.x, s.y)) {
      this.spineChill = true; if (this.conf < 0.4) { this.conf = 0.4; this.state = INFO.HEARD; this.t = m.time; }
    }
    // approach rate (positive = closing in) based on belief
    const bp = this.pos(); const bd = dist(s.x, s.y, bp.x, bp.y);
    if (this._prevD !== null) this.approach = this.approach * 0.7 + ((this._prevD - bd) / Math.max(dt, 0.05)) * 0.3;
    this._prevD = bd;
  }

  detectMindgame(k, m, d) {
    const sp = Math.hypot(k.vx, k.vy);
    if (sp > 1 * M) {
      const h = Math.atan2(k.vy, k.vx);
      if (this.lastHeading !== null && d < 14 * M && m.time - this.headingT < 0.6 && angleDiff(h, this.lastHeading) > 1.9) {
        if (m.time - this.mindgameT > 2) m.bb.model.mindgames++;
        this.mindgameT = m.time;
      }
      if (this.lastHeading === null || angleDiff(h, this.lastHeading) > 0.3) { this.lastHeading = h; this.headingT = m.time; }
    } else if (d < 10 * M && !k.action && k.stunT <= 0) this.standT = m.time;
  }
}

return { INFO, KillerBelief };
};
__defs["js/ai/chaseAI.js"] = function(){
// Chase AI: real looping from map structure.
// Two Dijkstra distance fields (killer-model and self) over the nav grid let the bot
// find cells where it keeps the most time advantage — around walls, through windows
// (fast for survivors, slow for the killer) and behind dropped pallets (killer must break).
// A tile graph handles strategy: which loop to run to next, avoiding dead zones.
const { M, dist, clamp, angleDiff, gauss } = __req("js/core/util.js");

const SURV_SPEED = 4.0;

class ChaseBrain {
  constructor(ai) {
    this.ai = ai; this.nextTile = null; this.nextTileT = -99; this.lastDir = null; this.debug = {};
    this.palletMemo = new Map(); this.blockedTiles = new Map(); this.leaveTile = null; this.dodgeCD = 0; this.sidestepUntil = 0; this.sidestep = null;
  }

  survivorLinkCost() {
    const ai = this.ai, m = ai.m, P = ai.params;
    const vaultCost = 2.0 + (1 - P.fastVault) * 2.6;
    return (L) => {
      const o = L.ref;
      if (L.kind === 'window') {
        if (o.blockedUntil > m.time) return Infinity;
        if (ai.trapNear(o.x, o.y, 2.2 * M)) return Infinity;
        return vaultCost / ai.knobs.windowValue;
      }
      if (o.state !== 'down') return Infinity;
      if (ai.trapNear(o.x, o.y, 2.0 * M)) return Infinity;
      return 2.0;
    };
  }
  killerLinkCost() {
    const m = this.ai.m, noVault = this.ai.fk?.power.noVault?.();
    return (L) => {
      const o = L.ref;
      if (L.kind === 'window') return o.killerBlockedUntil > m.time || noVault ? Infinity : 1.7 * 4.6 + 1;
      if (o.state === 'down') return 2.6 * 4.6 + 1.5;
      return Infinity;
    };
  }
  survivorCellCost() {
    const ai = this.ai, nav = ai.m.map.nav, P = ai.params;
    const trapper = ai.m.hasPower('traps');
    const cells = ai.trapCells(); const zc = ai.zoneCells();
    return (i) => {
      let c = zc.get(i) || 0;
      if (cells.has(i)) c += 40;
      if (trapper && nav.grass[i] && P.difficulty >= 7) c += 1.5 * P.trapSense;
      return c;
    };
  }

  // Strategic: which tile should this chase move toward?
  chooseNextTile(kp) {
    const ai = this.ai, m = ai.m, s = ai.sv, P = ai.params, map = m.map;
    const cur = map.tileAt(s.x, s.y);
    let best = null, bs = -1e9;
    const teamGens = P.leadAway > 0.3 ? m.bb.activeGens(s) : [];
    const zones = [...ai.memory.zones.values()].filter((z) => z.until > m.time && z.aiCost > 0);
    const pods = [...ai.memory.devices.values()].filter((d) => d.dangerRadius && !d.dead && (d.data?.disabledUntil || 0) <= m.time);
    const slip = s.status.has('slipstream');
    for (const t of map.tiles) {
      if (t.safety < 0.12) continue;
      if ((this.blockedTiles.get(t.id) || 0) > m.time) continue;
      const dMe = dist(s.x, s.y, t.x, t.y) / M; if (dMe > 48) continue;
      if (t === cur && t.safety > 0.35 && (t.resources.up + t.resources.down + t.resources.wins) > 0) continue;
      const dK = dist(kp.x, kp.y, t.x, t.y) / M;
      const away = ((t.x - s.x) * (s.x - kp.x) + (t.y - s.y) * (s.y - kp.y)) / Math.max(1, dist(t.x, t.y, s.x, s.y) * dist(s.x, s.y, kp.x, kp.y));
      let dead = 0; for (let k = 1; k <= 5; k++) { const f = k / 6; dead += map.nav.deadZone[map.nav.idxAt(s.x + (t.x - s.x) * f, s.y + (t.y - s.y) * f)]; }
      dead /= 5;
      let sc = t.safety * 3.2 + (t.resources.up * 0.5 + t.resources.wins * 0.35) + away * 1.6 - dMe * 0.06 - dead * 2.5 * P.deadZoneAware + t.connected.length * 0.08;
      if (dK < dMe) sc -= 2.5 * P.tileGraph;
      // unsafe loops: gas / torment already on the tile, or a watched pod nearby while slipstreamed
      const r = t.rect; const onTile = zones.filter((z) => z.x > r.x - z.r && z.x < r.x + r.w + z.r && z.y > r.y - z.r && z.y < r.y + r.h + z.r).length;
      if (onTile) sc -= Math.min(3, onTile * (P.zoneSense * 1.2)) * ai.knobs.zoneAvoid;
      if (pods.length) { const watched = pods.some((d) => dist(d.x, d.y, t.x, t.y) < d.dangerRadius + t.radius * 0.5); if (watched) sc -= (slip ? 2.5 : 1) * P.zoneSense; }
      if (teamGens.length) { const near = teamGens.some((g) => dist(g.x, g.y, t.x, t.y) < 18 * M); if (near) sc -= 2 * P.leadAway * ai.pers.teamwork; }
      const kn2 = ai.knobs; if (kn2.tightPenalty && t.radius < 6.5 * M) sc -= kn2.tightPenalty * P.loopIQ;
      if (kn2.windowValue !== 1) sc += (kn2.windowValue - 1) * t.resources.wins * 1.2 * P.loopIQ;
      sc = sc * P.tileGraph + (1 - P.tileGraph) * (-dMe * 0.15 + Math.random() * 1.5);
      if (sc > bs) { bs = sc; best = t; }
    }
    return best;
  }

  // Walk the local field back from target to source to recover a path.
  descend(F, targetIdx, linkCost) {
    const nav = this.ai.m.map.nav, cols = nav.cols;
    const lid = (i) => { const c = i % cols - F.ox, r = ((i / cols) | 0) - F.oy; return c < 0 || r < 0 || c >= F.size || r >= F.size ? -1 : r * F.size + c; };
    const pts = []; let cur = targetIdx; let guard = 0;
    while (guard++ < 60) {
      const cl = lid(cur); if (cl < 0) break; const dc = F.d[cl];
      const c = nav.center(cur); pts.push({ x: c.x, y: c.y, viaLink: null, _i: cur });
      if (dc <= 0.01) break;
      let best = -1, bd = dc, viaLink = null;
      const cx = cur % cols, cy = (cur / cols) | 0;
      for (let dy = -1; dy <= 1; dy++) for (let dx = -1; dx <= 1; dx++) {
        if (!dx && !dy) continue; const n = (cy + dy) * cols + (cx + dx); const nl = lid(n); if (nl < 0) continue;
        if (F.d[nl] < bd - 0.01) { bd = F.d[nl]; best = n; viaLink = null; }
      }
      const ls = nav.linksAt.get(cur);
      if (ls) for (const L of ls) { const c = linkCost(L); if (!isFinite(c)) continue; const n = L.a === cur ? L.b : L.a; const nl = lid(n); if (nl >= 0 && F.d[nl] + c <= dc + 0.05 && F.d[nl] < bd - 0.01) { bd = F.d[nl]; best = n; viaLink = L; } }
      if (best < 0) break;
      if (viaLink) pts[pts.length - 1].viaLink = viaLink; // arriving at this point required crossing the link
      cur = best;
    }
    pts.reverse();
    // viaLink was attached to the point *after* the crossing in walk order; shift to align with findPath convention
    return pts;
  }

  tick(urgent = true) {
    const ai = this.ai, m = ai.m, s = ai.sv, P = ai.params, pers = ai.pers, B = ai.belief, k = ai.fk, nav = m.map.nav;
    let lead = P.predictLead * (0.5 + 0.7 * pers.predictionSkill);
    // The killer cannot be predicted past the survivor: cap the lead by the gap that is left.
    const b0 = B.pos(0); const gap = dist(b0.x, b0.y, s.x, s.y); const kspd = Math.hypot(B.vx, B.vy);
    if (kspd > 1) lead = Math.min(lead, Math.max(0, (gap - 2.5 * M) / kspd));
    let kp = B.pos(lead);
    // Extrapolation never passes through walls: clip the prediction at the first obstacle.
    const hit = m.world.firstHit(B.x, B.y, kp.x, kp.y, 0.4 * M);
    if (hit) { const t = Math.max(0, hit.t - 0.05); kp = { x: B.x + (kp.x - B.x) * t, y: B.y + (kp.y - B.y) * t }; }
    // Read what the killer is committed to: a vault lands on the far side; a pallet being broken is effectively gone.
    const kAct = B.observing ? k.action : null;
    if (kAct && kAct.type === 'vault' && kAct.to && P.difficulty >= 4) kp = { x: kAct.to.x, y: kAct.to.y };
    const breakingPallet = kAct && kAct.type === 'break' && P.difficulty >= 3 ? kAct.target : null;
    if (breakingPallet) breakingPallet.state = 'broken';
    // Plan to use nearby standing pallets: model them as dropped for the killer's field,
    // so cells behind a pallet look safe and the bot plays the pallet instead of running past it.
    const kNow = B.pos(0.1);
    const planned = [];
    if (P.loopIQ > 0.3) for (const p of m.map.pallets) {
      if (p.state !== 'up' || p.dropping || this.isGreeding(p)) continue;
      const dp = dist(p.x, p.y, s.x, s.y) / M, dkp = dist(p.x, p.y, kNow.x, kNow.y) / M;
      if (dp < 6 && dkp > 2.2 && dkp < 18 && p.sideOf(kNow) !== p.sideOf(s) || (dp < 3 && dkp > 3 && dkp < 14)) { p.state = 'down'; planned.push(p); }
    }
    const dK = dist(s.x, s.y, kp.x, kp.y) / M;
    const mindgame = P.usesMindgames && m.time - B.mindgameT < 1.4 && Math.random() < P.mindgameRead + 0.1;
    const half = P.fieldHalf;
    const kF = nav.field(kp.x, kp.y, half + 12, this.killerLinkCost(), null, 70);
    let altF = null;
    if (mindgame && !B.observing) { const back = { x: B.x - B.vx * 0.9, y: B.y - B.vy * 0.9 }; altF = nav.field(back.x, back.y, half + 10, this.killerLinkCost(), null, 60); }
    const H = P.horizon * (0.8 + 0.4 * pers.loopSkill);
    if (breakingPallet) breakingPallet.state = 'down';
    for (const p of planned) p.state = 'up';
    const myF = nav.field(s.x, s.y, Math.ceil(H) + 3, this.survivorLinkCost(), this.survivorCellCost(), H + 4);

    // Tile strategy
    const curTile = m.map.tileAt(s.x, s.y);
    const depleted = curTile && curTile.resources && curTile.resources.up + curTile.resources.down + curTile.resources.wins === 0;
    const killerBreaking = B.observing && B.actionType === 'break';
    if (!this.nextTile || m.time - this.nextTileT > 1.5 || depleted || killerBreaking) { this.nextTile = this.chooseNextTile(kp); this.nextTileT = m.time; }
    if (killerBreaking && P.difficulty >= 6 && !this._brkNote) { this._brkNote = m.time; ai.note('Left the tile while the killer broke the pallet', 'tilechain'); }
    if (!killerBreaking) this._brkNote = 0;

    // Only the Hillbilly's long sprint makes line of sight matter; Bubba's sweep is about distance.
    const hilly = k.power.id === 'chainsaw';
    const revving = hilly && m.time - B.revvingT < 0.8 && B.conf > 0.5;
    const sprinting = hilly && m.time - B.sprintT < 0.5 && B.observing;
    const kn = ai.knobs, pw = k.power, zc = ai.zoneCells();
    // Ranged pressure from the current killer's power (only what the survivor can perceive).
    const ranged = revving || sprinting || (B.conf > 0.5 && dist(B.x, B.y, s.x, s.y) < 30 * M && ((pw.id === 'hatchets' && pw.count > 0) || (pw.id === 'shroud' && pw.shrouded && B.observing) || (pw.id === 'tonic' && pw.state === 'aiming' && B.observing) || (pw.id === 'redeemer' && pw.loaded) || (pw.id === 'lament' && pw.cd('chain') <= 1) || (pw.id === 'virulent' && pw.tokens > 0 && B.observing)));
    const losW = revving || sprinting ? 4 : ranged ? kn.losValue : 0;
    const straightW = (revving || sprinting ? 1.5 : ranged && kn.openPenalty > 1.4 ? 1.2 : 0) * P.lineAware;
    // Executioner in Punishment mode: loops do not protect from the wave — get distance, go lateral.
    const abandon = kn.loopAbandon && B.observing && ['punish', 'windup', 'recover'].includes(pw.state) && dist(B.x, B.y, s.x, s.y) < 19 * M;
    if (abandon && !this._abNote) { this._abNote = m.time; ai.note('Abandoned the loop — Punishment mode reaches through walls', 'abandon'); }
    if (!abandon) this._abNote = 0;
    const teamGens = P.leadAway > 0.3 && pers.teamwork > 0.4 ? m.bb.activeGens(s) : [];
    const ratio = 4.6 / SURV_SPEED;
    const onLoop = curTile && curTile.safety > 0.3 && !depleted;
    // Home tile: keep running the structure we are on while it still has resources.
    if (onLoop) this.home = curTile;
    else if (this.home && (dist(this.home.x, this.home.y, s.x, s.y) > this.home.radius + 8 * M || this.home.resources.up + this.home.resources.down + this.home.resources.wins === 0)) this.home = null;
    const home = killerBreaking || abandon ? null : this.home;
    const loop = 0.3 + 0.7 * P.loopIQ * (0.7 + 0.6 * pers.loopSkill);
    // Resources the survivor wants to put between itself and the killer (standing pallets).
    const pals = m.map.pallets.filter((p) => p.state === 'up' && !p.dropping && dist(p.x, p.y, s.x, s.y) < (H + 3) * M && dist(p.x, p.y, kp.x, kp.y) < 16 * M);
    let best = null, bestScore = -1e9, flat = 0, cnt = 0;
    const sz = myF.size;
    for (let r = 0; r < sz; r++) for (let c = 0; c < sz; c++) {
      const md = myF.d[r * sz + c]; if (!(md <= H) || md < 0.9) continue;
      const gi = (myF.oy + r) * nav.cols + (myF.ox + c); if (gi < 0 || gi >= nav.blocked.length) continue;
      if (nav.pallet[gi] >= 0) continue; // never plan to stand inside a pallet gap
      const p = nav.center(gi);
      let kd = kF.at(p.x, p.y); if (!isFinite(kd)) kd = 45;
      if (altF) { const a = altF.at(p.x, p.y); if (isFinite(a)) kd = Math.min(kd, a + 1.5); }
      const margin = kd - md * ratio;
      let sc = margin * loop + (1 - loop) * (dist(p.x, p.y, kp.x, kp.y) / M) * 0.6;
      const tile = nav.tileOf[gi] >= 0 ? m.map.tiles[nav.tileOf[gi]] : null;
      if (tile) sc += tile.safety * 1.6 * P.tileGraph;
      for (const pl of pals) if (dist(p.x, p.y, pl.x, pl.y) < 1.9 * M && pl.sideOf(p) !== pl.sideOf(kNow)) sc += 2.4 * (0.3 + P.loopIQ) * (0.6 + 0.4 * pers.palletDiscipline) * kn.palletValue;
      sc -= nav.deadZone[gi] * 3 * P.deadZoneAware * kn.openPenalty;
      const zcost = zc.get(gi); if (zcost) sc -= zcost * 0.35;
      if (abandon) { const fa = B.facing; const side = Math.abs(-(p.x - B.x) * Math.sin(fa) + (p.y - B.y) * Math.cos(fa)) / M; sc += Math.min(side, 4) * 0.6 * P.dodge; }
      if (this.nextTile) { const w = (onLoop && !killerBreaking && !abandon) ? (margin > 9 ? 0.15 : 0.03) : 0.32; sc -= dist(p.x, p.y, this.nextTile.x, this.nextTile.y) / M * w * P.tileGraph * (killerBreaking ? 2 : 1); }
      if (home) { const r = home.rect; const ex = Math.max(r.x - p.x, 0, p.x - (r.x + r.w)), ey = Math.max(r.y - p.y, 0, p.y - (r.y + r.h)); const out = Math.hypot(ex, ey) / M; if (out > 3) sc -= (out - 3) * 1.3 * P.tileGraph; }
      if (this.lastDir) { const dx = p.x - s.x, dy = p.y - s.y; const l = Math.hypot(dx, dy) || 1; sc += ((dx * this.lastDir.x + dy * this.lastDir.y) / l) * 0.7; }
      if (losW) {
        // break line of sight from ranged/stalking threats; avoid long straight lines away from them
        if (!m.world.losClear(p.x, p.y, B.x, B.y)) sc += losW * P.lineAware;
        if (straightW) {
          const ax = p.x - B.x, ay = p.y - B.y; const al = Math.hypot(ax, ay) || 1;
          const sx = s.x - B.x, sy = s.y - B.y; const sl = Math.hypot(sx, sy) || 1;
          sc -= ((ax * sx + ay * sy) / (al * sl)) * straightW;
        }
      }
      if (teamGens.length) for (const g of teamGens) { const gd = dist(g.x, g.y, p.x, p.y) / M; if (gd < 18) sc -= (18 - gd) * 0.12 * P.leadAway; }
      sc += md * 0.08; // prefer committing to a direction over shuffling between neighbours
      if (P.difficulty < 5) sc += gauss() * (5 - P.difficulty) * 0.6;
      if (sc > bestScore) { bestScore = sc; best = gi; }
      flat += margin; cnt++;
    }
    const meanMargin = cnt ? flat / cnt : 0;
    let decision = null;
    // --- chainsaw sidestep -------------------------------------------------
    if (sprinting && B.observing) {
      const toMe = Math.atan2(s.y - B.y, s.x - B.x);
      const d = dist(s.x, s.y, B.x, B.y);
      const eta = d / (9.2 * M);
      if (angleDiff(B.facing, toMe) < 0.4 && eta < 0.75 && this.sidestepUntil < m.time && Math.random() < P.dodge) {
        const side = Math.random() < 0.5 ? 1 : -1; const a = B.facing + side * Math.PI / 2;
        this.sidestep = { x: s.x + Math.cos(a) * 3 * M, y: s.y + Math.sin(a) * 3 * M }; this.sidestepUntil = m.time + 0.6;
        ai.note('Side-stepped the chainsaw', 'sidestep');
      }
    }
    if (this.sidestepUntil > m.time && this.sidestep) {
      ai.setPath([{ x: s.x, y: s.y }, this.sidestep]); decision = 'sidestep';
    } else if (best !== null) {
      const pts = this.descend(myF, best, this.survivorLinkCost());
      const bp = nav.center(best); let dx = bp.x - s.x, dy = bp.y - s.y; let l = Math.hypot(dx, dy);
      if (l < 0.5 * M && this.lastDir) { dx = this.lastDir.x; dy = this.lastDir.y; l = 1; }
      l = l || 1; this.lastDir = { x: dx / l, y: dy / l };
      if (pts.length) {
        pts[0] = { x: s.x, y: s.y, viaLink: pts[0].viaLink };
        // Never stall: if the plan is shorter than a couple of metres, keep running the same way.
        const end = pts[pts.length - 1]; const plen = dist(s.x, s.y, end.x, end.y);
        if (plen < 2.5 * M && !end.viaLink) {
          const ex = end.x + this.lastDir.x * 3 * M, ey = end.y + this.lastDir.y * 3 * M;
          if (m.world.sweepClear(end.x, end.y, ex, ey, s.r)) pts.push({ x: ex, y: ey, viaLink: null });
        }
        ai.setPath(pts, true);
      }
    }

    // --- pallet decisions ----------------------------------------------------
    decision = this.palletLogic(kF, kp, dK, meanMargin, urgent) || decision;
    // --- Dead Hard -----------------------------------------------------------
    if (s.health === 'injured' && s.hasPerk('dead_hard') && !s.status.has('exhausted') && B.observing && B.attacking && dist(s.x, s.y, B.x, B.y) < 3.2 * M && Math.random() < P.dodge * 0.8) {
      if (m.perks.activate(s, 'dead_hard')) { ai.note('Dead Hard dodge', 'dh'); decision = 'dead hard'; }
    }
    this.debug = { tile: curTile ? curTile.label : (nav.deadZone[nav.idxAt(s.x, s.y)] > 0.6 ? 'DEAD ZONE' : 'open'), nextTile: this.nextTile ? this.nextTile.label + ' #' + this.nextTile.id : '-', margin: meanMargin.toFixed(1), best: best !== null ? nav.center(best) : null, decision: decision || this.debug.decision, mindgame, killerD: dK.toFixed(1), kF, myF };
    return decision;
  }

  // Greed is decided once per pallet approach (not re-rolled every tick).
  isGreeding(p) { const g = this.greed?.get(p.id); return !!(g && g.greedy && this.ai.m.time - g.t < 7); }
  decideGreed(p, meanMargin) {
    if (!this.greed) this.greed = new Map();
    const ai = this.ai, m = ai.m, P = ai.params, g = this.greed.get(p.id);
    if (g && m.time - g.t < 7) return g.greedy;
    const chainsaw = ai.fk.power.id === 'chainsaw' || ai.fk.power.id === 'bubba';
    const others = m.map.pallets.filter((q) => q !== p && q.state === 'up' && dist(q.x, q.y, p.x, p.y) < 22 * M).length;
    const greedy = P.usesGreed && !chainsaw && others > 0 && meanMargin > 2.5 && Math.random() < P.greed * ai.knobs.greed * ai.pers.palletDiscipline * (0.5 + m.bb.tendencies.palletRespect);
    this.greed.set(p.id, { greedy, t: m.time });
    if (greedy) ai.note('Greeding the pallet — loop still safe', 'greed');
    return greedy;
  }

  palletLogic(kF, kp, dK, meanMargin, urgent) {
    const ai = this.ai, m = ai.m, s = ai.sv, P = ai.params, pers = ai.pers, B = ai.belief;
    if (s.action || s.vault || B.conf < 0.45) return null;
    kp = B.pos(0.1); // pallet calls use where the killer actually is, not a long extrapolation
    dK = dist(s.x, s.y, kp.x, kp.y) / M;
    for (const p of m.map.pallets) {
      if (p.state !== 'up' || p.dropping) continue;
      const along = Math.abs((s.x - p.x) * (p.alongX ? 1 : 0) + (s.y - p.y) * (p.alongX ? 0 : 1));
      const signed = (s.x - p.x) * p.nx + (s.y - p.y) * p.ny;
      if (along > p.len / 2 + 0.3 * M || Math.abs(signed) > 2.1 * M || Math.abs(signed) < 0.35 * M) continue;
      const mySide = signed < 0 ? 0 : 1; const kSide = p.sideOf(kp);
      if (mySide === kSide) continue;
      const kDistToPallet = dist(kp.x, kp.y, p.x, p.y) / M;
      if (kDistToPallet > 10) continue;
      // How much longer would the killer's path to me be with the pallet down?
      let gain = this.palletMemo.get(p.id);
      if (!gain || m.time - gain.t > 0.5) {
        p.state = 'down';
        const f = m.map.nav.field(kp.x, kp.y, 20, this.killerLinkCost(), null, 60);
        p.state = 'up';
        const f0 = m.map.nav.field(kp.x, kp.y, 20, this.killerLinkCost(), null, 60);
        const now = f0.at(s.x, s.y), after = f.at(s.x, s.y);
        gain = { v: (isFinite(after) ? after : 40) - (isFinite(now) ? now : 40), t: m.time };
        this.palletMemo.set(p.id, gain);
      }
      const stunPos = B.pos(0.35);
      const stun = p.stunZone(stunPos) && B.observing;
      const chainsaw = (ai.fk.power.id === 'chainsaw' || ai.fk.power.id === 'bubba') && (m.time - B.revvingT < 1);
      const pwr = ai.fk.power; const powerThreat = B.observing && (pwr.state === 'winding' || pwr.state === 'aiming' || pwr.state === 'charging' || pwr.state === 'revving' || pwr.frenzy);
      const throwDist = P.throwDist * (urgent ? 1 : 0.6);
      const greedy = dK < 7 ? this.decideGreed(p, meanMargin) : false;
      let why = null;
      if (stun && Math.random() < 0.25 + P.dodge * 0.7) why = 'stun';
      else if (dK < (greedy ? 2.9 : throwDist) && gain.v > 3.5) why = 'drop';
      else if (P.preDrops && gain.v > 4 && dK < 7 + ai.knobs.preDrop * 3 && (chainsaw || powerThreat || m.time - B.mindgameT < 1.5)) why = 'pre-drop';
      else if (dK < 9 && gain.v > 0 && Math.random() < P.palletWaste * 0.12) why = 'wasted';
      if (why && s.startAction('dropPallet', p, m)) {
        if (why === 'pre-drop') ai.note(chainsaw ? 'Pre-dropped the pallet against the chainsaw' : powerThreat ? 'Pre-dropped the pallet before the throw' : 'Pre-dropped the pallet expecting a cut-off', 'predrop');
        else if (why === 'stun') ai.note('Timed the pallet for a stun', 'stun');
        else if (why === 'wasted') ai.note('Panicked and dropped a pallet', 'waste');
        return 'pallet: ' + why;
      }
    }
    return null;
  }
}

return { ChaseBrain };
};
__defs["js/ai/utilityAI.js"] = function(){
// Utility AI: scores every goal a survivor could pursue from what it currently knows.
// Each option keeps its reasons so the debug overlay can explain the decision.
const { M, dist, clamp, smoothstep } = __req("js/core/util.js");
const { HOOK_STAGE_TIME } = __req("js/entities/survivor.js");

const GOAL_STATE = {
  REPAIR: 'REPAIRING_GENERATOR', RESCUE: 'RESCUING', HEAL: 'HEALING_TEAMMATE', BE_HEALED: 'HELPING_TEAMMATE', HEAL_SELF: 'HEALING_SELF',
  CLEANSE: 'INVESTIGATING', CHEST: 'SEARCHING_CHEST', DISARM: 'INVESTIGATING', FREE: 'HELPING_TEAMMATE', OPEN_GATE: 'OPENING_EXIT_GATE',
  ESCAPE: 'GOING_TO_EXIT', HATCH: 'GOING_TO_EXIT', PROTECT: 'TAKING_PROTECTION_HIT', FLASHLIGHT: 'HELPING_TEAMMATE', PATROL: 'PATROLLING', KEY_HATCH: 'GOING_TO_EXIT', PRINT_EMP: 'INVESTIGATING', USE_EMP: 'INVESTIGATING', DEVICE: 'INVESTIGATING', BURN_TRAP: 'INVESTIGATING', FOLLOW_CARRY: 'HELPING_TEAMMATE',
};

function evaluateGoals(ai) {
  const m = ai.m, s = ai.sv, P = ai.params, pers = ai.pers, bb = m.bb;
  const opts = [];
  const add = (type, target, score, why, extra = null) => { if (score > 0.5) opts.push({ type, target, key: type + ':' + (target?.id ?? ''), score, why, ...(extra || {}) }); };
  const myD = (x, y) => dist(s.x, s.y, x, y) / M;
  const safety = (x, y) => ai.safetyAt(x, y);
  const gatesPowered = m.gatesPowered, anyOpen = m.map.gates.some((g) => g.open);
  const others = m.survivors.filter((o) => o !== s && o.alive);
  const kn = ai.knobs, pb = ai.perkBrain.bias; // killer counterplay knobs, perk-driven biases
  const marked = s.status.has('marked') || s.status.has('exposed');
  const pods = [...ai.memory.devices.values()].filter((d) => d.kind === 'biopod' && !d.dead && (d.data.disabledUntil || 0) <= m.time);

  // ---- generators ------------------------------------------------------
  if (!gatesPowered) {
    const incomplete = m.map.gens.filter((g) => !g.done);
    const need = m.gensNeeded - m.gensDone;
    for (const g of incomplete) {
      if (g.blockedUntil > m.time) continue;
      const prog = ai.genKnowledge(g);
      const d = myD(g.x, g.y);
      let sc = 52 + 26 * pers.objectiveFocus;
      const fProg = 0.75 + 0.45 * prog;
      const fDist = 0.3 + 0.7 / (1 + d / 40);
      const fSafe = 0.2 + 0.8 * safety(g.x, g.y);
      const n = [...g.repairers].filter((o) => o !== s).length;
      let fCrowd = n === 0 ? 1 : n === 1 ? (prog > 0.55 ? 1.05 : 0.82) : n === 2 ? 0.5 : 0.25;
      if (n > 0 && kn.groupPenalty) fCrowd *= 1 - kn.groupPenalty * P.awareness;
      const claimer = bb.claimedBy('REPAIR:' + g.id);
      if (claimer && claimer !== s && !g.repairers.has(claimer) && Math.random() < P.comms) fCrowd *= 0.75;
      let fStrat = 1, why = [`prog ${Math.round(prog * 100)}%`, `${d.toFixed(0)}m`, `safe ${fSafe.toFixed(2)}`];
      if (P.genStrategy && incomplete.length > need) {
        const dens = incomplete.filter((o) => o !== g && dist(o.x, o.y, g.x, g.y) < 32 * M).length;
        fStrat = 1 + 0.14 * dens * P.clusterAware * pb.cluster;
        if (dens >= 2) why.push('breaks 3-gen cluster');
      }
      if (g.regressing && prog > 0.3) { fStrat *= 1.1; why.push('regressing'); }
      let fEsc = 1;
      if (P.difficulty >= 6) {
        const t = ai.nearestTile(g.x, g.y, 16 * M);
        fEsc = 0.85 + (t ? t.safety : 0) * 0.2 * kn.genEscape;
      }
      // surveillance / area denial around the generator (known pods, gas, trails)
      if (pods.length && P.difficulty >= 3) { const w = pods.filter((d) => dist(d.x, d.y, g.x, g.y) < d.dangerRadius + 2 * M).length; if (w) { fEsc *= Math.pow(0.8 - 0.25 * P.zoneSense, w) * (s.status.has('slipstream') ? 0.7 : 1); why.push(`${w} pod${w > 1 ? 's' : ''} watching`); } }
      const gz = [...ai.memory.zones.values()].filter((z) => z.aiCost > 0 && z.until > m.time && dist(z.x, z.y, g.x, g.y) < z.r + 1.2 * M);
      if (gz.length) { fEsc *= gz.some((z) => z.kind === 'tonic') ? 0.75 - 0.45 * P.zoneSense : 0.85; why.push('gas/trail on it'); }
      const pgf = ai.profile.genFactor?.(ai, g); if (pgf) { fEsc *= pgf.f; if (pgf.why) why.push(pgf.why); }
      fStrat *= pb.repair;
      if (n > 0) fStrat *= pb.joinGen;
      if (s.item?.id === 'toolbox' && s.item.charges > 4 && prog < 0.5) fStrat *= 1.08;
      sc *= fProg * fDist * fSafe * fCrowd * fStrat * fEsc;
      add('REPAIR', g, sc, why);
    }
  }

  // ---- rescues ----------------------------------------------------------
  for (const o of others) {
    if (o.health !== 'hooked') continue;
    const left = Math.max(0, m.hookStageTime - o.hookTimer);
    const urgency = o.hookStage >= 2 ? 0.95 + 0.1 * (1 - left / m.hookStageTime) : 0.66 + 0.34 * (1 - left / m.hookStageTime);
    const d = myD(o.x, o.y);
    let sc = 78 + 30 * pers.rescuePriority - 20 * pers.selfishness;
    const fDist = 0.75 + 0.25 / (1 + d / 50);
    const kh = ai.killerDistAt(o.x, o.y);
    const travel = d / 4;
    let fCamp = 1; const why = [`stage ${o.hookStage}`, `${left.toFixed(0)}s left`];
    if (kh < 16 * kn.rescueWait && ai.belief.conf > 0.25) {
      if (P.waitsForCamper && left > travel + 14) { fCamp = 0.45 / kn.rescueWait; why.push('killer near hook: wait'); }
      else fCamp = 0.85;
    }
    if (o.hook?.cage) why.push('caged');
    if (marked && !(left < travel + 15)) { fCamp *= 0.45; why.push('I am marked'); }
    sc *= pb.rescue;
    let urgent = o.hookStage >= 2;
    if (left < travel + 15) { sc *= 1.3; why.push('must go now'); urgent = true; }
    let fClaim = 1;
    const c = bb.claimedBy('RESCUE:' + o.id);
    if (c && c !== s && Math.random() < P.comms + 0.2) { fClaim = dist(c.x, c.y, o.x, o.y) < dist(s.x, s.y, o.x, o.y) ? 0.08 : 0.5; why.push(`${c.name} going`); }
    if (s.health === 'injured' && others.some((q) => q.health === 'healthy' && q !== o)) fClaim *= 0.85;
    add('RESCUE', o, sc * urgency * fDist * fCamp * fClaim, why);
    if (urgent && fClaim > 0.4) opts[opts.length - 1] && (opts[opts.length - 1].urgent = true);
  }

  // ---- healing ----------------------------------------------------------
  for (const o of others) {
    if (o.health !== 'injured' && o.health !== 'dying') continue;
    if (o.status.has('broken')) continue;
    if (bb.inChase(o) || (o.ai && o.ai.inChase)) continue;
    const d = myD(o.x, o.y);
    const dying = o.health === 'dying';
    if (dying && (o.knockOutUntil || 0) > m.time && d > 16) continue; // Knock Out: can't locate them
    if (s.status.has('blindness') && d > 24) continue; // Blinded: teammates' auras are hidden
    let sc = (dying ? 62 + 30 * pers.rescuePriority : 34 + 40 * pers.healingPriority) * kn.healBias * pb.heal;
    if (o.status.has('tormented') && !dying) sc *= 1.15;
    const fSafe = 0.15 + 0.85 * safety(o.x, o.y);
    let f = (0.25 + 0.75 / (1 + d / 22)) * fSafe;
    if (s.item?.id === 'medkit' && s.item.charges > 2) f *= 1.2;
    if (dying && ai.killerDistAt(o.x, o.y) < 12 && ai.belief.conf > 0.3) f *= 0.25; // slugged as bait
    const c = bb.claimedBy('HEAL:' + o.id); if (c && c !== s) f *= 0.25;
    if (s.health === 'injured' && !dying) f *= 0.6;
    if (anyOpen && !dying) f *= 0.3;
    add('HEAL', o, sc * f, [dying ? 'pick up' : 'injured', `${d.toFixed(0)}m`]);
  }
  // Teammate is coming to heal me
  if (s.health === 'injured') {
    const c = bb.claimedBy('HEAL:' + s.id);
    if (c && c !== s && dist(c.x, c.y, s.x, s.y) < 24 * M) add('BE_HEALED', c, 58 + 10 * pers.teamwork, [`${c.name} will heal`]);
    const can = (s.item?.id === 'medkit' && s.item.charges > 1) || s.hasPerk('self_care') || pb.selfHeal;
    if (can) {
      let sc = (22 + 36 * pers.healingPriority) * kn.healBias * pb.healSelf;
      sc *= 0.2 + 0.8 * safety(s.x, s.y);
      if (P.healWisdom > 0.5 && m.map.gens.some((g) => !g.done && g.progress > 0.7 && dist(g.x, g.y, s.x, s.y) < 20 * M)) sc *= 0.6;
      sc += (1 - P.healWisdom) * 18 * Math.random(); // inexperienced bots heal at bad times
      if (gatesPowered) sc *= 0.5;
      add('HEAL_SELF', null, sc, [s.item?.id === 'medkit' ? 'med-kit' : 'self-care']);
    }
  }

  // ---- traps (Trapper) --------------------------------------------------
  for (const o of others) if (o.trap) add('FREE', o, (68 + 20 * pers.teamwork) * (0.3 + 0.7 / (1 + myD(o.x, o.y) / 30)) * (0.3 + 0.7 * safety(o.x, o.y)), ['trapped']);
  if (m.hasPower('traps') && P.difficulty >= 4) {
    for (const t of ai.memory.traps.values()) {
      if (t.trap.state !== 'set') continue;
      const d = myD(t.x, t.y); if (d > 30) continue;
      const nearObj = m.map.gens.some((g) => !g.done && dist(g.x, g.y, t.x, t.y) < 12 * M) || !!m.map.tileAt(t.x, t.y);
      add('DISARM', t.trap, (14 + (nearObj ? 22 : 6) * P.trapSense) * (0.2 + 0.8 * safety(t.x, t.y)) / (1 + d / 25), ['known trap', nearObj ? 'blocks objective' : '']);
    }
  }

  // ---- totems -------------------------------------------------------------
  for (const [id, info] of ai.memory.totems) {
    const t = info.totem; if (t.cleansed) continue;
    const d = myD(t.x, t.y);
    let sc = (t.hex === 'ruin' ? 46 : t.hex === 'noed' ? 92 : t.hex ? 40 : (P.difficulty >= 8 && m.gensDone >= 2 ? 10 : 3)) * pb.cleanse;
    sc *= (0.3 + 0.7 / (1 + d / 30)) * (0.25 + 0.75 * safety(t.x, t.y));
    add('CLEANSE', t, sc, [t.hex ? 'hex: ' + t.hex : 'dull totem']);
  }
  // ---- chests --------------------------------------------------------------
  if (!s.item || s.item.charges <= 0) for (const c of m.map.chests) {
    if (c.opened) continue; const d = myD(c.x, c.y); if (d > 40) continue;
    add('CHEST', c, (10 + 14 * (m.time < 150 ? 1 : 0.4)) * pb.chest * (0.2 + 0.8 * safety(c.x, c.y)) / (1 + d / 25), ['no item']);
  }

  // ---- endgame ------------------------------------------------------------
  if (gatesPowered) {
    for (const g of m.map.gates) {
      if (g.open) { add('ESCAPE', g, 96 * (0.55 + 0.45 * safety(g.x, g.y)) * (0.6 + 0.4 / (1 + myD(g.x, g.y) / 60)), ['gate open']); continue; }
      let sc = 88 * pb.gate * (0.3 + 0.7 * safety(g.x, g.y)) * (0.5 + 0.5 / (1 + myD(g.x, g.y) / 50));
      const c = bb.claimedBy('OPEN_GATE:' + g.id); if (c && c !== s) sc *= 0.3;
      add('OPEN_GATE', g, sc, ['powered']);
    }
  }
  const h = m.map.hatch;
  if (h.state === 'open' && (myD(h.x, h.y) < 26 || ai.memory.hatchSeen)) add('HATCH', h, 130, ['hatch open']);
  if (h.state === 'closed' && s.item?.id === 'key' && m.aliveCount() <= 2 && ai.memory.hatchSeen) add('KEY_HATCH', h, 110, ['key']);

  // ---- Singularity: carry the EMP to the densest known pod cluster ----
  if (s.emp && pods.length) {
    for (const d of pods) {
      const n = pods.filter((o) => dist(o.x, o.y, d.x, d.y) < 9 * M).length;
      const onGen = m.map.gens.some((g) => !g.done && dist(g.x, g.y, d.x, d.y) < 14 * M);
      if (n < 2 && !onGen && !s.status.has('slipstream')) continue;
      add('USE_EMP', d, (44 + 14 * n + (onGen ? 14 : 0)) * (0.35 + 0.65 * P.zoneSense) * (0.3 + 0.7 * safety(d.x, d.y)) / (1 + myD(d.x, d.y) / 50), [`${n} pods here`]);
    }
  }
  // ---- protection / saves --------------------------------------------------
  // ---- Singularity: print an EMP when pods are watching our objectives ----
  if (!s.emp && m.hasPower('quantum') && P.difficulty >= 3) {
    const nearPods = pods.filter((d) => m.map.gens.some((g) => !g.done && dist(g.x, g.y, d.x, d.y) < 14 * M)).length;
    // Team plan: one or two EMP carriers keep the pod network down. Skilled teams value
    // this much more (zoneSense) — the Singularity snowballs if pods stay up.
    const carriers = m.survivors.filter((o) => o !== s && o.alive && (o.emp || o.ai?.goal?.type === 'PRINT_EMP')).length;
    const want = (s.status.has('slipstream') ? 1.2 : 0) + nearPods * 0.6 + (pods.length >= 4 ? 0.6 : 0) + (pods.length >= 6 ? 0.6 : 0);
    if (want > 0 && carriers < 2) for (const d of ai.memory.devices.values()) {
      if (d.kind !== 'printer' || !d.usable(s, m)) continue;
      const dd = myD(d.x, d.y);
      add('PRINT_EMP', d, (30 + 30 * want) * (0.35 + 0.65 * P.zoneSense) * (carriers ? 0.7 : 1) * (0.3 + 0.7 * safety(d.x, d.y)) / (1 + dd / 70), ['counter surveillance', `${nearPods} pods on gens`, `${pods.length} pods known`]);
    }
  }
  // ---- follow a carried teammate (Breakout / body-block / flashlight setups) ----
  const ck = m.killers.find((k) => k.carrying && k.carrying !== s);
  if (ck && (pb.followCarry > 1 || P.protectionHits) && s.health !== 'dying' && !marked) {
    const d = myD(ck.x, ck.y); if (d < 22) add('FOLLOW_CARRY', ck.carrying, 26 * pers.bravery * pb.followCarry / (1 + d / 18), ['shadow the carry']);
  }
  if (P.protectionHits && s.health === 'healthy' && !marked) {
    for (const o of others) {
      if (o.health !== 'injured' || !o.ai?.inChase) continue;
      const d = myD(o.x, o.y); if (d > 24) continue;
      add('PROTECT', o, 30 * pers.bravery * P.protect * pb.protect * (1 + pers.teamwork) / (1 + d / 15), ['body block']);
    }
  }
  if (s.item?.id === 'flashlight' && s.item.charges > 1 && P.flashlightSaves) {
    const k = m.killers.find((q) => q.carrying || q.action?.type === 'pickup') || ai.fk;
    const target = k.carrying || (k.action?.type === 'pickup' ? k.action.target : null);
    if (target && myD(k.x, k.y) < 20 && ai.belief.conf > 0.6) add('FLASHLIGHT', target, 80 * (0.5 + pers.bravery), ['flashlight save']);
  }
  // ---- killer-specific objectives (lock lockers, solve the box, sprays, Magic Items…) ----
  ai.profile.goals?.(ai, add, { myD, safety });
  add('PATROL', null, 6, ['nothing better']);
  return opts;
}

// Choose with commitment (hysteresis), softmax temperature and deliberate mistakes.
function chooseGoal(ai, opts) {
  const P = ai.params; const cur = ai.goal;
  opts.sort((a, b) => b.score - a.score);
  if (!opts.length) return null;
  // Commitment: keep the current goal unless something is clearly better.
  const curOpt = cur ? opts.find((o) => o.key === cur.key) : null;
  if (curOpt) {
    const working = ai.sv.action && (ai.sv.action.type === 'repair' || ai.sv.action.type === 'heal' || ai.sv.action.type === 'openGate');
    const bonus = curOpt.score * 0.12 + 6 + (ai.m.time < ai.goalCommitUntil ? 20 : 0) + (working ? 8 : 0);
    curOpt.commitBonus = bonus;
    const urgentBetter = opts[0].urgent && opts[0] !== curOpt && opts[0].score > curOpt.score;
    if (!urgentBetter && (opts[0] === curOpt || curOpt.score + bonus >= opts[0].score)) return curOpt;
    if (urgentBetter) return opts[0];
  }
  if (Math.random() < P.mistakeRate * 0.5 && opts.length > 2) { const o = opts[1 + Math.floor(Math.random() * Math.min(3, opts.length - 1))]; o.mistake = o.score < opts[0].score * 0.6; return o; }
  const T = P.temperature; const top = opts.slice(0, 6);
  const mx = top[0].score; const w = top.map((o) => Math.exp((o.score - mx) / T)); const tot = w.reduce((a, b) => a + b, 0);
  let r = Math.random() * tot; for (let i = 0; i < top.length; i++) { r -= w[i]; if (r <= 0) return top[i]; }
  return top[0];
}

return { GOAL_STATE, evaluateGoals, chooseGoal };
};
__defs["js/ai/behaviorTree.js"] = function(){
// Minimal behavior-tree runtime. Nodes record the active path for the debug overlay.
const S = 'SUCCESS', F = 'FAILURE', R = 'RUNNING';

class Node { constructor(name) { this.name = name; } tick() { return F; } }

class Selector extends Node {
  constructor(name, children) { super(name); this.children = children; }
  tick(ctx) { ctx.trace.push(this.name); for (const c of this.children) { const r = c.tick(ctx); if (r !== F) return r; } ctx.trace.pop(); return F; }
}
class Sequence extends Node {
  constructor(name, children) { super(name); this.children = children; }
  tick(ctx) { ctx.trace.push(this.name); for (const c of this.children) { const r = c.tick(ctx); if (r !== S) { if (r === F) ctx.trace.pop(); return r; } } return S; }
}
class Cond extends Node {
  constructor(name, fn) { super(name); this.fn = fn; }
  tick(ctx) { return this.fn(ctx) ? S : F; }
}
class Act extends Node {
  constructor(name, fn) { super(name); this.fn = fn; }
  tick(ctx) { ctx.trace.push(this.name); const r = this.fn(ctx) || R; if (r === F) ctx.trace.pop(); return r; }
}
// Guarded branch: if cond holds, run action (and never fall through).
const Branch = (name, cond, action) => new Sequence(name, [new Cond(name + '?', cond), new Act(name, action)]);

return { S, Node, Selector, Sequence, Cond, Act, Branch };
};
__defs["js/ai/counterplayKit.js"] = function(){
// Shared counterplay building blocks used by every killer profile.
const { M, dist, angleDiff, clamp } = __req("js/core/util.js");

const K = (o) => ({ hideBias: 0, palletValue: 1, windowValue: 1, preDrop: 0, greed: 1, openPenalty: 1, losValue: 0, loopAbandon: false,
  genEscape: 1, rescueWait: 1, healBias: 1, zoneAvoid: 1, leaveEarly: 1, stealthy: false, unknownParanoia: 0,
  tightPenalty: 0, groupPenalty: 0, windowBonus: 0, ...o });

// Shared reactions ------------------------------------------------------------
function sidestep(ai, fromDir, label, dist2 = 2.6) {
  const s = ai.sv, m = ai.m;
  const perp = fromDir + (Math.random() < 0.5 ? 1 : -1) * Math.PI / 2;
  // prefer the side with open ground
  let best = null;
  for (const sgn of [1, -1]) {
    const a = fromDir + sgn * Math.PI / 2; const tx = s.x + Math.cos(a) * dist2 * M, ty = s.y + Math.sin(a) * dist2 * M;
    if (m.world.sweepClear(s.x, s.y, tx, ty, s.r)) { if (!best || Math.random() < 0.5) best = { x: tx, y: ty }; }
  }
  if (!best) best = { x: s.x + Math.cos(perp) * dist2 * M, y: s.y + Math.sin(perp) * dist2 * M };
  ai.chase.sidestep = best; ai.chase.sidestepUntil = m.time + 0.45;
  ai.setPath([{ x: s.x, y: s.y }, best], true);
  if (label) ai.note(label, 'dodge-' + label);
  if (label && /dodg|lane|dash|bound|harpoon|hatchet|dog|chain|wave|damned/i.test(label)) m.events.emit('dodged', { survivor: s, label });
  return label;
}

// Dodge any visible killer projectile on a collision course. Quality depends on
// difficulty, distance (time to impact), reaction time, awareness and the throw speed.
function dodgeProjectiles(ai) {
  const s = ai.sv, m = ai.m, P = ai.params;
  for (const p of m.projectiles) {
    if (p.arc || p.dodgeRolled?.has(s)) continue;
    const d = dist(p.x, p.y, s.x, s.y); if (d > 32 * M) continue;
    if (!m.world.losClear(s.x, s.y, p.x, p.y)) continue;
    const ap = p.approach(s.x, s.y); if (ap.t < 0 || ap.d > s.r + p.r + 0.35 * M) continue;
    (p.dodgeRolled ||= new Set()).add(s);
    const speed = Math.hypot(p.vx, p.vy) / M;
    const react = P.reaction * (1.2 - 0.4 * ai.pers.killerAwareness) * (s.hasPerk('dodge_instinct') && !s.status.has('exhausted') ? 0.6 : 1);
    const window = ap.t - react;
    const timeFactor = clamp(window / 0.35, 0, 1);
    const aimFactor = 1 - clamp((speed - 22) / 40, 0, 0.35);
    const pDodge = P.dodge * timeFactor * aimFactor * (0.75 + 0.25 * P.awareness);
    if (Math.random() < pDodge) return sidestep(ai, Math.atan2(p.vy, p.vx), `Dodged a ${p.kind || 'projectile'}`, 1.8);
  }
  return null;
}


// Run from a threat point (a guard, the dog, a teleport spot). Samples nearby open cells and
// prefers ones that put a wall between us, are far from the threat and away from the killer
// belief; windows score extra when the chaser cannot vault (dog, guards).
function fleeFrom(ai, tx, ty, label, { windows = false, holdFor = 1.0, minD = 5, maxD = 10 } = {}) {
  const s = ai.sv, m = ai.m, nav = m.map.nav, B = ai.belief;
  const kb = B.conf > 0.3 ? B.pos() : null;
  let best = null, bs = -1e9;
  const cands = [];
  for (let k = 0; k < 18; k++) { const a = Math.random() * Math.PI * 2, r = (minD + Math.random() * (maxD - minD)) * M; cands.push({ x: s.x + Math.cos(a) * r, y: s.y + Math.sin(a) * r, win: null }); }
  if (windows) for (const w of m.map.windows) if (w.blockedUntil <= m.time && dist(w.x, w.y, s.x, s.y) < 10 * M) { const sd = w.sides(1.4 * M); const far = sd[w.sideOf({ x: tx, y: ty }) === 0 ? 1 : 0]; cands.push({ x: far.x, y: far.y, win: w }); }
  for (const c of cands) {
    const i = nav.idxAt(c.x, c.y); if (i < 0 || nav.isBlocked(i)) continue;
    let sc = dist(c.x, c.y, tx, ty) / M - dist(c.x, c.y, s.x, s.y) / M * 0.35;
    if (!m.world.losClear(tx, ty, c.x, c.y)) sc += 4;
    if (c.win) sc += 6;
    if (kb) sc -= Math.max(0, 10 - dist(c.x, c.y, kb.x, kb.y) / M) * 0.6;
    // don't run past the threat
    const toC = Math.atan2(c.y - s.y, c.x - s.x), toT = Math.atan2(ty - s.y, tx - s.x); if (angleDiff(toC, toT) < 0.7) sc -= 8;
    if (sc > bs) { bs = sc; best = c; }
  }
  if (!best) return null;
  const r = m.map.nav.findPath(s.x, s.y, best.x, best.y, ai.agent(), 3000);
  if (r) ai.setPath(m.map.nav.smooth(m.world, r.pts, s.r * 0.9), true); else ai.setPath([{ x: s.x, y: s.y }, best], true);
  s.moveMode = 'run'; ai.chase.sidestepUntil = m.time + holdFor;
  if (label) ai.note(label, 'flee-' + label);
  return label;
}
// The minion (dog / guard) this survivor currently knows about that is coming for it.
function minionOnMe(ai) {
  const s = ai.sv; let best = null, bd = Infinity;
  for (const e of ai.memory.minions.values()) {
    const n = e.n; if (n.dead) continue;
    const onMe = n.target === s && ['hunt', 'pursue', 'spotted'].includes(n.state);
    const charging = n.state === 'chase' && angleDiff(n.facing, Math.atan2(s.y - n.y, s.x - n.x)) < 0.5;
    if (!onMe && !charging) continue;
    const d = dist(n.x, n.y, s.x, s.y); if (d < bd) { bd = d; best = n; }
  }
  return best ? { n: best, d: bd } : null;
}
// Drop a standing pallet on a chasing minion when it is right behind us.
function palletOnMinion(ai, n) {
  const s = ai.sv, m = ai.m;
  if (s.action || s.vault || !n.stunnable || n.stunT > 0) return false;
  for (const p of m.map.pallets) {
    if (p.state !== 'up' || p.dropping || dist(p.x, p.y, s.x, s.y) > 2.2 * M) continue;
    if (p.sideOf(s) === p.sideOf(n) || dist(p.x, p.y, n.x, n.y) > 3.2 * M) continue;
    if (s.startAction('dropPallet', p, m)) { ai.note(`Dropped a pallet on the ${n.label}`, 'mpal'); return true; }
  }
  return false;
}

return { K, sidestep, dodgeProjectiles, fleeFrom, minionOnMe, palletOnMinion };
};
__defs["js/ai/profilesExpansion.js"] = function(){
// Counterplay profiles for the ten expansion killers. Same shape as killerProfiles.js:
// knobs bend the generic AI (pathing, loops, pallets, gens, rescues, healing), `tactical`
// runs killer-specific reactions every tactical tick, `goals` adds killer-specific
// objectives (lock lockers, solve the box, cure infection, loot Magic Items…),
// `genFactor`/`dynKnobs` adjust decisions from what the survivor currently knows.
// Everything reads the survivor's own perception (belief, memory) — no omniscience.
const { M, dist, angleDiff, clamp } = __req("js/core/util.js");
const { K, sidestep, dodgeProjectiles, fleeFrom, minionOnMe, palletOnMinion } = __req("js/ai/counterplayKit.js");

const knownDevices = (ai, kind) => [...ai.memory.devices.values()].filter((d) => d.kind === kind && !d.dead);
const inLane = (fromX, fromY, dir, s, len, half) => {
  const rx = s.x - fromX, ry = s.y - fromY; const along = rx * Math.cos(dir) + ry * Math.sin(dir);
  const side = Math.abs(-rx * Math.sin(dir) + ry * Math.cos(dir)); return along > 0 && along < len && side < half;
};
// Walk away from bunched-up teammates (Legion chains, Infectious Fright, Wesker grabs).
function spreadOut(ai, radius, label) {
  const s = ai.sv, m = ai.m; const near = m.survivors.filter((o) => o !== s && o.standing && dist(o.x, o.y, s.x, s.y) < radius * M);
  if (!near.length) return null;
  const cx = near.reduce((a, o) => a + o.x, 0) / near.length, cy = near.reduce((a, o) => a + o.y, 0) / near.length;
  return fleeFrom(ai, cx, cy, label, { minD: 6, maxD: 12, holdFor: 1.2 });
}

const EXPANSION_PROFILES = {
  // ------------------------------------------------------------------ LEGION
  legion: {
    power: 'Feral Frenzy', strengths: ['fast Frenzy that vaults pallets', 'chains Deep Wound across the team', 'reveals survivors near each Frenzy hit'], weaknesses: ['Frenzy cannot down', 'Fatigue after the Frenzy', 'Deep Wound can be mended'],
    avoid: ['grouping up on gens or heals', 'standing in his terror radius during a Frenzy', 'relying on pallets mid-Frenzy'], exploit: ['his Fatigue window', 'mending early', 'spreading the team out'],
    knobs: K({ groupPenalty: 0.35, palletValue: 0.8, windowValue: 0.9, greed: 1.2, healBias: 1.15, genEscape: 1.1, rescueWait: 0.9 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, k = ai.fk, B = ai.belief, P = ai.params, pw = k.power;
      if (!s.standing) return null;
      const frenzy = pw.frenzy && (B.observing || (m.time - (B.windupT || -99) < 4 && B.windupWhat === 'frenzy'));
      // Deep Wound: mend before the timer runs out, even with some pressure around
      const dw = s.status.get('deepWound');
      if (dw && dw.expiration < 9 && !s.action && ai.threat < 0.75 && P.difficulty >= 3) { s.startAction('mend', null, m); ai.note('Mending the Deep Wound before it runs out', 'legmend'); ai.chase.sidestepUntil = m.time + 0.3; return 'mend'; }
      // High-level teams refuse to give him an easy chain: scatter when he frenzies near a group.
      if (frenzy && P.difficulty >= 5 && !ai.inChase && Math.random() < 0.25 + 0.75 * P.awareness) {
        const kd = dist(B.x, B.y, s.x, s.y) / M;
        if (kd < 28) { const r = spreadOut(ai, 9, 'Spreading out — no easy Frenzy chain'); if (r) { if (s.action && s.action.type !== 'heal') s.stopAction(m); return r; } }
      }
      return null;
    },
    genFactor(ai, g) { const n = [...g.repairers].filter((o) => o !== ai.sv).length; return n && ai.params.difficulty >= 6 ? { f: 1 - 0.18 * n * ai.params.awareness, why: 'avoid stacking vs Legion' } : null; },
  },

  // ------------------------------------------------------------------ HAG
  hag: {
    power: 'Phantasm Traps + teleport', strengths: ['teleports to triggered traps', 'webs of traps around hooks and gens', 'hard to spot traps'], weaknesses: ['slow base speed', 'crouching avoids traps', 'flashlights burn traps'],
    avoid: ['walking over known traps', 'trapped loops', 'running straight into a trapped hook'], exploit: ['crouch past traps', 'burn traps with a flashlight', 'loops she has not trapped yet'],
    knobs: K({ zoneAvoid: 1.6, palletValue: 1.1, windowValue: 1.1, rescueWait: 1.15, genEscape: 1.15, hideBias: 0.1 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, P = ai.params;
      if (!s.standing || s.vault) return null;
      // Crouch-walk across a known trap that lies on my path (never sprint through one).
      const traps = knownDevices(ai, 'phantasm');
      if (!traps.length) return null;
      const dir = Math.hypot(s.vx, s.vy) > 0.5 * M ? Math.atan2(s.vy, s.vx) : s.facing;
      for (const t of traps) {
        const d = dist(t.x, t.y, s.x, s.y); if (d > 3.2 * M) continue;
        if (angleDiff(Math.atan2(t.y - s.y, t.x - s.x), dir) > 0.9 && d > 1.3 * M) continue;
        if (ai.inChase && ai.threat > 0.85 && P.difficulty < 7) continue; // panicking low skill bots just run
        ai.crouchUntil = m.time + 0.9; if (!ai._hagNoteT || m.time - ai._hagNoteT > 6) { ai._hagNoteT = m.time; ai.note('Crouching over a Phantasm trap', 'hagcrouch'); }
        return null;
      }
      return null;
    },
    goals(ai, add, { myD, safety }) {
      const s = ai.sv, P = ai.params;
      if (s.item?.id !== 'flashlight' || s.item.charges < 2 || P.difficulty < 4) return;
      for (const t of knownDevices(ai, 'phantasm')) {
        const nearObj = ai.m.map.gens.some((g) => !g.done && dist(g.x, g.y, t.x, t.y) < 8 * M) || ai.m.map.hooks.some((h) => dist(h.x, h.y, t.x, t.y) < 6 * M);
        add('BURN_TRAP', t, (14 + (nearObj ? 16 : 0)) * (0.3 + 0.7 * safety(t.x, t.y)) / (1 + myD(t.x, t.y) / 20), [nearObj ? 'trap guards an objective' : 'clear a trap']);
      }
    },
    genFactor(ai, g) { const n = knownDevices(ai, 'phantasm').filter((t) => dist(t.x, t.y, g.x, g.y) < 8 * M).length; return n ? { f: Math.pow(0.85, n), why: `${n} trap${n > 1 ? 's' : ''} at gen` } : null; },
  },

  // ------------------------------------------------------------------ CANNIBAL
  cannibal: {
    power: "Bubba's Chainsaw (sweep)", strengths: ['instant downs at close range', 'can down several survivors in one sweep', 'pallets are dangerous to greed'], weaknesses: ['short reach (sweep ramps up over 2.5 s)', 'tantrums when the sweep rams a wall', '2 s rev telegraph', 'cannot vault while sawing', 'a dropped pallet ends the sweep'],
    avoid: ['tight loops and short walls', 'greeding pallets', 'letting him get within 6 m while he revs', 'standing next to teammates (multi-downs)'], exploit: ['pre-dropping pallets so the sweep stops', 'windows and long loops', 'distance — the sweep is short', 'turning tight around walls so he rams them'],
    knobs: K({ losValue: 0.4, openPenalty: 0.9, preDrop: 1.6, greed: 0, palletValue: 1.35, windowValue: 1.35, tightPenalty: 1.3, groupPenalty: 0.15, rescueWait: 1.2, genEscape: 1.15 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, k = ai.fk, B = ai.belief, P = ai.params, pw = k.power;
      if (!s.standing || s.vault || s.action?.type === 'dropPallet') return null;
      const revving = (pw.state === 'revving' && B.observing) || (m.time - B.revvingT < 0.8 && B.conf > 0.5);
      const sweeping = pw.state === 'sweep' && B.observing;
      if (!revving && !sweeping) return null;
      const d = dist(B.x, B.y, s.x, s.y) / M;
      if (d > 7.5 || ai._bubbaT > m.time) return null;
      ai._bubbaT = m.time + 0.6;
      // Not Hillbilly: breaking line of sight is pointless — make distance or put a pallet/window between.
      if (Math.random() > 0.2 + 0.75 * P.dodge) return null;
      // multi-down risk: a teammate right next to me → split up first
      const mate = m.survivors.find((o) => o !== s && o.standing && dist(o.x, o.y, s.x, s.y) < 4 * M);
      if (mate && Math.random() < P.awareness) { const a = Math.atan2(s.y - mate.y, s.x - mate.x) * 0.5 + Math.atan2(s.y - B.y, s.x - B.x) * 0.5; s.moveTo = { x: s.x + Math.cos(a) * 5 * M, y: s.y + Math.sin(a) * 5 * M }; ai.note('Splitting up so one sweep can\'t down us both', 'split'); return 'split from the group'; }
      // sweeping already: turn hard around the nearest solid so he rams it
      if (sweeping && d < 5) {
        for (const w of m.map.windows) if (dist(w.x, w.y, s.x, s.y) < 4 * M) { s.moveTo = { x: w.x, y: w.y }; ai.note('Vaulting away from the sweep (he can\'t vault)', 'sweepwin'); return 'window vs sweep'; }
        const sideA = Math.atan2(s.y - B.y, s.x - B.x) + (Math.random() < 0.5 ? 1.35 : -1.35);
        const tx = s.x + Math.cos(sideA) * 3 * M, ty = s.y + Math.sin(sideA) * 3 * M;
        if (!m.world.pointBlocked(tx, ty, s.r)) { s.moveTo = { x: tx, y: ty }; ai.note('Hard turn so the saw rams the wall', 'sweepturn'); return 'side-step the sweep'; }
      }
      return fleeFrom(ai, B.x, B.y, 'Creating distance from the saw sweep', { windows: true, minD: 5, maxD: 9, holdFor: 0.8 });
    },
  },

  // ------------------------------------------------------------------ DEATHSLINGER
  deathslinger: {
    power: 'Harpoon gun', strengths: ['ranged hits through open lanes', 'reels survivors in', 'quick at mid range'], weaknesses: ['reload after every shot', 'chain snaps on obstacles', 'small terror radius but slow ADS'],
    avoid: ['straight sightlines', 'open fields', 'predictable movement while he aims'], exploit: ['his reload', 'obstacles to snap the chain', 'breaking line of sight'],
    knobs: K({ losValue: 3.5, openPenalty: 1.6, windowValue: 0.9, palletValue: 1.1, greed: 0.6, healBias: 1.2, rescueWait: 1.2, hideBias: 0.1 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, k = ai.fk, B = ai.belief, P = ai.params, pw = k.power;
      // speared: drag the chain across an obstacle so it snaps
      if (s.control?.kind === 'harpoon') {
        if (ai._snapT && ai._snapT > m.time) return 'snap the chain';
        let best = null, bs = Infinity;
        for (let a = 0; a < 12; a++) { const x = s.x + Math.cos(a * 0.52) * 3 * M, y = s.y + Math.sin(a * 0.52) * 3 * M; if (m.world.pointBlocked(x, y, s.r)) continue; if (!m.world.losClear(k.x, k.y, x, y)) { const c = dist(x, y, s.x, s.y); if (c < bs) { bs = c; best = { x, y }; } } }
        if (best && Math.random() < 0.3 + 0.7 * P.awareness) { s.moveTo = best; ai._snapT = m.time + 0.6; ai.note('Pulling the harpoon chain against an obstacle', 'snapit'); }
        else { const a = Math.atan2(s.y - k.y, s.x - k.x) + (Math.random() - 0.5); s.moveTo = { x: s.x + Math.cos(a) * 3 * M, y: s.y + Math.sin(a) * 3 * M }; }
        ai.chase.sidestepUntil = m.time + 0.4; return 'snap the chain';
      }
      const dodged = dodgeProjectiles(ai); if (dodged) return dodged;
      // He is aiming at me: juke out of the sightline / toward cover.
      if (B.observing && pw.state === 'aiming' && pw.loaded) {
        const d = dist(k.x, k.y, s.x, s.y);
        if (d > 3.5 * M && d < pw.range + 2 * M && angleDiff(k.facing, Math.atan2(s.y - k.y, s.x - k.x)) < 0.25 && m.time > ai.chase.sidestepUntil + 0.2) {
          if (Math.random() < P.dodge * 0.3) return sidestep(ai, Math.atan2(s.y - k.y, s.x - k.x), 'Juked out of the harpoon sightline', 1.5);
        }
      }
      return null;
    },
  },

  // ------------------------------------------------------------------ DREDGE
  dredge: {
    power: 'Locker teleports & Nightfall', strengths: ['crosses the map through lockers', 'Nightfall blinds survivors', 'Remnant snap-back mindgames'], weaknesses: ['lockers can be locked', 'teleports rattle the destination locker', 'Daytime cooldowns'],
    avoid: ['repairing next to unlocked lockers', 'being caught alone at Nightfall', 'standing near his Remnant'], exploit: ['lock lockers near objectives', 'his long teleport cooldown in Daytime', 'grouping up (auras) at Nightfall'],
    knobs: K({ genEscape: 1.2, leaveEarly: 1.15, rescueWait: 1.1, hideBias: 0.05 }),
    dynKnobs(ai) { return ai.sv.status.has('nightfall') ? { hideBias: 0.35, leaveEarly: 1.4, stealthy: true, unknownParanoia: 0.4 } : null; },
    tactical(ai) {
      const s = ai.sv, m = ai.m, k = ai.fk, pw = k.power, P = ai.params;
      if (s.status.has('nightfall') && !ai._nightNote) { ai._nightNote = true; ai.note('Nightfall — staying close to cover and teammates', 'night'); }
      if (!s.status.has('nightfall')) ai._nightNote = false;
      // a visible Remnant: he can snap back here at any moment
      if (pw.remnant && !ai.inChase && dist(pw.remnant.x, pw.remnant.y, s.x, s.y) < 9 * M && m.world.losClear(s.x, s.y, pw.remnant.x, pw.remnant.y) && Math.random() < P.awareness) {
        if (!ai._remT || m.time - ai._remT > 5) { ai._remT = m.time; return fleeFrom(ai, pw.remnant.x, pw.remnant.y, 'Backing off from the Dredge\'s Remnant', { minD: 6, maxD: 10 }); }
      }
      return null;
    },
    goals(ai, add, { myD, safety }) {
      const s = ai.sv, m = ai.m, P = ai.params; if (P.difficulty < 4) return;
      const myGen = ai.goal?.type === 'REPAIR' ? ai.goal.target : s.action?.type === 'repair' ? s.action.target : (ai.goal?.type === 'DEVICE' && ai.goal.target.kind === 'locker') ? ai._lockGen : null;
      if (myGen) ai._lockGen = myGen;
      for (const l of knownDevices(ai, 'locker')) {
        if (l.data.locked) continue;
        const nearGen = m.map.gens.some((g) => !g.done && g.progress > 0.15 && dist(g.x, g.y, l.x, l.y) < 12 * M);
        const nearMine = myGen && dist(myGen.x, myGen.y, l.x, l.y) < 12 * M;
        if (!nearGen && !nearMine) continue;
        const c = m.bb.claimedBy('DEVICE:' + l.id); if (c && c !== s) continue;
        add('DEVICE', l, (18 + (nearMine ? (dist(l.x, l.y, s.x, s.y) < 10 * M ? 70 : 18) : 0)) * (0.4 + 0.6 * P.zoneSense) * (0.3 + 0.7 * safety(l.x, l.y)) / (1 + myD(l.x, l.y) / 14), ['lock a locker near the objective'], { urgent: !!nearMine && dist(l.x, l.y, s.x, s.y) < 10 * M });
      }
    },
    genFactor(ai, g) {
      if (!ai.sv.status.has('nightfall')) return null;
      const n = knownDevices(ai, 'locker').filter((l) => !l.data.locked && dist(l.x, l.y, g.x, g.y) < 12 * M).length;
      return n ? { f: Math.pow(0.8, n), why: 'unlocked lockers at Nightfall' } : null;
    },
  },

  // ------------------------------------------------------------------ KNIGHT
  knight: {
    power: 'Guard patrols & hunts', strengths: ['guards zone generators and hooks', 'two threats at once', 'guards break pallets'], weaknesses: ['guards can be outrun and stunned', 'Standards end hunts', 'hunts drain if the survivor gets away'],
    avoid: ['a patrolling guard\'s vision cone', 'looping the Knight while a guard hunts you', 'gens on an active patrol path'], exploit: ['grab the Standard', 'pallets on guards', 'leading guards away from teammates'],
    knobs: K({ genEscape: 1.15, palletValue: 1.1, rescueWait: 1.1 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, P = ai.params;
      if (!s.standing || s.vault) return null;
      const on = minionOnMe(ai);
      if (on && on.n.kind === 'guard') {
        const g = on.n;
        // Standard: the cleanest way to end a hunt (skilled bots go straight for it)
        const st = knownDevices(ai, 'standard').find((d) => d.usable(s, m));
        if (st && P.difficulty >= 4) {
          const dS = dist(st.x, st.y, s.x, s.y), dG = dist(g.x, g.y, s.x, s.y);
          if (dS < 18 * M && (dS < dG * 1.6 || P.difficulty >= 8)) {
            if (dS < 1.6 * M) { s.startAction('device', st, m); ai.chase.sidestepUntil = m.time + 0.6; return 'grab Standard'; }
            ai.navigateTo(st.x, st.y, 1.2 * M); ai.chase.sidestepUntil = m.time + 0.5; if (!ai._stNote || m.time - ai._stNote > 6) { ai._stNote = m.time; ai.note('Going for the Standard to end the hunt', 'standard'); }
            return 'go to Standard';
          }
        }
        if (on.d < 3.5 * M && palletOnMinion(ai, g)) return 'pallet the guard';
        if (on.d < 14 * M && Math.random() < 0.4 + 0.6 * P.awareness) return fleeFrom(ai, g.x, g.y, `Running from ${g.label}`, { windows: false, minD: 6, maxD: 11, holdFor: 0.9 });
      }
      // Patrolling guard nearby that hasn't seen me: get out of its cone / line of sight.
      for (const e of ai.memory.minions.values()) {
        const g = e.n; if (g.kind !== 'guard' || g.state !== 'patrol' || g.dead) continue;
        const d = dist(g.x, g.y, s.x, s.y);
        if (d > (g.type.detect + 4) * M) continue;
        const facing = angleDiff(g.facing, Math.atan2(s.y - g.y, s.x - g.x)) < Math.PI / 2 + 0.3;
        if (!facing && d > 3 * M) continue;
        if (Math.random() > 0.3 + 0.7 * P.awareness) continue;
        if (s.action && s.action.type !== 'unhook') s.stopAction(m);
        return fleeFrom(ai, g.x, g.y, `Leaving ${g.label}'s patrol`, { minD: 6, maxD: 12, holdFor: 1.2 });
      }
      return null;
    },
    genFactor(ai, g) {
      for (const e of ai.memory.minions.values()) { const n = e.n; if (n.kind === 'guard' && !n.dead && n.patrol?.some((p) => dist(p.x, p.y, g.x, g.y) < (n.type.detect + 2) * M)) return { f: 0.55, why: 'guard patrols here' }; }
      return null;
    },
  },

  // ------------------------------------------------------------------ CENOBITE
  cenobite: {
    power: 'Possessed Chains & the Lament Configuration', strengths: ['chains through long sightlines', 'Chain Hunt pressure', 'teleports to box solvers'], weaknesses: ['chains stop on walls', 'box solving ends the pressure', 'slow while aiming chains'],
    avoid: ['long open sightlines', 'leaving the box alone too long', 'solving the box with the killer close'], exploit: ['walls to block chains', 'solving in safe spots', 'breaking chains fast'],
    knobs: K({ losValue: 3, openPenalty: 1.4, palletValue: 1.1, healBias: 1.1, rescueWait: 1.1 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, k = ai.fk, B = ai.belief, P = ai.params, pw = k.power;
      if (!s.standing) return null;
      // break free of chains — quickly, unless the killer is right on top of us
      // break free of chains when there is time; with the killer on top of us keep running (they snap on their own)
      if (s.status.has('chained') && !s.action) {
        const kd = ai.killerDistAt(s.x, s.y);
        if (kd > 9 || (!ai.inChase && ai.threat < 0.6) || P.difficulty < 4) { s.moveTo = null; ai.path = null; ai.chase.sidestepUntil = m.time + 1.05; s.startAction('breakChain', null, m); return 'break chains'; }
      }
      if (s.action?.type === 'breakChain') { ai.chase.sidestepUntil = Math.max(ai.chase.sidestepUntil, m.time + 0.2); return 'breaking chains'; }
      // a chain head coming at me in line of sight: put a wall between us / side-step
      const chains = [pw.chain, ...(pw.hunters || [])].filter((c) => c && !c.dead);
      for (const c of chains) {
        const d = dist(c.x, c.y, s.x, s.y); if (d > 14 * M || !m.world.losClear(c.x, c.y, s.x, s.y)) continue;
        if (angleDiff(c.dir, Math.atan2(s.y - c.y, s.x - c.x)) > 0.6) continue;
        if (ai._chainDodge === c) return null; ai._chainDodge = c;
        if (Math.random() < 0.15 + 0.7 * P.dodge) return fleeFrom(ai, c.x, c.y, 'Breaking line of sight from a chain', { minD: 2, maxD: 4, holdFor: 0.5 });
      }
      // solving: only when it is safe
      if (s.box && !s.action && ai.threat < 0.45 && ai.killerDistAt(s.x, s.y) > 18 && !ai.inChase) { s.startAction('solveBox', null, m); ai.note('Solving the Lament Configuration somewhere safe', 'solve'); return 'solve box'; }
      if (s.action?.type === 'solveBox' && ai.threat > 0.5) s.stopAction(m);
      return null;
    },
    goals(ai, add, { myD, safety }) {
      const s = ai.sv, m = ai.m, P = ai.params, pw = ai.fk.power;
      const box = pw.box; if (!box || s.box || s.status.has('chained')) return;
      const claim = m.bb.claimedBy('DEVICE:' + box.id); if (claim && claim !== s && claim.alive) return;
      // who should get it: the closest healthy survivor not in a chase
      const mates = m.survivors.filter((o) => o.standing && !o.ai?.inChase);
      const closest = mates.sort((a, b) => dist(a.x, a.y, box.x, box.y) - dist(b.x, b.y, box.x, box.y))[0];
      const urgency = pw.hunt ? 2.2 : pw.boxCharge / pw.huntCharge;
      if (closest !== s && P.difficulty >= 4 && !pw.hunt) return;
      // The box is a team objective: the designated runner leaves early enough to beat the timer
      // (travel time vs. time left); during a Chain Hunt everyone free considers it.
      const left = pw.huntCharge - pw.boxCharge, travel = myD(box.x, box.y) / 4;
      const late = pw.hunt ? 1 : clamp(1.4 - (left - travel) / 40, 0, 1);
      add('DEVICE', box, (24 + 50 * urgency + 60 * late) * (0.55 + 0.45 * P.zoneSense) * (0.35 + 0.65 * safety(box.x, box.y)) / (1 + myD(box.x, box.y) / 70), [pw.hunt ? 'CHAIN HUNT — get the box' : `Chain Hunt in ${Math.ceil(pw.huntCharge - pw.boxCharge)} s`], { urgent: pw.hunt });
    },
  },

  // ------------------------------------------------------------------ MASTERMIND
  mastermind: {
    power: 'Virulent Bound', strengths: ['huge dash distance', 'grabs and slams survivors into walls', 'infection Hinders'], weaknesses: ['dashes are straight lines', 'charge-up telegraph', 'sprays cure infection'],
    avoid: ['straight open dash lanes', 'walls behind you when he dashes', 'staying fully infected'], exploit: ['side-step the bound', 'windows and tall loops', 'First Aid Sprays'],
    knobs: K({ openPenalty: 1.5, losValue: 2, windowValue: 1.2, palletValue: 1.1, greed: 0.7, groupPenalty: 0.1, healBias: 1.1 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, k = ai.fk, B = ai.belief, P = ai.params, pw = k.power;
      if (!s.standing || s.vault) return null;
      // cure the infection when it is safe
      const inf = s.status.get('infected');
      if (inf && (s.sprays || 0) > 0 && !s.action && ai.threat < 0.35 && (inf.value > 55 || P.difficulty < 6)) { s.startAction('spray', null, m); return 'spray'; }
      // he is charging / between bounds and I am in the lane: side-step out of it
      // Timing matters: side-stepping early just lets him re-aim. Good survivors wait until the
      // charge is nearly full (or the second bound is primed) and step out at the last moment.
      if (B.observing && (pw.state === 'charging' || pw.state === 'between' || pw.state === 'bound')) {
        const len = (pw.state === 'between' ? 15 : pw.state === 'bound' ? 14 * pw.boundT + 1 : 8.5) * M;
        if (inLane(k.x, k.y, k.facing, s, len, 1.5 * M)) {
          const d = dist(k.x, k.y, s.x, s.y) / M;
          const late = pw.state === 'charging' ? pw.charge > 0.97 - 0.08 * P.awareness : true;
          // reacting to a bound already in flight needs time: the closer he is, the less chance
          const react = pw.state === 'bound' ? clamp((d / 14 - P.reaction) / 0.35, 0, 1) : 1;
          const key = pw.state + ':' + (pw.state === 'charging' ? 'c' : Math.round(m.time * 2));
          if (late && ai._wkKey !== key) { ai._wkKey = key; if (Math.random() < (0.05 + 0.85 * P.dodge) * react) return sidestep(ai, k.facing, 'Side-stepped out of the bound lane', 2.4); }
          // he is slowed while charging: skilled survivors open the distance (or break LOS) so
          // the bound falls short, instead of looping in front of him
          if (pw.state === 'charging' && !late && P.difficulty >= 6 && d < 10 && m.time - (ai._wkRun || -9) > 0.8) { ai._wkRun = m.time; return fleeFrom(ai, k.x, k.y, 'Opening distance while he charges', { windows: true, minD: 4, maxD: 7, holdFor: 0.8 }); }
          // low-skill survivors flinch early (and get re-aimed)
          if (!late && P.difficulty <= 4 && ai._wkEarly !== Math.round(m.time) && Math.random() < 0.3) { ai._wkEarly = Math.round(m.time); return sidestep(ai, k.facing, 'Panicked out of the lane too early', 2); }
        }
      }
      return null;
    },
    goals(ai, add, { myD, safety }) {
      const s = ai.sv, inf = s.status.get('infected'); if (!inf || (s.sprays || 0) > 0) return;
      for (const c of knownDevices(ai, 'supply')) if (c.data.sprays > 0) add('DEVICE', c, (10 + 40 * inf.value / 100) * (0.3 + 0.7 * safety(c.x, c.y)) / (1 + myD(c.x, c.y) / 25), [`infection ${Math.round(inf.value)}%`]);
    },
  },

  // ------------------------------------------------------------------ HOUNDMASTER
  houndmaster: {
    power: 'The dog (Chase & Search Commands)', strengths: ['the dog finds and drags survivors', 'two threats at once', 'fast while searching'], weaknesses: ['the dog cannot vault', 'pallets stun the dog', 'Chase Command runs in a straight line'],
    avoid: ['the dog\'s charge lane', 'open ground near the dog', 'its lullaby radius'], exploit: ['windows (the dog goes around)', 'pallets on the dog', 'side-stepping the charge'],
    knobs: K({ windowValue: 1.3, palletValue: 1.2, openPenalty: 1.2, rescueWait: 1.1 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, P = ai.params, k = ai.fk;
      // grabbed: slam a pallet on the dog if one is right here
      if (s.control?.kind === 'dog') { const d = k.power.dog; if (d && palletOnMinion(ai, d)) return 'pallet the dog'; return 'grabbed'; }
      if (!s.standing || s.vault) return null;
      const on = minionOnMe(ai); if (!on || on.n.kind !== 'dog') return null;
      const dog = on.n;
      if (dog.state === 'chase') {
        // charge: get out of the straight lane (or behind something)
        if (ai._dogDodge === dog.t0 || on.d > 16 * M) return null; ai._dogDodge = dog.t0;
        if (Math.random() < 0.1 + 0.8 * P.dodge) return sidestep(ai, dog.facing, 'Side-stepped the dog\'s charge', 2.2);
        return null;
      }
      if (on.d < 3.5 * M && palletOnMinion(ai, dog)) return 'pallet the dog';
      if (Math.random() < 0.3 + 0.7 * P.awareness) return fleeFrom(ai, dog.x, dog.y, 'Breaking the dog\'s pursuit through a window', { windows: true, minD: 5, maxD: 10, holdFor: 0.8 });
      return null;
    },
  },

  // ------------------------------------------------------------------ LICH
  lich: {
    power: 'Four spells', strengths: ['Flight of the Damned through walls', 'Mage Hand undoes pallets', 'Fly crosses loops'], weaknesses: ['long spell cooldowns', 'crouching dodges the damned', 'Magic Items counter each spell'],
    avoid: ['dropping a pallet he can Mage Hand', 'standing tall in front of the damned', 'the Dispelling Sphere\'s path'], exploit: ['his cooldown windows', 'Magic Items from chests', 'crouching against Flight of the Damned'],
    knobs: K({ palletValue: 1, windowValue: 1.1, losValue: 1, rescueWait: 1.05 }),
    // Survivors who heard a spell know its cooldown; Mage Hand being up makes pallets worth less.
    dynKnobs(ai) {
      const P = ai.params; if (P.difficulty < 6) return null;
      const last = ai.m.bb.lichCasts?.magehand ?? -99;
      return ai.m.time - last > 35 ? { palletValue: 0.75, preDrop: -0.5, windowValue: 1.3 } : { palletValue: 1.2 };
    },
    tactical(ai) {
      const s = ai.sv, m = ai.m, P = ai.params;
      if (!s.standing || s.vault) return null;
      const nightwatch = s.magic === 'nightwatch' && !s.status.has('dispelled');
      for (const p of m.projectiles) {
        if (p.kind !== 'damned') continue;
        const d = dist(p.x, p.y, s.x, s.y); if (d > 12 * M) continue;
        if (!nightwatch && !m.world.losClear(s.x, s.y, p.x, p.y)) continue;
        const ap = p.approach(s.x, s.y); if (ap.t < 0 || ap.d > 1.4 * M) continue;
        (p.crouchRolled ||= new Set());
        if (!p.crouchRolled.has(s)) { p.crouchRolled.add(s); p['c_' + s.id] = Math.random() < 0.2 + 0.75 * (nightwatch ? 1 : P.dodge); }
        if (p['c_' + s.id]) { ai.crouchUntil = m.time + 0.7; if (!ai._fotdN || m.time - ai._fotdN > 4) { ai._fotdN = m.time; ai.note(nightwatch ? 'Nightwatch: crouched under the damned' : 'Crouched under Flight of the Damned', 'fotdc'); } }
      }
      // Archivist sees the sphere: step out of its path
      const sp = ai.fk.power.sphere;
      if (sp && s.magic === 'archivist' && !s.status.has('dispelled') && dist(sp.x, sp.y, s.x, s.y) < 10 * M && inLane(sp.x, sp.y, sp.dir, s, 12 * M, sp.r + 0.6 * M)) return sidestep(ai, sp.dir, 'Archivist: stepped out of the Dispelling Sphere', 3);
      return null;
    },
    goals(ai, add, { myD, safety }) {
      const s = ai.sv, P = ai.params; if (s.magic || P.difficulty < 3) return;
      for (const c of knownDevices(ai, 'magic_chest')) if (!c.data.opened) add('DEVICE', c, (14 + 10 * P.awareness) * (ai.m.time < 200 ? 1.2 : 0.6) * (0.3 + 0.7 * safety(c.x, c.y)) / (1 + myD(c.x, c.y) / 22), ['loot a Magic Item']);
    },
  },
};

return { EXPANSION_PROFILES };
};
__defs["js/ai/profilesNew.js"] = function(){
// Counterplay profiles for the six newest killers (Skull Merchant, Blight, Nurse, Unknown,
// Nemesis, Ghoul). Same shape as killerProfiles.js / profilesExpansion.js. Everything reads
// the survivor's own perception (belief, memory, sounds) — no omniscience.
const { M, dist, angleDiff, clamp } = __req("js/core/util.js");
const { K, sidestep, fleeFrom, minionOnMe, palletOnMinion } = __req("js/ai/counterplayKit.js");

const knownDevices = (ai, kind) => [...ai.memory.devices.values()].filter((d) => d.kind === kind && !d.dead);
const activeDrone = (d, m) => !d.dead && (d.data.disabledUntil || 0) <= m.time;
const inLane = (fromX, fromY, dir, s, len, half) => {
  const rx = s.x - fromX, ry = s.y - fromY; const along = rx * Math.cos(dir) + ry * Math.sin(dir);
  const side = Math.abs(-rx * Math.sin(dir) + ry * Math.cos(dir)); return along > 0 && along < len && side < half;
};

const NEW_PROFILES = {
  // ------------------------------------------------------------------ SKULL MERCHANT
  skull_merchant: {
    power: 'Drones (Eyes in the Sky)', strengths: ['map-wide information', 'lock-ons injure without a chase', 'claw traps track and hinder'], weaknesses: ['scan lines are visible and predictable', 'crouching / standing still avoids detection', 'drones can be hacked'],
    avoid: ['running through scan lines', 'stacking lock-ons', 'repairing inside overlapping zones'], exploit: ['crouch as the line passes', 'hack drones near objectives', 'claw traps come off by hacking'],
    knobs: K({ zoneAvoid: 1.3, hideBias: 0.15, genEscape: 1.1, stealthy: true, leaveEarly: 1.1 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, P = ai.params;
      if (!s.standing || s.vault || ai.inChase) return null;
      // the scan line is about to sweep over me: crouch (or freeze) until it has passed
      for (const d of knownDevices(ai, 'drone')) {
        if (!activeDrone(d, m)) continue;
        const r = dist(s.x, s.y, d.x, d.y); if (r > 10.5 * M || !m.world.losClear(d.x, d.y, s.x, s.y)) continue;
        const b = Math.atan2(s.y - d.y, s.x - d.x);
        // angle the line still has to turn (it sweeps counter-clockwise in screen space: +105°/s)
        let ahead = b - d.data.a; while (ahead < 0) ahead += Math.PI * 2; while (ahead > Math.PI * 2) ahead -= Math.PI * 2;
        const tIn = ahead / (105 * Math.PI / 180);
        const react = 0.25 + (1 - P.awareness) * 0.6;
        if (tIn < 0.9 && tIn > react * 0.5 && Math.random() < 0.15 + 0.85 * P.awareness) {
          if (s.action?.type === 'repair' || s.action?.type === 'heal' || s.action?.type === 'device') return null; // working = standing still: safe
          s.moveMode = 'crouch'; ai.crouchUntil = m.time + tIn + 0.35; ai.chase.sidestepUntil = m.time + tIn + 0.35; ai.setPath([{ x: s.x, y: s.y }], true);
          ai.note('Crouched under the drone\'s scan line', 'scancrouch'); return 'crouch for the scan';
        }
      }
      return null;
    },
    goals(ai, add, { myD, safety }) {
      const s = ai.sv, m = ai.m, P = ai.params;
      const trapped = s.status.has('clawTrap'), stacks = s.status.get('lockOn')?.value ?? 0;
      for (const d of knownDevices(ai, 'drone')) {
        if (!activeDrone(d, m) || !d.usable(s, m)) continue;
        const onGen = m.map.gens.some((g) => !g.done && dist(g.x, g.y, d.x, d.y) < 12 * M);
        const v = (trapped ? 120 : 0) + (onGen ? 34 : 8) + stacks * 12;
        add('DEVICE', d, v * (0.35 + 0.65 * P.zoneSense) * (0.3 + 0.7 * safety(d.x, d.y)) / (1 + myD(d.x, d.y) / 60), [trapped ? 'claw trap: hack to remove it' : onGen ? 'drone watching a generator' : 'hack a drone']);
      }
    },
    genFactor(ai, g) { const n = knownDevices(ai, 'drone').filter((d) => activeDrone(d, ai.m) && dist(d.x, d.y, g.x, g.y) < 10 * M).length; return n ? { f: Math.pow(0.8, n), why: `${n} drone${n > 1 ? 's' : ''} on gen` } : null; },
  },

  // ------------------------------------------------------------------ BLIGHT
  blight: {
    power: 'Blighted Rush', strengths: ['crosses the map in seconds', 'lethal rush after a slam', 'smashes pallets during lethal rushes'], weaknesses: ['turns slowly while rushing', 'bumps do no damage', 'long fatigue after the chain'],
    avoid: ['open fields', 'running in a straight line from him', 'standing next to walls he can slam'], exploit: ['side-step late', 'tall loops and windows', 'run during fatigue'],
    knobs: K({ openPenalty: 1.6, losValue: 1.8, windowValue: 1.15, palletValue: 0.9, greed: 0.5, tightPenalty: 0.6 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, k = ai.fk, B = ai.belief, P = ai.params, pw = k.power;
      if (!s.standing || s.vault || !B.observing) return null;
      // rushing at me: side-step at the last moment (he can't turn in time)
      if (pw.state === 'rush' || pw.state === 'special') {
        const d = dist(k.x, k.y, s.x, s.y) / M;
        if (inLane(k.x, k.y, pw.dir, s, (pw.state === 'special' ? 4 : 9) * M, 1.6 * M) && d < (pw.lethal ? 6.5 : 5) && ai._blKey !== Math.round(m.time * 3)) {
          ai._blKey = Math.round(m.time * 3);
          const late = d < 2.5 + 2.5 * P.awareness; // skilled survivors wait for the last moment
          if (late && Math.random() < 0.1 + 0.8 * P.dodge) return sidestep(ai, pw.dir, pw.lethal ? 'Side-stepped the lethal rush' : 'Side-stepped the rush', 2.4);
        }
      }
      // fatigued: make distance now
      if (pw.state === 'fatigue' && ai.inChase && m.time - (ai._blRun || -9) > 1) { ai._blRun = m.time; return fleeFrom(ai, k.x, k.y, 'He is fatigued — running', { windows: true, minD: 6, maxD: 11, holdFor: 1 }); }
      return null;
    },
  },

  // ------------------------------------------------------------------ NURSE
  nurse: {
    power: "Blink (Spencer's Last Breath)", strengths: ['blinks through walls', 'ignores pallets and windows', 'closes 20 m instantly'], weaknesses: ['slow walker', 'long fatigue after blinking', 'charge-up is audible and must be predicted'],
    avoid: ['predictable straight lines', 'relying on pallets/windows', 'open space when she has blinks'], exploit: ['double back as her charge ends', 'break line of sight at the last moment', 'distance during fatigue'],
    knobs: K({ palletValue: 0.45, windowValue: 0.45, losValue: 1.6, openPenalty: 1.1, preDrop: 0, greed: 0, loopAbandon: true, hideBias: 0.25, stealthy: true }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, k = ai.fk, B = ai.belief, P = ai.params, pw = k.power;
      if (!s.standing || s.vault) return null;
      const d = dist(B.x, B.y, s.x, s.y) / M;
      // charging (seen or heard): she will blink to where I'm heading → reverse / cut sideways
      // right before the charge completes. Beginners keep running straight.
      const charging = (pw.state === 'charging' && (B.observing || m.time - (B.windupT || -9) < 0.4));
      if (charging && d < 22 && ai._nuKey !== (pw.blinks + ':' + Math.round(pw.charge * 2))) {
        const expect = pw.blinkDist / M;
        if (Math.abs(expect - d) < 4 + 4 * P.awareness && Math.random() < 0.1 + 0.75 * P.dodge) {
          ai._nuKey = pw.blinks + ':' + Math.round(pw.charge * 2);
          const back = Math.atan2(k.y - s.y, k.x - s.x); // double back toward her side, diagonally
          const a = back + (Math.random() < 0.5 ? 1 : -1) * (0.9 + Math.random() * 0.5);
          const tx = s.x + Math.cos(a) * 3 * M, ty = s.y + Math.sin(a) * 3 * M;
          if (!m.world.pointBlocked(tx, ty, s.r)) { ai.setPath([{ x: s.x, y: s.y }, { x: tx, y: ty }], true); ai.chase.sidestepUntil = m.time + 0.5; ai.note('Doubled back as the Nurse charged her blink', 'nursejuke'); return 'juke the blink'; }
        }
      }
      // fatigued: run, hard
      if (pw.state === 'fatigue' && B.observing && d < 14 && m.time - (ai._nuRun || -9) > 1) { ai._nuRun = m.time; return fleeFrom(ai, k.x, k.y, 'She is fatigued — gaining distance', { minD: 6, maxD: 12, holdFor: 1 }); }
      return null;
    },
  },

  // ------------------------------------------------------------------ UNKNOWN
  unknown: {
    power: 'UVX & hallucinations', strengths: ['bouncing ranged bursts', 'Weakened turns bursts into damage', 'teleports to hallucinations'], weaknesses: ['UVX needs a 1 s charge', 'Weakened is cleared by looking at him', 'hallucinations can be dispelled'],
    avoid: ['grouping (bursts hit several)', 'straight lanes while he charges', 'walking into hallucinations'], exploit: ['side-step the UVX', 'stare him down to clear Weakened', 'dispel hallucinations near gens'],
    knobs: K({ losValue: 2.5, openPenalty: 1.4, groupPenalty: 0.2, zoneAvoid: 1.2 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, k = ai.fk, B = ai.belief, P = ai.params, pw = k.power;
      if (!s.standing || s.vault) return null;
      // UVX charging at me: open the angle (late side-step)
      if (pw.state === 'charging' && B.observing && pw.charge > 0.8 - 0.3 * P.awareness && ai._unKey !== Math.round(m.time)) {
        ai._unKey = Math.round(m.time);
        if (inLane(k.x, k.y, k.facing, s, 22 * M, 2.4 * M) && Math.random() < 0.1 + 0.7 * P.dodge) return sidestep(ai, k.facing, 'Side-stepped the UVX', 2.4);
      }
      // Weakened and not chased: turn and look at him to clear it (skilled survivors do it from cover)
      if (s.status.has('weakened') && B.observing && !ai.inChase && dist(k.x, k.y, s.x, s.y) < 24 * M && dist(k.x, k.y, s.x, s.y) > 10 * M && P.difficulty >= 3) {
        s.facing = Math.atan2(k.y - s.y, k.x - s.x); s.lookTarget = { x: k.x, y: k.y }; s.lookUntil = m.time + 0.6;
        if (m.time - (ai._unLook || -9) > 4) { ai._unLook = m.time; ai.note('Staring at the Unknown to shake off Weakened', 'stare'); }
      }
      // a hallucination I know about near me: stare at it (dispel) rather than walking into it
      for (const h of knownDevices(ai, 'hallucination')) {
        const d = dist(h.x, h.y, s.x, s.y);
        if (d < 11 * M && d > 3.5 * M && !ai.inChase && m.world.losClear(s.x, s.y, h.x, h.y) && Math.random() < 0.3 + 0.7 * P.awareness) {
          s.facing = Math.atan2(h.y - s.y, h.x - s.x); s.lookTarget = { x: h.x, y: h.y }; s.lookUntil = m.time + 0.6;
          if (s.action?.type !== 'repair') { ai.chase.sidestepUntil = m.time + 0.4; ai.setPath([{ x: s.x, y: s.y }], true); }
          if (m.time - (ai._unHal || -9) > 4) { ai._unHal = m.time; ai.note('Dispelling a hallucination', 'dispelling'); }
          return 'dispel';
        }
      }
      return null;
    },
    genFactor(ai, g) { const n = knownDevices(ai, 'hallucination').filter((h) => dist(h.x, h.y, g.x, g.y) < 8 * M).length; return n ? { f: Math.pow(0.8, n), why: 'hallucination at gen' } : null; },
  },

  // ------------------------------------------------------------------ NEMESIS
  nemesis: {
    power: 'Tentacle Strike & zombies', strengths: ['5–6.5 m tentacle', 'smashes pallets at MR 2+', 'zombies add a second threat'], weaknesses: ['tentacle wind-up', 'first hit only contaminates', 'vaccines cure', 'zombies are slow'],
    avoid: ['staying at 3–6 m from him', 'zombie sightlines', 'staying contaminated'], exploit: ['vaccines when safe', 'pallet-stun zombies', 'windows before MR 2'],
    knobs: K({ losValue: 1.4, palletValue: 1.05, windowValue: 1.1, greed: 0.6, zoneAvoid: 1.1 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, k = ai.fk, B = ai.belief, P = ai.params, pw = k.power;
      if (!s.standing || s.vault) return null;
      // cure contamination when it is safe
      if (s.status.has('contaminated') && (s.vaccines || 0) > 0 && !s.action && ai.threat < 0.35) { s.startAction('vaccinate', null, m); ai.note('Injecting a vaccine', 'vaxuse'); return 'vaccinate'; }
      // a zombie is coming for me: drop a pallet on it or leave its path
      const z = minionOnMe(ai);
      if (z && z.n.kind === 'zombie' && z.d < 6 * M) { if (palletOnMinion(ai, z.n)) return 'pallet on zombie'; if (m.time - (ai._zRun || -9) > 1.5) { ai._zRun = m.time; return fleeFrom(ai, z.n.x, z.n.y, 'Walking away from a zombie', { windows: true, minD: 5, maxD: 9, holdFor: 1 }); } }
      // tentacle wind-up in my lane: side-step out (late), or step out of range
      if (pw.state === 'charging' && B.observing && inLane(k.x, k.y, k.facing, s, pw.reach + 0.5 * M, 1.4 * M) && ai._neKey !== Math.round(m.time * 2)) {
        ai._neKey = Math.round(m.time * 2);
        if (Math.random() < 0.1 + 0.7 * P.dodge) return sidestep(ai, k.facing, 'Side-stepped the tentacle', 2.2);
      }
      return null;
    },
    goals(ai, add, { myD, safety }) {
      const s = ai.sv; if (!s.status.has('contaminated') || (s.vaccines || 0) > 0) return;
      for (const c of knownDevices(ai, 'vaccine')) if (c.data.vaccines > 0) add('DEVICE', c, 75 * (0.3 + 0.7 * safety(c.x, c.y)) / (1 + myD(c.x, c.y) / 30), ['contaminated: get a vaccine']);
    },
  },

  // ------------------------------------------------------------------ GHOUL
  ghoul: {
    power: 'Kagune Leap', strengths: ['latches from 14 m', 'grab attacks inflict Deep Wound and the Kagune Mark', 'Enraged while anyone is marked'], weaknesses: ['leaps are straight lines', 'a latch alone does no damage', 'the mark is cleared by mending'],
    avoid: ['open lanes', 'leaving the Deep Wound unmended', 'straight runs while he has tokens'], exploit: ['tight corners and tall walls', 'mend quickly', 'turn hard right after a latch'],
    knobs: K({ openPenalty: 1.5, losValue: 2, tightPenalty: -0.3, windowValue: 1.1, greed: 0.7, healBias: 1.1 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, k = ai.fk, B = ai.belief, P = ai.params, pw = k.power;
      if (!s.standing || s.vault) return null;
      // mend the Deep Wound fast: it also clears the Kagune Mark
      if (s.status.has('deepWound') && !s.action && (ai.threat < 0.5 || s.status.get('deepWound').expiration < 8) && P.difficulty >= 2) { s.startAction('mend', null, m); ai.note(s.status.has('kaguneMark') ? 'Mending to clear the Kagune Mark' : 'Mending the Deep Wound', 'kmend'); return 'mend'; }
      // latched: break away around the nearest corner before the grab
      if (s.status.has('latched') && ai._ghKey !== Math.round(m.time * 2)) {
        ai._ghKey = Math.round(m.time * 2);
        if (Math.random() < 0.15 + 0.7 * P.dodge) return fleeFrom(ai, k.x, k.y, 'Breaking away from the latch', { windows: true, minD: 3, maxD: 6, holdFor: 0.6 });
      }
      // leap incoming (seen): side-step
      if (pw.state === 'leap' && B.observing && inLane(k.x, k.y, pw.dir, s, 8 * M, 1.4 * M) && ai._ghL !== Math.round(m.time * 3)) { ai._ghL = Math.round(m.time * 3); if (Math.random() < 0.05 + 0.5 * P.dodge) return sidestep(ai, pw.dir, 'Dodged the Kagune leap', 2.2); }
      return null;
    },
  },
};

return { NEW_PROFILES };
};
__defs["js/ai/killerProfiles.js"] = function(){
// Killer counterplay profiles. Survivors read the profile of the killer they face and it
// shapes pathing, loops, hiding, generator choice, rescue timing, pallet use, vaults,
// healing and positioning. Each profile answers:
//   What killer am I facing? What is its power? Strengths? Weaknesses? Avoid? Exploit?
// `knobs` are multipliers the generic AI applies; `tactical` is killer-specific reaction
// code run every tactical tick (returns a label when it took over movement).
const { M, dist, angleDiff, clamp } = __req("js/core/util.js");
const { K, sidestep, dodgeProjectiles, fleeFrom, minionOnMe, palletOnMinion } = __req("js/ai/counterplayKit.js");
const { EXPANSION_PROFILES } = __req("js/ai/profilesExpansion.js");
const { NEW_PROFILES } = __req("js/ai/profilesNew.js");



// Profiles ---------------------------------------------------------------------
const KILLER_PROFILES = {
  trapper: {
    power: 'Bear traps', strengths: ['area denial at loops and gens', 'hidden traps in tall grass'], weaknesses: ['slow to set up', 'no ranged pressure'],
    avoid: ['tall grass near windows and pallets', 'known trap locations'], exploit: ['disarm traps near objectives', 'long chases on trap-free tiles'],
    knobs: K({ genEscape: 1.0, openPenalty: 0.9, palletValue: 1.05, zoneAvoid: 1 }),
  },
  hillbilly: {
    power: 'Chainsaw sprint', strengths: ['instant downs', 'fast map traversal'], weaknesses: ['collides with obstacles', 'loud rev', 'overheat'],
    avoid: ['long straight runs', 'open fields when he revs'], exploit: ['tight loops with obstacles', 'break line of sight while he revs'],
    knobs: K({ openPenalty: 1.6, losValue: 3.5, preDrop: 0.4, greed: 0.5, genEscape: 1.1, rescueWait: 1.2 }),
  },
  wraith: {
    power: 'Cloaking', strengths: ['undetectable ambushes', 'fast while cloaked'], weaknesses: ['bell warns on cloak/uncloak', 'cannot attack while cloaked'],
    avoid: ['repairing far from loops', 'staying put after hearing the bell'], exploit: ['the uncloak window', 'watching for shimmer'],
    knobs: K({ genEscape: 1.6, stealthy: true, unknownParanoia: 0.3, leaveEarly: 1.15 }),
  },
  ghostface: {
    power: 'Night Shroud (stalk & expose)', strengths: ['no heartbeat', 'one-hit downs on marked survivors', 'hard to spot when crouched'], weaknesses: ['revealed by being looked at', 'slow while stalking'],
    avoid: ['standing in his line of sight', 'chasing or rescuing while marked'], exploit: ['stare at him to reveal', 'break line of sight to deny stalk'],
    knobs: K({ genEscape: 1.4, stealthy: true, unknownParanoia: 0.35, hideBias: 0.25, losValue: 2.5, rescueWait: 1.15, leaveEarly: 1.1 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, k = ai.fk, B = ai.belief, P = ai.params;
      s.lookTarget = null;
      if (!B.observing || !k.power.shrouded || !s.standing) return null;
      const d = dist(k.x, k.y, s.x, s.y);
      const facingMe = angleDiff(k.facing, Math.atan2(s.y - k.y, s.x - k.x)) < 0.6;
      const stalked = facingMe && (k.power.stalking || k.lastSpeed < 1.5 * M) && d > 5 * M;
      if (stalked && m.time - (ai._stalkCall || 0) > 4) { ai._stalkCall = m.time; m.bb.report(s, k.x, k.y, 0, 0, k); m.bb.stalker = { x: k.x, y: k.y, t: m.time, by: s }; ai.note('Called out Ghost Face stalking', 'gfcall'); }
      // Reveal him by looking straight at him (skill gated), otherwise break line of sight.
      // Decide once per encounter whether this bot has the nerve/skill to stare him down.
      if (m.time - (ai._gfRollT || -99) > 6) { ai._gfRollT = m.time; ai._gfStare = P.difficulty >= 3 && Math.random() < 0.3 + 0.7 * P.awareness; }
      const canReveal = d < 13 * M && d > 4.5 * M && ai._gfStare;
      if (canReveal && (stalked || d < 9 * M)) {
        s.lookTarget = { x: k.x, y: k.y };
        if (s.action && s.action.type !== 'heal' && P.difficulty >= 6) s.stopAction(m);
        // Stand still and face him: running away shows him my back and lets him finish the stalk.
        if (!s.action) { s.moveTo = null; ai.path = null; ai.chase.sidestepUntil = m.time + 0.35; }
        if (stalked) ai.note('Staring down Ghost Face to reveal him', 'gfstare');
        return 'stare down';
      }
      if (stalked && P.difficulty >= 5 && !ai.inChase) { ai.evadeUntil = m.time + 2; ai.evadeMode = 'hide'; ai.note('Breaking line of sight from Ghost Face', 'gflos'); }
      return null;
    },
  },
  clown: {
    power: 'Gas bottles', strengths: ['slows survivors in loops', 'herds survivors with gas', 'antidote speed bursts'], weaknesses: ['slow base speed', 'reload downtime'],
    avoid: ['running through purple gas', 'loops already covered in gas'], exploit: ['his reload window', 'long loops with several exits', 'pallets before gas lands'],
    knobs: K({ zoneAvoid: 1.4, palletValue: 1.2, preDrop: 0.2, greed: 0.7, openPenalty: 0.9 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, nt = ai.chase.nextTile, P = ai.params;
      // Gassed while working: step out of the cloud (coughing gives away my position and
      // the slow makes me easy prey) and do something else until it clears.
      if (!ai.inChase && s.standing) {
        for (const z of m.zones) {
          if (z.kind !== 'tonic' || !z.contains(s.x, s.y)) continue;
          (z.rolled ||= new Map());
          if (!z.rolled.has(s)) z.rolled.set(s, Math.random() < 0.25 + 0.75 * P.zoneSense);
          if (!z.rolled.get(s)) break;
          ai.memory.zones.set(z.id, z); ai._zoneV++;
          if (s.action && s.action.type !== 'unhook') s.stopAction(m);
          const a = Math.atan2(s.y - z.y, s.x - z.x) + (Math.random() - 0.5) * 0.6; const r = z.r + 1.4 * M;
          let tx = z.x + Math.cos(a) * r, ty = z.y + Math.sin(a) * r;
          if (m.world.pointBlocked(tx, ty, s.r)) { tx = z.x - Math.cos(a) * r; ty = z.y - Math.sin(a) * r; }
          ai.setPath([{ x: s.x, y: s.y }, { x: tx, y: ty }], true); ai.chase.sidestepUntil = m.time + 0.9; ai.goal = null; ai.sT = Math.min(ai.sT, 0.9);
          ai.note('Gassed — stepping out of the tonic', 'gas');
          return 'leave gas';
        }
      }
      // Herding: gas landing ahead of me (between me and my next tile) means he wants me elsewhere.
      if (!ai.inChase || !nt) return null;
      for (const z of m.zones) {
        if (z.kind !== 'tonic' || m.time - z.t0 > 1) continue;
        const toT = Math.atan2(nt.y - s.y, nt.x - s.x), toZ = Math.atan2(z.y - s.y, z.x - s.x);
        if (dist(z.x, z.y, s.x, s.y) < 12 * M && angleDiff(toT, toZ) < 0.6) { ai.chase.blockedTiles.set(nt.id, m.time + 8); ai.chase.nextTileT = -99; ai.note('Clown is herding me — switching route', 'herd'); break; }
      }
      return null;
    },
  },
  executioner: {
    power: 'Torment trails & ground wave', strengths: ['wave goes through walls', 'trails deny areas', 'remote caging'], weaknesses: ['slow in Punishment mode', 'long telegraphed wind-up'],
    avoid: ['torment trails', 'loops while he is in Punishment mode', 'standing in the wave line'], exploit: ['side-step the telegraph', 'distance when he enters Punishment mode'],
    knobs: K({ zoneAvoid: 1.3, loopAbandon: true, windowValue: 0.8, palletValue: 0.9, healBias: 1.25, openPenalty: 0.8, rescueWait: 0.9 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, k = ai.fk, B = ai.belief, P = ai.params, pw = k.power;
      if (!B.observing || pw.state !== 'windup') return null;
      const a = k.facing; const rx = s.x - k.x, ry = s.y - k.y;
      const along = rx * Math.cos(a) + ry * Math.sin(a), side = Math.abs(-rx * Math.sin(a) + ry * Math.cos(a));
      if (along < 0 || along > pw.waveLen + 1 * M || side > 1.8 * M) return null;
      if (ai._telegraphSeen === pw.windT0) return null;
      ai._telegraphSeen = pw.windT0;
      if (Math.random() < 0.15 + 0.8 * P.dodge) return sidestep(ai, a, 'Side-stepped the Punishment wave', 2.8);
      return null;
    },
  },
  singularity: {
    power: 'Biopods & teleport', strengths: ['map-wide pressure', 'teleports to slipstreamed survivors'], weaknesses: ['pods can be dodged and EMPed', 'needs slipstream to teleport'],
    avoid: ['line of sight of known pods', 'being near pods while slipstreamed'], exploit: ['EMPs', 'objectives away from pods'],
    knobs: K({ zoneAvoid: 1.2, losValue: 1.5, hideBias: 0.1, genEscape: 1.2, leaveEarly: 1.1 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, P = ai.params;
      if (!s.emp || s.action || !s.standing || P.difficulty < 3) return null;
      const slip = s.status.has('slipstream');
      const pods = [...ai.memory.devices.values()].filter((d) => d.kind === 'biopod' && !d.dead && (d.data.disabledUntil || 0) <= m.time && dist(d.x, d.y, s.x, s.y) < 10 * M);
      if (pods.length >= (slip ? 1 : 2) || (pods.length && s.action?.type === 'repair')) { s.startAction('useEmp', null, m); return 'emp'; }
      return null;
    },
  },
  huntress: {
    power: 'Hatchet throws', strengths: ['ranged hits in the open', 'punishes long loops and window vaults in sight'], weaknesses: ['slow base speed', 'limited hatchets', 'wind-up telegraph'],
    avoid: ['open fields', 'predictable straight runs', 'staying in her line of sight'], exploit: ['walls and tall structures', 'her reload trips to lockers', 'dodging the wind-up'],
    knobs: K({ openPenalty: 1.8, losValue: 4, windowValue: 0.85, palletValue: 1.1, greed: 0.6, healBias: 1.3, rescueWait: 1.3, hideBias: 0.15 }),
    tactical(ai) {
      const s = ai.sv, m = ai.m, k = ai.fk, B = ai.belief, P = ai.params, pw = k.power;
      const dodged = dodgeProjectiles(ai); if (dodged) return dodged;
      // She is aiming at me: juke (fake movement) instead of running in a straight line.
      if (B.observing && pw.state === 'winding' && pw.count > 0) {
        const d = dist(k.x, k.y, s.x, s.y);
        if (d > 4 * M && d < 28 * M && angleDiff(k.facing, Math.atan2(s.y - k.y, s.x - k.x)) < 0.3 && m.time > ai.chase.sidestepUntil + 0.3) {
          if (Math.random() < P.dodge * 0.35 * (1 / 6)) return sidestep(ai, Math.atan2(s.y - k.y, s.x - k.x), 'Juked her wind-up', 1.6);
        }
      }
      return null;
    },
  },
};

Object.assign(KILLER_PROFILES, EXPANSION_PROFILES, NEW_PROFILES);
function profileFor(m, k = m.killer) { return KILLER_PROFILES[k.def.id] || KILLER_PROFILES.trapper; }
// Generic projectile reaction used against any killer that throws things.


return { KILLER_PROFILES, profileFor, dodgeProjectiles };
};
__defs["js/ai/perkAI.js"] = function(){
// Perk utility layer. Every survivor bot owns a PerkBrain that
//  1. turns its own perks into goal biases (Empathy → heal more, Breakout → follow carries…)
//  2. decides WHEN to fire active / exhaustion perks (and saves them otherwise)
//  3. learns the killer's perks as they reveal themselves and adapts the team plan.
// Everything scales with the difficulty knob perkIQ: low-skill bots misjudge the moment,
// waste exhaustion perks and barely change their plan; high-skill bots play around perks.
const { M, dist } = __req("js/core/util.js");

const BASE = () => ({ repair: 1, heal: 1, healSelf: 1, rescue: 1, cleanse: 1, hide: 0, protect: 1, gate: 1, joinGen: 1, followCarry: 1, cluster: 1, chest: 1, stunHunt: 1, selfHeal: false });

// How a known killer perk changes the plan (multipliers, applied with perkIQ weight).
const KILLER_PERK_REACT = {
  coulrophobia: { heal: 0.85, healSelf: 0.85 },
  genetic_limits: { heal: 0.9 },
  deathbound: { heal: 0.85 },
  make_your_choice: { rescue: 0.85, protect: 1.1 },
  starstruck: { followCarry: 0.75, protect: 0.85 },
  forced_penance: { protect: 0.7 },
  eruption: { repair: 0.95, joinGen: 0.85 },
  pain_resonance: { repair: 0.95, joinGen: 0.85 },
  jolt: { repair: 0.95 },
  grim_embrace: { rescue: 1.1 },
  deadlock: { cluster: 1.2 },
  machine_learning: { repair: 0.97 },
  ruin: { cleanse: 1.4, repair: 1.05 },
  noed: { cleanse: 1.8, gate: 1.1 },
  no_way_out: { gate: 0.95 },
  infectious_fright: { joinGen: 0.85 },
  discordance: { joinGen: 0.75 },
  thanatophobia: { heal: 1.2 },
  nurses_calling: { heal: 0.85, healSelf: 0.85 },
  bbq: { hide: 0.15 },
};
// Killer perks that are visible to survivors when they fire (notifications, screams, gens exploding…)
const OBSERVABLE = new Set(['eruption', 'grim_embrace', 'jolt', 'overcharge', 'deadlock', 'pain_resonance', 'starstruck', 'forced_penance', 'genetic_limits', 'forced_hesitation',
  'infectious_fright', 'deathbound', 'make_your_choice', 'no_way_out', 'blood_warden', 'rancor', 'noed', 'ruin', 'bbq', 'discordance', 'nurses_calling', 'corrupt', 'pop', 'spirit_fury', 'bitter_murmur']);

class PerkBrain {
  constructor(ai) {
    this.ai = ai; this.m = ai.m; this.sv = ai.sv;
    this.bias = BASE(); this.lastUse = new Map(); this._sbNow = false;
    // Team knowledge of killer perks lives on the blackboard so callouts are shared.
    const bb = this.m.bb; bb.killerPerksKnown ||= new Map();
    if (!this.m._perkWatch) {
      this.m._perkWatch = true;
      this.m.events.on('perkProc', (d) => {
        const p = d.perk; if (p.def.side !== 'killer' || !OBSERVABLE.has(p.id)) return;
        if (!bb.killerPerksKnown.has(p.id)) { bb.killerPerksKnown.set(p.id, this.m.time); const s = this.m.survivors.find((q) => q.ai && q.alive); s?.ai.note(`Killer has ${p.def.name}`, 'kp_' + p.id); }
      });
    }
    this.recompute();
  }
  get iq() { return this.ai.params.perkIQ ?? 0.5; }
  // weight a multiplier by skill: 1 + (k-1)*w
  w(k, w) { return 1 + (k - 1) * w; }
  recompute() {
    const b = BASE(), iq = this.iq, wt = 0.35 + 0.65 * iq, s = this.sv, m = this.m;
    for (const p of s.perks) {
      const a = p.def.ai; if (!a) continue;
      const apply = (src) => { for (const k in src) { if (k === 'hide') b.hide += src.hide * wt; else b[k] *= this.w(src[k], wt); } };
      if (a.bias) apply(a.bias);
      if (a.dynBias) { const d = a.dynBias(p, this.ai); if (d) apply(d); }
    }
    // Killer perks the team has figured out (only skilled bots adapt much)
    const known = m.bb.killerPerksKnown;
    if (known?.size) for (const id of known.keys()) { const r = KILLER_PERK_REACT[id]; if (!r) continue; for (const k in r) { if (k === 'hide') b.hide += r.hide * iq; else b[k] *= this.w(r[k], iq * 0.9); } }
    b.selfHeal = m.perks.anySurvivorHas('circle_of_healing');
    this.bias = b;
  }
  has(id) { return this.sv.perks.find((p) => p.id === id); }

  // Exhaustion perks: Sprint Burst is fired by the brain at the right moment (see tick),
  // other callers ask through here. Low perkIQ wastes them on nothing.
  wantsExhaustion(p) {
    const ai = this.ai, iq = this.iq;
    if (p.id === 'sprint_burst') {
      if (this._sbNow) return true;
      if (iq < 0.45) return Math.random() < 0.7 - iq; // careless: bursts while just jogging around
      return ai.inChase || ai.threat > 0.7;
    }
    return true;
  }
  wantsBurst(kind) {
    const iq = this.iq;
    if (kind === 'save') return Math.random() < 0.35 + 0.65 * iq && this.ai.threat < 0.75;
    return true;
  }
  tick() {
    const ai = this.ai, s = this.sv, m = this.m, iq = this.iq;
    if (!s.standing) return;
    if (m.time - (this._rc || -9) > 1) { this._rc = m.time; this.recompute(); }
    // Sprint Burst at the decisive moment: the killer is closing in and we are not yet
    // committed to a loop. Skilled bots wait for it; poor bots fire early or late.
    const sb = this.has('sprint_burst');
    if (sb && !s.status.has('exhausted')) {
      const B = ai.belief; const kd = B.conf > 0.3 ? dist(s.x, s.y, B.x, B.y) / M : 99;
      const ideal = (ai.inChase && kd < 9) || (B.observing && kd < 11 && ai.threat > 0.6);
      const jitter = (1 - iq) * 6;
      if (ideal && kd < 9 + (Math.random() - 0.5) * jitter && Math.random() < 0.25 + 0.75 * iq) {
        this._sbNow = true; sb.def.onStartRun(sb, s, m); this._sbNow = false;
        if (s.status.has('exhausted')) ai.note('Sprint Burst away from the killer', 'sbuse');
      }
    }
    // Active perks with an AI value function
    for (const p of s.perks) {
      const use = p.def.ai?.use; if (!use || !p.def.activate) continue;
      if (m.time - (this.lastUse.get(p.id) || -99) < 1.5) continue;
      const v = use.value(p, ai);
      let fire = v >= 1 && Math.random() < 0.3 + 0.7 * iq;
      if (!fire && iq < 0.4 && Math.random() < (0.4 - iq) * 0.03) fire = true; // misclick / bad timing
      if (fire && m.perks.activate(s, p.id)) this.lastUse.set(p.id, m.time);
    }
  }
  describe() {
    const b = this.bias, out = [];
    for (const k of ['repair', 'heal', 'healSelf', 'rescue', 'cleanse', 'protect', 'gate', 'joinGen', 'followCarry', 'cluster']) if (Math.abs(b[k] - 1) > 0.04) out.push(`${k}×${b[k].toFixed(2)}`);
    if (b.hide) out.push(`hide+${b.hide.toFixed(2)}`);
    return out.join(' · ');
  }
}

return { PerkBrain };
};
__defs["js/ai/survivorAI.js"] = function(){
// Survivor brain: perception → belief → utility goal selection (strategic, ~1–2 Hz)
// → behavior tree (tactical, 4–10 Hz) → path following / actions (every frame).
const { M, dist, clamp, smoothstep, gauss, angleDiff } = __req("js/core/util.js");
const { aiParams } = __req("js/ai/difficulty.js");
const { KillerBelief, INFO } = __req("js/ai/killerPrediction.js");
const { ChaseBrain } = __req("js/ai/chaseAI.js");
const { evaluateGoals, chooseGoal, GOAL_STATE } = __req("js/ai/utilityAI.js");
const { Selector, Branch, Act, S, F, R } = __req("js/ai/behaviorTree.js");
const { HOOK_STAGE_TIME } = __req("js/entities/survivor.js");
const { profileFor, dodgeProjectiles } = __req("js/ai/killerProfiles.js");
const { PerkBrain } = __req("js/ai/perkAI.js");

const GOAL_ACTION = { REPAIR: 'repair', HEAL: 'heal', HEAL_SELF: 'healSelf', CLEANSE: 'cleanse', CHEST: 'searchChest', DISARM: 'disarm', FREE: 'freeTrapped', OPEN_GATE: 'openGate',
  DEVICE: 'device', PRINT_EMP: 'device', BURN_TRAP: 'flashlight', RESCUE: 'unhook', KEY_HATCH: 'openHatch', USE_EMP: 'useEmp' };
const KEEP_ACTIONS = new Set(['useEmp', 'dropPallet', 'vault', 'wiggle', 'breakChain', 'solveBox', 'mend', 'spray', 'vaccinate', 'escapeTrap', 'recover', 'flashlight']);

class SurvivorAI {
  constructor(sv, m, difficulty, pers) {
    this.sv = sv; this.m = m; this.pers = pers; this.setDifficulty(difficulty);
    // One belief per killer; `belief`/`fk` point at the killer that matters most right now.
    this.beliefs = new Map(m.killers.map((k) => [k, new KillerBelief(this, k)]));
    this.fk = m.killer; this.belief = this.beliefs.get(this.fk); this.chase = new ChaseBrain(this);
    this.memory = { traps: new Map(), totems: new Map(), gens: new Map(), hatchSeen: false, zones: new Map(), devices: new Map(), minions: new Map() };
    this.crouchUntil = 0;
    this.profile = profileFor(m, this.fk); this.knobs = this.profile.knobs; this.perkBrain = new PerkBrain(this);
    this._zoneCells = new Map(); this._zoneV = 0; this._zoneCellsV = -1; this._overrideUntil = 0;
    this._trapCells = new Set(); this._trapVersion = -1; this._trapCellsV = -2;
    this.goal = null; this.goalCommitUntil = 0; this.options = [];
    this.state = 'IDLE'; this.trace = []; this.threat = 0; this.inChase = false; this.chaseSince = 0; this.lostT = 0;
    this.path = null; this.pathIdx = 0; this.pathTarget = null; this.pathT = -99; this.stuckT = 0; this.lastPos = { x: 0, y: 0 };
    this.tT = Math.random() * 0.2; this.sT = Math.random() * 0.8; this.pT = Math.random() * 0.1;
    this.evadeMode = null; this.evadeUntil = 0; this.hideSpot = null; this.waitSpot = null; this.forced = null;
    this.notes = new Map(); this.selfUnhookT = 0;
    this.tree = this.buildTree();
    sv.ai = this;
  }
  setDifficulty(d) { this.difficulty = d; this.params = aiParams(d); this.sv.difficulty = d; }

  // ---------------------------------------------------------------- helpers
  note(text, tag) {
    const last = this.notes.get(tag) || -99; if (this.m.time - last < 5) return;
    this.notes.set(tag, this.m.time);
    this.m.aiLog.push({ t: this.m.time, who: this.sv.name, color: this.sv.color, text, tag });
    if (this.m.aiLog.length > 200) this.m.aiLog.shift();
  }
  // Killer-power callbacks (things the survivor directly experiences)
  onPhantasm(d) { this.note('Triggered a Phantasm trap — the Hag can teleport here', 'phantasm'); this.evadeUntil = this.m.time + 3; this.evadeMode = 'run'; this.evadeDecideT = this.m.time + 3; }
  onGuardSpotted(g) { this.note(`${g.label} spotted me`, 'gspot'); this.sv.stopAction(this.m); }
  onStandard(d) { this.memory.devices.set(d.id, d); this.note('A Standard appeared — grabbing it ends the hunt', 'stdseen'); }
  onDogPursuit(dog) { this.memory.minions.set(dog.id, { n: dog, x: dog.x, y: dog.y, t: this.m.time }); this.note('The dog is on my trail', 'dogtrail'); this.sv.stopAction(this.m); }
  beliefFor(k) { return this.beliefs.get(k) || this.belief; }
  // 2v8: focus on the killer that threatens me most (seen/heard, close, approaching). A small
  // hysteresis keeps the focus from flickering between two killers at similar distances.
  pickFocus() {
    const s = this.sv; let best = null, bs = -1;
    for (const [k, b] of this.beliefs) {
      if (b.conf < 0.12 && !b.heartbeat) continue;
      const p = b.pos(); const d = dist(p.x, p.y, s.x, s.y) / M;
      let sc = b.conf / (1 + d / 14) + (b.observing ? 0.4 : 0) + b.heartbeat * 0.5 + Math.max(0, b.approach) / M * 0.05;
      if (k === this.fk) sc *= 1.15;
      if (sc > bs) { bs = sc; best = k; }
    }
    if (best && best !== this.fk) { this.fk = best; this.belief = this.beliefs.get(best); this.profile = profileFor(this.m, best); this.knobs = this.profile.knobs; this.note(`Watching ${best.def.short}`, 'focus'); }
  }
  wantsExhaustionPerk(p) { return this.perkBrain.wantsExhaustion(p); }
  killerDistAt(x, y) {
    const B = this.belief; const kn = this.knobs;
    // Against stealth killers an unknown position is treated as closer (ambush paranoia).
    const unknown = kn.stealthy ? 32 - (6 + 10 * kn.unknownParanoia) * this.params.awareness : 32;
    if (B.conf < 0.12) return unknown;
    const p = B.pos(); const d = dist(p.x, p.y, x, y) / M;
    return d * B.conf + unknown * (1 - B.conf);
  }
  safetyAt(x, y) { return smoothstep(6, 30, this.killerDistAt(x, y)); }
  nearestTile(x, y, maxD) { let best = null, bd = maxD; for (const t of this.m.map.tiles) { if (t.safety < 0.2) continue; const d = dist(t.x, t.y, x, y); if (d < bd) { bd = d; best = t; } } return best; }
  genKnowledge(g) {
    const mem = this.memory.gens.get(g.id), bbk = this.m.bb.knownGen(g);
    let p = mem ? mem.p : 0; if (bbk && Math.random() < this.params.comms && (!mem || bbk.t > mem.t)) p = bbk.p;
    return p;
  }
  trapNear(x, y, r) { for (const t of this.memory.traps.values()) if (t.trap.state === 'set' && dist(t.x, t.y, x, y) < r) return true; return false; }
  trapCells() {
    if (this._trapCellsV === this._trapVersion) return this._trapCells;
    const nav = this.m.map.nav; const set = new Set();
    for (const t of this.memory.traps.values()) {
      if (t.trap.state !== 'set') continue;
      const [c, r] = nav.cellOf(t.x, t.y);
      for (let dy = -1; dy <= 1; dy++) for (let dx = -1; dx <= 1; dx++) set.add((r + dy) * nav.cols + c + dx);
    }
    this._trapCells = set; this._trapCellsV = this._trapVersion; return set;
  }
  // Known danger areas (gas, torment trails, biopod surveillance) as per-cell path costs.
  zoneCells() {
    if (this._zoneCellsV === this._zoneV && this.m.time - (this._zoneBuiltT || 0) < 1) return this._zoneCells;
    const m = this.m, nav = m.map.nav, P = this.params, kn = this.knobs, s = this.sv;
    const map = new Map(); const w = kn.zoneAvoid * (0.25 + 0.75 * P.zoneSense);
    const slip = s.status.has('slipstream');
    const add = (x, y, r, cost) => {
      const [c0, r0] = nav.cellOf(x - r, y - r), [c1, r1] = nav.cellOf(x + r, y + r);
      for (let rr = r0; rr <= r1; rr++) for (let cc = c0; cc <= c1; cc++) { const i = rr * nav.cols + cc; const p = nav.center(i); if ((p.x - x) ** 2 + (p.y - y) ** 2 < r * r) map.set(i, (map.get(i) || 0) + cost); }
    };
    for (const z of this.memory.zones.values()) if (z.until > m.time && z.aiCost > 0) add(z.x, z.y, z.r + 0.4 * M, z.aiCost * w);
    for (const d of this.memory.devices.values()) if (!d.dead && d.dangerRadius && (d.data?.disabledUntil || 0) <= m.time) add(d.x, d.y, d.dangerRadius, (d.kind === 'phantasm' ? d.aiCost : slip ? 6 : 2.5) * w);
    this._zoneCells = map; this._zoneCellsV = this._zoneV; this._zoneBuiltT = m.time; return map;
  }
  agent() {
    const cellCost = this.chase.survivorCellCost(); const P = this.params; const m = this.m; const B = this.belief;
    const kb = B.conf > 0.3 ? B.pos() : null;
    return {
      linkCost: this.chase.survivorLinkCost(),
      cellCost: (i) => {
        let c = cellCost(i);
        // high-level bots route around the killer's believed position
        if (kb && P.difficulty >= 4) { const p = m.map.nav.center(i); const d = dist(p.x, p.y, kb.x, kb.y) / M; if (d < 14) c += (14 - d) * 0.35 * B.conf * (0.4 + 0.6 * P.awareness); }
        return c;
      },
    };
  }

  // -------------------------------------------------------------- movement
  setPath(pts, fromChase = false) {
    const P = this.params;
    if (!fromChase && P.pathNoise > 0.1) for (let i = 1; i < pts.length - 1; i++) if (!pts[i].viaLink && !(pts[i + 1] && pts[i + 1].viaLink)) { pts[i] = { ...pts[i], x: pts[i].x + gauss() * P.pathNoise * M * 0.3, y: pts[i].y + gauss() * P.pathNoise * M * 0.3 }; }
    this.path = pts; this.pathIdx = Math.min(1, pts.length - 1); this.pathT = this.m.time;
  }
  navigateTo(x, y, tol = 0.6 * M) {
    const s = this.sv, m = this.m;
    if (dist(s.x, s.y, x, y) <= tol) { this.path = null; s.moveTo = null; return true; }
    const tgtMoved = !this.pathTarget || dist(this.pathTarget.x, this.pathTarget.y, x, y) > 1.5 * M;
    if (!this.path || tgtMoved || m.time - this.pathT > 3) {
      if (m.pathBudget <= 0) return false;
      m.pathBudget--;
      const r = m.map.nav.findPath(s.x, s.y, x, y, this.agent());
      if (r) { const sm = m.map.nav.smooth(m.world, r.pts, s.r * 0.9); this.setPath(sm); this.pathTarget = { x, y }; }
      else { this.path = [{ x: s.x, y: s.y }, { x, y }]; this.pathIdx = 1; this.pathT = m.time; this.pathTarget = { x, y }; }
    }
    return false;
  }
  followPath(dt) {
    const s = this.sv, m = this.m;
    if (s.busy && !(s.action && s.action.type === 'flashlight')) { s.moveTo = null; return; }
    if (!this.path || this.pathIdx >= this.path.length) { s.moveTo = null; return; }
    let p = this.path[this.pathIdx];
    // Link crossing: vault the window / dropped pallet between previous point and p
    if (p.viaLink) {
      const L = p.viaLink, o = L.ref;
      const near = dist(s.x, s.y, o.x, o.y) < (o.kind === 'window' ? 1.75 : 1.85) * M;
      const crossed = o.sideOf(s) === o.sideOf(p);
      if (crossed) { this.pathIdx++; return; }
      if (near) {
        const can = o.kind === 'window' ? o.blockedUntil <= m.time : o.state === 'down';
        if (can) { s.startVault(o, this.vaultQuality(o), m); this.pathIdx++; return; }
        this.path = null; return; // blocked: replan
      }
      // walk to the near side of the link first
      const side = o.sideOf(s); const sp = o.sides(1.2 * M)[side];
      s.moveTo = sp; return;
    }
    s.moveTo = p;
    if (dist(s.x, s.y, p.x, p.y) < 0.45 * M || (this.pathIdx < this.path.length - 1 && dist(s.x, s.y, p.x, p.y) < 0.9 * M && !this.path[this.pathIdx + 1].viaLink)) this.pathIdx++;
    if (this.pathIdx >= this.path.length) {
      const e = this.path[this.path.length - 1]; const ld = this.chase.lastDir;
      // in a chase, momentum carries on between replans instead of stopping dead
      s.moveTo = (this.inChase || this.evadeUntil > m.time) && ld ? { x: e.x + ld.x * 1.5 * M, y: e.y + ld.y * 1.5 * M } : e;
    }
  }
  vaultQuality(o) {
    const P = this.params, s = this.sv;
    const speed = Math.hypot(s.vx, s.vy);
    const r = Math.random();
    if (speed > 3 * M && r < P.fastVault * (0.75 + 0.35 * this.pers.vaultSkill)) return 'fast';
    if (P.difficulty <= 2 && r > 0.85) return 'slow';
    return 'medium';
  }

  // -------------------------------------------------------------- perception
  perceiveWorld() {
    const s = this.sv, m = this.m, P = this.params;
    const smallGame = s.hasPerk('small_game');
    for (const t of m.traps) {
      if (t.state !== 'set') { if (this.memory.traps.has(t.id) && t.state !== 'set') { this.memory.traps.delete(t.id); this._trapVersion++; } continue; }
      if (this.memory.traps.has(t.id)) continue;
      const d = dist(s.x, s.y, t.x, t.y);
      let r = (t.inGrass ? 2.5 : 7.5) * M * P.trapSense; if (smallGame) r = Math.max(r, 8 * M); if (s.hasPerk('vigilant_eye')) r = Math.max(r, 12 * M);
      if (d < r && m.world.losClear(s.x, s.y, t.x, t.y)) {
        this.memory.traps.set(t.id, { x: t.x, y: t.y, trap: t, t: m.time }); this._trapVersion++;
        if (Math.random() < P.comms) m.bb.reportTrap(t);
        this.note('Spotted a bear trap', 'trapseen');
      }
    }
    // Team-shared trap callouts
    if (Math.random() < P.comms * 0.5) for (const [id, info] of m.bb.traps) if (!this.memory.traps.has(id) && info.trap.state === 'set') { this.memory.traps.set(id, { ...info }); this._trapVersion++; }
    for (const t of m.map.totems) if (!this.memory.totems.has(t.id) && dist(s.x, s.y, t.x, t.y) < (t.hex ? 22 : 12) * M && m.world.losClear(s.x, s.y, t.x, t.y)) this.memory.totems.set(t.id, { totem: t, t: m.time });
    for (const g of m.map.gens) if (dist(s.x, s.y, g.x, g.y) < 18 * M) { this.memory.gens.set(g.id, { p: g.progress, t: m.time }); if (g.repairers.has(s)) m.bb.reportGen(g); }
    const h = m.map.hatch; if (h.state !== 'hidden' && dist(s.x, s.y, h.x, h.y) < 24 * M) this.memory.hatchSeen = true;
    // Killer-power objects: gas clouds, torment trails, pods, printers, lockers.
    const vig = s.hasPerk('vigilant_eye') ? 12 * M : 0;
    for (const z of m.zones) {
      if (this.memory.zones.has(z.id)) continue;
      if (dist(s.x, s.y, z.x, z.y) < vig || dist(s.x, s.y, z.x, z.y) < z.seenRange && (dist(s.x, s.y, z.x, z.y) < 6 * M || m.world.losClear(s.x, s.y, z.x, z.y))) { this.memory.zones.set(z.id, z); this._zoneV++; }
    }
    for (const [id, z] of this.memory.zones) if (z.until <= m.time) { this.memory.zones.delete(id); this._zoneV++; }
    for (const d of m.devices) {
      if (this.memory.devices.has(d.id)) continue;
      if (d.kind === 'printer' || dist(s.x, s.y, d.x, d.y) < vig || (dist(s.x, s.y, d.x, d.y) < d.seenRange && m.world.losClear(s.x, s.y, d.x, d.y))) {
        this.memory.devices.set(d.id, d); this._zoneV++;
        if (d.kind === 'biopod') { this.note('Spotted a biopod', 'pod'); if (Math.random() < P.comms) for (const o of m.survivors) if (o.ai && o !== s) { o.ai.memory.devices.set(d.id, d); o.ai._zoneV++; } }
      }
    }
    for (const [id, d] of this.memory.devices) if (d.dead) { this.memory.devices.delete(id); this._zoneV++; }
    // Killer minions (the dog, guards): tracked separately from the killer, by sight only.
    const sight = (s.status.mods().sightMult ?? 1);
    for (const n of m.minions) {
      const d = dist(s.x, s.y, n.x, n.y);
      if (d < n.visRange * sight && m.world.losClear(s.x, s.y, n.x, n.y)) {
        if (!this.memory.minions.has(n.id)) this.note(`Spotted the ${n.label}`, 'minion-' + n.kind);
        this.memory.minions.set(n.id, { n, x: n.x, y: n.y, t: m.time });
      }
    }
    for (const [id, e] of this.memory.minions) if (e.n.dead || m.time - e.t > 6) this.memory.minions.delete(id);
  }

  computeThreat() {
    const B = this.belief, s = this.sv, m = this.m, P = this.params;
    const kp = B.pos(); const kd = dist(s.x, s.y, kp.x, kp.y) / M;
    let th = B.conf * (1 - smoothstep(4, 30, kd));
    if (B.approach > 0.6 * M) th += 0.2 * B.conf;
    th += B.heartbeat * P.threatSense * 0.55 * (B.observing ? 0 : 1);
    if (B.spineChill) th += 0.3;
    if (m.time - B.bellT < 2 && B.bellKind === 'uncloak' && kd < 16) th += 0.5;
    if (m.time - B.revvingT < 1 && kd < 22) th += 0.35;
    if (m.time - (B.teleportT || -99) < 2 && kd < 18) th += 0.6;
    if (s.status.has('marked')) th += 0.15;
    for (const e of this.memory.minions.values()) { const n = e.n; if (n.target === s && ['hunt', 'pursue', 'spotted'].includes(n.state)) th += 0.45 * (1 - smoothstep(4, 18, dist(n.x, n.y, s.x, s.y) / M)); }
    if (s.status.has('nightfall')) th += 0.1;
    this.threat = clamp(th, 0, 1);
    // chase state with hysteresis
    const seenRecently = B.observing || B.age < 2.5;
    const hitRecently = m.time - s.lastHitT < 6;
    const close = kd < 10 + 6 * this.pers.killerAwareness;
    const coming = B.approach > 0.3 * M || kd < 6;
    if (!this.inChase) {
      if ((seenRecently && close && coming && B.conf > 0.5) || (hitRecently && B.conf > 0.3) || (m.time - B.bellT < 1.5 && B.bellKind === 'uncloak' && kd < 12)) {
        this.inChase = true; this.chaseSince = m.time; this.lostT = 0; this.sv.stopAction(m); m.bb.reportChase(s);
      }
    } else {
      if (!B.observing && kd > 14) this.lostT += 1 / P.tacticalHz; else this.lostT = 0;
      if (this.lostT > 3 + 2 * this.pers.killerAwareness || kd > 30 || B.conf < 0.2) { this.inChase = false; this.note(kd > 25 ? 'Broke the chase' : 'Lost the killer', 'lost'); }
      else m.bb.reportChase(s);
    }
  }

  // -------------------------------------------------------------- behavior tree
  buildTree() {
    const ai = this;
    return new Selector('Root', [
      Branch('Spectate', () => !ai.sv.alive, () => { ai.state = 'SPECTATING'; return R; }),
      Branch('Hook', () => ai.sv.health === 'hooked', () => ai.hookBehavior()),
      Branch('Carried', () => ai.sv.health === 'carried', () => { ai.state = 'DOWNED'; return R; }),
      Branch('Downed', () => ai.sv.health === 'dying', () => ai.downedBehavior()),
      Branch('Trapped', () => !!ai.sv.trap, () => ai.trappedBehavior()),
      Branch('Forced', () => !!ai.forced, () => ai.forcedBehavior()),
      Branch('BeingHealed', () => ai.beingHealed() && ai.threat < 0.55 && !ai.inChase, () => { ai.state = 'RECOVERING'; ai.sv.moveTo = null; ai.path = null; return R; }),
      Branch('Chase', () => ai.inChase, () => ai.chaseBehavior()),
      Branch('Evade', () => ai.shouldEvade(), () => ai.evadeBehavior()),
      Branch('Mend', () => ai.sv.status.has('deepWound') && ai.threat < 0.4, () => { ai.state = 'RECOVERING'; ai.sv.startAction('mend', null, ai.m); return R; }),
      new Act('Goal', () => ai.executeGoal()),
    ]);
  }
  beingHealed() { const s = this.sv; return this.m.survivors.some((o) => o !== s && o.action?.type === 'heal' && o.action.target === s); }

  shouldEvade() {
    const P = this.params, m = this.m;
    if (this.evadeUntil > m.time) return true;
    const thr = 0.62 - 0.22 * this.pers.riskTolerance + (this.sv.action?.type === 'repair' ? 0.05 : 0);
    if (this.threat > thr) { this.evadeUntil = m.time + 2.5; this.evadeMode = null; return true; }
    return false;
  }

  hookBehavior() {
    const s = this.sv, m = this.m, P = this.params;
    this.state = 'HOOKED'; s.moveTo = null; this.path = null;
    if (s.hookStage === 1) {
      const dl = s.perk('deliverance');
      if (dl && dl.tokens > 0 && s.hookTimer > 3) { dl.tokens = 0; m.selfUnhook(s, true); this.note('Self-unhooked with Deliverance', 'deliv'); return R; }
      const others = m.survivors.filter((o) => o !== s && o.alive && o.standing);
      const hopeless = others.length === 0 || (m.collapse && m.collapseT < 25);
      if ((hopeless || (this.pers.selfishness > 0.75 && P.difficulty <= 3)) && s.unhookAttempts < 3 && s.hookTimer > 5 + s.unhookAttempts * 8) { m.selfUnhookAttempt(s); }
    }
    return R;
  }

  downedBehavior() {
    const s = this.sv, m = this.m, B = this.belief, P = this.params;
    this.state = 'DOWNED';
    const healer = m.survivors.find((o) => o !== s && o.action?.type === 'heal' && o.action.target === s);
    const coming = m.bb.claimedBy('HEAL:' + s.id);
    if (healer || (coming && dist(coming.x, coming.y, s.x, s.y) < 5 * M)) { s.moveTo = null; this.path = null; s.stopAction(m); return R; }
    const kd = this.killerDistAt(s.x, s.y);
    const canFull = s.hasPerk('unbreakable') && !s.unbreakableUsed;
    if ((kd > 22 || B.conf < 0.2) && (canFull || s.recovery < 0.95)) {
      if (!coming || canFull) { s.startAction('recover', null, m); if (canFull && s.recovery > 0.9) this.note('Picked themselves up (Unbreakable)', 'unb'); return R; }
    }
    if (s.action?.type === 'recover') s.stopAction(m);
    // crawl: away from the killer, toward healthy teammates / cover
    if (!this._crawlT || m.time - this._crawlT > 1.5) {
      this._crawlT = m.time;
      let tx = s.x, ty = s.y;
      const mates = m.survivors.filter((o) => o !== s && o.standing);
      if (mates.length && P.difficulty >= 4) { const o = mates.sort((a, b) => dist(a.x, a.y, s.x, s.y) - dist(b.x, b.y, s.x, s.y))[0]; tx = o.x; ty = o.y; }
      if (B.conf > 0.2) { const kp = B.pos(); const ax = s.x - kp.x, ay = s.y - kp.y; const l = Math.hypot(ax, ay) || 1; tx = (tx + s.x + ax / l * 10 * M) / 2; ty = (ty + s.y + ay / l * 10 * M) / 2; }
      this._crawlTarget = { x: tx, y: ty };
    }
    if (this._crawlTarget) this.navigateTo(this._crawlTarget.x, this._crawlTarget.y, 1.5 * M);
    return R;
  }

  trappedBehavior() {
    const s = this.sv, m = this.m; this.state = 'RECOVERING';
    const freer = m.survivors.find((o) => o !== s && o.action?.type === 'freeTrapped' && o.action.target === s);
    if (!freer && !s.action) s.startAction('escapeTrap', null, m);
    return R;
  }

  forcedBehavior() {
    // Debug "force state": temporarily pins a behavior
    const f = this.forced; const m = this.m;
    if (f.until < m.time) { this.forced = null; return F; }
    if (f.state === 'HIDING') return this.hide(true);
    if (f.state === 'CHASE') { this.state = 'CHASE'; this.chase.tick(true); return R; }
    if (f.state === 'REPAIRING_GENERATOR') { const g = m.map.gens.filter((g) => !g.done).sort((a, b) => dist(a.x, a.y, this.sv.x, this.sv.y) - dist(b.x, b.y, this.sv.x, this.sv.y))[0]; if (g) return this.doRepair(g); }
    if (f.state === 'IDLE') { this.state = 'IDLE'; this.sv.moveTo = null; this.path = null; return R; }
    this.forced = null; return F;
  }

  chaseBehavior() {
    const s = this.sv, m = this.m;
    this.sv.moveMode = 'run';
    if (s.action && s.action.type !== 'dropPallet') s.stopAction(m);
    const tile = m.map.tileAt(s.x, s.y);
    this.state = s.vault ? 'VAULTING' : s.action?.type === 'dropPallet' ? 'USING_PALLET' : tile && tile.safety > 0.25 ? 'LOOPING' : 'CHASE';
    if (!this._chaseNoteT && this.params.difficulty >= 7 && m.bb.activeGens(s).length) { this._chaseNoteT = 1; }
    this.chase.tick(true);
    if (this.chase.debug.nextTile && this.params.leadAway > 0.5 && m.bb.activeGens(s).length && Math.random() < 0.02) this.note('Leading the killer away from a teammate\'s generator', 'leadaway');
    s.stats.chaseTime += 1 / this.params.tacticalHz;
    return R;
  }

  evadeBehavior() {
    const s = this.sv, m = this.m, P = this.params, B = this.belief, pers = this.pers;
    if (s.action && s.action.type !== 'dropPallet') {
      if (s.action.type === 'repair' && this.threat > 0.5) this.note(B.state === INFO.HEARD ? 'Left the generator on the heartbeat' : 'Stopped repairing — killer approaching', 'leavegen');
      s.stopAction(m);
    }
    if (!this.evadeMode || m.time > this.evadeDecideT) {
      // Hide when stealthy, uninjured (no blood/grunts) and the killer has not seen us; else pre-run to a tile.
      const kp = B.pos(); const seen = B.observing && m.world.losClear(kp.x, kp.y, s.x, s.y) && dist(kp.x, kp.y, s.x, s.y) < 18 * M;
      const hideScore = (pers.stealthSkill * P.stealth * (s.health === 'healthy' ? 1 : 0.5) + this.knobs.hideBias + this.perkBrain.bias.hide) * (seen ? 0.25 : 1) * (this.fk.power.id === 'cloak' ? 0.7 : 1);
      const runScore = (1 - pers.stealthSkill * 0.6) * (0.4 + pers.riskTolerance * 0.6) * (seen ? 1.6 : 0.8);
      this.evadeMode = hideScore > runScore ? 'hide' : 'run'; this.evadeDecideT = m.time + 2.5;
      this.hideSpot = null;
    }
    if (this.evadeMode === 'hide') return this.hide(false);
    this.state = 'FLEEING'; s.moveMode = 'run';
    // Pre-run: skilled survivors head for a strong tile before the killer arrives,
    // so the chase starts where they can loop instead of in the open.
    const kp = B.pos(); const kd = dist(kp.x, kp.y, s.x, s.y) / M;
    if (P.difficulty >= 5 && kd > 9 && B.conf > 0.3) {
      if (!this.preTile || m.time - this.preTileT > 2) { this.preTile = this.chase.chooseNextTile(kp); this.preTileT = m.time; }
      const t = this.preTile;
      if (t) {
        const ax = t.x - kp.x, ay = t.y - kp.y; const l = Math.hypot(ax, ay) || 1;
        const spot = { x: t.x + ax / l * Math.min(t.radius * 0.5, 3 * M), y: t.y + ay / l * Math.min(t.radius * 0.5, 3 * M) };
        const i = m.map.nav.nearestOpen(spot.x, spot.y, 4); const c = i >= 0 ? m.map.nav.center(i) : spot;
        if (!this._preNote) { this._preNote = true; this.note(`Pre-running to the ${t.label.toLowerCase()}`, 'prerun'); }
        if (this.navigateTo(c.x, c.y, 1.2 * M)) { s.moveTo = null; this.path = null; }
        // keep evading while the killer keeps coming
        if (B.approach > 0.2 * M && kd < 30) this.evadeUntil = Math.max(this.evadeUntil, m.time + 1.5);
        return R;
      }
    }
    this._preNote = false;
    this.chase.tick(false);
    return R;
  }

  hide(forced) {
    const s = this.sv, m = this.m, B = this.belief, nav = m.map.nav;
    this.state = 'HIDING';
    const kp = B.conf > 0.15 ? B.pos() : (m.bb.sighting ? { x: m.bb.sighting.x, y: m.bb.sighting.y } : { x: s.x + 1, y: s.y });
    if (!this.hideSpot || (m.time - this.hideSpotT > 3)) {
      let best = null, bs = -1e9;
      for (let k = 0; k < 40; k++) {
        const a = Math.random() * Math.PI * 2, r = (2 + Math.random() * 12) * M;
        const x = s.x + Math.cos(a) * r, y = s.y + Math.sin(a) * r; const i = nav.idxAt(x, y); if (nav.isBlocked(i)) continue;
        const c = nav.center(i);
        let sc = 0; if (!m.world.losClear(kp.x, kp.y, c.x, c.y)) sc += 4; if (nav.grass[i]) sc += 2.5;
        sc += dist(c.x, c.y, kp.x, kp.y) / M * 0.15 - r / M * 0.12;
        if (this.trapCells().has(i)) sc -= 20;
        if (sc > bs) { bs = sc; best = c; }
      }
      this.hideSpot = best; this.hideSpotT = m.time;
    }
    if (this.hideSpot) {
      const arrived = this.navigateTo(this.hideSpot.x, this.hideSpot.y, 0.5 * M);
      s.moveMode = arrived || dist(s.x, s.y, kp.x, kp.y) < 14 * M ? 'crouch' : 'walk';
    }
    // If the killer clearly sees us up close, give up hiding and run.
    if (!forced && B.observing && dist(B.x, B.y, s.x, s.y) < 9 * M && m.world.losClear(B.x, B.y, s.x, s.y)) { this.evadeMode = 'run'; this.evadeDecideT = m.time + 3; }
    return R;
  }

  // -------------------------------------------------------------- goals
  strategic() {
    const opts = evaluateGoals(this); this.options = opts;
    const pick = chooseGoal(this, opts);
    if (!pick) return;
    if (!this.goal || pick.key !== this.goal.key) {
      if (this.goal) this.m.bb.release(this.sv);
      this.goal = pick; this.goalCommitUntil = this.m.time + 2.5 + this.pers.objectiveFocus;
      if (['REPAIR', 'RESCUE', 'HEAL', 'OPEN_GATE', 'DEVICE', 'BURN_TRAP'].includes(pick.type)) this.m.bb.claim(pick.key, this.sv);
      this.waitSpot = null;
      if (pick.mistake) this.note('Made a questionable call: ' + pick.type.toLowerCase(), 'mistake');
    } else { this.goal.score = pick.score; this.goal.why = pick.why; this.m.bb.refreshClaim(this.sv, this.goal.key); }
  }

  executeGoal() {
    const g = this.goal, s = this.sv, m = this.m; s.moveMode = 'run';
    if (!g) { this.state = 'IDLE'; return R; }
    this.state = GOAL_STATE[g.type] || 'IDLE';
    const t = g.target;
    // Switching goals mid-action (e.g. leaving a generator to lock a locker): let go of the old action.
    const want = GOAL_ACTION[g.type];
    if (want !== undefined && s.action && !KEEP_ACTIONS.has(s.action.type) && (s.action.type !== want || (s.action.target && t && s.action.target !== t && want !== 'unhook'))) s.stopAction(m);
    switch (g.type) {
      case 'REPAIR': return this.doRepair(t);
      case 'RESCUE': return this.doRescue(t);
      case 'HEAL': if (t.health !== 'injured' && t.health !== 'dying') { this.goal = null; return S; }
        if (this.navigateTo(t.x, t.y, 1.4 * M) || dist(s.x, s.y, t.x, t.y) < 1.6 * M) s.startAction('heal', t, m); return R;
      case 'BE_HEALED': if (dist(s.x, s.y, t.x, t.y) > 1.5 * M) this.navigateTo(t.x, t.y, 1.2 * M); else { s.moveTo = null; this.path = null; } return R;
      case 'HEAL_SELF': if (!s.action) s.startAction('healSelf', null, m); return R;
      case 'CLEANSE': if (this.navigateTo(t.x, t.y, 1.2 * M)) s.startAction('cleanse', t, m); return R;
      case 'CHEST': if (this.navigateTo(t.x, t.y, 1.2 * M)) s.startAction('searchChest', t, m); return R;
      case 'DISARM': if (t.state !== 'set') { this.goal = null; return S; } if (this.navigateTo(t.x, t.y, 1.1 * M)) s.startAction('disarm', t, m); return R;
      case 'FREE': if (!t.trap) { this.goal = null; return S; } if (this.navigateTo(t.x, t.y, 1.2 * M)) s.startAction('freeTrapped', t, m); return R;
      case 'OPEN_GATE': if (t.open) { this.goal = null; return S; } if (this.navigateTo(t.x, t.y, 1.0 * M)) s.startAction('openGate', t, m); return R;
      case 'ESCAPE': { const e = t.exitRect; this.navigateTo(e.x + e.w / 2, e.y + e.h / 2, 0.3 * M); return R; }
      case 'HATCH': case 'KEY_HATCH': if (this.navigateTo(t.x, t.y, 0.8 * M)) { if (t.state === 'open') m.escapeHatch(s); else s.startAction('openHatch', t, m); } return R;
      case 'PROTECT': { const kp = this.belief.pos(); const ax = kp.x - t.x, ay = kp.y - t.y; const l = Math.hypot(ax, ay) || 1; this.navigateTo(t.x + ax / l * 1.4 * M, t.y + ay / l * 1.4 * M, 0.5 * M); return R; }
      case 'FLASHLIGHT': return this.doFlashlight();
      case 'DEVICE': if (t.dead || !t.usable(s, m)) { this.goal = null; return S; } if (s.action?.type === 'device') return R; if (this.navigateTo(t.x, t.y, 1.2 * M)) s.startAction('device', t, m); return R;
      case 'BURN_TRAP': {
        if (t.dead || !s.item || s.item.charges <= 0) { this.goal = null; return S; }
        if (dist(s.x, s.y, t.x, t.y) < 5 * M && m.world.losClear(s.x, s.y, t.x, t.y)) { s.moveTo = null; this.path = null; s.facing = Math.atan2(t.y - s.y, t.x - s.x); if (s.action?.type !== 'flashlight') s.startAction('flashlight', t, m); return R; }
        this.navigateTo(t.x, t.y, 4 * M); return R;
      }
      case 'USE_EMP': if (!s.emp || t.dead) { this.goal = null; return S; } if (this.navigateTo(t.x, t.y, 3 * M)) { s.startAction('useEmp', null, m); this.goal = null; } return R;
      case 'PRINT_EMP': if (t.dead || !t.usable(s, m)) { this.goal = null; return S; } if (s.action?.type === 'device') return R; if (this.navigateTo(t.x, t.y, 1.2 * M)) s.startAction('device', t, m); return R;
      case 'FOLLOW_CARRY': { const k = m.carrier(t) || this.fk; if (!k.carrying) { this.goal = null; return S; } this.navigateTo(k.x - Math.cos(k.facing) * 3 * M, k.y - Math.sin(k.facing) * 3 * M, 1 * M); return R; }
      case 'PATROL': default: return this.doPatrol();
    }
  }

  doRepair(gen) {
    const s = this.sv, m = this.m, P = this.params, B = this.belief;
    if (gen.done || gen.blockedUntil > m.time) { this.goal = null; return S; }
    if (s.action?.type === 'repair' && s.action.target === gen) {
      this.state = 'REPAIRING_GENERATOR';
      // Predictive early leave: estimate killer ETA from the belief, not true position.
      // Heartbeat-only cue: distance shrinking fast and already close.
      if (!B.dirKnown && B.heardDist !== null && P.leavesEarly) {
        const closing = B.hbClosing(); const eta = Math.max(0, B.heardDist / M - 3) / 4.6;
        if (closing > 2.2 && eta < P.leaveAhead * 0.7 * (1.25 - 0.5 * this.pers.riskTolerance) && gen.progress < 0.92) {
          s.stopAction(m); this.evadeUntil = m.time + 3; this.evadeMode = null;
          this.note('Heartbeat rising fast — left the generator', 'hbleave'); return R;
        }
      }
      if (P.leavesEarly && B.conf > 0.3 && B.dirKnown) {
        const kp = B.pos(); const kd = dist(kp.x, kp.y, gen.x, gen.y) / M;
        const eta = Math.max(0, kd - 3) / 4.6;
        const approaching = B.approach > 0.4 * M || (B.observing && angleDiff(Math.atan2(B.vy, B.vx), Math.atan2(gen.y - B.y, gen.x - B.x)) < 0.6 && Math.hypot(B.vx, B.vy) > 2 * M);
        const thresh = P.leaveAhead * this.knobs.leaveEarly * (s.status.has('marked') ? 1.4 : 1) * (1.25 - 0.5 * this.pers.riskTolerance) * (gen.progress > 0.9 ? 0.4 : 1);
        if (approaching && eta < thresh) {
          s.stopAction(m); this.evadeUntil = m.time + 3; this.evadeMode = null;
          this.note(`Stopped repairing — predicted the killer in ${eta.toFixed(1)}s`, 'predictleave');
          return R;
        }
      }
      return R;
    }
    this.state = 'REPAIRING_GENERATOR';
    // pick a slot not occupied, preferring one facing away from the believed killer
    if (!this._slot || this._slotGen !== gen || m.time - this._slotT > 4) {
      const occ = [...gen.repairers].map((o) => ({ x: o.x, y: o.y }));
      const kp = B.conf > 0.2 ? B.pos() : null;
      const slots = gen.slots().filter((p) => !occ.some((o) => dist(o.x, o.y, p.x, p.y) < 0.8 * M));
      slots.sort((a, b) => (dist(a.x, a.y, s.x, s.y) - (kp ? dist(a.x, a.y, kp.x, kp.y) * 0.3 : 0)) - (dist(b.x, b.y, s.x, s.y) - (kp ? dist(b.x, b.y, kp.x, kp.y) * 0.3 : 0)));
      this._slot = slots[0] || gen.slots()[0]; this._slotGen = gen; this._slotT = m.time;
    }
    const arrived = this.navigateTo(this._slot.x, this._slot.y, 0.4 * M) || dist(s.x, s.y, gen.x, gen.y) < 1.9 * M;
    if (arrived) { s.moveTo = null; this.path = null; if (!s.startAction('repair', gen, m)) this._slot = null; }
    return R;
  }

  doRescue(o) {
    const s = this.sv, m = this.m, P = this.params, B = this.belief;
    if (o.health !== 'hooked') { this.goal = null; return S; }
    const left = m.hookStageTime - o.hookTimer; const d = dist(s.x, s.y, o.x, o.y) / M;
    const kh = this.killerDistAt(o.x, o.y);
    const camping = B.conf > 0.3 && kh < 14;
    if (camping && P.waitsForCamper && left > d / 4 + 10 && !(o.hookStage >= 2 && left < 20)) {
      this.state = 'GOING_TO_HOOK';
      if (!this.waitSpot) {
        const kp = B.pos(); const ax = o.x - kp.x, ay = o.y - kp.y; const l = Math.hypot(ax, ay) || 1;
        this.waitSpot = { x: o.x + ax / l * 20 * M, y: o.y + ay / l * 20 * M };
        this.note('Waiting for the killer to leave the hook', 'waitcamp');
        m.bb.model.camping += 0.2;
      }
      const arrived = this.navigateTo(this.waitSpot.x, this.waitSpot.y, 2 * M);
      s.moveMode = arrived ? 'crouch' : 'walk';
      return R;
    }
    this.waitSpot = null; this.state = 'RESCUING';
    if (this.navigateTo(o.x, o.y, 1.3 * M) || dist(s.x, s.y, o.x, o.y) < 1.7 * M) s.startAction('unhook', o, m);
    return R;
  }

  doFlashlight() {
    const s = this.sv, m = this.m, k = this.fk;
    const target = k.carrying || (k.action?.type === 'pickup' ? k.action.target : null);
    if (!target || !s.item || s.item.charges <= 0) { this.goal = null; return S; }
    this.state = 'HELPING_TEAMMATE';
    const spot = { x: k.x + Math.cos(k.facing) * 4 * M, y: k.y + Math.sin(k.facing) * 4 * M };
    if (dist(s.x, s.y, k.x, k.y) < 6.5 * M) { s.facing = Math.atan2(k.y - s.y, k.x - s.x); if (!s.action) s.startAction('flashlight', k, m); this.navigateTo(spot.x, spot.y, 1 * M); }
    else this.navigateTo(spot.x, spot.y, 1 * M);
    return R;
  }

  doPatrol() {
    const s = this.sv, m = this.m;
    this.state = 'PATROLLING';
    if (!this._patrol || dist(s.x, s.y, this._patrol.x, this._patrol.y) < 2 * M || m.time - this._patrolT > 15) {
      const gens = m.map.gens.filter((g) => !g.done);
      const pick = gens.length ? gens[Math.floor(Math.random() * gens.length)] : m.map.gates[0] || m.map.tiles[Math.floor(Math.random() * m.map.tiles.length)] || { x: s.x, y: s.y };
      this._patrol = { x: pick.x + (Math.random() - 0.5) * 6 * M, y: pick.y + (Math.random() - 0.5) * 6 * M }; this._patrolT = m.time;
    }
    this.navigateTo(this._patrol.x, this._patrol.y, 1.5 * M);
    return R;
  }

  // -------------------------------------------------------------- main update
  update(dt) {
    const m = this.m, s = this.sv;
    if (m.aiPaused) { s.moveTo = null; return; }
    if (!s.alive) { this.state = 'SPECTATING'; return; }
    this.pT -= dt;
    if (this.pT <= 0) { const step = 0.1 - this.pT; this.pT = 0.1; for (const b of this.beliefs.values()) b.update(step, m); if (this.beliefs.size > 1) this.pickFocus(); this.perceiveWorld(); }
    this.sT -= dt;
    if (this.sT <= 0) { this.sT = this.params.strategicEvery * (0.8 + Math.random() * 0.4); if (s.standing) this.strategic(); }
    this.tT -= dt;
    if (this.tT <= 0) {
      this.tT = 1 / (this.params.tacticalHz * (m.thinkScale || 1));
      this.computeThreat();
      // Killer-specific reactions (reveal Ghost Face, dodge hatchets, side-step waves, EMP pods…)
      const dk = this.profile.dynKnobs?.(this); this.knobs = dk ? { ...this.profile.knobs, ...dk } : this.profile.knobs;
      const tk = this.profile.tactical?.(this) || (m.projectiles.length && this.profile !== undefined ? dodgeProjectiles(this) : null);
      if (tk && this.chase.sidestepUntil > m.time) this._overrideUntil = this.chase.sidestepUntil;
      this.perkBrain.tick();
      if (m.time >= this._overrideUntil) { this.trace = []; this.tree.tick({ trace: this.trace, ai: this }); }
      else this.trace = ['Root', 'Counterplay', tk || 'dodge'];
      if (this.crouchUntil > m.time && s.standing && !s.vault) s.moveMode = 'crouch';
      // Stuck detection → replan
      if (s.moveTo && !s.busy) {
        if (dist(s.x, s.y, this.lastPos.x, this.lastPos.y) < 0.15 * M) { this.stuckT += this.tT; if (this.stuckT > 0.8) { this.path = null; this.stuckT = 0; this._slot = null; } }
        else this.stuckT = 0;
      }
      this.lastPos = { x: s.x, y: s.y };
    }
    this.followPath(dt);
  }
}

return { SurvivorAI };
};
__defs["js/ai/killer/params.js"] = function(){
// Killer bot difficulty (1–10). Difficulty changes how well the killer perceives, decides and
// plays its power — never its movement speed, damage or information access.
const lerp = (a, b, t) => a + (b - a) * t;

const KILLER_DIFFICULTY_LABELS = ['', 'Beginner', 'Beginner+', 'Novice', 'Novice+', 'Competent', 'Competent+', 'Advanced', 'Advanced+', 'Expert', 'Expert+'];

function killerParams(d) {
  const t = Math.max(0, Math.min(1, (d - 1) / 9));
  const e = t * t; // "expert-only" features ramp late
  return {
    difficulty: d,
    // --- timing --------------------------------------------------------------
    tacticalHz: lerp(4, 10, t),           // chase / power decisions per second
    strategicEvery: lerp(1.2, 0.5, t),    // seconds between plan re-evaluations
    reaction: lerp(0.75, 0.16, t),        // s before a newly visible survivor is noticed
    // --- perception (what the bot *notices*, never more than the screen shows) --
    attention: lerp(0.55, 1.0, t),        // chance per tick to notice a visible survivor at the edge of view
    viewRange: lerp(15, 21, t),           // m, how far across the "screen" the bot pays attention
    scratchUse: lerp(0.15, 1.0, t),       // follows scratch marks / blood
    gruntUse: lerp(0.1, 1.0, t),          // follows grunts of pain
    noiseUse: lerp(0.35, 1.0, t),         // reacts to loud noise notifications
    memory: lerp(4, 14, t),               // s a lost sighting stays useful
    // --- chase -------------------------------------------------------------------
    predict: lerp(0.0, 1.0, t),           // leads targets, cuts loops
    lungeSkill: lerp(0.35, 1.0, t),       // lunge timing quality
    swingNoise: lerp(0.35, 0.03, t),      // chance to swing at a bad moment / whiff
    mindgame: lerp(0.0, 0.55, e),         // chance to fake/turn back at a loop
    palletIQ: lerp(0.1, 1.0, t),          // break vs. go around, avoid stuns
    stunAvoid: lerp(0.05, 0.85, t),       // keeps out of stun range when a pallet can drop
    abandon: lerp(0.0, 1.0, t),           // recognises unproductive chases
    chasePatience: lerp(70, 34, t),       // s before a chase is reconsidered (high = stubborn)
    // --- strategy ----------------------------------------------------------------
    patrolIQ: lerp(0.0, 1.0, t),          // gen choice: progress, clusters, likely objectives
    pressure: lerp(0.2, 1.0, t),          // kicks gens, rotates, defends 3-gens
    hookIQ: lerp(0.1, 1.0, t),            // hook choice, rescue reads, anti-camping balance
    tunnel: lerp(0.0, 0.45, e),           // willingness to go back for the unhooked survivor late
    slug: lerp(0.0, 0.5, e),              // leaves survivors downed to pressure others
    perkIQ: lerp(0.1, 1.0, t),            // plays around survivor perks it has seen
    powerIQ: lerp(0.25, 1.0, t),          // power used at the right time, aimed well
    aim: lerp(0.45, 0.97, t),             // projectile / dash aim quality
    temperature: lerp(0.55, 0.05, t),     // randomness of strategic picks
    mistakes: lerp(0.18, 0.01, t),        // odd decisions per strategic tick
    coordination: lerp(0.2, 1.0, t),      // 2v8 teamwork
  };
}

return { KILLER_DIFFICULTY_LABELS, killerParams };
};
__defs["js/ai/killer/perception.js"] = function(){
// Killer bot perception. The bot gets exactly what a human killer's screen and HUD give:
//  • survivors inside its view (≈ the camera area) with line of sight, after a reaction delay
//  • scratch marks, blood and grunts of pain near it
//  • loud-noise notifications, generator completions, hook / unhook / dying states (HUD)
//  • aura reveals from its own perks / power (m.reveals)
// It never reads a survivor's true position otherwise — beliefs decay and are predicted.
const { M, dist, gauss } = __req("js/core/util.js");

const SRC = { SIGHT: 'sight', AURA: 'aura', SCRATCH: 'scratch', BLOOD: 'blood', GRUNT: 'grunt', NOISE: 'noise', HUD: 'hud', TEAM: 'team', PREDICT: 'predicted' };

class KillerPerception {
  constructor(ai) {
    this.ai = ai; this.m = ai.m; this.k = ai.k;
    this.beliefs = new Map();   // survivor -> belief
    this.leads = [];            // anonymous clues { x, y, t, kind, w }
    this.gens = new Map();      // gen -> { progress, repairing, t, kicked }
    this.lastNoiseT = 0; this.lastScratchT = 0;
    this.events = [];           // recent notable events (unhook, gen done…) for the strategist
  }
  belief(s) {
    let b = this.beliefs.get(s);
    if (!b) { b = { s, x: s.x, y: s.y, vx: 0, vy: 0, t: -999, conf: 0, src: null, visible: false, noticeAt: Infinity, seenHealth: 'healthy', lastSeenT: -999, firstSeenT: -999, lastAction: null }; this.beliefs.set(s, b); }
    return b;
  }
  pos(b, lead = 0) {
    const age = Math.min(3, this.m.time - b.t), k = (b.visible ? lead : age + lead) * 0.85;
    return { x: b.x + b.vx * k, y: b.y + b.vy * k };
  }
  canSee(s, d) {
    const m = this.m, k = this.k, P = this.ai.P;
    // remote eyes (biopod control): the pod sees what is within its range and sight line
    const vp = k.power.viewPoint?.(m);
    if (vp && !s.hiddenIn && dist(vp.x, vp.y, s.x, s.y) < 20 * M && m.world.losClear(vp.x, vp.y, s.x, s.y)) return true;
    if (k.blindT > 0 && d > 3 * M) return false;
    if (d > P.viewRange * M) return false;
    if (s.hiddenIn) return false;
    if (s.moveMode === 'crouch' && m.map.isGrass(s.x, s.y) && d > 5 * M && !s.action) return false;
    if (m.nightfallFor?.(k) && d > 14 * M) return false;
    return m.world.losClear(k.x, k.y, s.x, s.y);
  }
  update(dt) {
    const m = this.m, k = this.k, P = this.ai.P, now = m.time;
    for (const s of m.survivors) {
      const b = this.belief(s);
      if (!s.alive) { b.conf = 0; b.visible = false; b.gone = true; continue; }
      // HUD states are always known (hooked / dying / carried icons)
      if (s.health === 'hooked' || s.health === 'carried') { b.x = s.x; b.y = s.y; b.vx = b.vy = 0; b.t = now; b.conf = 1; b.src = SRC.HUD; b.visible = false; b.seenHealth = s.health; continue; }
      const d = dist(k.x, k.y, s.x, s.y);
      const inView = this.canSee(s, d);
      if (inView) {
        if (!b.visible) {
          if (b.noticeAt === Infinity) {
            const far = d > 12 * M ? (1 - P.attention) * 1.2 : 0;
            // re-acquiring someone tracked a moment ago (around a corner, after a blink) is near-instant
            const tracking = now - b.lastSeenT < 1.5 && b.conf > 0.5 && dist(b.x, b.y, s.x, s.y) < 6 * M;
            b.noticeAt = now + (tracking ? P.reaction * 0.25 : P.reaction * (0.7 + Math.random() * 0.6) + far);
          }
          if (now >= b.noticeAt) { b.visible = true; if (now - b.lastSeenT > 6) b.firstSeenT = now; }
        }
        if (b.visible) {
          b.x = s.x; b.y = s.y; b.vx = s.vx; b.vy = s.vy; b.t = now; b.conf = 1; b.src = SRC.SIGHT; b.lastSeenT = now;
          b.seenHealth = s.health; b.lastAction = s.action?.type ?? (s.vault ? 'vault' : null); b.moveMode = s.moveMode;
          this.ai.team?.share(this.k, s, b);
        }
      } else { b.visible = false; b.noticeAt = Infinity; }
      if (s.health === 'dying') b.seenHealth = 'dying';
      // decay
      if (!b.visible) b.conf *= Math.exp(-dt / P.memory);
    }
    // Aura reveals produced by this killer's perks / power (and the 2v8 team's Revealed status)
    for (const r of m.reveals) {
      const t = r.target; if (!t || r.until < now) continue;
      if (t.kind === 'survivor') { const b = this.belief(t); if (!b.visible) { b.x = t.x; b.y = t.y; b.vx = t.vx; b.vy = t.vy; b.t = now; b.conf = Math.max(b.conf, 0.95); b.src = SRC.AURA; b.seenHealth = t.health; } }
      else if (t.kind === 'generator' || t.charges !== undefined) { const g = this.genInfo(t); g.repairing = true; g.t = now; g.progress = t.progress ?? g.progress; }
    }
    // Killer Instinct-style power reveals live in m.instinct (Legion, Unknown, Houndmaster…)
    for (const r of m.instinct || []) if (r.until > now && (!r.killer || r.killer === k)) { const b = this.belief(r.target); if (!b.visible) { b.x = r.target.x; b.y = r.target.y; b.t = now; b.conf = Math.max(b.conf, 0.9); b.src = SRC.AURA; } }
    // Scratch marks / blood / grunts — anonymous leads, attributed to the most plausible belief
    if (Math.random() < P.scratchUse) {
      for (const sc of m.scratches) {
        if (sc.t <= this.lastScratchT || now - sc.t > 2.5) continue;
        if (dist(sc.x, sc.y, k.x, k.y) > 16 * M) continue;
        this.addLead(sc.x, sc.y, sc.t, 'scratch', 1);
      }
      for (const bl of m.blood) if (now - bl.t < 2 && dist(bl.x, bl.y, k.x, k.y) < 10 * M && m.world.losClear(k.x, k.y, bl.x, bl.y)) this.addLead(bl.x, bl.y, bl.t, 'blood', 0.8);
    }
    this.lastScratchT = now;
    if (Math.random() < P.gruntUse) for (const g of m.grunts) if (now - g.t < 0.2 && dist(g.x, g.y, k.x, k.y) < 22 * M) this.addLead(g.x + gauss() * 1.5 * M, g.y + gauss() * 1.5 * M, now, 'grunt', 1.2);
    // Loud noises (fast vaults, pallets, failed skill checks, gens, gates)
    for (const n of m.loudNoises) {
      if (n.t <= this.lastNoiseT) continue;
      const d = dist(n.x, n.y, k.x, k.y);
      if (n.kind !== 'gen' && n.kind !== 'gate' && d > 48 * M) continue;
      if (Math.random() < P.noiseUse) this.addLead(n.x, n.y, n.t, 'noise:' + n.kind, n.kind === 'skillcheck' ? 2.2 : n.kind === 'gen' ? 0.6 : 1.5);
    }
    this.lastNoiseT = now;
    // Generators: progress is readable only when the gen is on screen
    for (const g of m.map.gens) {
      const gi = this.genInfo(g);
      if (g.done) { gi.done = true; continue; }
      const d = dist(g.x, g.y, k.x, k.y);
      if (d < P.viewRange * M && m.world.losClear(k.x, k.y, g.x, g.y)) { gi.progress = g.progress; gi.repairing = g.repairers.size > 0; gi.t = now; gi.regressing = g.regressing; }
      else if (d < 16 * M && g.repairers.size > 0) { gi.repairing = true; gi.t = now; } // generator noise carries a short way
    }
    // Attribute fresh leads to beliefs
    for (const L of this.leads) {
      if (L.used || now - L.t > 1.0) continue;
      let best = null, bd = 7 * M;
      for (const b of this.beliefs.values()) { if (b.visible || b.gone || b.conf < 0.08) continue; const p = this.pos(b); const dd = dist(p.x, p.y, L.x, L.y); if (dd < bd) { bd = dd; best = b; } }
      if (best) { const p0 = { x: best.x, y: best.y }; best.vx = (L.x - p0.x) / Math.max(0.3, L.t - best.t); best.vy = (L.y - p0.y) / Math.max(0.3, L.t - best.t); best.x = L.x; best.y = L.y; best.t = L.t; best.conf = Math.max(best.conf, 0.6); best.src = L.kind.startsWith('noise') ? SRC.NOISE : SRC.SCRATCH; L.used = true; }
    }
    this.leads = this.leads.filter((L) => now - L.t < 8);
  }
  addLead(x, y, t, kind, w) {
    for (const L of this.leads) if (L.kind === kind && dist(L.x, L.y, x, y) < 1.5 * M && Math.abs(L.t - t) < 0.6) { L.t = Math.max(L.t, t); return; }
    this.leads.push({ x, y, t, kind, w, used: false });
  }
  genInfo(g) { let gi = this.gens.get(g); if (!gi) { gi = { g, progress: 0, repairing: false, t: -999, done: false, regressing: false }; this.gens.set(g, gi); } return gi; }
  // Best uninvestigated clue (weighted by freshness, weight and distance)
  bestLead() {
    const m = this.m, k = this.k; let best = null, bs = 0;
    for (const L of this.leads) {
      if (L.investigated) continue;
      const age = m.time - L.t, d = dist(L.x, L.y, k.x, k.y) / M;
      const sc = L.w * Math.exp(-age / 4) / (1 + d / 25);
      if (sc > bs) { bs = sc; best = L; }
    }
    return best ? { ...best, score: bs, ref: best } : null;
  }
  // Visible survivors right now
  visible() { const out = []; for (const b of this.beliefs.values()) if (b.visible && b.s.standing) out.push(b); return out; }
  known(minConf = 0.25) { const out = []; for (const b of this.beliefs.values()) if (!b.gone && b.conf >= minConf && (b.s.standing || b.s.health === 'dying')) out.push(b); return out; }
}

return { SRC, KillerPerception };
};
__defs["js/ai/killer/steering.js"] = function(){
// Killer bot movement: A* over the shared nav grid (windows are vault links, dropped pallets
// are "break" links), path following with stuck recovery, and direct pursuit when the way is
// clear. Writes only into the killer's input (mx, my, aim, spaceP) like a human would.
const { M, dist } = __req("js/core/util.js");

class KillerSteering {
  constructor(ai) { this.ai = ai; this.m = ai.m; this.k = ai.k; this.path = null; this.idx = 0; this.goal = null; this.repathT = 0; this.stuckT = 0; this.last = { x: 0, y: 0 }; this.nudge = null; }
  linkCost() {
    const m = this.m, k = this.k, P = this.ai.P;
    const breakCost = 9 + 6 * P.palletIQ; // smarter killers value the time a break costs
    return (L) => {
      if (L.kind === 'window') return (L.ref.killerBlockedUntil || 0) > m.time || k.power.noVault?.() ? Infinity : this.ai.vaultCost ?? 7;
      if (L.kind === 'pallet') return L.ref.state === 'down' ? (this.ai.powerCtl?.passesPallets?.(this.ai) ? 1 : breakCost) : Infinity;
      return Infinity;
    };
  }
  // Move toward (x, y). Returns distance remaining (px).
  goTo(x, y, { direct = true, arrive = 0.6 * M, repath = 0.5 } = {}) {
    const m = this.m, k = this.k, inp = k.input;
    const d = dist(k.x, k.y, x, y);
    if (d < arrive) { inp.mx = inp.my = 0; return d; }
    // straight line when nothing is in the way (cheap and smooth)
    if (direct && d < 26 * M && m.world.sweepClear(k.x, k.y, x, y, k.r * 0.9, (s) => s.kind !== 'gate' || !s.ref.open)) {
      this.path = null; this.steer(x, y); this.checkStuck(); return d;
    }
    this.repathT -= 1 / 60;
    const far = !this.goal || dist(this.goal.x, this.goal.y, x, y) > 1.8 * M;
    if (!this.path || far || this.repathT <= 0) {
      if (m.pathBudget > 0 || !this.path) {
        m.pathBudget--;
        const r = m.map.nav.findPath(k.x, k.y, x, y, { linkCost: this.linkCost() });
        this.path = r ? m.map.nav.smooth(m.world, r.pts, k.r * 0.9) : [{ x: k.x, y: k.y }, { x, y }];
        this.idx = 1; this.goal = { x, y }; this.repathT = repath + Math.random() * repath * 0.5;
      }
    }
    const P = this.path; let p = P[Math.min(this.idx, P.length - 1)];
    // link handling: vault the window / break the pallet that the path crosses
    const next = P[this.idx];
    if (next && next.viaLink) {
      const L = next.viaLink, ref = L.ref;
      if (dist(k.x, k.y, ref.x, ref.y) < 2.0 * M) {
        const ctx = m.killerContext(k);
        if (ctx && (ctx.type === 'vault' || ctx.type === 'break') && ctx.target === ref) { inp.spaceP = true; this.idx++; inp.mx = inp.my = 0; return d; }
        if (L.kind === 'pallet' && ref.state !== 'down') this.idx++;
        // line up with the obstacle so the context action becomes available
        this.steer(ref.x, ref.y); return d;
      }
    }
    if (p && dist(k.x, k.y, p.x, p.y) < 0.7 * M && this.idx < P.length - 1) { this.idx++; p = P[this.idx]; }
    if (!p) p = { x, y };
    this.steer(p.x, p.y); this.checkStuck();
    return d;
  }
  steer(x, y) {
    const k = this.k, inp = k.input;
    let dx = x - k.x, dy = y - k.y; const l = Math.hypot(dx, dy) || 1; dx /= l; dy /= l;
    if (this.nudge && this.m.time < this.nudge.until) { dx = dx * 0.4 + this.nudge.x; dy = dy * 0.4 + this.nudge.y; const l2 = Math.hypot(dx, dy) || 1; dx /= l2; dy /= l2; }
    inp.mx = dx; inp.my = dy;
    if (!this.ai.aimLocked) inp.aim = Math.atan2(dy, dx);
  }
  checkStuck() {
    const k = this.k, m = this.m;
    if (k.action || k.stunT > 0) { this.stuckT = 0; return; }
    if (dist(k.x, k.y, this.last.x, this.last.y) < 0.03 * M) {
      this.stuckT += 1 / 60;
      if (this.stuckT > 0.7) { this.path = null; this.stuckT = 0; const a = Math.random() * Math.PI * 2; this.nudge = { x: Math.cos(a), y: Math.sin(a), until: m.time + 0.5 }; }
    } else this.stuckT = Math.max(0, this.stuckT - 1 / 120);
    this.last = { x: k.x, y: k.y };
  }
  stop() { const inp = this.k.input; inp.mx = inp.my = 0; }
  faceTo(x, y) { this.k.input.aim = Math.atan2(y - this.k.y, x - this.k.x); }
}

return { KillerSteering };
};
__defs["js/ai/killer/chase.js"] = function(){
// Killer bot chase controller. Runs every frame while the strategist has chosen CHASE.
// It decides where to run (prediction, loop cuts, mindgames), when to swing (lunge timing,
// Dead Hard / endurance reads), how to deal with pallets (respect, bait, break, go around)
// and hands control to the killer's power controller when the power is the better tool.
const { M, dist, angleDiff, clamp } = __req("js/core/util.js");

class ChaseController {
  constructor(ai) { this.ai = ai; this.m = ai.m; this.k = ai.k; this.reset(null); }
  reset(target) {
    this.target = target; this.startT = this.m.time; this.lastHitT = -99; this.mind = null; this.swingHold = 0; this.baitT = 0;
    this.hits = 0; this.lostT = 0; this.palletWaits = 0; this.loopT = 0; this.lastTile = null;
  }
  get elapsed() { return this.m.time - this.startT; }

  // Main chase step (every frame). Returns 'lost' when the target can no longer be followed.
  step(tick) {
    const ai = this.ai, m = this.m, k = this.k, P = ai.P, inp = k.input;
    const s = this.target; if (!s || !s.alive) return 'lost';
    if (s.health === 'dying') return 'downed';
    if (!s.standing) return 'lost';
    const b = ai.per.belief(s);
    if (!b.visible) this.lostT += 1 / 60; else this.lostT = 0;
    if (b.conf < 0.18 || this.lostT > 4 + 4 * P.predict) return 'lost';
    const d = dist(k.x, k.y, b.x, b.y);
    // --- power first: the power controller may want to take over this frame
    const pc = ai.powerCtl;
    if (pc?.chase && pc.chase(ai, b, d, tick)) return 'ok';
    // --- where to run
    // Lead the target only where the lead point is actually reachable from them (no cutting
    // through walls); around a loop the killer follows the real route instead.
    const lead = b.visible ? (d < 5 * M ? 0.15 : clamp(d / (4.6 * M), 0, 1.2) * P.predict) : 0.4 + 1.0 * P.predict;
    let aim = ai.per.pos(b, lead);
    if (!m.world.sweepClear(b.x, b.y, aim.x, aim.y, s.r)) aim = b.visible ? { x: b.x, y: b.y } : ai.per.pos(b, 0.3);
    // stay inside the map and on walkable ground
    aim = { x: clamp(aim.x, 2 * M, m.map.w - 2 * M), y: clamp(aim.y, 2 * M, m.map.h - 2 * M) };
    const tile = m.map.tileAt?.(s.x, s.y);
    if (tile && tile === this.lastTile) this.loopT += 1 / 60; else { this.loopT = 0; this.lastTile = tile; }
    // Mindgame at a loop: decided once each time line of sight breaks around a structure
    const losBreak = this.wasVisible && !b.visible; this.wasVisible = b.visible;
    if (losBreak && tile && d < 10 * M && !this.mind && Math.random() < P.mindgame) {
      const back = { x: tile.x + (tile.x - k.x), y: tile.y + (tile.y - k.y) };
      this.mind = { x: back.x, y: back.y, until: m.time + 1.1 + Math.random() * 0.6 };
      ai.note('Mindgame at the loop');
    }
    if (this.mind && m.time < this.mind.until && !b.visible) aim = this.mind; else this.mind = null;
    // --- pallet respect / bait: don't run into a stun when the target is standing at an up pallet
    const pal = this.threatPallet(s);
    if (pal && pal !== this.baited && d < 4.5 * M && dist(s.x, s.y, pal.x, pal.y) < 1.8 * M) {
      if (this.baitRoll?.p !== pal) this.baitRoll = { p: pal, yes: Math.random() < P.stunAvoid * 0.7 };
      const side = pal.sideOf(k), sSide = pal.sideOf(s);
      if (this.baitRoll.yes && side !== sSide) {
        // hold just outside stun range for a moment to bait the drop
        this.baitT += 1 / 60;
        if (this.baitT > 0.5 + 0.5 * P.palletIQ) { this.baited = pal; this.baitT = 0; }
        else {
          const away = Math.atan2(k.y - pal.y, k.x - pal.x);
          const hx = pal.x + Math.cos(away) * 3.0 * M, hy = pal.y + Math.sin(away) * 3.0 * M;
          ai.steer.goTo(hx, hy, { arrive: 0.5 * M }); ai.steer.faceTo(s.x, s.y);
          return 'ok';
        }
      }
    } else this.baitT = Math.max(0, this.baitT - 1 / 30);
    ai.steer.goTo(aim.x, aim.y, { arrive: 0.3 * M, repath: 0.25 });
    // --- swing
    if (b.visible) this.trySwing(s, b, d, tick); else inp.lmb = false;
    return 'ok';
  }

  // An up pallet the target can throw on us in the next moment
  threatPallet(s) {
    let best = null, bd = 2.6 * M;
    for (const p of this.m.map.pallets) { if (p.state !== 'up') continue; const d = dist(p.x, p.y, s.x, s.y); if (d < bd) { bd = d; best = p; } }
    return best;
  }

  trySwing(s, b, d, tick) {
    const ai = this.ai, m = this.m, k = this.k, P = ai.P, inp = k.input;
    const at = k.attack;
    if (at.state === 'lunge') {
      // keep the blade on the target; hold longer for distant lunges
      inp.aim = Math.atan2(s.y - k.y, s.x - k.x); this.swingHold -= 1 / 60; inp.lmb = this.swingHold > 0; return;
    }
    if (at.state !== 'ready' || !k.power.canAttack(m) || k.carrying || k.status.has('cannotAttack')) { inp.lmb = false; return; }
    if (!tick) return;
    // closing speed: survivors running straight away eat lunge distance
    const rx = s.x - k.x, ry = s.y - k.y; const rl = Math.hypot(rx, ry) || 1;
    const away = (s.vx * rx + s.vy * ry) / rl; // + = moving away
    const reach = k.r + 1.0 * M + s.r;
    const lungeGain = (4.6 * 1.55 - 4.0) * M * 0.45; // ~ what a full lunge gains on a runner
    const swingAt = reach + (away > 1 * M ? lungeGain * P.lungeSkill : 1.6 * M);
    const clear = m.world.losClear(k.x, k.y, s.x, s.y);
    if (!clear || d > swingAt) {
      // occasional bad swing at low skill (whiffs into pallets / out of range)
      if (clear && d < swingAt + 1.5 * M && Math.random() < P.swingNoise * 0.3) this.swing(s, 0.12);
      return;
    }
    // Perk reads: Dead Hard (seen before or likely) — wait out the dodge window when it's up
    if (s.health === 'injured' && !s.status.has('exhausted') && ai.knowsPerk(s, 'dead_hard') && Math.random() < P.perkIQ) {
      if (s.dodgeT && s.dodgeT > m.time) return; // let the i-frames run out
      if (!this.dhBait) { this.dhBait = m.time; ai.note(`Baiting ${s.name}'s Dead Hard`); }
      if (m.time - this.dhBait < 0.35 && d > reach * 0.9) return;
    }
    this.dhBait = null;
    const hold = d > reach + 0.6 * M ? 0.45 : 0.18;
    this.swing(s, hold);
  }
  swing(s, hold) {
    const k = this.k, inp = k.input;
    inp.aim = Math.atan2(s.y - k.y, s.x - k.x); inp.lmbP = true; inp.lmb = true; this.swingHold = hold;
  }
}

return { ChaseController };
};
__defs["js/ai/killer/powers.js"] = function(){
// Per-killer AI power controllers. Each one plays its killer's real power through the same
// inputs a human uses (hold/press RMB, E, LMB, aim, aim distance). Hooks:
//   chase(ai, b, d, tick)  → use the power on the chase target; return true to own this frame
//   patrol(ai)             → between chases (set up traps, pods, guards, scouting); true = owns movement
//   travel(ai, x, y)       → cover distance with the power (cloak, teleports, sprints)
//   passesPallets(ai)      → movement that ignores dropped pallets (pathing cost)
// Quality scales with ai.P.powerIQ (timing/choice) and ai.P.aim (aiming).
const { M, dist, angleDiff, clamp, normAngle } = __req("js/core/util.js");

const CTL = {};
function powerControllerFor(id) { return CTL[id] || null; }
function registerPowerController(id, ctl) { CTL[id] = ctl; }

// ---- shared helpers -----------------------------------------------------------
const H = {
  aimAt(ai, p) { const k = ai.k; k.input.aim = Math.atan2(p.y - k.y, p.x - k.x); k.input.aimDist = dist(k.x, k.y, p.x, p.y); },
  // lead a believed survivor by t seconds; aim error grows as skill drops
  lead(ai, b, t) {
    const err = (1 - ai.P.aim) * 1.6 * M;
    const p = ai.per.pos(b, t * (0.4 + 0.6 * ai.P.predict));
    if (!ai.mem._aimErr || ai.m.time - ai.mem._aimErrT > 0.8) { ai.mem._aimErr = { x: (Math.random() - 0.5) * 2 * err, y: (Math.random() - 0.5) * 2 * err }; ai.mem._aimErrT = ai.m.time; }
    return { x: p.x + ai.mem._aimErr.x, y: p.y + ai.mem._aimErr.y };
  },
  clear(ai, s) { return ai.m.world.losClear(ai.k.x, ai.k.y, s.x, s.y); },
  lane(ai, x, y) { return ai.m.world.sweepClear(ai.k.x, ai.k.y, x, y, ai.k.r * 0.8); },
  // the power is worth using this tick (skill gated)
  roll(ai, p = 1) { return ai.tick && Math.random() < ai.P.powerIQ * p; },
  nearestGen(ai, pred = () => true) { let best = null, bd = Infinity; for (const g of ai.m.map.gens) { if (g.done || !pred(g)) continue; const d = dist(g.x, g.y, ai.k.x, ai.k.y); if (d < bd) { bd = d; best = g; } } return best; },
  hotGen(ai) { // the gen the bot believes is most valuable to cover
    let best = null, bs = -1;
    for (const [g, gi] of ai.per.gens) { if (g.done) continue; const sc = (gi.repairing && ai.m.time - gi.t < 15 ? 2 : 0) + gi.progress * 2 - dist(g.x, g.y, ai.k.x, ai.k.y) / (60 * M); if (sc > bs) { bs = sc; best = g; } }
    return best;
  },
};

// ---- Trapper: traps at loops, windows and generators -----------------------------
CTL.traps = {
  spots(ai) {
    if (ai.mem.trapSpots) return ai.mem.trapSpots;
    const m = ai.m, out = [];
    for (const w of m.map.windows) out.push({ x: w.x + w.nx * 1.3 * M, y: w.y + w.ny * 1.3 * M, w: 1.3 }, { x: w.x - w.nx * 1.3 * M, y: w.y - w.ny * 1.3 * M, w: 1.3 });
    for (const p of m.map.pallets) out.push({ x: p.x + p.nx * 1.2 * M, y: p.y + p.ny * 1.2 * M, w: 1.1 });
    for (const g of m.map.gens) out.push({ x: g.x + 2 * M, y: g.y, w: 0.8 }, { x: g.x - 2 * M, y: g.y, w: 0.8 });
    for (const s of out) s.grass = m.map.isGrass(s.x, s.y);
    return (ai.mem.trapSpots = out);
  },
  chase(ai, b, d) {
    const k = ai.k, pw = k.power;
    if (pw.setting) { k.input.rmb = d > 8 * M; return true; }
    return false;
  },
  patrol(ai) {
    const m = ai.m, k = ai.k, pw = k.power, P = ai.P, inp = k.input;
    if (pw.setting) { inp.rmb = true; ai.state = 'USING_POWER'; return true; }
    if (pw.picking) return true;
    // pick up spare traps lying around
    if (pw.carried < pw.maxCarry) {
      const t = m.traps.filter((q) => q.state === 'unset' && dist(q.x, q.y, k.x, k.y) < 14 * M).sort((a, b) => dist(a.x, a.y, k.x, k.y) - dist(b.x, b.y, k.x, k.y))[0];
      if (t && Math.random() < 0.3 + 0.7 * P.powerIQ) { if (dist(t.x, t.y, k.x, k.y) < 1.6 * M) { inp.eP = true; return true; } ai.steer.goTo(t.x, t.y, { arrive: 1.2 * M }); ai.state = 'USING_POWER'; return true; }
    }
    if (pw.carried <= 0) return false;
    // choose a spot near the generator we are heading to: windows/pallets of nearby loops, grass preferred
    const g = ai.goal?.target; if (!g || g.x === undefined) return false;
    if (!ai.mem.trapPlan || ai.m.time - ai.mem.trapPlan.t > 20 || ai.mem.trapPlan.done) {
      const cands = CTL.traps.spots(ai).filter((s) => dist(s.x, s.y, g.x, g.y) < 18 * M && !m.traps.some((t) => t.state === 'set' && dist(t.x, t.y, s.x, s.y) < 6 * M));
      const pick = cands.sort((a, b) => (b.w + (b.grass ? 0.6 * P.powerIQ : 0)) - (a.w + (a.grass ? 0.6 * P.powerIQ : 0)) + (Math.random() - 0.5) * (1 - P.powerIQ))[0];
      // decide once per plan whether this trap is worth the detour (skill gated)
      ai.mem.trapPlan = pick && Math.random() < 0.25 + 0.75 * P.powerIQ ? { ...pick, t: m.time } : { done: true, t: m.time };
    }
    const tp = ai.mem.trapPlan; if (tp.done) return false;
    if (m.time - tp.t > 12) { tp.done = true; return false; } // couldn't get there: drop the plan
    const dd = dist(k.x, k.y, tp.x, tp.y);
    if (dd < 2.0 * M || (dd < 3 * M && ai.steer.stuckT > 0.3)) { H.aimAt(ai, { x: tp.x + (tp.x - k.x), y: tp.y + (tp.y - k.y) }); inp.rmbP = true; inp.rmb = true; tp.done = true; ai.note('Setting a bear trap'); return true; }
    if (dd < 22 * M) { ai.steer.goTo(tp.x, tp.y, { arrive: 1.2 * M }); ai.state = 'USING_POWER'; return true; }
    return false;
  },
};

// ---- Hillbilly: chainsaw sprints down clear lanes, never into clutter ----------
CTL.chainsaw = {
  chase(ai, b, d) {
    const k = ai.k, pw = k.power, inp = k.input;
    if (pw.state === 'sprint') { const p = H.lead(ai, b, 0.4); H.aimAt(ai, p); inp.rmb = true; return true; }
    if (pw.state === 'revving') { const p = H.lead(ai, b, 0.6); H.aimAt(ai, p); inp.rmb = H.lane(ai, p.x, p.y) || pw.charge < 0.4; return true; }
    if ((pw.overheatT ?? 0) > 0 || pw.state !== 'idle') return false;
    if (d > 6 * M && d < 22 * M && b.visible && H.lane(ai, b.x, b.y) && H.roll(ai, 0.9)) { inp.rmb = true; H.aimAt(ai, b); return true; }
    return false;
  },
  travel(ai, x, y) {
    const k = ai.k, pw = k.power, inp = k.input, st = ai.steer;
    const p = st.path?.[st.idx]; if (!p || (pw.overheatT ?? 0) > 0 || (pw.heat ?? 0) > 0.6) return;
    if (dist(p.x, p.y, k.x, k.y) > 14 * M && H.lane(ai, p.x, p.y) && Math.random() < ai.P.powerIQ) { inp.rmb = true; H.aimAt(ai, p); }
  },
};

// ---- Wraith: cloak to travel and ambush, uncloak behind the target ------------
CTL.cloak = {
  chase(ai, b, d) {
    const pw = ai.k.power, inp = ai.k.input;
    if (pw.state === 'cloaked' && d < (5 + 4 * ai.P.powerIQ) * M && H.roll(ai, 1.2)) { inp.rmbP = true; ai.note('Uncloaking for the hit'); }
    if (pw.state === 'visible' && d > 18 * M && !b.visible && H.roll(ai, 0.3)) inp.rmbP = true;
    return false;
  },
  patrol(ai) { const pw = ai.k.power; if (pw.state === 'visible' && H.roll(ai, 0.15)) ai.k.input.rmbP = true; return false; },
  travel(ai) { const pw = ai.k.power; if (pw.state === 'visible' && H.roll(ai, 0.3)) ai.k.input.rmbP = true; },
};

// ---- Ghost Face: shroud, stalk from cover, mark, then one-hit -----------------
CTL.shroud = {
  chase(ai, b, d) {
    const pw = ai.k.power, inp = ai.k.input, s = b.s;
    if (pw.state === 'visible' && H.roll(ai, 0.6)) inp.rmbP = true;
    if (pw.shrouded && b.visible && d > 7 * M && d < 24 * M && !s.status.has('marked') && !s.status.has('exposed')) {
      // stalk while the target isn't looking at us (they'd reveal us)
      const looking = angleDiff(s.facing, Math.atan2(ai.k.y - s.y, ai.k.x - s.x)) < 0.7;
      if (!looking || Math.random() > ai.P.powerIQ) { inp.rmb = true; H.aimAt(ai, b); ai.state = 'USING_POWER'; ai.steer.stop(); return true; }
    }
    return false;
  },
  patrol(ai) { const pw = ai.k.power; if (pw.state === 'visible' && H.roll(ai, 0.5)) ai.k.input.rmbP = true; return false; },
};

// ---- Clown: gas thrown ahead of the target to cut loops; antidote to catch up -----
CTL.tonic = {
  chase(ai, b, d) {
    const pw = ai.k.power, inp = ai.k.input;
    if (pw.state === 'aiming') { const p = H.lead(ai, b, 0.7); H.aimAt(ai, p); ai.mem.aimT = (ai.mem.aimT || 0) + 1 / 60; inp.rmb = ai.mem.aimT < 0.3; return false; }
    ai.mem.aimT = 0;
    if (pw.type === 'antidote' && H.roll(ai, 0.5)) inp.eP = true; // default to tonic in chases
    if (pw.bottles > 0 && d > 3.5 * M && d < 14 * M && H.roll(ai, 0.35)) inp.rmb = true;
    return false;
  },
};

// ---- Executioner: punishment wave through walls, torment trails around loops ----
CTL.judgement = {
  chase(ai, b, d) {
    const pw = ai.k.power, inp = ai.k.input;
    if (pw.state === 'windup') { H.aimAt(ai, H.lead(ai, b, 0.9)); return true; }
    if (pw.state === 'normal' && d < 16 * M && d > 4 * M && H.roll(ai, 0.7)) inp.rmbP = true;
    else if (pw.state === 'punish' && d < 2.8 * M) inp.rmbP = true; // M1 range: drop the power and swing
    else if (pw.state === 'punish' && d < 15 * M && pw.cd('wave') <= 0 && H.roll(ai, 0.4)) { H.aimAt(ai, H.lead(ai, b, 0.9)); inp.lmbP = true; return true; }
    return false;
  },
  patrol(ai) { const pw = ai.k.power; if (pw.state === 'punish') ai.k.input.rmbP = true; return false; },
};

// ---- Huntress: hatchets on clean sightlines, reload at lockers ----------------
CTL.hatchets = {
  chase(ai, b, d) {
    const k = ai.k, pw = k.power, inp = k.input;
    if (pw.state === 'winding') {
      const want = clamp(d / (24 * M), 0.45, 1) * pw.maxWind; const p = H.lead(ai, b, d / (34 * M));
      H.aimAt(ai, p); ai.mem.windT = (ai.mem.windT || 0) + 1 / 60; inp.rmb = ai.mem.windT < want && H.clear(ai, b.s); if (!inp.rmb) ai.mem.windT = 0; return true;
    }
    ai.mem.windT = 0;
    if (pw.count > 0 && d > 4 * M && d < 26 * M && b.visible && H.clear(ai, b.s) && H.roll(ai, 0.3)) { inp.rmb = true; H.aimAt(ai, b); return true; }
    return false;
  },
  patrol(ai) {
    const m = ai.m, k = ai.k, pw = k.power; if (pw.count > 1) return false;
    const l = m.devices.filter((d) => d.kind === 'locker').sort((a, b) => dist(a.x, a.y, k.x, k.y) - dist(b.x, b.y, k.x, k.y))[0];
    if (!l) return false;
    if (dist(l.x, l.y, k.x, k.y) < 1.7 * M) { k.input.eP = true; ai.state = 'USING_POWER'; return true; }
    ai.steer.goTo(l.x, l.y, { arrive: 1.2 * M }); ai.state = 'REPOSITIONING'; return true;
  },
};

// ---- Legion: Frenzy into groups, chain between survivors, vault through loops ------
CTL.frenzy = {
  chase(ai, b, d) {
    const m = ai.m, k = ai.k, pw = k.power, inp = k.input, s = b.s;
    if (pw.state === 'ready' && d < 14 * M && H.roll(ai, 0.8)) inp.rmbP = true;
    if (pw.state !== 'frenzy') return false;
    // chain: never re-slash the same survivor; go for the nearest unslashed one
    let tgt = s;
    if (s === pw.lastHit || s.status.has('deepWoundFrenzy')) {
      const o = m.survivors.filter((q) => q.standing && q !== pw.lastHit && !q.status.has('deepWoundFrenzy') && dist(q.x, q.y, k.x, k.y) < 24 * M).sort((a, bb) => dist(a.x, a.y, k.x, k.y) - dist(bb.x, bb.y, k.x, k.y))[0];
      if (o) tgt = o; else return false;
    }
    const td = dist(tgt.x, tgt.y, k.x, k.y);
    ai.steer.goTo(tgt.x, tgt.y, { arrive: 0.3 * M });
    if (td < 2.3 * M && pw.slashT <= 0) { H.aimAt(ai, tgt); inp.lmbP = true; }
    const c = m.killerContext(k); if (c?.type === 'vault' && td < 8 * M) inp.spaceP = true;
    ai.state = 'USING_POWER';
    return true;
  },
};

// ---- Hag: traps at hooks/gens/loops, teleport to triggered phantoms ------------------
CTL.phantasm = {
  chase(ai, b, d) {
    const pw = ai.k.power, inp = ai.k.input;
    const t = pw.teleportTarget(ai.m); if (t && dist(t.x, t.y, b.x, b.y) < 6 * M && H.roll(ai, 1.2)) inp.eP = true;
    // trap loops mid-chase when the target is circling a structure
    if (pw.state === 'drawing') { inp.rmb = true; return true; }
    if (d > 10 * M && !b.visible && H.roll(ai, 0.15) && pw.traps.length < pw.maxTraps) { inp.rmb = true; return true; }
    return false;
  },
  patrol(ai) {
    const m = ai.m, k = ai.k, pw = k.power, inp = k.input;
    if (pw.teleportTarget(m) && H.roll(ai, 1.5)) { inp.eP = true; ai.note('Teleporting to a triggered Phantasm'); return true; }
    if (pw.state === 'drawing') { inp.rmb = true; ai.steer.stop(); ai.state = 'USING_POWER'; return true; }
    if (pw.traps.length >= pw.maxTraps) return false;
    // trap the approach to the goal generator / a hook, spaced out
    const g = ai.goal?.target; if (!g || g.x === undefined) return false;
    const d = dist(k.x, k.y, g.x, g.y);
    if (d < 7 * M && d > 2 * M && !pw.traps.some((t) => dist(t.x, t.y, k.x, k.y) < 6 * M) && H.roll(ai, 0.5)) { inp.rmb = true; ai.steer.stop(); ai.state = 'USING_POWER'; return true; }
    return false;
  },
};

// ---- Deathslinger: aim from mid range, fire on clean lines, reel and strike -------
CTL.redeemer = {
  chase(ai, b, d) {
    const k = ai.k, pw = k.power, inp = k.input, s = b.s;
    if (pw.state === 'reel') { if (dist(s.x, s.y, k.x, k.y) < 2.6 * M) { inp.lmbP = true; inp.lmb = true; } H.aimAt(ai, pw.speared || s); ai.steer.goTo(k.x - Math.cos(k.facing) * 2 * M, k.y - Math.sin(k.facing) * 2 * M); return true; }
    if (pw.state === 'aiming') {
      const p = H.lead(ai, b, d / (55 * M)); H.aimAt(ai, p); inp.rmb = true; ai.mem.aimT = (ai.mem.aimT || 0) + 1 / 60;
      // fire when the line is clean and the aim has settled (experts wait for the commit)
      if (ai.mem.aimT > 0.6 - 0.25 * ai.P.powerIQ && H.clear(ai, s) && d < pw.range * 0.95) { inp.lmbP = true; ai.mem.aimT = 0; }
      return true;
    }
    ai.mem.aimT = 0;
    if (pw.loaded && pw.state === 'idle' && d > 3 * M && d < pw.range * 0.9 && b.visible && H.clear(ai, s) && H.roll(ai, 0.6)) { inp.rmb = true; H.aimAt(ai, b); return true; }
    return false;
  },
};

// ---- Dredge: locker hops toward activity, Remnant returns, Nightfall pushes ----------
CTL.nightfall = {
  travel(ai, x, y) {
    const k = ai.k, pw = k.power, inp = k.input;
    if (pw.state === 'idle' && pw.tokens > 0 && pw.cd('tp') <= 0 && H.roll(ai, 0.35)) inp.rmbP = true;
    if (pw.state === 'gloaming') CTL.nightfall.pickLocker(ai, x, y);
  },
  pickLocker(ai, x, y) {
    const k = ai.k, pw = k.power, inp = k.input;
    const l = pw.lockers.filter((q) => !q.data.locked).sort((a, b) => dist(a.x, a.y, x, y) - dist(b.x, b.y, x, y))[0];
    if (!l || dist(l.x, l.y, x, y) > dist(k.x, k.y, x, y) * 0.7) { inp.eP = true; return; }
    k.input.aim = Math.atan2(l.y - k.y, l.x - k.x); inp.aimDist = dist(l.x, l.y, k.x, k.y) / 2.2;
    if (pw.target === l) inp.rmbP = true;
  },
  chase(ai, b, d) {
    const pw = ai.k.power, inp = ai.k.input;
    if (pw.state === 'gloaming') { if (d < 12 * M) inp.eP = true; else CTL.nightfall.pickLocker(ai, b.x, b.y); return true; }
    if (pw.state !== 'idle') return true;
    if (d > 28 * M && pw.tokens > 0 && pw.cd('tp') <= 0 && H.roll(ai, 0.4)) inp.rmbP = true;
    return false;
  },
  patrol(ai) {
    const pw = ai.k.power;
    if (pw.state === 'gloaming') { const g = H.hotGen(ai) || ai.goal?.target; if (g) CTL.nightfall.pickLocker(ai, g.x, g.y); if (pw.gloamT > 3) ai.k.input.eP = true; return true; }
    return pw.state !== 'idle';
  },
};

// ---- Knight: guards across survivors' escape routes and onto generators -------------
CTL.guardia = {
  chase(ai, b, d) {
    const k = ai.k, pw = k.power, inp = k.input;
    if (pw.state === 'drawing') {
      // draw the patrol line toward / past the target, then summon
      ai.mem.knightDraw = (ai.mem.knightDraw || 0) + 1 / 60; H.aimAt(ai, b); inp.mx = Math.cos(k.facing); inp.my = Math.sin(k.facing);
      if (ai.mem.knightDraw > 0.9 + 0.6 * ai.P.powerIQ || pw.pathLen?.() > d) inp.rmbP = true;
      return true;
    }
    if (pw.state === 'idle' && !pw.guard && pw.cd('power') <= 0 && d > 6 * M && d < 20 * M && H.roll(ai, 0.5)) { inp.rmbP = true; H.aimAt(ai, b); ai.mem.knightDraw = 0; return true; }
    return false;
  },
  patrol(ai) {
    const k = ai.k, pw = k.power, inp = k.input;
    if (pw.state === 'drawing') { ai.mem.knightDraw = (ai.mem.knightDraw || 0) + 1 / 60; if (ai.mem.knightDraw > 1.6) inp.rmbP = true; return false; }
    // cycle to the Jailer for generator patrols when idle
    if (pw.state === 'idle' && !pw.guard && pw.cd('power') <= 0) {
      const g = ai.goal?.target; if (g && g.x !== undefined && dist(g.x, g.y, k.x, k.y) < 14 * M && H.roll(ai, 0.4)) {
        if (pw.type?.id !== 'jailer' && Math.random() < ai.P.powerIQ) inp.eP = true;
        else { H.aimAt(ai, g); inp.rmbP = true; ai.mem.knightDraw = 0; }
      }
    }
    return false;
  },
};

// ---- Cenobite: steer chains around cover, manage the box ----------------------
CTL.lament = {
  chase(ai, b, d) {
    const pw = ai.k.power, inp = ai.k.input;
    if (pw.chain) { H.aimAt(ai, H.lead(ai, b, 0.2)); inp.rmb = true; return true; }
    if (pw.state === 'idle' && pw.cd('chain') <= 0 && d > 4 * M && d < 20 * M && b.visible && H.roll(ai, 0.55)) { inp.rmb = true; H.aimAt(ai, b); return true; }
    return false;
  },
  patrol(ai) {
    const m = ai.m, k = ai.k, pw = k.power, inp = k.input;
    if (m.survivors.some((s) => s.action?.type === 'solveBox') && pw.state === 'idle' && H.roll(ai, 1.5)) { inp.eP = true; ai.note('Summoning to the box solver'); return true; }
    if (pw.box && dist(pw.box.x, pw.box.y, k.x, k.y) < 1.7 * M && ai.P.powerIQ > 0.5) { inp.spaceP = true; return true; }
    return false;
  },
};

// ---- Mastermind: bounds on open lanes, chain the second bound -----------------------
CTL.virulent = {
  chase(ai, b, d) {
    const pw = ai.k.power, inp = ai.k.input;
    if (pw.state === 'charging') { H.aimAt(ai, H.lead(ai, b, 0.45)); inp.rmb = pw.charge < 1.0; return true; }
    if (pw.state === 'between') { if (d < 13 * M && b.visible && H.lane(ai, b.x, b.y)) { H.aimAt(ai, H.lead(ai, b, 0.5)); inp.rmbP = true; } return true; }
    if (pw.state === 'bound' || pw.state === 'recover') return true;
    if (pw.state === 'idle' && pw.tokens > 0 && d > 3 * M && d < 11 * M && b.visible && H.lane(ai, b.x, b.y) && H.roll(ai, 0.7)) { inp.rmb = true; H.aimAt(ai, b); return true; }
    return false;
  },
};

// ---- Houndmaster: Chase Command down clean lines, Search Command toward activity ----
CTL.hound = {
  chase(ai, b, d) {
    const pw = ai.k.power, inp = ai.k.input, dog = pw.dog;
    if (pw.state === 'plotting') { H.aimAt(ai, H.lead(ai, b, 0.8)); inp.rmb = pw.plot < 0.9; return true; }
    if (dog?.state === 'follow' && pw.cd('dog') <= 0 && d > 4 * M && d < 18 * M && b.visible && H.clear(ai, b.s) && H.roll(ai, 0.5)) { inp.rmb = true; H.aimAt(ai, b); return true; }
    return false;
  },
  patrol(ai) {
    const pw = ai.k.power, inp = ai.k.input, k = ai.k;
    if (pw.dog?.state === 'follow' && pw.cd('dog') <= 0 && H.roll(ai, 0.12)) {
      const g = H.hotGen(ai) || ai.goal?.target;
      if (g && g.x !== undefined && dist(g.x, g.y, k.x, k.y) < 40 * M) { H.aimAt(ai, g); inp.eP = true; }
    }
    return false;
  },
};

// ---- Lich: pick the spell for the moment ---------------------------------------
CTL.vecna = {
  chase(ai, b, d) {
    const m = ai.m, k = ai.k, pw = k.power, inp = k.input;
    if (pw.state !== 'idle') return false;
    const pick = (i) => { inp.slot = i + 1; return pw.sel === i; };
    if (pw.ready('fotd') && d < 16 * M && d > 4 * M && H.roll(ai, 0.5)) { if (pick(0)) { H.aimAt(ai, H.lead(ai, b, 0.6)); inp.rmbP = true; } return false; }
    const pal = m.map.pallets.find((q) => q.state === 'down' && dist(q.x, q.y, k.x, k.y) < 7 * M && q.sideOf(k) !== q.sideOf(b.s));
    if (pw.ready('magehand') && pal && H.roll(ai, 0.8)) { if (pick(2)) { H.aimAt(ai, pal); inp.rmbP = true; } return false; }
    if (pw.ready('fly') && d > 10 * M && d < 26 * M && H.roll(ai, 0.3)) { if (pick(1)) inp.rmbP = true; }
    return false;
  },
  patrol(ai) { const pw = ai.k.power, inp = ai.k.input; if (pw.state === 'idle' && pw.ready('sphere') && H.roll(ai, 0.05)) { inp.slot = 4; if (pw.sel === 3) inp.rmbP = true; } return false; },
  travel(ai) { const pw = ai.k.power, inp = ai.k.input; if (pw.state === 'idle' && pw.ready('fly') && H.roll(ai, 0.15)) { inp.slot = 2; if (pw.sel === 1) inp.rmbP = true; } },
};

// Singularity and Cannibal controllers live with their reworked powers (see below).

// ---- Singularity: biopods on walls overlooking objectives, pod scouting, tag + teleport ----
const podActive = (p, m) => !p.dead && (p.data.disabledUntil || 0) <= m.time;
CTL.quantum = {
  // a wall spot (raycast) in 22 m whose surface overlooks a generator / loop
  podShot(ai, focus) {
    const m = ai.m, k = ai.k, pw = k.power; let best = null, bs = -Infinity;
    for (let i = 0; i < 24; i++) {
      const a = i / 24 * Math.PI * 2, ex = k.x + Math.cos(a) * 21 * M, ey = k.y + Math.sin(a) * 21 * M;
      const h = m.world.firstHit(k.x, k.y, ex, ey, 0.15 * M, (q) => q.blocksLOS); if (!h || h.t < 0.12) continue;
      const x = k.x + (ex - k.x) * h.t - Math.cos(a) * 0.35 * M, y = k.y + (ey - k.y) * h.t - Math.sin(a) * 0.35 * M;
      if (m.map.gens.some((g) => dist(g.x, g.y, x, y) < 2.5 * M) || m.map.windows.some((w) => dist(w.x, w.y, x, y) < 2.5 * M) || pw.pods.some((p) => dist(p.x, p.y, x, y) < 7 * M)) continue;
      let sc = -dist(x, y, focus.x, focus.y) / M;
      sc += m.map.gens.filter((g) => !g.done && dist(g.x, g.y, x, y) < 16 * M && m.world.losClear(x, y, g.x, g.y)).length * 6 * ai.P.powerIQ;
      if (sc > bs) { bs = sc; best = { a, x, y }; }
    }
    return best;
  },
  shoot(ai, a) { const k = ai.k; k.input.aim = a; k.input.rmb = true; ai.mem.podShot = { a, t: ai.m.time }; },
  always(ai, g) {
    const m = ai.m, k = ai.k, pw = k.power, inp = k.input, P = ai.P;
    if (pw.state === 'teleporting') { ai.state = 'USING_POWER'; ai.steer.stop(); return true; }
    if (pw.state === 'aiming') { // finish the pod shot (hold ≥0.15 s on the chosen angle, then release)
      const sh = ai.mem.podShot; if (sh) inp.aim = sh.a; inp.rmb = pw.charge < 0.22; ai.steer.stop(); ai.state = 'USING_POWER'; return true;
    }
    if (pw.state !== 'control') { ai.mem.ctrlT = 0; delete inp.aimX; delete inp.aimY; return false; }
    ai.state = 'USING_POWER'; ai.steer.stop();
    ai.mem.ctrlT = (ai.mem.ctrlT || 0) + 1 / 60;
    if (pw.ctrlT < 1 / 30) ai.mem.ctrlTotal = 0; ai.mem.ctrlTotal = (ai.mem.ctrlTotal || 0) + 1 / 60;
    const c = pw.ctrl;
    // anything urgent pulls us out of the pod
    const close = ai.per.known(0.6).find((b) => b.visible && dist(b.x, b.y, k.x, k.y) < 9 * M && b.s.standing);
    if (!c || g?.type === 'CARRY' || g?.type === 'PICKUP' || (close && m.world.losClear(k.x, k.y, close.x, close.y)) || ai.mem.ctrlT > 4 + 4 * P.powerIQ || ai.mem.ctrlTotal > 7 + 5 * P.powerIQ) { inp.eP = true; ai.mem.podScoutT = m.time; return true; }
    if (!podActive(c, m)) {
      if (Math.random() < 0.5 + 0.5 * P.powerIQ) { inp.lmb = true; ai.note('Destroying a disabled biopod'); }
      else inp.eP = true;
      return true;
    }
    // look for someone this pod can see; aim error makes low-skill locks wobble off target
    let tgt = null, bd = Infinity;
    for (const s of m.survivors) {
      if (!s.standing || dist(s.x, s.y, c.x, c.y) > 20 * M || !m.world.losClear(c.x, c.y, s.x, s.y)) continue;
      const v = (s.status.has('slipstream') ? -6 * M : 0) + dist(s.x, s.y, c.x, c.y) + (s.action?.type === 'repair' ? -4 * M : 0);
      if (v < bd) { bd = v; tgt = s; }
    }
    if (!tgt) { if (ai.mem.ctrlT > 1.2 + Math.random() * 2) { if ((ai.mem.cycles = (ai.mem.cycles || 0) + 1) < 3 && pw.pods.length > 1) { inp.cycleP = true; ai.mem.ctrlT = 0.5; } else { inp.eP = true; ai.mem.cycles = 0; ai.mem.podScoutT = m.time; } } return true; }
    if (!ai.mem.aimErr || m.time - (ai.mem.aimErrT || 0) > 0.5) { const e = (1 - P.aim) * 2.2 * M; ai.mem.aimErr = { x: (Math.random() - 0.5) * e, y: (Math.random() - 0.5) * e }; ai.mem.aimErrT = m.time; }
    inp.aimX = tgt.x + tgt.vx * 0.1 + ai.mem.aimErr.x; inp.aimY = tgt.y + tgt.vy * 0.1 + ai.mem.aimErr.y;
    if (!tgt.status.has('slipstream')) ai.mem.ctrlT = Math.min(ai.mem.ctrlT, 2.5);
    // locked on a Slipstreamed survivor: teleport when it's worth it (not right next to us already)
    const L = pw.lock;
    if (L && L.s === tgt && tgt.status.has('slipstream') && L.t >= 0.8 && dist(k.x, k.y, tgt.x, tgt.y) > 12 * M && !tgt.status.has('hooked') && Math.random() < 0.15 + 0.6 * P.powerIQ) { inp.lmbP = true; ai.note(`Quantum Instantiation to ${tgt.name}`); }
    return true;
  },
  chase(ai, b, d) {
    const m = ai.m, k = ai.k, pw = k.power, inp = k.input, s = b.s, P = ai.P;
    if (pw.overheatT > 0 || pw.state !== 'idle' || pw.cd('reload') > 0) return false;
    // direct shot: tags, or teleports straight onto an already Slipstreamed target across a loop
    if (b.visible && d > 4 * M && d < 18 * M && H.clear(ai, s) && H.roll(ai, s.status.has('slipstream') ? (d > 8 * M ? 0.6 : 0.1) : 0.35)) {
      const p = H.lead(ai, b, d / (50 * M) + 0.05); const a = Math.atan2(p.y - k.y, p.x - k.x);
      if (m.world.sweepClear(k.x, k.y, p.x, p.y, 0.2 * M)) { CTL.quantum.shoot(ai, a); return true; }
    }
    // lost sight: check a pod that overlooks where they went
    if (!b.visible && d > 10 * M && H.roll(ai, 0.25)) {
      const pod = pw.pods.filter((p) => podActive(p, m) && dist(p.x, p.y, b.x, b.y) < 18 * M).sort((a, q) => dist(a.x, a.y, b.x, b.y) - dist(q.x, q.y, b.x, b.y))[0];
      if (pod) { inp.aimX = pod.x; inp.aimY = pod.y; inp.eP = true; ai.mem.ctrlT = 1.5; ai.note('Checking a biopod for the lost survivor'); return true; }
    }
    return false;
  },
  patrol(ai) {
    const m = ai.m, k = ai.k, pw = k.power, inp = k.input, P = ai.P;
    if (pw.overheatT > 0 || pw.state !== 'idle' || k.carrying) return false;
    const pods = pw.pods.filter((p) => podActive(p, m));
    // re-seed the pod net around the area we're heading to
    const g = ai.goal?.target; const focus = g && g.x !== undefined ? g : H.hotGen(ai);
    if (focus && pw.cd('reload') <= 0 && pods.filter((p) => dist(p.x, p.y, focus.x, focus.y) < 18 * M).length < 2 && dist(focus.x, focus.y, k.x, k.y) < 24 * M && m.time - (ai.mem.podTry || 0) > 2.5 - 1.5 * P.powerIQ) {
      ai.mem.podTry = m.time;
      if (Math.random() < 0.3 + 0.7 * P.powerIQ) { const sh = CTL.quantum.podShot(ai, focus); if (sh) { CTL.quantum.shoot(ai, sh.a); ai.note('Shooting a biopod onto a wall'); return true; } }
    }
    // scout through the pod network now and then
    if (pods.length >= 2 && m.time - (ai.mem.podScoutT || 0) > 14 - 8 * P.powerIQ && H.roll(ai, 0.2)) {
      const hot = H.hotGen(ai) || focus; const pod = hot ? pods.sort((a, b) => dist(a.x, a.y, hot.x, hot.y) - dist(b.x, b.y, hot.x, hot.y))[0] : pods[0];
      inp.aimX = pod.x; inp.aimY = pod.y; inp.eP = true; ai.mem.podScoutT = m.time; ai.mem.ctrlT = 0; ai.mem.cycles = 0; ai.note('Scouting through a biopod');
      return true;
    }
    return false;
  },
  travel(ai, x, y) {
    // a Slipstreamed survivor near the destination + a pod near them = free teleport
    const m = ai.m, k = ai.k, pw = k.power; if (pw.state !== 'idle' || pw.overheatT > 0 || k.carrying) return;
    const s = ai.per.known(0.4).map((b) => b.s).find((q) => q.standing && q.status.has('slipstream') && dist(q.x, q.y, x, y) < 14 * M);
    if (!s || !H.roll(ai, 0.5)) return;
    const pod = pw.pods.find((p) => podActive(p, m) && dist(p.x, p.y, s.x, s.y) < 18 * M);
    if (pod) { k.input.aimX = pod.x; k.input.aimY = pod.y; k.input.eP = true; ai.mem.ctrlT = 0; }
  },
};

// ---- Cannibal: short-range sweeps at tight loops and groups, rev-cancel reads, dash to finish ----
CTL.bubba = {
  ahead(ai, dir, d = 1.6 * M) { const k = ai.k; return !ai.m.world.sweepClear(k.x, k.y, k.x + Math.cos(dir) * d, k.y + Math.sin(dir) * d, k.r * 0.8); },
  chase(ai, b, d) {
    const m = ai.m, k = ai.k, pw = k.power, inp = k.input, P = ai.P, s = b.s;
    if (pw.state === 'tantrum') { ai.steer.stop(); return true; }
    if (pw.state === 'revving') {
      // keep closing in at rev speed; release the full rev only when the target is in reach
      ai.steer.goTo(b.x, b.y, { arrive: 0.3 * M, repath: 0.25 }); H.aimAt(ai, H.lead(ai, b, 0.5));
      const tooFar = d > (9 + 2 * P.powerIQ) * M || b.conf < 0.3;
      if (pw.rev < 1) { inp.rmb = !(tooFar && Math.random() < 0.3 + 0.7 * P.powerIQ); if (!inp.rmb) ai.note('Cancelled the rev'); return true; }
      const inReach = b.visible && d < (3.8 + 1.2 * P.powerIQ) * M && H.lane(ai, b.x, b.y);
      const limit = 2.85 - (1 - P.powerIQ) * 0.15 + (Math.random() < P.mistakes * 0.05 ? 1 : 0); // beginners sometimes over-rev
      inp.rmb = !inReach && pw.revHeld < limit && !tooFar;
      if (!inp.rmb && !inReach) ai.note(tooFar ? 'Rev wasted — target too far' : 'Released the rev to avoid a tantrum');
      return true;
    }
    if (pw.state === 'sweep') {
      const p = H.lead(ai, b, 0.35); H.aimAt(ai, p);
      // stop before ramming a wall (tantrum) unless the target is right there
      const wall = CTL.bubba.ahead(ai, pw.dir, (1.2 + 0.8 * P.powerIQ) * M);
      if ((wall && d > 2.2 * M && Math.random() < P.powerIQ) || d > 12 * M) inp.rmbP = true; // stop the sweep
      if (pw.charges >= 1 && d > 2.4 * M && d < (4 + 1.5 * P.powerIQ) * M && b.visible && H.lane(ai, b.x, b.y) && H.roll(ai, 0.5)) { inp.lmbP = true; ai.note('Chainsaw dash'); }
      return true;
    }
    if (pw.state !== 'idle' || pw.charges < 1) return false;
    // when to rev: close, in sight, and (skilled) the target is stuck at a tight tile / pallet
    // or a second survivor is close enough to share the sweep
    const others = ai.per.known(0.6).filter((o) => o.s !== s && o.s.standing && dist(o.x, o.y, b.x, b.y) < 4 * M).length;
    const pal = m.map.pallets.some((q) => q.state !== 'broken' && dist(q.x, q.y, b.x, b.y) < 3 * M);
    // pre-rev: start the rev while closing in (even around a loop) so the sweep is ready on contact
    const want = b.conf > 0.5 && d < (6 + 3 * P.powerIQ + (others ? 1.5 : 0)) * M && d > 1.8 * M;
    if (want && H.roll(ai, (pal || others ? 0.9 : 0.35 + 0.4 * P.powerIQ))) { inp.rmb = true; H.aimAt(ai, b); ai.note(others ? 'Revving on a group' : 'Pre-revving the chainsaw'); return false; }
    return false;
  },
  patrol(ai) {
    const pw = ai.k.power; if (pw.state === 'tantrum') { ai.steer.stop(); return true; }
    if (pw.state === 'revving') { ai.k.input.rmb = false; } // never sit in the rev
    if (pw.state === 'sweep') ai.k.input.rmbP = true;
    return false;
  },
};

// ---- Skull Merchant: drones on contested generators and on the chase loop, recall to stalk ----
CTL.drones = {
  always(ai) { const pw = ai.k.power; if (pw.state === 'deploying') { ai.k.input.rmb = true; H.aimAt(ai, ai.mem.droneAt || ai.k); } return false; },
  deployAt(ai, p) { const k = ai.k; ai.mem.droneAt = { x: p.x, y: p.y }; H.aimAt(ai, p); k.input.rmb = true; },
  chase(ai, b, d) {
    const m = ai.m, pw = ai.k.power;
    if (pw.state !== 'idle' || pw.cd('deploy') > 0) return false;
    const covered = pw.drones.some((q) => pw.active(q, m) && dist(q.x, q.y, b.x, b.y) < 9 * M);
    if (!covered && b.visible && d < 10 * M && d > 3 * M && H.roll(ai, 0.1 + 0.4 * ai.P.powerIQ)) { CTL.drones.deployAt(ai, H.lead(ai, b, 0.8)); ai.note('Drone on the chase loop'); }
    return false;
  },
  patrol(ai) {
    const m = ai.m, k = ai.k, pw = k.power;
    if (pw.state !== 'idle' || k.carrying) return false;
    const g = ai.goal?.target?.x !== undefined ? ai.goal.target : H.hotGen(ai);
    if (g && pw.cd('deploy') <= 0 && dist(g.x, g.y, k.x, k.y) < 10 * M && !pw.drones.some((q) => pw.active(q, m) && dist(q.x, q.y, g.x, g.y) < 12 * M) && H.roll(ai, 0.15 + 0.55 * ai.P.powerIQ)) {
      // low-skill bots drop drones roughly where they stand instead of over the generator
      const p = Math.random() < ai.P.powerIQ ? g : { x: k.x + (Math.random() - 0.5) * 8 * M, y: k.y + (Math.random() - 0.5) * 8 * M };
      CTL.drones.deployAt(ai, p); ai.note('Drone over a generator'); return false;
    }
    return false;
  },
  travel(ai, x, y) {
    // recall a nearby drone to go Undetectable before arriving at a contested spot
    const m = ai.m, k = ai.k, pw = k.power; if (pw.state !== 'idle' || k.status.has('undetectable')) return;
    const dd = dist(k.x, k.y, x, y); if (dd > 40 * M || dd < 14 * M) return;
    const q = pw.drones.find((d) => dist(d.x, d.y, k.x, k.y) < 20 * M && dist(d.x, d.y, x, y) > 16 * M);
    if (q && pw.drones.length >= 3 && H.roll(ai, 0.15)) { H.aimAt(ai, q); k.input.eP = true; ai.note('Recalling a drone to sneak up'); }
  },
};

// ---- Blight: rush down lanes, slam a wall next to the target, lethal rush + special attack ----
CTL.rush = {
  slamSpot(ai, s) { // a wall the bot can bounce off close to the target (within ~70° of the rush)
    const m = ai.m, k = ai.k, pw = k.power; let best = null, bs = Infinity;
    for (let i = -4; i <= 4; i++) {
      const a = pw.dir + i * 0.3, ex = k.x + Math.cos(a) * 3.5 * M, ey = k.y + Math.sin(a) * 3.5 * M;
      const h = m.world.firstHit(k.x, k.y, ex, ey, k.r * 0.8); if (!h) continue;
      const c = Math.abs(i) + dist(ex, ey, s.x, s.y) / M * 0.2; if (c < bs) { bs = c; best = a; }
    }
    return best;
  },
  chase(ai, b, d) {
    const m = ai.m, k = ai.k, pw = k.power, inp = k.input, P = ai.P, s = b.s;
    if (pw.state === 'fatigue') return false;
    if (pw.state === 'rush') {
      const p = H.lead(ai, b, Math.min(0.6, d / (9.2 * M)));
      if (pw.lethal) { H.aimAt(ai, p); if (d < 2.8 * M && b.visible) { inp.lmbP = true; } return true; }
      // normal rush: bumping the survivor wastes it → slam a wall close to them instead (skill)
      if (d < (5 + 4 * P.powerIQ) * M && pw.tokens >= 1) {
        if (ai.mem.slamA === undefined || m.time - (ai.mem.slamT || 0) > 0.3) { ai.mem.slamA = Math.random() < 0.4 + 0.6 * P.powerIQ ? CTL.rush.slamSpot(ai, s) : null; ai.mem.slamT = m.time; }
        if (ai.mem.slamA !== null) { inp.aim = ai.mem.slamA; return true; }
      }
      H.aimAt(ai, p); return true;
    }
    if (pw.state === 'window') {
      if (pw.tokens >= 1 && (d < 16 * M || pw.tokens >= 2) && (b.visible || b.conf > 0.5)) { const p = H.lead(ai, b, d / (9.2 * M)); H.aimAt(ai, p); inp.rmbP = true; ai.note('Lethal rush'); }
      return true;
    }
    if (pw.state === 'special') return true;
    if (pw.state !== 'idle' || pw.tokens < 2) return false;
    if (b.visible && d > 7 * M && d < 26 * M && H.roll(ai, 0.7)) {
      const p = H.lead(ai, b, d / (9.2 * M) * 0.6);
      if (H.lane(ai, p.x, p.y) || Math.random() > P.powerIQ) { H.aimAt(ai, p); inp.rmbP = true; }
    }
    return false;
  },
  travel(ai, x, y) {
    const k = ai.k, pw = k.power, st = ai.steer;
    if (pw.state === 'rush') { const p = st.path?.[st.idx] || { x, y }; H.aimAt(ai, p); if (dist(k.x, k.y, x, y) < 8 * M) k.input.aim = pw.dir; return; }
    if (pw.state === 'window') { if (pw.tokens >= 2) { const p = st.path?.[st.idx] || { x, y }; H.aimAt(ai, p); k.input.rmbP = true; } return; }
    const p = st.path?.[st.idx]; if (!p || pw.state !== 'idle' || pw.tokens < 4) return;
    if (dist(p.x, p.y, k.x, k.y) > 12 * M && H.lane(ai, p.x, p.y) && H.roll(ai, 0.5)) { H.aimAt(ai, p); k.input.rmbP = true; }
  },
};

// ---- Nurse: predict where the survivor will be when the charge ends and blink there ----
CTL.blink = {
  // one blink plan per charge: { x, y, s } — re-aimed while the survivor stays tracked
  always(ai, g) {
    const k = ai.k, pw = k.power, inp = k.input, P = ai.P;
    if (pw.state !== 'charging') return false;
    const plan = ai.mem.blinkPlan; if (!plan) { inp.rmb = false; return true; }
    if (plan.s) {
      const b = ai.per.belief(plan.s);
      if (b.conf > 0.3) {
        const max = (pw.blinks ? 12 : 20) * M, rate = (max - 1.5 * M) / (pw.blinks ? 1.2 : 2);
        const left = Math.max(0, (dist(k.x, k.y, b.x, b.y) - pw.blinkDist) / rate);
        const p = H.lead(ai, b, left + 0.12); plan.x = p.x; plan.y = p.y;
      }
    }
    H.aimAt(ai, plan); ai.steer.stop();
    const need = dist(k.x, k.y, plan.x, plan.y) - (plan.s ? 0.9 : 1.5) * M;
    inp.rmb = pw.blinkDist < need && pw.blinkDist < (pw.blinks ? 12 : 20) * M - 2;
    if (!inp.rmb) ai.mem.blinkPlan = null;
    return true;
  },
  start(ai, x, y, s = null) { ai.mem.blinkPlan = { x, y, s }; ai.k.input.rmb = true; H.aimAt(ai, { x, y }); },
  chase(ai, b, d) {
    const m = ai.m, k = ai.k, pw = k.power, inp = k.input, P = ai.P;
    if (pw.state === 'fatigue') return false;
    if (pw.state === 'chain') {
      ai.steer.goTo(b.x, b.y, { arrive: 0.3 * M });
      if ((b.visible || b.conf > 0.8) && d < 2.7 * M) { H.aimAt(ai, b); inp.lmbP = true; inp.lmb = true; return true; }
      if (pw.tokens >= 1 && d > 3 * M && d < 13 * M && b.conf > 0.5 && H.roll(ai, 1.2)) CTL.blink.start(ai, b.x, b.y, b.s);
      return true;
    }
    if (pw.state !== 'idle' || pw.tokens < 1) return false;
    // blink when the target is out of reach or behind a wall — not when a basic hit is coming
    const ok = b.visible ? d > 5 * M && d < 21 * M : b.conf > 0.55 && d > 4 * M && d < 15 * M;
    if (ok && H.roll(ai, b.visible ? 0.8 : 0.4 * P.predict)) { CTL.blink.start(ai, b.x, b.y, b.s); return true; }
    return false;
  },
  travel(ai, x, y) {
    const k = ai.k, pw = k.power; const d = dist(k.x, k.y, x, y);
    if (pw.state === 'chain' && pw.tokens >= 1 && d > 10 * M) { CTL.blink.start(ai, x, y); return; }
    if (pw.state === 'idle' && pw.tokens >= 2 && d > 12 * M && H.roll(ai, 0.5)) CTL.blink.start(ai, x, y);
  },
};

// ---- Unknown: UVX at the target (bank shots off walls), hallucinations on gens, teleports ----
CTL.uvx = {
  chase(ai, b, d) {
    const m = ai.m, k = ai.k, pw = k.power, inp = k.input, P = ai.P;
    if (pw.state === 'charging') { H.aimAt(ai, H.lead(ai, b, d / (35 * M) + 0.1)); inp.rmb = pw.charge < 1.02; return false; }
    if (pw.state !== 'idle' || pw.cd('uvx') > 0) return false;
    const weak = b.s.status.has('weakened');
    if (b.visible && d > 3.5 * M && d < 18 * M && H.clear(ai, b.s) && H.roll(ai, weak ? 0.7 : 0.4)) { inp.rmb = true; H.aimAt(ai, b); }
    return false;
  },
  patrol(ai) {
    const m = ai.m, k = ai.k, pw = k.power;
    if (pw.state !== 'idle' || pw.cd('hal') > 0) return false;
    const g = H.nearestGen(ai); if (!g || dist(g.x, g.y, k.x, k.y) > 4 * M) return false;
    if (!pw.hallucinations.some((h) => dist(h.x, h.y, g.x, g.y) < 15 * M) && H.roll(ai, 0.6)) { k.input.eP = true; ai.note('Left a hallucination at a generator'); }
    return false;
  },
  travel(ai, x, y) {
    const k = ai.k, pw = k.power; if (pw.state !== 'idle' || pw.cd('tp') > 0) return;
    const h = pw.hallucinations.filter((q) => dist(q.x, q.y, x, y) < 12 * M).sort((a, b) => dist(a.x, a.y, x, y) - dist(b.x, b.y, x, y))[0];
    if (h && dist(k.x, k.y, x, y) > 28 * M && H.roll(ai, 0.6)) { H.aimAt(ai, h); k.input.cycleP = true; ai.note('Teleporting to a hallucination'); }
  },
};

// ---- Nemesis: tentacle at the edge of basic range, through pallets at MR 2+ ----
CTL.tentacle = {
  chase(ai, b, d) {
    const m = ai.m, k = ai.k, pw = k.power, inp = k.input, P = ai.P;
    if (pw.state === 'charging') { H.aimAt(ai, H.lead(ai, b, 0.3)); inp.rmb = pw.charge < 0.36 + 0.15 * (1 - P.powerIQ); return false; }
    if (pw.state !== 'idle' || pw.cd('tentacle') > 0) return false;
    const reach = pw.reach - 0.4 * M;
    const palletInWay = !H.clear(ai, b.s) ? false : m.map.pallets.some((q) => q.state === 'down' && dist(q.x, q.y, (k.x + b.x) / 2, (k.y + b.y) / 2) < d / 2);
    if (b.visible && d > 2.2 * M && d < reach && (H.lane(ai, b.x, b.y) || (palletInWay && pw.mr >= 2)) && H.roll(ai, 0.8)) { inp.rmb = true; H.aimAt(ai, b); }
    return false;
  },
};

// ---- Ghoul: leap to latch, leap again to grab; cling off walls to cut corners ----
CTL.kagune = {
  chase(ai, b, d) {
    const m = ai.m, k = ai.k, pw = k.power, inp = k.input, P = ai.P;
    if (pw.state === 'leap') return true;
    if (pw.state === 'latch') { const s = pw.latchedOn; if (s && pw.tokens >= 1 && pw.winT < 1.6 - 0.8 * P.powerIQ) { H.aimAt(ai, H.lead(ai, ai.per.belief(s), 0.2)); inp.rmbP = true; ai.note('Grab attack'); } else if (s) H.aimAt(ai, s); return true; }
    if (pw.state === 'cling') { if (pw.tokens >= 1 && b.conf > 0.4 && d < 14 * M && H.lane(ai, b.x, b.y)) { H.aimAt(ai, H.lead(ai, b, d / (22 * M))); inp.rmbP = true; } return true; }
    if (pw.state !== 'idle' || pw.tokens < 1) return false;
    const marked = b.s.status.has('kaguneMark');
    if (b.visible && d > (marked ? 2.5 : 3.5) * M && d < 13 * M && H.roll(ai, marked ? 0.9 : 0.6)) {
      const p = H.lead(ai, b, d / (22 * M)); if (H.lane(ai, p.x, p.y)) { H.aimAt(ai, p); inp.rmbP = true; }
    }
    return false;
  },
  travel(ai, x, y) {
    const k = ai.k, pw = k.power, st = ai.steer; const p = st.path?.[st.idx];
    if (!p || pw.state !== 'idle' || pw.tokens < pw.maxTokens) return;
    if (dist(p.x, p.y, k.x, k.y) > 10 * M && H.lane(ai, p.x, p.y) && H.roll(ai, 0.4)) { H.aimAt(ai, p); k.input.rmbP = true; }
  },
};

return { powerControllerFor, registerPowerController, H };
};
__defs["js/ai/killer/team.js"] = function(){
// 2v8 killer coordination. Both killer bots (and a human killer, whose state is read but never
// steered) share a board: sightings, who is chasing whom, which generators each one covers,
// pickups in progress and requests for help. Information is shared with a delay/quality that
// depends on each bot's coordination skill — teammates talk, they don't share a brain.
const { M, dist } = __req("js/core/util.js");

const ROLE = {
  // mobile chasers
  hillbilly: 'chaser', blight: 'chaser', nurse: 'chaser', legion: 'chaser', mastermind: 'chaser', ghoul: 'chaser', cannibal: 'chaser', wraith: 'chaser', ghostface: 'chaser',
  // area control / information
  trapper: 'zoner', hag: 'zoner', knight: 'zoner', singularity: 'zoner', skull_merchant: 'zoner', dredge: 'zoner', cenobite: 'zoner', houndmaster: 'zoner',
  // ranged pressure
  huntress: 'ranged', deathslinger: 'ranged', unknown: 'ranged', executioner: 'ranged', nemesis: 'ranged', clown: 'ranged', lich: 'ranged',
};

class KillerTeam {
  static for(m) { if (!m.killerTeam) m.killerTeam = new KillerTeam(m); return m.killerTeam; }
  constructor(m) {
    this.m = m; this.sightings = new Map(); this.chasing = new Map(); this.pickups = new Map(); this.help = null;
    // split the generators: each killer covers the half nearer its spawn (re-balanced as gens pop)
    this.regions = new Map(); this.assignRegions();
  }
  roleFor(k) { return ROLE[k.def.id] || 'chaser'; }
  assignRegions() {
    const ks = this.m.killers; if (ks.length < 2) return;
    const [a, b] = ks; const ax = a.x, ay = a.y, bx = b.x, by = b.y;
    for (const g of this.m.map.gens) this.regions.set(g, dist(g.x, g.y, ax, ay) < dist(g.x, g.y, bx, by) ? a : b);
  }
  regionOwner(g) { return this.regions.get(g); }
  share(k, s, b) { this.sightings.set(s, { x: b.x, y: b.y, vx: b.vx, vy: b.vy, t: this.m.time, by: k }); }
  claim(k, goal) {
    for (const [s, c] of this.chasing) if (c.k === k) this.chasing.delete(s);
    if (goal.type === 'CHASE') this.chasing.set(goal.target, { k, t: this.m.time });
    if (goal.type === 'PICKUP') this.pickups.set(goal.target, k);
  }
  chaserOf(s, me) { const c = this.chasing.get(s); if (!c || c.k === me || this.m.time - c.t > 30) return null; return c.k; }
  claimedPickup(s, me) { const k = this.pickups.get(s); return k && k !== me && s.health === 'dying' && dist(k.x, k.y, s.x, s.y) < 20 * M; }
  // Assist when the partner's chase has dragged on and we are close enough to cut the target off.
  shouldAssist(me, mate, s) {
    const c = this.chasing.get(s); if (!c) return false;
    const long = this.m.time - c.t > 18; const near = dist(me.x, me.y, s.x, s.y) < 24 * M;
    return long && near && (me.bot?.P.coordination ?? 0.5) > Math.random();
  }
  // Teammate sightings flow into each bot's perception (quality-gated).
  feed(ai) {
    const P = ai.P, now = this.m.time;
    for (const [s, sg] of this.sightings) {
      if (sg.by === ai.k || now - sg.t > 6 || !s.standing) continue;
      if (Math.random() > P.coordination) continue;
      const b = ai.per.belief(s); if (b.visible || b.t >= sg.t) continue;
      b.x = sg.x; b.y = sg.y; b.vx = sg.vx; b.vy = sg.vy; b.t = sg.t; b.conf = Math.max(b.conf, 0.7 * Math.exp(-(now - sg.t) / 6)); b.src = 'team';
    }
  }
}

return { KillerTeam };
};
__defs["js/ai/killer/killerAI.js"] = function(){
// Killer bot brain. Layers (each at its own rate):
//   perception (10 Hz)  → beliefs about survivors, generator knowledge, anonymous clues
//   strategy   (1–2 Hz) → utility over goals with commitment timers (no frame-to-frame flip-flop)
//   behavior   (every frame) → a small behavior tree / state machine executing the goal:
//                CHASING, SEARCHING, INVESTIGATING, PATROLLING, DEFENDING_OBJECTIVE,
//                DAMAGING_GENERATOR, BREAKING_PALLET, PICKING_UP_SURVIVOR, CARRYING_SURVIVOR,
//                HOOKING_SURVIVOR, INTERRUPTING_RESCUE, REPOSITIONING, ENDGAME_PRESSURE, USING_POWER
//   power controller (per killer, see powers.js) → plays the killer's actual power
// It only writes the killer's input — exactly what a human player could do.
const { M, dist, clamp } = __req("js/core/util.js");
const { EV } = __req("js/core/events.js");
const { killerParams } = __req("js/ai/killer/params.js");
const { KillerPerception } = __req("js/ai/killer/perception.js");
const { KillerSteering } = __req("js/ai/killer/steering.js");
const { ChaseController } = __req("js/ai/killer/chase.js");
const { powerControllerFor } = __req("js/ai/killer/powers.js");
const { KillerTeam } = __req("js/ai/killer/team.js");

// Survivor perks a killer can recognise when they trigger in front of it.
const VISIBLE_PERKS = new Set(['dead_hard', 'decisive_strike', 'sprint_burst', 'lithe', 'overcome', 'borrowed_time', 'unbreakable', 'adrenaline', 'smash_hit', 'background_player', 'for_the_people', 'off_the_record', 'deliverance', 'flashlight']);

class KillerAI {
  constructor(k, m, difficulty = 5) {
    this.k = k; this.m = m; this.setDifficulty(difficulty);
    this.per = new KillerPerception(this);
    this.steer = new KillerSteering(this);
    this.chase = new ChaseController(this);
    this.powerCtl = powerControllerFor(k.power.id);
    this.team = m.killers.length > 1 ? KillerTeam.for(m) : null;
    this.goal = null; this.commitUntil = 0; this.state = 'PATROLLING';
    this.pT = Math.random() * 0.1; this.tT = 0; this.sT = 0.3; this.tick = false;
    this.knownPerks = new Set(); this.log = []; this.options = [];
    this.patrolGen = null; this.visited = new Map(); this.search = null; this.guardHook = null; this.lastHookT = -99;
    this.role = this.team ? this.team.roleFor(k) : 'solo';
    this.mem = {}; // scratch space for power controllers
    m.events.on('perkProc', (d) => {
      const p = d.perk; if (p.owner?.kind !== 'survivor' || !VISIBLE_PERKS.has(p.id)) return;
      if (dist(p.owner.x, p.owner.y, k.x, k.y) < 22 * M) { if (!this.knownPerks.has(p.owner.id + ':' + p.id)) this.note(`Saw ${p.owner.name} use ${p.def.name}`); this.knownPerks.add(p.owner.id + ':' + p.id); }
    });
    m.events.on(EV.KILLER_HIT, (d) => { if (d.result === 'endurance' && d.killer === k) this.knownPerks.add(d.survivor.id + ':borrowed_time'); });
    m.events.on(EV.SURVIVOR_UNHOOKED, (d) => {
      // the hook HUD changes: the killer knows where the rescue happened
      const h = d.survivor; this.per.addLead(h.x, h.y, m.time, 'unhook', 2.4);
      this.lastUnhook = { s: d.survivor, rescuer: d.rescuer, t: m.time, x: h.x, y: h.y };
    });
    m.events.on(EV.GENERATOR_REPAIRED, (d) => { this.per.addLead(d.gen.x, d.gen.y, m.time, 'gen-done', 0.8); });
    m.events.on(EV.SURVIVOR_HOOKED, (d) => { if (d.killer === k) { this.lastHookT = m.time; this.lastHooked = d.survivor; } });
  }
  setDifficulty(d) { this.difficulty = d; this.P = killerParams(d); this.k.botDifficulty = d; }
  knowsPerk(s, id) { return this.knownPerks.has(s.id + ':' + id); }
  hasPerk(id) { return this.k.hasPerk(id); }
  note(text) { this.log.push({ t: this.m.time, text }); if (this.log.length > 30) this.log.shift(); this.m.killerLog?.push({ t: this.m.time, who: this.k.def.short, text }); }

  // ------------------------------------------------------------------ main
  update(dt) {
    const m = this.m, k = this.k, inp = k.input;
    if (m.state === 'MATCH_START' || m.aiPaused) { inp.mx = inp.my = 0; return; }
    inp.lmb = false; inp.rmb = false; inp.aimDist = inp.aimDist ?? 8 * M;
    this.pT -= dt; if (this.pT <= 0) { this.pT = 0.1; this.per.update(0.1); this.team?.feed(this); }
    this.tT -= dt; this.tick = this.tT <= 0; if (this.tick) this.tT = 1 / (this.P.tacticalHz * (m.thinkScale || 1));
    this.sT -= dt; if (this.sT <= 0) { this.sT = this.P.strategicEvery * (0.8 + Math.random() * 0.4); this.strategize(); }
    if (k.stunT > 0) { this.state = 'STUNNED'; return; }
    if (k.action) {
      this.state = { break: 'BREAKING_PALLET', kick: 'DAMAGING_GENERATOR', pickup: 'PICKING_UP_SURVIVOR', hook: 'HOOKING_SURVIVOR', vault: 'VAULTING', custom: 'USING_POWER' }[k.action.type] || 'ACTING';
      return;
    }
    this.execute();
  }

  // ------------------------------------------------------------------ strategy
  strategize() {
    const m = this.m, k = this.k, P = this.P, per = this.per;
    const opts = []; const add = (type, target, score, why = '') => opts.push({ type, target, score, why, key: type + ':' + (target?.id ?? target?.x ?? '') });
    const myD = (x, y) => dist(x, y, k.x, k.y) / M;
    const gensLeft = m.gensNeeded - m.gensDone;
    const aliveN = m.aliveCount();
    if (k.carrying) { add('CARRY', k.carrying, 1000); this.commit(opts[0], 0); return; }
    // --- downed survivors: pick up (or slug) ---
    for (const s of m.survivors) {
      if (s.health !== 'dying' || m.carrier(s) || this.team?.claimedPickup(s, k)) continue;
      const b = per.belief(s); const d = myD(b.x, b.y); if (b.conf < 0.3 && d > 30) continue;
      let sc = 120 / (1 + d / 30);
      // Decisive Strike read: freshly unhooked survivor might have it
      if (m.time - s.unhookedAt < 40 && (this.knowsPerk(s, 'decisive_strike') || Math.random() < 0.3 * P.perkIQ)) sc *= 0.35;
      add('PICKUP', s, sc, `${d.toFixed(0)}m`);
    }
    // --- chase candidates (visible or strongly believed standing survivors) ---
    for (const b of per.known(0.3)) {
      const s = b.s; if (!s.standing) continue;
      const p = per.pos(b); const d = myD(p.x, p.y);
      let sc = 95 * (b.visible ? 1 : 0.55 * b.conf) / (1 + d / 18);
      if (s.health === 'injured' || b.seenHealth === 'injured') sc *= 1.3;
      if (s.status.has('exposed') || s.status.has('marked')) sc *= 1.4;
      if (s.hookStage >= 2) sc *= 1 + 0.25 * P.patrolIQ; // one hook from death
      // recently unhooked: protected by endurance / Borrowed Time; tunnelling is a late-game choice
      const fresh = m.time - s.unhookedAt < 15;
      if (fresh) sc *= gensLeft <= 2 ? 0.6 + P.tunnel : 0.35 + P.tunnel * 0.5;
      // strong loop read: an expert avoids long chases at god tiles when gens are flying
      const tile = m.map.tileAt?.(s.x, s.y);
      if (tile && tile.safety > 0.6) sc *= 1 - 0.25 * P.abandon * tile.safety;
      // the current chase: commitment, but reconsider after the patience window
      if (this.goal?.type === 'CHASE' && this.goal.target === s) {
        const t = this.chase.elapsed; const over = Math.max(0, t - P.chasePatience * (gensLeft <= 1 ? 1.6 : 1));
        sc *= 1.6 - Math.min(1.2, over / 25 * P.abandon);
      }
      // 2v8: someone else is already on them
      const mate = this.team?.chaserOf(s, k);
      if (mate) sc *= this.team.shouldAssist(k, mate, s) ? 0.9 : 0.35;
      add('CHASE', s, sc, `${d.toFixed(0)}m ${b.visible ? 'seen' : b.src}`);
    }
    // --- rescue reads: someone is going for the hook ---
    for (const s of m.survivors) {
      if (s.health !== 'hooked') continue;
      const near = per.known(0.4).filter((b) => b.s !== s && b.s.standing && dist(b.x, b.y, s.x, s.y) < 16 * M);
      const d = myD(s.x, s.y);
      if (near.length && d < 36) add('INTERRUPT_RESCUE', s, (60 + 30 * P.hookIQ) / (1 + d / 25), 'rescuer spotted');
      // low skill: camping
      if (this.guardHook === s && m.time - this.lastHookT < 25) add('GUARD_HOOK', s, 45 * (1 - P.hookIQ), 'camping');
    }
    if (this.lastUnhook && m.time - this.lastUnhook.t < 6) {
      const u = this.lastUnhook; const d = myD(u.x, u.y);
      if (d < 40) add('INVESTIGATE', { x: u.x, y: u.y, id: 'unhook' }, (70 + 40 * P.hookIQ) / (1 + d / 30), 'unhook');
    }
    // --- clues: scratch marks, noises, grunts ---
    const L = per.bestLead();
    if (L) add('INVESTIGATE', { x: L.x, y: L.y, id: 'lead', ref: L.ref }, 70 * L.score + 8, L.kind);
    // --- search where the chase was lost ---
    if (this.search && m.time < this.search.until) add('SEARCH', this.search, 55 + 10 * P.predict, 'lost them');
    // --- generators: defend / patrol ---
    const gens = m.map.gens.filter((g) => !g.done);
    if (!m.gatesPowered && gens.length) {
      for (const g of gens) {
        const gi = per.genInfo(g); const d = myD(g.x, g.y);
        const recent = m.time - gi.t < 30;
        const prog = recent ? gi.progress : 0.25 + 0.2 * P.patrolIQ * Math.min(1, m.gensDone / 3);
        let sc = (14 + 34 * prog * P.patrolIQ + (gi.repairing && m.time - gi.t < 8 ? 40 : 0)) / (1 + d / (30 + 30 * P.patrolIQ));
        // 3-gen awareness: keep the cluster of remaining gens together
        if (P.patrolIQ > 0.5 && gens.length <= gensLeft + 2) { const dens = gens.filter((o) => o !== g && dist(o.x, o.y, g.x, g.y) < 32 * M).length; sc *= 1 + 0.15 * dens * P.patrolIQ; }
        if (this.hasPerk('pop') && m.time - this.lastHookT < 45 && prog > 0.4) sc *= 1.5;
        // keep walking to the gen we picked; once checked, rotate away from it for a while
        if (this.goal && (this.goal.type === 'PATROL' || this.goal.type === 'DEFEND_GEN') && this.goal.target === g) sc *= 1.6;
        const seenAt = this.visited.get(g); if (seenAt !== undefined) sc *= Math.min(1, 0.25 + (m.time - seenAt) / 40);
        if (this.team && this.team.regionOwner(g) !== this.k && Math.random() < this.P.coordination) sc *= 0.6;
        add(gi.repairing && recent ? 'DEFEND_GEN' : 'PATROL', g, sc + Math.random() * 6 * P.temperature, `${Math.round(prog * 100)}%`);
      }
    }
    // --- endgame ---
    if (m.gatesPowered) for (const gt of m.map.gates) { const d = myD(gt.x, gt.y); add('ENDGAME', gt, (gt.user ? 70 : 30) / (1 + d / 40) + (gt.open ? 10 : 0), gt.user ? 'gate being opened' : 'patrol gates'); }
    if (m.hatch.state === 'open' && aliveN === 1 && myD(m.hatch.x, m.hatch.y) < 40) add('HATCH', m.hatch, 80, 'close the hatch');
    if (!opts.length) add('PATROL', gens[0] || m.map.gates[0], 1);
    // --- pick, with commitment + noise ---
    for (const o of opts) o.score *= 1 + (Math.random() - 0.5) * P.temperature;
    opts.sort((a, b) => b.score - a.score);
    let pick = opts[0];
    if (Math.random() < P.mistakes && opts.length > 1) pick = opts[1 + Math.floor(Math.random() * Math.min(3, opts.length - 1))];
    const cur = this.goal && opts.find((o) => o.key === this.goal.key);
    if (cur && m.time < this.commitUntil && cur.score * 1.3 >= pick.score && pick.type !== 'PICKUP' && pick.type !== 'CHASE') pick = cur;
    // never hop between chase targets on a whim: a new target must be clearly better
    if (cur && cur.type === 'CHASE' && pick.type === 'CHASE' && pick !== cur && cur.score * 1.6 > pick.score && cur.target.standing) pick = cur;
    this.options = opts.slice(0, 6);
    this.commit(pick);
  }
  commit(o, hold = 2.5) {
    const changed = !this.goal || this.goal.key !== o.key;
    this.goal = o;
    if (changed) {
      this.commitUntil = this.m.time + hold;
      if (o.type === 'CHASE') { if (this.chase.target !== o.target) { this.chase.reset(o.target); this.note(`Chasing ${o.target.name}`); } }
      if (o.type === 'PATROL' || o.type === 'DEFEND_GEN') this.patrolGen = o.target;
      if (o.type === 'INVESTIGATE' && o.target.ref) o.target.ref.investigated = true;
      this.team?.claim(this.k, o);
    }
  }

  // ------------------------------------------------------------------ behavior
  execute() {
    const m = this.m, k = this.k, g = this.goal, inp = k.input, P = this.P;
    if (!g) { this.steer.stop(); return; }
    const t = g.target;
    const ctx = m.killerContext(k);
    // Modal powers (biopod control, mid-teleport…) own the frame whatever the goal is
    if (this.powerCtl?.always?.(this, g)) return;
    // Between chases the power controller may take the wheel (setting traps, pods, guards…)
    if (!['CARRY', 'PICKUP', 'CHASE'].includes(g.type) && this.powerCtl?.patrol?.(this)) return;
    switch (g.type) {
      case 'CARRY': {
        this.state = 'CARRYING_SURVIVOR';
        const h = this.chooseHook();
        if (!h) { this.steer.stop(); return; }
        if (ctx?.type === 'hook') { inp.spaceP = true; this.guardHook = Math.random() < 1 - this.P.hookIQ ? k.carrying : null; return; }
        this.steer.goTo(h.x, h.y, { arrive: 1.2 * M });
        return;
      }
      case 'PICKUP': {
        this.state = 'PICKING_UP_SURVIVOR';
        if (t.health !== 'dying' || m.carrier(t)) { this.goal = null; return; }
        if (ctx?.type === 'pickup' && ctx.target === t) { inp.spaceP = true; return; }
        this.steer.goTo(t.x, t.y, { arrive: 1.2 * M });
        return;
      }
      case 'CHASE': {
        this.state = 'CHASING';
        if (ctx?.type === 'break' && this.palletBlocks(ctx.target, t)) { inp.spaceP = true; return; }
        const r = this.chase.step(this.tick);
        if (r === 'lost') { const b = this.per.belief(t); this.search = { x: b.x, y: b.y, until: m.time + 5 + 10 * P.predict, t0: m.time, id: 'search' }; this.note(`Lost ${t.name}`); this.goal = null; this.sT = 0; }
        else if (r === 'downed') { this.goal = null; this.sT = 0; }
        return;
      }
      case 'INTERRUPT_RESCUE': case 'GUARD_HOOK': {
        this.state = g.type === 'GUARD_HOOK' ? 'DEFENDING_OBJECTIVE' : 'INTERRUPTING_RESCUE';
        if (t.health !== 'hooked') { this.goal = null; this.sT = 0; return; }
        const d = this.steer.goTo(t.x, t.y, { arrive: (g.type === 'GUARD_HOOK' ? 7 : 3) * M });
        if (this.powerCtl?.travel && d > 25 * M) this.powerCtl.travel(this, t.x, t.y);
        return;
      }
      case 'INVESTIGATE': case 'SEARCH': {
        this.state = g.type === 'SEARCH' ? 'SEARCHING' : 'INVESTIGATING';
        let tx = t.x, ty = t.y;
        if (g.type === 'SEARCH') {
          // sweep around the last known spot: check nearby cover in a widening spiral
          const a = (m.time - t.t0) * 1.3, r = (2 + (m.time - t.t0) * 0.9) * M;
          tx = t.x + Math.cos(a) * Math.min(r, 9 * M); ty = t.y + Math.sin(a) * Math.min(r, 9 * M);
        }
        const d = this.steer.goTo(tx, ty, { arrive: 1.0 * M });
        if (d < 1.3 * M && g.type === 'INVESTIGATE') { this.goal = null; this.sT = Math.min(this.sT, 0.2); }
        if (this.powerCtl?.travel && d > 25 * M) this.powerCtl.travel(this, tx, ty);
        return;
      }
      case 'PATROL': case 'DEFEND_GEN': {
        this.state = g.type === 'DEFEND_GEN' ? 'DEFENDING_OBJECTIVE' : 'PATROLLING';
        if (t.done) { this.goal = null; this.sT = 0; return; }
        const d = this.steer.goTo(t.x, t.y, { arrive: 1.9 * M });
        if (this.powerCtl?.travel && d > 25 * M) this.powerCtl.travel(this, t.x, t.y);
        if (ctx?.type === 'kick' && ctx.target === t && Math.random() < 0.4 + 0.6 * P.pressure) { inp.spaceP = true; this.visited.set(t, m.time); this.goal = null; this.sT = Math.min(this.sT, 0.5); return; }
        if (d < 2.2 * M) { this.visited.set(t, m.time); this.goal = null; this.sT = 0; }
        return;
      }
      case 'ENDGAME': {
        this.state = 'ENDGAME_PRESSURE';
        const d = this.steer.goTo(t.x, t.y, { arrive: 3 * M });
        if (this.powerCtl?.travel && d > 25 * M) this.powerCtl.travel(this, t.x, t.y);
        if (d < 4 * M && !t.user) { this.goal = null; this.sT = Math.min(this.sT, 1); }
        return;
      }
      case 'HATCH': {
        this.state = 'ENDGAME_PRESSURE';
        if (ctx?.type === 'closeHatch') { inp.spaceP = true; return; }
        this.steer.goTo(t.x, t.y, { arrive: 1.2 * M }); return;
      }
    }
  }
  // Break a dropped pallet when it separates us from a target who keeps looping it.
  palletBlocks(p, s) { return p.state === 'down' && s && p.sideOf(this.k) !== p.sideOf(s) && dist(s.x, s.y, p.x, p.y) < 8 * M; }
  chooseHook() {
    const m = this.m, k = this.k, P = this.P;
    let best = null, bs = Infinity;
    for (const h of m.map.hooks) {
      if (h.survivor || h.broken) continue;
      let c = dist(h.x, h.y, k.x, k.y);
      // smarter killers avoid hooks right next to known survivors (easy rescues) when close alternatives exist
      for (const b of this.per.known(0.5)) if (b.s !== k.carrying && b.s.standing && dist(b.x, b.y, h.x, h.y) < 12 * M) c += 10 * M * P.hookIQ;
      if (c < bs) { bs = c; best = h; }
    }
    return best;
  }
}

return { KillerAI };
};
__defs["js/ai/blackboard.js"] = function(){
// Team blackboard: information survivors could plausibly share (callouts, HUD states).
// Reading it is gated by each bot's comms quality — nobody gets perfect omniscience.
const { M, dist } = __req("js/core/util.js");

class Blackboard {
  constructor(m) {
    this.m = m;
    this.sighting = null;          // latest { x, y, vx, vy, t, by, k } of any killer
    this.sightings = new Map();    // killer id -> latest sighting of that killer (2v8)
    this.killerTargets = new Map(); // killer coordination (2v8 killer AI uses its own board)
    this.claims = new Map();       // key -> { sv, t }
    this.chased = new Map();       // survivor id -> time last reported in chase
    this.traps = new Map();        // trap id -> { x, y, t }
    this.gens = new Map();         // gen id -> { p, t }
    this.totems = new Map();       // totem id -> { hex, t }
    // Learned killer tendencies (observed by anyone, shared over time)
    this.model = { palletBreaks: 0, palletRespects: 0, mindgames: 0, camping: 0, tunnels: 0, hooks: 0, chainsawUses: 0, cloaks: 0, trapCatches: 0 };
  }
  report(sv, x, y, vx, vy, k = this.m.killer) { this.sighting = { x, y, vx, vy, t: this.m.time, by: sv, k }; this.sightings.set(k.id, this.sighting); }
  sightingOf(k) { return this.sightings.get(k.id) || null; }
  claim(key, sv) { this.claims.set(key, { sv, t: this.m.time }); }
  release(sv) { for (const [k, c] of this.claims) if (c.sv === sv) this.claims.delete(k); }
  claimedBy(key) { const c = this.claims.get(key); if (!c) return null; if (this.m.time - c.t > 6 || !c.sv.alive || !c.sv.standing) { this.claims.delete(key); return null; } if (c.sv.ai && (c.sv.ai.inChase || c.sv.trap)) return null; return c.sv; }
  refreshClaim(sv, key) { const c = this.claims.get(key); if (c && c.sv === sv) c.t = this.m.time; }
  reportChase(sv) { this.chased.set(sv.id, this.m.time); }
  inChase(sv, within = 3) { const t = this.chased.get(sv.id); return t !== undefined && this.m.time - t < within; }
  reportTrap(t) { this.traps.set(t.id, { x: t.x, y: t.y, t: this.m.time, trap: t }); }
  forgetTrap(t) { this.traps.delete(t.id); }
  reportGen(g) { this.gens.set(g.id, { p: g.progress, t: this.m.time }); }
  knownGen(g) { return this.gens.get(g.id); }
  // gens currently being worked on by teammates (survivors call these out)
  activeGens(exclude) { return this.m.map.gens.filter((g) => !g.done && [...g.repairers].some((s) => s !== exclude)); }
  get tendencies() {
    const md = this.model;
    const respect = (md.palletRespects + 1) / (md.palletBreaks + md.palletRespects + 2);
    return { camping: Math.min(1, md.camping / Math.max(1, md.hooks)), tunneling: Math.min(1, md.tunnels / Math.max(1, md.hooks)), palletRespect: respect, mindgamer: Math.min(1, md.mindgames / 6) };
  }
}

return { Blackboard };
};
__defs["js/core/game.js"] = function(){
// Match: the rules engine and match state machine. DOM-free so it runs headless in tests.
// States: MAP_LOADING → MATCH_START → NORMAL_MATCH → ENDGAME_COLLAPSE → MATCH_END → RESULTS
const { M, dist, clamp, mulberry32, angleDiff, uid } = __req("js/core/util.js");
const { EventBus, EV } = __req("js/core/events.js");
const { FX } = __req("js/core/fx.js");
const { GameMap } = __req("js/maps/map.js");
const { MAPS } = __req("js/maps/maps.js");
const { KILLERS } = __req("js/killers/index.js");
const { Killer } = __req("js/entities/killer.js");
const { Survivor, HOOK_STAGE_TIME } = __req("js/entities/survivor.js");
const { GEN_CHARGES } = __req("js/entities/objects.js");
const { PerkSystem } = __req("js/perks/perkSystem.js");
const { randomSurvivorPerks } = __req("js/perks/survivorPerks.js");
const { ITEM_POOL, ITEM_ADDONS, makeItem } = __req("js/items/items.js");
const { SurvivorAI } = __req("js/ai/survivorAI.js");
const { KillerAI } = __req("js/ai/killer/killerAI.js");
const { Blackboard } = __req("js/ai/blackboard.js");
const { rollPersonality, SURVIVOR_ROSTER } = __req("js/ai/personality.js");
const { install2v8, scaleMapDef, pickCage } = __req("js/modes/twoVeight.js");

const MATCH_STATES = ['LOBBY', 'LOADOUT', 'MAP_LOADING', 'MATCH_START', 'NORMAL_MATCH', 'ENDGAME_COLLAPSE', 'EXIT', 'MATCH_END', 'RESULTS'];
const COLLAPSE_TIME = 120;

const DEFAULT_CONFIG = {
  mapId: 'rotwood', seed: null, killerId: 'trapper', killerPerks: [], killerAddons: [],
  survivorCount: 4, difficulty: 5, difficulties: null, perkMode: 'random', itemMode: 'random', survivors: null,
  gensRequired: 5, gensOnMap: 7, hookCount: null, gateCount: 2, modifiers: {},
  // Roles: 'killer' (player controls killers[0]), 'survivor' (player controls survivors[0]),
  // 'spectate' (everyone is a bot). killers: optional [{ killerId, perks, addons, difficulty }].
  playerRole: 'killer', killers: null, killerDifficulty: 5, mode: '1v4',
};

class Match {
  constructor(config = {}, deps = {}) {
    this.cfg = { ...DEFAULT_CONFIG, ...config, modifiers: { ...(config.modifiers || {}) } };
    this.audio = deps.audio || null;
    this.state = 'MAP_LOADING'; this.stateT = 0; this.time = 0; this.realTime = 0;
    this.events = new EventBus(); this.fx = new FX();
    this.rng = mulberry32(this.cfg.seed ?? Math.floor(Math.random() * 1e9));
    const def0 = MAPS[this.cfg.mapId] || MAPS.rotwood;
    const def = this.cfg.mode === '2v8' ? scaleMapDef(def0) : def0;
    this.map = deps.map || new GameMap(def, { seed: this.cfg.seed ?? Math.floor(this.rng() * 1e9), gens: this.cfg.gensOnMap, hooks: this.cfg.hookCount ?? undefined, gates: this.cfg.gateCount });
    this.world = this.map.world;
    this.gensNeeded = Math.min(this.cfg.gensRequired, this.map.gens.length); this.gensDone = 0; this.gatesPowered = false;
    this.collapse = false; this.collapseT = COLLAPSE_TIME;
    this.hookStageTime = this.cfg.modifiers.hardcoreHooks ? 40 : HOOK_STAGE_TIME;
    this.traps = []; this.projectiles = []; this.zones = []; this.devices = []; this.minions = []; this.scratches = []; this.blood = []; this.grunts = []; this.sounds = []; this.loudNoises = [];
    this.reveals = []; this.killerReveals = []; this.aiLog = []; this.feed = []; this.notify = [];
    this.score = { brutality: 0, deviousness: 0, hunter: 0, sacrifice: 0 };
    this.pathBudget = 6; this.aiPaused = false; this.camping = 0;
    // bot think-rate scale (Settings → Simulation detail): lower = cheaper on big matches
    const perf = this.cfg.perf || 'high'; this.thinkScale = perf === 'low' ? 0.6 : perf === 'balanced' && this.cfg.mode === '2v8' ? 0.75 : 1;
    this.perks = new PerkSystem(this);
    this.bb = new Blackboard(this);
    this.hatch = this.map.hatch;
    // Killers. killers[0] is the primary killer (the player's in killer mode); 2v8 adds a second.
    const kcfg = this.cfg.killers || [{ killerId: this.cfg.killerId, perks: this.cfg.killerPerks, addons: this.cfg.killerAddons, difficulty: this.cfg.killerDifficulty }];
    this.killers = kcfg.map((c, i) => {
      const kd = KILLERS[c.killerId] || KILLERS.trapper;
      const k = new Killer(kd, kd.Power, { addons: c.addons || [] }); k.index = i; k.team = i;
      const sp = i === 0 ? this.map.spawn.killer : this.map.spawn.killer2 || this.farSpawn(this.map.spawn.killer);
      k.x = sp.x; k.y = sp.y;
      this.perks.register(k, (c.perks || []).slice(0, 4));
      k.botDifficulty = c.difficulty ?? this.cfg.killerDifficulty;
      k.isPlayer = !!c.human || (i === 0 && this.cfg.playerRole === 'killer');
      return k;
    });
    this.killer = this.killers[0];
    // Survivors
    this.survivors = [];
    let roster = [...SURVIVOR_ROSTER].sort(() => this.rng() - 0.5);
    // the player's chosen character comes first; bots never share their name
    const pn = this.cfg.playerRole === 'survivor' ? (this.cfg.playerName || this.cfg.survivors?.[0]?.name) : null;
    if (pn) { const me = roster.find((r) => r.name === pn); if (me) roster = [me, ...roster.filter((r) => r !== me)]; }
    const n = this.cfg.survivors ? this.cfg.survivors.length : this.cfg.survivorCount;
    for (let i = 0; i < n; i++) {
      const sc = this.cfg.survivors?.[i] || {};
      const diff = sc.difficulty ?? this.cfg.difficulties?.[i] ?? this.cfg.difficulty;
      const pers = rollPersonality(this.rng, sc.archetype || null);
      const item = sc.item === 'random' ? ITEM_POOL[Math.floor(this.rng() * ITEM_POOL.length)] : sc.item !== undefined ? sc.item : this.cfg.itemMode === 'none' ? null : this.cfg.itemMode === 'random' ? ITEM_POOL[Math.floor(this.rng() * ITEM_POOL.length)] : this.cfg.itemMode;
      let addons = sc.itemAddons || [];
      if (!sc.itemAddons && this.cfg.itemAddons === 'random' && item) { const pool = Object.values(ITEM_ADDONS).filter((a) => a.item === item); if (pool.length) addons = [pool[Math.floor(this.rng() * pool.length)].id]; }
      const sv = new Survivor({ name: (i === 0 && pn) || sc.name || roster[i % roster.length].name, color: roster[i % roster.length].color, difficulty: diff, personality: pers, item, itemAddons: addons });
      const sp = this.map.spawn.survivors[i % this.map.spawn.survivors.length];
      sv.x = sp.x + (i >= 4 ? 20 : 0); sv.y = sp.y;
      const perkIds = sc.perks || randomSurvivorPerks(this.rng, sc.perkMode || this.cfg.perkMode, i);
      this.perks.register(sv, perkIds);
      if (sc.human || (i === 0 && this.cfg.playerRole === 'survivor')) { sv.isPlayer = true; if (!sc.human) sv.name = this.cfg.playerName || sv.name; } else new SurvivorAI(sv, this, diff, pers);
      if (this.cfg.modifiers.startInjured) sv.health = 'injured';
      this.survivors.push(sv);
    }
    if (this.cfg.mode === '2v8') install2v8(this);
    for (const k of this.killers) k.power.onInit?.(this);
    // Killer bots: every killer the player does not control gets the killer AI.
    for (const k of this.killers) if (!k.isPlayer) k.bot = new KillerAI(k, this, k.botDifficulty);
    this.perks.init();
    this.wireEvents();
    this.setState('MATCH_START');
  }

  setState(s) { this.state = s; this.stateT = 0; this.events.emit('state', { state: s }); }

  // ------------------------------------------------------------ event wiring
  wireEvents() {
    const E = this.events, sc = (cat, pts, label) => this.addScore(cat, pts, label);
    E.on(EV.KILLER_HIT, (d) => { sc('brutality', d.result === 'downed' ? 500 : 300, d.result === 'downed' ? 'Down' : 'Hit'); this.endChaseOn(d.survivor, d.killer); if (this.time - d.survivor.unhookedAt < 30) this.bb.model.tunnels++; if (!d.survivor.hasPerk('calm_spirit')) this.audio?.play('scream'); });
    E.on(EV.SURVIVOR_DOWNED, (d) => { (d.killer || this.nearestKiller(d.survivor.x, d.survivor.y)).stats.downs++; this.notifyK(`${d.survivor.name} is down`); });
    E.on(EV.GENERATOR_REPAIRED, (d) => { this.loudNoise(d.gen.x, d.gen.y, 'gen'); this.notifyK(`Generator ${d.gen.name} repaired`); });
    E.on(EV.PALLET_DESTROYED, () => sc('brutality', 100, 'Break'));
    E.on(EV.GENERATOR_DAMAGED, () => sc('brutality', 100, 'Damage'));
    E.on(EV.SURVIVOR_HOOKED, () => sc('sacrifice', 500, 'Hooked'));
    E.on(EV.SURVIVOR_KILLED, (d) => sc('sacrifice', 1000, d.how === 'hook' ? 'Sacrificed' : 'Killed'));
    E.on(EV.TRAP_TRIGGERED, (d) => { sc('deviousness', 400, 'Trap catch'); if (d.survivor) E.emit('powerTriggered', { survivor: d.survivor, x: d.survivor.x, y: d.survivor.y, trap: d.trap }); });
    E.on(EV.KILLER_STUNNED, (d) => { this.endBloodlust(d.killer); });
    E.on(EV.CHASE_START, () => { sc('hunter', 100, 'Chase'); });
    E.on(EV.POWER_USED, (d) => { if (d.power === 'uncloaked' || d.power === 'sprint') sc('deviousness', 50, d.power === 'sprint' ? 'Rev' : 'Ambush'); });
  }
  addScore(cat, pts, label) { this.score[cat] += pts; this.feed.push({ t: this.time, cat, pts, label }); if (this.feed.length > 8) this.feed.shift(); }
  notifyK(text, color = '#e8e2d6') { this.notify.push({ t: this.time, text, color }); if (this.notify.length > 4) this.notify.shift(); }

  // ------------------------------------------------------------ helpers
  aliveCount() { return this.survivors.filter((s) => s.alive).length; }
  // --- multi-killer helpers ---
  nearestKiller(x, y) { let best = this.killers[0], bd = Infinity; for (const k of this.killers) { const d = dist(k.x, k.y, x, y); if (d < bd) { bd = d; best = k; } } return best; }
  killerDist(x, y) { const k = this.nearestKiller(x, y); return dist(k.x, k.y, x, y); }
  killerWith(method) { return this.killers.find((k) => typeof k.power[method] === 'function') || this.killer; }
  hasPower(id) { return this.killers.some((k) => k.power.id === id); }
  carrier(s) { return this.killers.find((k) => k.carrying === s) || null; }
  inChase(s) { return this.killers.some((k) => k.chaseTarget === s); }
  get chaseTarget() { return this.killer.chaseTarget || null; }
  get player() { return this.cfg.playerRole === 'survivor' ? this.survivors.find((s) => s.isPlayer) : this.cfg.playerRole === 'killer' ? this.killer : null; }
  farSpawn(from) {
    const spots = this.map.openSpots(10, 6, []); let best = null, bd = -1;
    for (const p of spots) { const d = dist(p.x, p.y, from.x, from.y); if (d > bd) { bd = d; best = p; } }
    return best || { x: this.map.w - from.x, y: this.map.h - from.y };
  }
  // Sounds are attributed to the killer that made them (or the nearest one) so each survivor
  // belief only reacts to its own killer's cues.
  sound(type, x, y, radius, data = {}) { this.sounds.push({ type, x, y, radius, t: this.time, data, src: data.killer || (this.killers ? this.nearestKiller(x, y) : null) }); }
  quiet(s) { const p = s.perks?.find((q) => q.id === 'quick_and_quiet'); if (!p || p.cooldownT > 0) return false; p.cooldownT = 20; this.perks.proc(p, 'Quick & Quiet'); return true; }
  loudNoise(x, y, kind) { this.loudNoises.push({ x, y, t: this.time, kind }); if (this.loudNoises.length > 20) this.loudNoises.shift(); }
  reveal(target, dur, label, silent = false) {
    if (target && target.perks && target.ai && this.auraShield(target, label)) return;
    const r = this.reveals.find((q) => q.target === target && q.label === label);
    if (r) r.until = Math.max(r.until, this.time + dur); else this.reveals.push({ target, until: this.time + dur, label, silent });
  }
  // Distortion / Sole Survivor: a survivor's aura reveal can be blocked by tokens.
  auraShield(s, label) {
    const key = label + ':' + Math.floor(this.time); if (s._lastShield === key) return true;
    const d = s.perks.find((p) => p.id === 'distortion' && p.charges > 0) || (this.aliveCount() === 1 && s.perks.find((p) => p.id === 'sole_survivor' && p.tokens > 0));
    if (!d) return false;
    if (d.id === 'distortion') d.charges--; else d.tokens--;
    s._lastShield = key; s.noScratchUntil = this.time + 10; this.perks.proc(d, d.def.name); s.ai?.note(`${d.def.name} hid my aura`, 'aurashield'); return true;
  }
  // Directional warnings for the human survivor (Spine Chill, Premonition, hums, …).
  warn(s, text, dir = null) { if (!s.isPlayer) return; (s.warnings ||= []).push({ t: this.time, text, dir }); if (s.warnings.length > 4) s.warnings.shift(); }
  revealKillerTo(s, dur, label, k = null) { this.killerReveals.push({ to: s, until: this.time + dur, label, killer: k }); }
  killerRevealedTo(s, k = null) { return this.killerReveals.some((r) => r.to === s && r.until > this.time && (!k || !r.killer || r.killer === k)); }
  lightHex(id) {
    const free = this.map.totems.filter((t) => !t.hex && !t.cleansed); if (!free.length) return null;
    const t = free[Math.floor(this.rng() * free.length)]; t.hex = id; return t;
  }
  addScratch(s) { if ((s.noScratchUntil || 0) > this.time) return; if (s.hasPerk('low_profile') && this.aliveCount() === 1) return; const life = 7 * this.perks.killerProduct('scratchLife') * (s.hasPerk('lightweight') ? 0.55 : 1); const a = Math.random() * Math.PI * 2, r = Math.random() * 0.8 * M; this.scratches.push({ life, x: s.x + Math.cos(a) * r, y: s.y + Math.sin(a) * r, t: this.time, a: Math.random() * Math.PI, sv: s }); }
  addBlood(s) { if ((s.silentUntil || 0) > this.time || (s.hasPerk('low_profile') && this.aliveCount() === 1)) return; const lb = s.perks?.find((q) => q.id === 'lucky_break'); if (lb && s.health === 'injured' && (lb.data.used || 0) < 180) return; this.blood.push({ life: 12 * this.perks.killerProduct('bloodLife'), x: s.x + (Math.random() - 0.5) * 12, y: s.y + (Math.random() - 0.5) * 12, t: this.time, r: 2 + Math.random() * 3 }); }
  addGrunt(s, color = null) { this.grunts.push({ x: s.x, y: s.y, t: this.time, sv: s, color }); }
  // --- power framework hooks (see killers/framework.js) ---
  spawnProjectile(p) { this.projectiles.push(p); return p; }
  addMinion(n) { this.minions.push(n); return n; }
  removeMinion(n) { n.dead = true; this.minions = this.minions.filter((q) => q !== n); }
  updateMinions(dt) { for (const n of this.minions) { if (n.stunT > 0) { n.stunT -= dt; n.vx = n.vy = 0; continue; } n.update?.(dt, this); } this.minions = this.minions.filter((n) => !n.dead); }
  addZone(z) { this.zones.push(z); if (this.zones.length > 320) this.zones.splice(0, this.zones.length - 320); return z; }
  addDevice(d) { this.devices.push(d); return d; }
  removeDevice(d) { d.dead = true; this.devices = this.devices.filter((q) => q !== d); }
  updateProjectiles(dt) { for (const p of this.projectiles) p.update(dt, this); this.projectiles = this.projectiles.filter((p) => !p.dead); }
  updateZones(dt) {
    const t = this.time; this.zones = this.zones.filter((z) => z.until > t);
    for (const z of this.zones) {
      if (!z.armed(this)) continue;
      if (z.effect) for (const s of this.survivors) if (s.standing && z.contains(s.x, s.y)) z.effect(s, dt, this, z);
      if (z.killerEffect) for (const k of this.killers) if (z.contains(k.x, k.y) && (!z.owner || z.owner === k)) z.killerEffect(k, dt, this, z);
    }
  }
  zonesAt(x, y, pad = 0) { return this.zones.filter((z) => z.armed(this) && z.contains(x, y, pad)); }
  // Send a dying survivor straight to a hook (Executioner's cage). Prefers a far hook.
  cageSurvivor(s, minDist = 24 * M, k = this.carrier(s) || this.killer) { const free = this.map.hooks.filter((h) => !h.survivor && !h.broken);
    if (!free.length || s.health !== 'dying') return false;
    const far = free.filter((h) => dist(h.x, h.y, k.x, k.y) > minDist);
    const pool = far.length ? far : free; const h = pool[Math.floor(this.rng() * pool.length)];
    h.cage = true; k.carrying = s; s.health = 'carried'; this.hookSurvivor(h, true, k); return true;
  }

  // ------------------------------------------------------------ main update
  update(dt) {
    this.realTime += dt; this.stateT += dt;
    if (this.fx.hitStop > 0) { this.fx.hitStop -= dt; dt *= 0.15; }
    this.fx.update(dt);
    if (this.state === 'MATCH_START') { if (this.stateT > 2.6) { this.setState('NORMAL_MATCH'); this.events.emit('matchStart', {}); } return; }
    if (this.state === 'MATCH_END') { if (this.stateT > 2.5) this.setState('RESULTS'); return; }
    if (this.state === 'RESULTS') return;
    this.time += dt;
    this.pathBudget = 6;
    for (const k of this.killers) {
      if (k.bot && !this.aiPaused) k.bot.update(dt);
      k.update(dt, this);
      k.power.tick?.(dt, this);
    }
    if (this.playerCtls) for (const c of this.playerCtls) c.update(dt); else this.playerCtl?.update(dt);
    this.updateMinions(dt);
    this.updateProjectiles(dt); this.updateZones(dt);
    for (const s of this.survivors) { if (s.ai) s.ai.update(dt); s.update(dt, this); }
    this.separateSurvivors();
    this.perks.update(dt);
    this.updateCarry(dt); this.updateHooks(dt); this.updateGens(dt); this.updateTraps(dt);
    this.updateChase(dt); this.updateGates(dt); this.updatePallets();
    // decay lists
    const t = this.time;
    if (this.scratches.length > 400) this.scratches.splice(0, this.scratches.length - 400);
    this.scratches = this.scratches.filter((q) => t - q.t < q.life);
    this.blood = this.blood.filter((q) => t - q.t < q.life);
    this.grunts = this.grunts.filter((q) => t - q.t < 1.2);
    this.sounds = this.sounds.filter((q) => t - q.t < 1.5);
    this.loudNoises = this.loudNoises.filter((q) => t - q.t < 3);
    this.reveals = this.reveals.filter((q) => q.until > t && (q.target.kind !== 'survivor' || q.target.alive));
    this.killerReveals = this.killerReveals.filter((q) => q.until > t);
    this.map.refreshTileSafety(t);
    // end check
    if (this.survivors.every((s) => !s.alive)) { this.setState('MATCH_END'); this.events.emit('matchEnd', {}); }
  }

  // ------------------------------------------------------------ killer interactions
  killerContext(k = this.killer) {
    if (k.action || k.stunT > 0) return null;
    const pc = k.power.context?.(this); if (pc) return pc;
    if (k.power.noContext?.(this)) return null;
    const cloaked = k.power.id === 'cloak' && k.power.state !== 'visible';
    const near = (o, r) => dist(k.x, k.y, o.x, o.y) < r * M;
    if (k.carrying) {
      let best = null, bd = 2.2 * M;
      for (const h of this.map.hooks) { if (h.survivor || h.broken) continue; const d = dist(k.x, k.y, h.x, h.y); if (d < bd) { bd = d; best = h; } }
      return best ? { type: 'hook', target: best, label: 'Hook survivor' } : null;
    }
    if (!cloaked) for (const s of this.survivors) if (s.health === 'dying' && near(s, 1.9) && !this.killers.some((q) => q !== k && q.action?.type === 'pickup' && q.action.target === s)) return { type: 'pickup', target: s, label: `Pick up ${s.name}` };
    if (!cloaked) for (const p of this.map.pallets) if (p.state === 'down' && dist(k.x, k.y, p.x, p.y) < Math.max(p.len / 2, 1.2 * M) + 0.8 * M && Math.abs((k.x - p.x) * p.nx + (k.y - p.y) * p.ny) < 1.5 * M) return { type: 'break', target: p, label: 'Break pallet' };
    if (!k.power.noVault?.()) for (const w of this.map.windows) if (near(w, 1.7) && w.killerBlockedUntil <= this.time && Math.abs((k.x - w.x) * w.nx + (k.y - w.y) * w.ny) > 0.4 * M) return { type: 'vault', target: w, label: 'Vault' };
    if (!cloaked) for (const g of this.map.gens) if (!g.done && g.charges > 0 && !g.regressing && g.repairers.size === 0 && near(g, 2.3)) return { type: 'kick', target: g, label: 'Damage generator' };
    if (!cloaked && this.hatch.state === 'open' && near(this.hatch, 1.8)) return { type: 'closeHatch', target: this.hatch, label: 'Close hatch' };
    return null;
  }
  separateKiller(k = this.killer) {
    for (const s of this.survivors) {
      if (!s.standing || s.vault) continue;
      const d = dist(k.x, k.y, s.x, s.y), r = k.r + s.r - 2;
      if (d < r && d > 0.01) { const p = (r - d); k.x += (k.x - s.x) / d * p; k.y += (k.y - s.y) / d * p; }
    }
  }
  separateSurvivors() {
    const L = this.survivors.filter((s) => s.standing && !s.vault);
    for (let i = 0; i < L.length; i++) for (let j = i + 1; j < L.length; j++) {
      const a = L[i], b = L[j]; const d = dist(a.x, a.y, b.x, b.y), r = a.r + b.r;
      if (d < r && d > 0.01) { const p = (r - d) / 2; const nx = (a.x - b.x) / d, ny = (a.y - b.y) / d; a.x += nx * p; a.y += ny * p; b.x -= nx * p; b.y -= ny * p; }
    }
  }

  kickGenerator(g, k = this.killer) {
    if (g.done) return;
    g.charges = Math.max(0, g.charges - GEN_CHARGES * 0.025); g.regressing = g.charges > 0; g.kickT = this.time;
    k.stats.kicks++;
    this.sound('killer_action', g.x, g.y, 30 * M, { what: 'kick' });
    this.events.emit(EV.GENERATOR_DAMAGED, { gen: g, killer: k }); this.audio?.play('kick'); this.fx.sparks(g.x, g.y);
  }
  breakPallet(p, k = this.killer) {
    if (p.state !== 'down') return;
    p.state = 'broken'; this.world.remove(p.solid); k.stats.palletsBroken++;
    this.bb.model.palletBreaks++; p.breakCounted = true;
    this.sound('killer_action', p.x, p.y, 36 * M, { what: 'break' });
    this.events.emit(EV.PALLET_DESTROYED, { pallet: p, killer: k }); this.fx.splinters(p.x, p.y); this.fx.shake(5); this.audio?.play('break');
    this.endBloodlust(k);
  }
  pickUp(s, k = this.killer) {
    if (s.health !== 'dying' || this.carrier(s)) return;
    // Decisive Strike: anti-tunnel escape with a skill check
    if (s.hasPerk('decisive_strike') && this.time - s.unhookedAt < 40 && !s.dsUsed) {
      s.dsUsed = true;
      const p = 0.45 + 0.5 * (s.ai ? s.ai.params.dodge : 0.5);
      if (Math.random() < p) { s.health = 'injured'; s.status.add('deepWound', { duration: 20 }); k.stun(this, 4, 'ds'); this.fx.text(s.x, s.y - 30, 'DECISIVE STRIKE', '#ffd27a'); s.ai?.note('Hit Decisive Strike', 'ds'); this.notifyK(`${s.name} used Decisive Strike`); return; }
    }
    s.health = 'carried'; s.wiggle = 0; s.carriedSince = this.time; s.stopAction(this); s.healers.clear();
    k.carrying = s;
    this.events.emit(EV.SURVIVOR_PICKED_UP, { survivor: s, killer: k });
    this.audio?.play('pickup');
  }
  dropCarried(reason, k = this.killer) {
    const s = k.carrying; if (!s) return;
    k.carrying = null;
    const a = k.facing + Math.PI; s.x = k.x + Math.cos(a) * 0.9 * M; s.y = k.y + Math.sin(a) * 0.9 * M;
    this.world.resolve(s, s.r);
    if (reason === 'drop') { s.health = 'dying'; }
    else { s.health = 'injured'; s.status.add('haste', { duration: 3, value: 0.3, source: 'escape' }); this.fx.text(s.x, s.y - 30, reason === 'wiggle' ? 'WIGGLED FREE' : 'SAVED', '#ffd27a'); s.ai?.note(reason === 'wiggle' ? 'Wiggled free' : 'Got saved from the carry', 'wiggle'); }
    this.events.emit(EV.SURVIVOR_DROPPED, { survivor: s, reason, killer: k });
    if (reason === 'wiggle') k.stun(this, 3, 'wiggle');
  }
  updateCarry(dt) { for (const k of this.killers) this.updateCarryFor(dt, k); }
  updateCarryFor(dt, k) {
    const s = k.carrying; if (!s) return;
    s.x = k.x - Math.cos(k.facing) * 0.4 * M; s.y = k.y - Math.sin(k.facing) * 0.4 * M;
    const breakout = this.survivors.some((o) => o !== s && o.standing && o.hasPerk('breakout') && dist(o.x, o.y, k.x, k.y) < 6 * M);
    // Bots wiggle automatically; the human survivor's controller adds wiggle by alternating keys.
    if (!s.isPlayer) s.wiggle += dt / (16 * this.perks.killerProduct('wiggleMult', k)) * (breakout ? 1.2 : 1) * (s.hasPerk('boil_over') ? 1.25 : 1);
    else s.wiggleMult = (breakout ? 1.2 : 1) * (s.hasPerk('boil_over') ? 1.25 : 1) / this.perks.killerProduct('wiggleMult', k);
    if (s.wiggle >= 1) this.dropCarried('wiggle', k);
  }
  hookSurvivor(h, caged = false, k = this.killer) {
    const s = k.carrying; if (!s || h.survivor) return;
    // 2v8: hooking sends the survivor to a cage somewhere else on the map
    if (this.is2v8 && !caged) { const c = pickCage(this, h); if (c && c !== h) { c.cage = true; return this.hookSurvivor(c, true, k); } }
    if (!caged) h.cage = false;
    k.carrying = null; k.stats.hooks++;
    s.health = 'hooked'; s.hook = h; h.survivor = s; s.x = h.x; s.y = h.y + 0.5 * M; s.hookCount++; s.hookedAt = this.time; s.unhookAttempts = 0;
    this.bb.model.hooks++; this.lastHook = { s, t: this.time, h, campT: 0 };
    if (s.hookStage >= 2) { this.events.emit(EV.SURVIVOR_HOOKED, { survivor: s, hook: h, killer: k }); this.killSurvivor(s, 'hook'); return; }
    s.hookStage++; s.hookTimer = 0;
    this.events.emit(EV.SURVIVOR_HOOKED, { survivor: s, hook: h, killer: k });
    this.fx.burst(h.x, h.y, 30, '#c9a46a', 120, 1.5, 2.5, 1); this.audio?.play('hook');
    this.notifyK(`${s.name} ${caged ? 'caged' : 'hooked'} (${s.hookStage === 1 ? 'first' : 'second'} stage)`);
  }
  updateHooks(dt) {
    for (const s of this.survivors) {
      if (s.health !== 'hooked') continue;
      if ((s.hookPauseUntil || 0) > this.time) continue;
      s.hookTimer += dt;
      if (s.hookStage >= 2) {
        // Struggle phase: skill checks; failures cost time
        s._strT = (s._strT ?? 4) - dt;
        if (s._strT <= 0) { s._strT = 3 + Math.random() * 4; if (s.isPlayer) this.startPlayerCheck(s, 'struggle', s.hook); else { const fail = s.ai ? s.ai.params.scFail * 0.6 : 0.1; if (Math.random() < fail) s.hookTimer += 8; } }
      }
      if (s.hookTimer >= this.hookStageTime) {
        if (s.hookStage === 1) { s.hookStage = 2; s.hookTimer = 0; this.events.emit(EV.HOOK_STAGE, { survivor: s, stage: 2 }); this.notifyK(`${s.name} entered struggle phase`); }
        else this.killSurvivor(s, 'hook');
      }
    }
    // camping tracker (survivors infer it from the killer lingering by the hook)
    if (this.lastHook && this.lastHook.s.health === 'hooked' && this.killerDist(this.lastHook.h.x, this.lastHook.h.y) < 12 * M) {
      this.lastHook.campT += dt; if (this.lastHook.campT > 15 && !this.lastHook.counted) { this.lastHook.counted = true; this.bb.model.camping++; }
    }
  }
  unhook(s, rescuer) {
    if (s.health !== 'hooked') return;
    const h = s.hook; h.survivor = null; s.hook = null;
    s.health = 'injured'; s.healProgress = 0; s.unhookedAt = this.time;
    if (rescuer) { const a = Math.atan2(rescuer.y - h.y, rescuer.x - h.x); s.x = h.x + Math.cos(a) * 0.9 * M; s.y = h.y + Math.sin(a) * 0.9 * M; rescuer.stats.unhooks++; }
    else { s.x = h.x; s.y = h.y + 0.9 * M; }
    this.world.resolve(s, s.r);
    s.status.add('endurance', { duration: 10, source: 'unhook' }); s.status.add('haste', { duration: 10, value: 0.07, source: 'unhook' });
    this.events.emit(EV.SURVIVOR_UNHOOKED, { survivor: s, rescuer });
    this.notifyK(rescuer ? `${rescuer.name} unhooked ${s.name}` : `${s.name} escaped the hook`, '#cfe3e3');
    this.audio?.play('unhook');
  }
  selfUnhook(s) { this.unhook(s, null); }
  selfUnhookAttempt(s) {
    s.unhookAttempts++;
    if (Math.random() < (s.hasPerk('slippery_meat') ? 0.12 : 0.04)) { this.unhook(s, null); s.ai?.note('Lucky self-unhook!', 'luck'); }
    else s.hookTimer += 20;
  }
  killSurvivor(s, how) {
    if (s.health === 'dead') return;
    if (s.hook) { s.hook.survivor = null; s.hook = null; }
    for (const k of this.killers) if (k.carrying === s) k.carrying = null;
    if (s.trap) { s.trap.victim = null; s.trap.state = 'unset'; s.trap = null; }
    s.stopAction(this); s.health = 'dead'; s.outcome = how === 'hook' ? 'Sacrificed' : how === 'collapse' ? 'Killed by the Fog' : 'Killed';
    this.bb.release(s);
    this.events.emit(EV.SURVIVOR_KILLED, { survivor: s, how });
    this.fx.burst(s.x, s.y, 50, '#6b0b14', 90, 2, 3, 1); this.fx.shake(6);
    this.notifyK(`${s.name} ${s.outcome.toLowerCase()}`, '#e05a5a');
    this.checkHatch();
  }
  healed(t, healer) {
    if (t.health === 'dying') t.health = 'injured';
    else if (t.health === 'injured') { t.health = 'healthy'; t.status.remove('deepWound'); t.status.remove('mangled'); t.status.remove('hemorrhage'); }
    t.healProgress = 0; t.recovery = 0;
    if (healer && healer !== t) healer.stats.heals++;
    this.events.emit(EV.SURVIVOR_HEALED, { survivor: t, healer });
  }

  // ------------------------------------------------------------ generators
  // Skill checks. Bots roll against their difficulty; the human survivor gets a real timed
  // check (needle + good/great zones) that the controller resolves. Both go through
  // resolveSkillCheck so perks and penalties are identical.
  skillCheck(s, g, kind = 'repair') {
    this.events.emit(EV.SKILL_CHECK, { survivor: s, gen: g, kind });
    const mod = this.perks.skillCheckMods(s, g);
    if (s.isPlayer) { this.startPlayerCheck(s, kind, g, mod); return; }
    const P = s.ai ? s.ai.params : { scFail: 0.1, scGreat: 0.3 };
    const fail = mod.forceGreat ? 0 : Math.min(0.9, P.scFail + mod.fail), great = mod.forceGreat ? 1 : Math.max(0, P.scGreat + mod.great);
    const r = Math.random();
    this.resolveSkillCheck(s, g, r < fail ? 'fail' : (mod.forceGreat || r < fail + great) ? 'great' : 'good', mod, kind);
  }
  startPlayerCheck(s, kind, target, mod = { fail: 0, great: 0, failExtra: 0, forceGreat: false }) {
    if (s.skillCheck) return;
    // harder checks from killer perks shrink the zones; Stake Out style tokens make any success great
    const shrink = Math.max(0.35, 1 - mod.fail * 2.2);
    s.skillCheck = { kind, target, mod, t0: this.time, warn: 0.55, dur: 1.15, zone: 0.55 + Math.random() * 0.3, good: 0.13 * shrink, great: 0.04 * shrink };
    this.audio?.play('ui');
  }
  // Called by the player controller when SPACE is pressed (or the needle runs out).
  // forced: a result judged on a network client (its needle is what the player saw)
  answerPlayerCheck(s, pressed, forced = null) {
    const c = s.skillCheck; if (!c) return null;
    const pos = (this.time - c.t0 - c.warn) / c.dur;
    let res = 'fail';
    if (forced) res = forced === 'great' || forced === 'good' ? (c.mod.forceGreat ? 'great' : forced) : 'fail';
    else if (pressed && pos >= c.zone && pos <= c.zone + c.good) res = c.mod.forceGreat || pos <= c.zone + c.great ? 'great' : 'good';
    s.skillCheck = null; s.lastCheck = { res, t: this.time };
    if (c.kind === 'repair') this.resolveSkillCheck(s, c.target, res, c.mod, 'repair');
    else if (c.kind === 'heal') { if (res === 'fail') { const t = c.target; t.healProgress = Math.max(0, t.healProgress - 0.1); this.addGrunt(t); this.loudNoise(s.x, s.y, 'skillcheck'); this.audio?.play('genfail'); } else if (res === 'great') c.target.healProgress = Math.min(0.999, c.target.healProgress + 0.02); }
    else if (c.kind === 'struggle') { if (res === 'fail') { s.hookTimer += 8; this.audio?.play('genfail'); } }
    else if (c.kind === 'wiggle') { if (res !== 'fail') s.wiggle += 0.05; }
    else if (c.kind === 'device') c.target.owner?.power?.onPlayerCheck?.(this, s, c.target, res);
    this.events.emit(res === 'fail' ? EV.SKILL_CHECK_FAILURE : EV.SKILL_CHECK_SUCCESS, { survivor: s, gen: c.kind === 'repair' ? c.target : null, great: res === 'great', kind: c.kind });
    return res;
  }
  resolveSkillCheck(s, g, res, mod, kind = 'repair') {
    const tech = s.hasPerk('technician');
    if (res === 'fail') {
      g.charges = Math.max(0, g.charges - GEN_CHARGES * ((tech ? 0.05 : 0.1) + mod.failExtra));
      if (!tech) this.loudNoise(g.x, g.y, 'skillcheck'); this.fx.sparks(g.x, g.y); this.audio?.play('genfail', { x: g.x, y: g.y });
      if (!s.isPlayer) this.events.emit(EV.SKILL_CHECK_FAILURE, { survivor: s, gen: g });
      s.stopAction(this); if (s.ai) s.ai.tT = 0.6;
    } else {
      const isGreat = res === 'great';
      if (isGreat) g.charges += GEN_CHARGES * 0.01;
      if (!s.isPlayer) this.events.emit(EV.SKILL_CHECK_SUCCESS, { survivor: s, gen: g, great: isGreat });
    }
  }
  completeGenerator(g) {
    if (g.done) return;
    g.done = true; g.charges = GEN_CHARGES; g.regressing = false;
    for (const s of [...g.repairers]) s.stopAction(this);
    this.gensDone++;
    this.events.emit(EV.GENERATOR_REPAIRED, { gen: g });
    this.fx.gen(g.x, g.y); this.audio?.play('gendone');
    if (this.gensDone >= this.gensNeeded && !this.gatesPowered) this.powerGates();
  }
  powerGates() {
    this.gatesPowered = true;
    for (const g of this.map.gates) g.powered = true;
    for (const g of this.map.gens) if (!g.done) { g.blockedUntil = Infinity; }
    this.events.emit(EV.EXIT_GATES_POWERED, {});
    this.notifyK('The exit gates are powered', '#ffd27a'); this.audio?.play('gates');
  }
  updateGens(dt) {
    for (const g of this.map.gens) {
      if (g.done) continue;
      if (g.repairers.size) { g.regressing = false; for (const s of g.repairers) if (s.action?.target !== g) g.repairers.delete(s); }
      else if (g.regressing) { g.charges -= dt * 0.25; if (g.charges <= 0) { g.charges = 0; g.regressing = false; } }
    }
  }

  // ------------------------------------------------------------ pallets & vaults
  dropPallet(p, s) {
    p.dropping = false; if (p.state !== 'up') return;
    p.state = 'down'; p.dropT = this.time; p.dropper = s; s.stats.pallets++;
    this.world.add(p.solid);
    // who stands in the stun zone is decided before anyone is pushed out of the footprint
    const inZone = new Set(this.killers.filter((k) => p.stunZone(k)));
    // push entities out of the pallet footprint toward their side
    for (const e of [...this.killers, ...this.survivors.filter((q) => q.standing)]) {
      if (e.x > p.rect.x - e.r && e.x < p.rect.x + p.rect.w + e.r && e.y > p.rect.y - e.r && e.y < p.rect.y + p.rect.h + e.r) {
        const side = e === s ? p.sideOf(s) : 1 - p.sideOf(s);
        const sp = p.sides(0.75 * M + e.r)[side]; e.x = sp.x + (e.x - p.x) * (p.alongX ? 1 : 0) * 0.5; e.y = sp.y + (e.y - p.y) * (p.alongX ? 0 : 1) * 0.5;
      }
    }
    for (const n of this.minions) if (n.stunnable && p.stunZone(n) && n.stunT <= 0) { n.stunT = n.stunTime ?? 3; n.onStun?.(this, s); this.fx.text(n.x, n.y - 30, 'STUNNED', '#ffd27a'); s.ai?.note(`Stunned the ${n.label} with a pallet`, 'mstun'); }
    for (const k of this.killers) if (inZone.has(k) && !k.action && k.stunT <= 0 && k.power.stunImmune?.(this)) { inZone.delete(k); k.power.onStunImmune?.(this, p, s); s.ai?.note('Pallet broke on the Overclocked killer', 'oc'); }
    for (const k of this.killers) if (inZone.has(k) && !k.action && k.stunT <= 0 && !k.power.stunImmune?.(this)) {
      const sf = k.perks.find((q) => q.id === 'spirit_fury' && q.data.ready);
      this.lastStunner = s; k.stun(this, sf ? 1.2 : 2, 'pallet');
      if (sf) { sf.data.ready = false; sf.tokens = 0; sf.active = false; this.perks.proc(sf, 'Spirit Fury'); k.stunT = Math.min(k.stunT, 0.8); this._sfBreak = p; } s.stats.stuns++; this.fx.shake(10); this.fx.text(k.x, k.y - 40, 'STUNNED', '#ffd27a');
      this.bb.model.palletBreaks += 0; s.ai?.note('Pallet stun!', 'stunok');
    }
    if (!this.quiet(s)) this.loudNoise(p.x, p.y, 'pallet');
    this.events.emit(EV.PALLET_DROPPED, { pallet: p, survivor: s }); this.fx.splinters(p.x, p.y); this.audio?.play('pallet');
  }
  updatePallets() {
    if (this._sfBreak && this.killers.every((k) => k.stunT < 0.4)) { const p = this._sfBreak; this._sfBreak = null; if (p.state === 'down') { this.breakPallet(p); this.fx.text(p.x, p.y - 30, 'SPIRIT FURY', '#e05a5a'); } }
    for (const p of this.map.pallets) if (p.state === 'down' && !p.respectCounted && this.time - p.dropT > 8) { p.respectCounted = true; this.bb.model.palletRespects++; }
  }
  onVault(s, obj, quality) {
    s.stats.vaults++;
    if (quality === 'fast' && !this.quiet(s)) this.loudNoise(obj.x, obj.y, 'vault');
    if (obj.kind === 'window') {
      obj.vaultLog = (obj.vaultLog || []).filter((v) => this.time - v.t < 60);
      obj.vaultLog.push({ t: this.time, s });
      if (obj.vaultLog.filter((v) => v.s === s).length >= 3 && this.inChase(s)) { obj.blockedUntil = this.time + 16; obj.vaultLog = []; s.ai?.note('Window got blocked after 3 vaults', 'winblock'); }
    }
    this.events.emit(EV.VAULT_PERFORMED, { who: s, obj, quality });
    if (quality === 'fast') this.audio?.play('vault');
  }

  // ------------------------------------------------------------ traps
  updateTraps(dt) {
    for (const t of this.traps) {
      if (t.state !== 'set') continue;
      for (const s of this.survivors) {
        if (!s.standing || s.vault || s.trap) continue;
        if (dist(s.x, s.y, t.x, t.y) < 0.55 * M) { this.trapSurvivor(t, s); break; }
      }
    }
  }
  trapSurvivor(t, s) {
    t.state = 'triggered'; t.victim = s; s.trap = t; s.trapAttempts = 0; s.stopAction(this);
    s.x = t.x; s.y = t.y;
    if (s.health === 'healthy') { s.health = 'injured'; this.events.emit(EV.SURVIVOR_INJURED, { survivor: s, source: 'trap' }); }
    else { s.goDown(this, 'trap'); }
    if (this.killers.some((k) => k.power.serrated)) { s.status.add('mangled', { duration: 90 }); s.status.add('hemorrhage', { duration: 90 }); }
    this.bb.model.trapCatches++;
    for (const o of this.survivors) if (o.ai && o !== s && Math.random() < o.ai.params.comms) { o.ai.memory.traps.set(t.id, { x: t.x, y: t.y, trap: t, t: this.time }); o.ai._trapVersion++; }
    this.loudNoise(t.x, t.y, 'trap'); this.fx.shake(5); this.audio?.play('snap');
    this.notifyK(`${s.name} caught in a trap!`, '#ffd27a');
    this.events.emit(EV.TRAP_TRIGGERED, { trap: t, survivor: s });
  }
  trapEscapeAttempt(s) {
    if (!s.trap) return;
    const coil = this.killers.some((k) => k.power.ironCoil) ? 0.6 : 1;
    const ch = (0.16 + 0.06 * s.trapAttempts) * coil;
    s.trapAttempts++;
    if (Math.random() < ch) this.releaseTrap(s, null);
  }
  releaseTrap(s, rescuer) {
    const t = s.trap; if (!t) return;
    t.state = 'unset'; t.victim = null; s.trap = null;
    if (rescuer) rescuer.stats.unhooks += 0;
    this.notifyK(`${s.name} escaped a trap`, '#cfe3e3');
  }
  disarmTrap(t, s) { if (t.state === 'set') { t.state = 'unset'; this.events.emit(EV.TRAP_DISARMED, { trap: t, survivor: s }); s.ai?.note('Disarmed a trap', 'disarm'); } }
  checkKillerTraps(k = this.killer) {
    if (k.power.id !== 'traps') return;
    for (const t of this.traps) if (t.state === 'set' && this.time - t.setT > 2 && dist(k.x, k.y, t.x, t.y) < 0.4 * M) { t.state = 'unset'; k.stun(this, 2, 'trap'); this.fx.text(k.x, k.y - 40, 'OWN TRAP', '#ffd27a'); }
  }

  // ------------------------------------------------------------ misc objectives
  openChest(c, s) { c.opened = true; c.user = null; if (!s.item || s.item.charges <= 0) { const pool = ['medkit', 'toolbox', 'flashlight', 'medkit', 'toolbox', 'key']; const id = pool[Math.floor(Math.random() * pool.length)]; s.item = makeItem(id); s.ai?.note('Found a ' + s.item.name, 'chest'); } s.stats.chests++; this.events.emit(EV.CHEST_OPENED, { chest: c, survivor: s }); }
  cleanseTotem(t, s) { const hex = t.hex; t.cleansed = true; t.user = null; s.stats.totems++; this.events.emit(EV.TOTEM_CLEANSED, { totem: t, survivor: s, hex }); if (hex) { this.notifyK(`Your hex was cleansed`, '#e05a5a'); s.ai?.note('Cleansed the hex totem', 'hex'); } }
  flashlightTick(s, dt) {
    // flashlights burn away Phantasm traps caught in the beam
    for (const d of this.devices) if (d.kind === 'phantasm' && dist(s.x, s.y, d.x, d.y) < 7 * M && angleDiff(Math.atan2(d.y - s.y, d.x - s.x), s.facing) < 0.45 && this.world.losClear(s.x, s.y, d.x, d.y)) {
      d.data.burn = (d.data.burn || 0) + dt; if (d.data.burn > 0.8) { this.removeDevice(d); this.fx.burst(d.x, d.y, 12, '#e8e0a0', 60, 0.6, 2, 2); s.ai?.note('Burned a Phantasm trap with the flashlight', 'burn'); s.stopAction(this); return; }
    }
    const k = this.killers.reduce((b, q) => (angleDiff(Math.atan2(q.y - s.y, q.x - s.x), s.facing) < angleDiff(Math.atan2(b.y - s.y, b.x - s.x), s.facing) ? q : b), this.killers[0]);
    if (this.perks.killerHas('flashImmune', k) || k.power.flashImmune?.(this)) return; const d = dist(s.x, s.y, k.x, k.y);
    const toK = Math.atan2(k.y - s.y, k.x - s.x);
    const ok = d < 7 * M && angleDiff(toK, s.facing) < 0.4 * (s.item?.beam || 1) && angleDiff(k.facing, toK + Math.PI) < 1.25 && this.world.losClear(s.x, s.y, k.x, k.y);
    k.flashAcc = Math.max(0, (k.flashAcc || 0) + (ok ? dt * (s.hasPerk('champion_of_light') ? 1.5 : 1) : -dt * 0.5));
    if (k.flashAcc > 0.6) {
      k.flashAcc = 0; k.blindT = 2.5 * this.perks.killerProduct('stunMult', k); k.status.add('blindness', { duration: 6 }); this.fx.flash = 1; this.fx.flashColor = '255,250,230';
      if (k.carrying) { this.dropCarried('flashlight', k); s.ai?.note('Flashlight save!', 'flash'); }
      else if (k.action?.type === 'pickup') { k.action = null; s.ai?.note('Blinded the killer during a pickup', 'flash'); }
      s.stopAction(this);
    }
  }
  openGate(g, s) {
    if (g.open) return;
    g.open = true; g.user = null; this.world.remove(g.solid);
    this.events.emit(EV.EXIT_GATE_OPENED, { gate: g, survivor: s }); this.notifyK(`${g.name} is open`, '#ffd27a');
    this.loudNoise(g.x, g.y, 'gate');
    if (!this.collapse) this.startCollapse();
  }
  startCollapse() { this.collapse = true; this.collapseT = COLLAPSE_TIME; this.setState('ENDGAME_COLLAPSE'); this.events.emit(EV.ENDGAME_COLLAPSE, {}); this.audio?.play('collapse'); }
  updateGates(dt) {
    for (const s of this.survivors) {
      if (!s.standing) continue;
      for (const g of this.map.gates) {
        if (!g.open || (this.gatesSealedUntil || 0) > this.time || (s.gateBlockUntil || 0) > this.time || s.status.has('chained') || s.status.has('grabbed')) continue; const r = g.exitRect;
        if (s.x > r.x && s.x < r.x + r.w && s.y > r.y && s.y < r.y + r.h) this.escape(s, 'gate');
      }
    }
    if (this.collapse) {
      const slow = this.survivors.some((s) => s.health === 'hooked' || s.health === 'dying' || s.health === 'carried');
      this.collapseT -= dt * (slow ? 0.5 : 1);
      if (this.collapseT <= 0) { for (const s of this.survivors) if (s.alive) this.killSurvivor(s, 'collapse'); }
    }
  }
  escape(s, how) {
    if (!s.alive) return;
    s.stopAction(this); if (s.trap) s.trap = null;
    s.health = 'escaped'; s.outcome = how === 'hatch' ? 'Escaped (hatch)' : 'Escaped'; s.stats.escapes++;
    this.bb.release(s);
    this.events.emit(EV.SURVIVOR_ESCAPED, { survivor: s, how }); this.notifyK(`${s.name} escaped`, '#9fd0a0');
    this.checkHatch();
  }
  checkHatch() {
    const alive = this.aliveCount();
    if (this.hatch.state === 'shut') return;
    if (alive === 1 && this.hatch.state !== 'open' && !this.cfg.modifiers.noHatch) { this.hatch.state = 'open'; this.events.emit(EV.HATCH_OPENED, {}); this.notifyK('The hatch has opened', '#9fd0d0'); }
    else if (alive === 2 && this.hatch.state === 'hidden' && this.survivors.some((s) => s.alive && s.item?.id === 'key')) this.hatch.state = 'closed';
  }
  openHatchWithKey(s) { if (this.hatch.state === 'closed') { this.hatch.state = 'open'; s.item.charges = 0; s.ai?.note('Opened the hatch with a key', 'key'); } }
  escapeHatch(s) { if (this.hatch.state === 'open' && dist(s.x, s.y, this.hatch.x, this.hatch.y) < 1.2 * M) this.escape(s, 'hatch'); }
  closeHatch() {
    if (this.hatch.state !== 'open') return;
    this.hatch.state = 'shut'; this.events.emit(EV.HATCH_CLOSED, {}); this.notifyK('Hatch closed — gates powered', '#ffd27a');
    if (!this.gatesPowered) this.powerGates();
    if (!this.collapse) this.startCollapse();
  }

  // ------------------------------------------------------------ chase (killer side)
  updateChase(dt) { for (const k of this.killers) this.updateChaseFor(dt, k); }
  updateChaseFor(dt, k) {
    let tgt = k.chaseTarget;
    if (tgt && (!tgt.standing || dist(k.x, k.y, tgt.x, tgt.y) > 28 * M)) { this.endChase(k); tgt = null; }
    if (tgt) {
      const los = this.world.losClear(k.x, k.y, tgt.x, tgt.y);
      k.chaseLOS = los ? 0 : (k.chaseLOS || 0) + dt;
      if (k.chaseLOS > 5) { this.endChase(k); return; }
      this.bb.reportChase(tgt); // teammates can see who is being chased (HUD icon)
      k.chaseT += dt * (this.cfg.modifiers.bloodMoon ? 2 : 1);
      const tier = k.chaseT > 45 ? 3 : k.chaseT > 30 ? 2 : k.chaseT > 15 ? 1 : 0;
      if (tier > k.bloodlust) { k.bloodlust = tier; this.fx.text(k.x, k.y - 44, `BLOODLUST ${'I'.repeat(tier)}`, '#d33'); }
      tgt.stats.chaseTime += 0;
      this.score.hunter += dt * 4;
    } else {
      for (const s of this.survivors) {
        if (!s.standing) continue;
        const d = dist(k.x, k.y, s.x, s.y); if (d > 14 * M) continue;
        if (!this.world.losClear(k.x, k.y, s.x, s.y)) continue;
        if (angleDiff(Math.atan2(s.y - k.y, s.x - k.x), k.facing) > 1.2) continue;
        if (s.moveMode === 'crouch' && this.map.isGrass(s.x, s.y) && d > 4 * M) continue;
        s._spotT = (s._spotT || 0) + dt;
        if (s._spotT > 0.8) { k.chaseTarget = s; k.chaseLOS = 0; k.chaseT = 0; this.events.emit(EV.CHASE_START, { survivor: s, killer: k }); if (k.isPlayer || s.isPlayer) this.audio?.play('chase'); break; }
      }
    }
  }
  endChase(k = this.killer) { if (k.chaseTarget) { this.events.emit(EV.CHASE_END, { survivor: k.chaseTarget, killer: k }); k.chaseTarget._spotT = 0; } k.chaseTarget = null; this.endBloodlust(k); }
  endChaseOn(s, k) { this.endBloodlust(k || this.killers.find((q) => q.chaseTarget === s) || this.killer); }
  endBloodlust(k = this.killer) { k.bloodlust = 0; k.chaseT = 0; }

  // ------------------------------------------------------------ results
  results() {
    const kills = this.survivors.filter((s) => s.health === 'dead').length;
    const esc = this.survivors.filter((s) => s.health === 'escaped').length;
    const n = this.survivors.length;
    const grade = kills === n ? 'Merciless Killer' : kills >= Math.ceil(n * 0.75) ? 'Ruthless Killer' : kills >= Math.ceil(n / 2) ? 'Brutal Killer' : kills >= 1 ? 'Ruthless... almost' : 'Entity Displeased';
    return { kills, escapes: esc, total: n, grade, score: { ...this.score }, time: this.time, survivors: this.survivors.map((s) => ({ name: s.name, color: s.color, cls: s.cls || null, isPlayer: !!s.isPlayer, outcome: s.outcome || 'Escaped', difficulty: s.difficulty, archetype: s.personality.label, perks: s.perks.map((p) => p.def.name), item: s.item?.name || '—', stats: s.stats })), log: this.aiLog.slice(-40), killerStats: this.killer.stats, killers: this.killers.map((k) => ({ name: k.def.name, accent: k.def.accent, cls: k.cls || null, bot: !!k.bot, difficulty: k.bot ? k.botDifficulty : null, stats: k.stats })), mode: this.cfg.mode, playerRole: this.cfg.playerRole, player: this.player && this.player.kind === 'survivor' ? { name: this.player.name, outcome: this.player.outcome || (this.player.alive ? 'Alive' : '—') } : null };
  }
}

return { MATCH_STATES, DEFAULT_CONFIG, Match };
};
__defs["js/player/survivorController.js"] = function(){
// Human survivor controller. Turns the player's input into the same intents the survivor AI
// uses (moveTo / moveMode / startAction / startVault), so every rule, perk and status effect
// works identically for the player and the bots.
//   WASD move · SHIFT walk · CTRL / C crouch · SPACE vault / drop pallet / skill checks
//   E (hold) interact with the nearest valid object · Q / RMB (hold) use item · F active perk
//   Carried: alternate A / D to wiggle · Hooked: E attempts an escape (first stage)
const { M, dist, angleDiff } = __req("js/core/util.js");
const { ACTION_DEF } = __req("js/entities/survivor.js");

// Actions that continue only while E (or the item key) is held
const HOLD = new Set(['repair', 'heal', 'healSelf', 'unhook', 'openGate', 'searchChest', 'cleanse', 'disarm', 'freeTrapped', 'recover', 'mend', 'openHatch', 'device', 'breakChain', 'solveBox', 'spray', 'flashlight']);

class SurvivorController {
  constructor(m, s) {
    this.m = m; this.s = s; m.playerCtl ||= this; (m.playerCtls ||= []).push(this); this.grace = 0;
    this.in = { mx: 0, my: 0, walk: false, crouch: false, aimX: s.x + 40, aimY: s.y, e: false, eP: false, space: false, spaceP: false, item: false, itemP: false, perkP: false, left: false, right: false, leftP: false, rightP: false };
    this.held = null; this.lastWig = null; this.wigT = 0;
  }
  setInput(i) { Object.assign(this.in, i); }

  update(dt) {
    const m = this.m, s = this.s, I = this.in;
    if (!s.alive) { this.clear(); return; }
    s.lookTarget = { x: I.aimX, y: I.aimY };
    // --- skill checks first: SPACE answers them, the needle running out fails them
    if (s.skillCheck) {
      const c = s.skillCheck; const pos = (m.time - c.t0 - c.warn) / c.dur;
      if (I.spaceP && pos > -0.2) { m.answerPlayerCheck(s, true); I.spaceP = false; }
      else if (pos > 1.05 + this.grace) m.answerPlayerCheck(s, false);
      else if (c.kind === 'repair' && s.action?.type !== 'repair') s.skillCheck = null;
      else if (c.kind === 'heal' && s.action?.type !== 'heal') s.skillCheck = null;
    }
    // device interactions with a skill check (hacking a drone): one check halfway through
    const a = s.action;
    if (a?.type === 'device' && a.target.owner?.power?.onPlayerCheck && !a.checked && a.t > a.target.useTime * 0.45) { a.checked = true; m.startPlayerCheck(s, 'device', a.target); }
    switch (s.health) {
      case 'carried': return this.wiggle(dt);
      case 'hooked': return this.hooked();
      case 'dying': return this.downed();
    }
    if (s.trap) { if (I.eP && !s.action) s.startAction('escapeTrap', null, m); this.clear(); return; }
    // --- movement
    const moving = Math.hypot(I.mx, I.my) > 0.1;
    s.moveMode = I.crouch ? 'crouch' : I.walk ? 'walk' : 'run';
    if (moving) {
      // moving cancels most interactions, as in the original game
      if (s.action && !['flashlight', 'dropPallet', 'useEmp'].includes(s.action.type)) s.stopAction(m);
      s.moveTo = { x: s.x + I.mx * 2 * M, y: s.y + I.my * 2 * M };
    } else s.moveTo = null;
    // --- SPACE: vault / pallet
    if (I.spaceP && !s.vault) {
      const c = this.spaceContext();
      if (c?.type === 'vault') s.startVault(c.target, this.vaultQuality(c.target), m);
      else if (c?.type === 'dropPallet') s.startAction('dropPallet', c.target, m);
    }
    // --- E: hold to interact
    if (I.e) {
      const cur = s.action;
      if (!cur || !HOLD.has(cur.type)) {
        const c = this.eContext();
        if (c) {
          if (c.type === 'escapeHatch') m.escapeHatch(s);
          else if (c.type === 'useEmp' || c.type === 'spray') s.startAction(c.type, null, m);
          else s.startAction(c.type, c.target, m);
          this.held = 'e';
        }
      }
    } else if (this.held === 'e' && s.action && HOLD.has(s.action.type)) { s.stopAction(m); this.held = null; }
    // --- item (Q / RMB)
    const it = s.item;
    if (I.item && it && it.charges > 0) {
      if (it.id === 'flashlight') { if (s.action?.type !== 'flashlight') s.startAction('flashlight', null, m); s.facing = Math.atan2(I.aimY - s.y, I.aimX - s.x); this.held = 'item'; }
      else if (it.id === 'medkit' && s.health === 'injured' && s.action?.type !== 'healSelf') { s.startAction('healSelf', null, m); this.held = 'item'; }
    } else if (this.held === 'item' && s.action && (s.action.type === 'flashlight' || s.action.type === 'healSelf')) { s.stopAction(m); this.held = null; }
    if (I.itemP) {
      if (s.emp) s.startAction('useEmp', null, m);
      else if ((s.sprays || 0) > 0 && s.status.has('infected')) s.startAction('spray', null, m);
      else if (s.vaccines > 0 && s.status.has('contaminated')) s.startAction('vaccinate', null, m);
    }
    // --- F: active perk (first one that can fire right now)
    if (I.perkP) for (const p of s.perks) if (p.def.activate && m.perks.activate(s, p.id)) { m.warn(s, `${p.def.name}!`); break; }
    this.clearPresses();
  }
  clear() { this.clearPresses(); this.s.moveTo = null; }
  clearPresses() { const I = this.in; I.eP = I.spaceP = I.itemP = I.perkP = I.leftP = I.rightP = false; }

  wiggle(dt) {
    const s = this.s, I = this.in;
    // alternate A / D: each correct alternation keeps the wiggle going for a moment
    const press = I.leftP ? 'L' : I.rightP ? 'R' : null;
    if (press && press !== this.lastWig) { this.lastWig = press; this.wigT = 0.4; }
    this.wigT -= dt;
    if (this.wigT > 0) s.wiggle += dt / 16 * (s.wiggleMult ?? 1) * 1.15;
    this.clearPresses();
  }
  hooked() {
    const m = this.m, s = this.s, I = this.in;
    if (I.eP && s.hookStage === 1) {
      const dl = s.perk('deliverance');
      if (dl && dl.tokens > 0) { dl.tokens = 0; m.selfUnhook(s); m.warn(s, 'Deliverance!'); }
      else if (s.unhookAttempts < 3) m.selfUnhookAttempt(s);
    }
    this.clearPresses();
  }
  downed() {
    const m = this.m, s = this.s, I = this.in;
    const moving = Math.hypot(I.mx, I.my) > 0.1;
    if (moving) { if (s.action) s.stopAction(m); s.moveTo = { x: s.x + I.mx * 2 * M, y: s.y + I.my * 2 * M }; }
    else s.moveTo = null;
    if (I.e && !moving) { if (s.action?.type !== 'recover') s.startAction('recover', null, m); }
    else if (s.action?.type === 'recover' && !I.e) s.stopAction(m);
    this.clearPresses();
  }

  vaultQuality(obj) {
    const s = this.s; const sp = Math.hypot(s.vx, s.vy);
    const toward = Math.atan2(obj.y - s.y, obj.x - s.x); const heading = Math.atan2(s.vy, s.vx);
    if (s.moveMode === 'run' && sp > 3.2 * M && angleDiff(toward, heading) < 0.9) return 'fast';
    return sp > 1.5 * M ? 'medium' : 'slow';
  }
  // SPACE: vault a window or dropped pallet, or throw an up pallet
  spaceContext() {
    const m = this.m, s = this.s; if (!s.standing || s.action?.type === 'dropPallet') return null;
    let best = null, bd = 1.5 * M;
    for (const w of m.map.windows) { if (w.blockedUntil > m.time) continue; const d = dist(s.x, s.y, w.x, w.y); if (d < bd) { bd = d; best = { type: 'vault', target: w, label: 'Vault' }; } }
    for (const p of m.map.pallets) {
      const d = dist(s.x, s.y, p.x, p.y);
      if (p.state === 'down' && d < Math.max(bd, 1.4 * M)) { bd = d; best = { type: 'vault', target: p, label: 'Vault pallet' }; }
      else if (p.state === 'up' && d < (ACTION_DEF.dropPallet.range - 0.6) * M && d < bd + 0.6 * M && s.canStart('dropPallet', p, m)) { bd = d; best = { type: 'dropPallet', target: p, label: 'Drop pallet' }; }
    }
    return best;
  }
  // E: the most relevant interaction in reach
  eContext() {
    const m = this.m, s = this.s; if (!s.standing) return null;
    const near = (o, r) => dist(s.x, s.y, o.x, o.y) < r * M;
    const tryT = (type, target, label) => (s.canStart(type, target, m) ? { type, target, label } : null);
    for (const o of m.survivors) if (o !== s && o.health === 'hooked' && near(o, 1.9)) { const c = tryT('unhook', o, `Unhook ${o.name}`); if (c) return c; }
    for (const o of m.survivors) if (o !== s && o.trap && near(o, 1.6)) { const c = tryT('freeTrapped', o, `Free ${o.name}`); if (c) return c; }
    for (const o of m.survivors) if (o !== s && (o.health === 'dying' || o.health === 'injured') && near(o, 1.9)) { const c = tryT('heal', o, o.health === 'dying' ? `Pick up ${o.name}` : `Heal ${o.name}`); if (c) return c; }
    if (m.hatch.state === 'open' && near(m.hatch, 1.3)) return { type: 'escapeHatch', target: m.hatch, label: 'Jump into the hatch' };
    if (m.hatch.state === 'closed' && near(m.hatch, 1.6)) { const c = tryT('openHatch', m.hatch, 'Open the hatch (key)'); if (c) return c; }
    let best = null, bd = Infinity;
    const consider = (type, o, r, label) => { const d = dist(s.x, s.y, o.x, o.y); if (d < r * M && d < bd && s.canStart(type, o, m)) { bd = d; best = { type, target: o, label }; } };
    for (const g of m.map.gens) if (!g.done) consider('repair', g, 2.6, `Repair generator ${g.name}`);
    for (const g of m.map.gates) consider('openGate', g, 1.8, 'Open exit gate');
    for (const c of m.map.chests) consider('searchChest', c, 1.6, 'Search chest');
    for (const t of m.map.totems) consider('cleanse', t, 1.6, t.hex ? 'Cleanse hex totem' : 'Cleanse totem');
    for (const t of m.traps) if (t.state === 'set') consider('disarm', t, 1.5, 'Disarm trap');
    for (const d of m.devices) if (d.survivorUsable) consider('device', d, 1.8, d.useLabel || `Use ${d.label}`);
    if (best) return best;
    // actions on yourself
    if (s.status.has('chained')) return { type: 'breakChain', target: null, label: 'Break the chains' };
    if (s.box) return { type: 'solveBox', target: null, label: 'Solve the Lament Configuration' };
    if (s.status.has('deepWound')) return { type: 'mend', target: null, label: 'Mend' };
    if (s.health === 'injured' && s.canStart('healSelf', null, m)) return { type: 'healSelf', target: null, label: 'Heal yourself' };
    return null;
  }
  // Prompts for the HUD: what SPACE and E would do right now
  prompts() {
    const s = this.s, out = [];
    if (!s.alive) return out;
    if (s.health === 'carried') return [{ key: 'A / D', label: 'Wiggle free', prog: s.wiggle }];
    if (s.health === 'hooked') return s.hookStage === 1 && s.unhookAttempts < 3 ? [{ key: 'E', label: `Attempt to escape (${3 - s.unhookAttempts} left, 4%)`, prog: null }] : [];
    if (s.health === 'dying') return [{ key: 'E', label: 'Hold to recover', prog: s.recovery }];
    if (s.trap) return [{ key: 'E', label: 'Try to escape the trap', prog: null }];
    if (s.action) out.push({ key: '', label: s.action.type === 'device' && s.action.target?.useLabel ? s.action.target.useLabel + '…' : s.action.label, prog: this.progressOf(s.action) });
    const sp = this.spaceContext(); if (sp && !s.action) out.push({ key: 'SPACE', label: sp.label, prog: null });
    const e = this.eContext(); if (e && (!s.action || s.action.target !== e.target)) out.push({ key: 'E', label: e.label, prog: null });
    if (s.item?.id === 'flashlight' && s.item.charges > 0 && !s.action) out.push({ key: 'RMB', label: 'Flashlight', prog: null });
    if (s.emp) out.push({ key: 'Q', label: 'Use EMP', prog: null });
    return out;
  }
  progressOf(a) {
    const s = this.s, t = a.target;
    switch (a.type) {
      case 'repair': return t.progress;
      case 'heal': return t.healProgress;
      case 'healSelf': return s.healProgress;
      case 'recover': return s.recovery;
      case 'device': return t.kind === 'printer' ? t.data.charge / 100 : a.t / (t.useTime || 1);
      default: return a.dur ? a.t / a.dur : null;
    }
  }
}

return { SurvivorController };
};
__defs["js/net/protocol.js"] = function(){
// Network state sync shared by the server (Node) and the client (browser).
//
// The server runs the real Match. Clients build a "mirror" Match from the same config and seed
// (so the map, perks and characters are identical) and never simulate it: every snapshot
// patches the mirror's live state. The renderer and HUDs then draw the mirror exactly as they
// draw a local match.
//
// Encoding: values are turned into JSON-safe data; references to game objects become
// { $r: 'sv:0' } style refs (survivor 0, killer 1, pallet 7, device #12…), Sets/Maps/Infinity are
// tagged, functions are dropped. The server diffs every object field-by-field per client, so
// only what changed is sent.
const { Trap } = __req("js/entities/objects.js");
const { Device, Projectile, Zone, Minion } = __req("js/killers/framework.js");

const NET_VERSION = 1;
const SNAP_HZ = 20;
// fields never synced (bot brains, static definitions, collision geometry, scratch state)
const SKIP = new Set(['ai', 'bot', 'def', 'Power', 'solid', 'rect', 'doorRect', 'exitRect', 'tile', 'aiProfile', 'unsubs', 'controller', 'netId']);
// dynamic object lists: server objects get a numeric net id
const LISTS = { traps: 'tp', devices: 'dv', minions: 'mn', projectiles: 'pj', zones: 'zn' };
// static map arrays, matched by index (same seed → same map)
const STATIC = { gens: 'gn', pallets: 'pl', windows: 'wn', hooks: 'hk', gates: 'gt', chests: 'ch', totems: 'tm' };
// append-only feeds streamed as new items
const FEEDS = ['scratches', 'blood', 'grunts', 'sounds', 'loudNoises', 'notify', 'feed', 'aiLog'];
// match-level fields patched like an object
const MATCH_FIELDS = ['time', 'state', 'stateT', 'gensDone', 'gatesPowered', 'collapse', 'collapseT', 'score', 'reveals', 'killerReveals', 'instinct', 'hookStageTime', 'camping'];

const round = (n) => (Number.isInteger(n) ? n : Math.round(n * 100) / 100);

class Registry {
  constructor(m) { this.m = m; this.byObj = new Map(); this.byRef = new Map(); this.nid = new WeakMap(); this.next = 1; }
  set(obj, ref) { if (!obj) return; this.byObj.set(obj, ref); this.byRef.set(ref, obj); }
  // server: refresh every tick (lists change)
  rebuild() {
    const m = this.m; this.byObj.clear(); this.byRef.clear();
    this.set(m, 'm'); this.set(m.map, 'map'); this.set(m.world, 'world'); this.set(m.map.nav, 'nav'); this.set(m.bb, 'bb'); this.set(m.perks, 'perks'); this.set(m.events, 'events'); this.set(m.fx, 'fx'); this.set(m.map.hatch, 'ht');
    m.survivors.forEach((s, i) => this.set(s, 'sv:' + i)); m.killers.forEach((k, i) => this.set(k, 'kl:' + i));
    for (const [arr, p] of Object.entries(STATIC)) (m.map[arr] || []).forEach((o, i) => this.set(o, p + ':' + i));
    for (const [arr, p] of Object.entries(LISTS)) for (const o of m[arr]) this.set(o, p + ':' + this.idOf(o));
    for (const t of m.map.tiles || []) this.set(t, 'ti:' + t.id);
  }
  idOf(o) { let i = this.nid.get(o); if (!i) { i = this.next++; this.nid.set(o, i); } return i; }
}

// ---------------------------------------------------------------- encode (server)
function enc(v, reg, depth = 0) {
  if (v === null || v === undefined) return null;
  const t = typeof v;
  if (t === 'number') return Number.isFinite(v) ? round(v) : Number.isNaN(v) ? 0 : { $i: v > 0 ? 1 : -1 };
  if (t === 'string' || t === 'boolean') return v;
  if (t === 'function' || t === 'symbol' || t === 'bigint') return undefined;
  const r = reg.byObj.get(v); if (r) return { $r: r };
  if (depth > 6) return undefined;
  if (Array.isArray(v)) return v.map((x) => { const e = enc(x, reg, depth + 1); return e === undefined ? null : e; });
  if (v instanceof Set) return { $s: [...v].map((x) => enc(x, reg, depth + 1)).filter((x) => x !== undefined) };
  if (v instanceof Map) return { $m: [...v].map(([a, b]) => [enc(a, reg, depth + 1), enc(b, reg, depth + 1)]).filter(([a, b]) => a !== undefined && b !== undefined) };
  if (ArrayBuffer.isView(v)) return undefined;
  const o = {};
  for (const k of Object.keys(v)) { if (SKIP.has(k) || k[0] === '_') continue; const e = enc(v[k], reg, depth + 1); if (e !== undefined) o[k] = e; }
  return o;
}
// Encode an object's own fields into { key: jsonString } (for diffing).
function fieldsOf(obj, reg, only = null) {
  const out = {};
  for (const k of only || Object.keys(obj)) {
    if (SKIP.has(k) || k[0] === '_') continue;
    const e = enc(obj[k], reg, 1); if (e === undefined) continue;
    out[k] = JSON.stringify(e);
  }
  return out;
}
function classTag(o) {
  if (o instanceof Trap) return 'Trap';
  if (o instanceof Device) return 'Device';
  if (o instanceof Projectile) return 'Projectile';
  if (o instanceof Zone) return 'Zone';
  if (o instanceof Minion) return 'Minion';
  return 'Obj';
}

// ---------------------------------------------------------------- decode (client)
const PROTO = { Trap: Trap.prototype, Device: Device.prototype, Projectile: Projectile.prototype, Zone: Zone.prototype, Minion: Minion.prototype, Obj: Object.prototype };
function makeObj(tag) { return Object.create(PROTO[tag] || Object.prototype); }

const isTagged = (v) => v && typeof v === 'object' && !Array.isArray(v) && ('$r' in v || '$s' in v || '$m' in v || '$i' in v);
function dec(v, resolve) {
  if (v === null || typeof v !== 'object') return v;
  if (Array.isArray(v)) return v.map((x) => dec(x, resolve));
  if ('$r' in v) return resolve(v.$r);
  if ('$i' in v) return v.$i > 0 ? Infinity : -Infinity;
  if ('$s' in v) return new Set(v.$s.map((x) => dec(x, resolve)));
  if ('$m' in v) return new Map(v.$m.map(([a, b]) => [dec(a, resolve), dec(b, resolve)]));
  const o = {}; for (const k in v) o[k] = dec(v[k], resolve); return o;
}
const isPlainTarget = (t) => t && typeof t === 'object' && !Array.isArray(t) && !(t instanceof Set) && !(t instanceof Map);
// Patch one field into a live object, keeping existing nested objects (and their prototypes).
function applyField(target, key, val, resolve, isEntity) {
  const cur = target[key];
  if (val && typeof val === 'object' && !Array.isArray(val) && !isTagged(val) && isPlainTarget(cur) && !isEntity(cur)) {
    for (const k in val) applyField(cur, k, val[k], resolve, isEntity);
    for (const k of Object.keys(cur)) if (!(k in val) && !SKIP.has(k) && k[0] !== '_' && typeof cur[k] !== 'function') delete cur[k];
    return;
  }
  if (Array.isArray(val) && Array.isArray(cur) && cur.length === val.length && val.every((x) => x && typeof x === 'object' && !Array.isArray(x) && !isTagged(x)) && cur.every((x) => isPlainTarget(x) && !isEntity(x))) {
    val.forEach((x, i) => { for (const k in x) applyField(cur[i], k, x[k], resolve, isEntity); });
    return;
  }
  try { target[key] = dec(val, resolve); } catch (e) { /* getter-only property: ignore */ }
}

return { NET_VERSION, SNAP_HZ, SKIP, LISTS, STATIC, FEEDS, MATCH_FIELDS, Registry, enc, fieldsOf, classTag, makeObj, dec, applyField };
};
__defs["js/net/host.js"] = function(){
// Server-side match host: runs the authoritative Match, applies human inputs, turns humans
// into bots when they disconnect, and builds per-client snapshot diffs. No networking here —
// server/server.js owns the sockets. Runs in Node (and in the browser for tests).
const { Match } = __req("js/core/game.js");
const { SurvivorController } = __req("js/player/survivorController.js");
const { SurvivorAI } = __req("js/ai/survivorAI.js");
const { KillerAI } = __req("js/ai/killer/killerAI.js");
const { Registry, enc, fieldsOf, classTag, LISTS, STATIC, FEEDS, MATCH_FIELDS } = __req("js/net/protocol.js");

const FX_SKIP = new Set(['constructor', 'update', 'draw']);

class NetHost {
  // humans: [{ pid, role: 'killer' | 'survivor', index }]
  constructor(cfg, humans) {
    this.cfg = cfg; this.humans = new Map(); this.events = [];
    const audio = { play: (name, opts) => this.events.push({ a: name, o: opts && typeof opts === 'object' ? opts : undefined }) };
    this.m = new Match(cfg, { audio });
    const m = this.m;
    this.reg = new Registry(m);
    // record top-level fx calls so clients can replay the effects
    let depth = 0;
    const proto = Object.getPrototypeOf(m.fx);
    for (const f of Object.getOwnPropertyNames(proto)) {
      if (FX_SKIP.has(f) || typeof proto[f] !== 'function') continue;
      const orig = m.fx[f].bind(m.fx);
      m.fx[f] = (...args) => { if (depth === 0) this.events.push({ f, args }); depth++; try { return orig(...args); } finally { depth--; } };
    }
    for (const h of humans) {
      const e = { ...h, seq: 0 };
      if (h.role === 'survivor') { const s = m.survivors[h.index]; e.ent = s; e.ctl = new SurvivorController(m, s); e.ctl.grace = 0.35; }
      else e.ent = m.killers[h.index];
      this.humans.set(h.pid, e);
    }
    this.feedSeq = {}; this.seqOf = new WeakMap(); for (const f of FEEDS) this.feedSeq[f] = 0;
    this.clients = new Map(); // pid → per-client diff state
    this.frame = null;
  }
  get over() { return this.m.state === 'RESULTS'; }
  step(dt) { this.m.update(dt); }

  // ------------------------------------------------------------ inputs
  input(pid, msg) {
    const h = this.humans.get(pid); if (!h || h.bot) return;
    if (msg.seq) h.seq = Math.max(h.seq, msg.seq);
    const P = msg.p || {};
    if (h.role === 'killer') {
      const i = h.ent.input;
      i.mx = +msg.mx || 0; i.my = +msg.my || 0; i.aim = msg.aim ?? i.aim; i.aimDist = msg.aimDist ?? i.aimDist;
      if (msg.aimX !== undefined) { i.aimX = msg.aimX; i.aimY = msg.aimY; } else { delete i.aimX; delete i.aimY; }
      i.lmb = !!msg.lmb; i.rmb = !!msg.rmb; i.e = !!msg.e; i.space = !!msg.space;
      for (const k of ['lmbP', 'rmbP', 'rmbR', 'eP', 'spaceP', 'cycleP']) if (P[k]) i[k] = true;
      if (P.wheel) i.wheel = (i.wheel || 0) + P.wheel;
      if (P.slot) i.slot = P.slot;
    } else {
      const c = h.ctl;
      c.setInput({ mx: +msg.mx || 0, my: +msg.my || 0, walk: !!msg.walk, crouch: !!msg.crouch, aimX: msg.aimX ?? c.in.aimX, aimY: msg.aimY ?? c.in.aimY, e: !!msg.e, space: !!msg.space, item: !!msg.item });
      for (const k of ['eP', 'spaceP', 'itemP', 'perkP', 'leftP', 'rightP']) if (P[k]) c.in[k] = true;
    }
  }
  check(pid, res) { const h = this.humans.get(pid); if (h?.role === 'survivor' && h.ent.skillCheck) this.m.answerPlayerCheck(h.ent, true, res); }
  // a human left: their character keeps playing as a bot
  dropHuman(pid, difficulty = 5) {
    const h = this.humans.get(pid); if (!h || h.bot) return; h.bot = true;
    const m = this.m;
    if (h.role === 'survivor') {
      const s = h.ent; s.isPlayer = false; s.skillCheck = null; s.moveTo = null;
      m.playerCtls = m.playerCtls.filter((c) => c !== h.ctl); if (m.playerCtl === h.ctl) m.playerCtl = m.playerCtls[0] || null;
      if (s.alive) new SurvivorAI(s, m, difficulty, s.personality);
    } else {
      const k = h.ent; k.isPlayer = false; k.bot = new KillerAI(k, m, difficulty);
    }
    m.notifyK(`${h.name || 'A player'} left — a bot took over`, '#9fd0d0');
    this.clients.delete(pid);
  }

  // ------------------------------------------------------------ snapshots
  // Encode the whole live state once per snapshot tick (shared by every client).
  buildFrame() {
    const m = this.m, reg = this.reg; reg.rebuild();
    const ents = new Map(); // ref → { fields }
    ents.set('m', fieldsOf(m, reg, MATCH_FIELDS.filter((k) => k in m)));
    ents.set('ht', fieldsOf(m.map.hatch, reg));
    m.survivors.forEach((s, i) => ents.set('sv:' + i, fieldsOf(s, reg)));
    m.killers.forEach((k, i) => { const f = fieldsOf(k, reg); delete f.power; ents.set('kl:' + i, f); ents.set('kp:' + i, fieldsOf(k.power, reg)); });
    for (const [arr, p] of Object.entries(STATIC)) (m.map[arr] || []).forEach((o, i) => ents.set(p + ':' + i, fieldsOf(o, reg)));
    const lists = {};
    for (const [arr, p] of Object.entries(LISTS)) {
      lists[arr] = [];
      for (const o of m[arr]) { const ref = p + ':' + reg.idOf(o); lists[arr].push(ref); ents.set(ref, fieldsOf(o, reg)); ents.get(ref).$cls = classTag(o); }
    }
    // feeds: tag new items with a sequence number
    const feeds = {};
    for (const f of FEEDS) {
      const arr = m[f]; if (!Array.isArray(arr)) continue;
      let min = Infinity;
      for (const it of arr) { let q = this.seqOf.get(it); if (!q) { q = ++this.feedSeq[f]; this.seqOf.set(it, q); } if (q < min) min = q; }
      feeds[f] = { arr, min: min === Infinity ? this.feedSeq[f] + 1 : min };
    }
    const fx = { flash: m.fx.flash, flashColor: m.fx.flashColor };
    const events = this.events.map((e) => (e.f ? { f: e.f, a: e.args.map((x) => enc(x, reg)) } : { a: e.a, o: e.o ? enc(e.o, reg) : undefined }));
    this.events = [];
    this.frame = { ents, lists, feeds, events, fx };
    return this.frame;
  }
  // Diff of the current frame against what this client last received.
  snapshotFor(pid) {
    const F = this.frame, reg = this.reg;
    let c = this.clients.get(pid);
    if (!c) { c = { last: new Map(), feed: {}, full: true }; this.clients.set(pid, c); }
    const out = { t: 'snap', e: {}, l: {}, f: {}, ev: F.events };
    for (const [ref, fields] of F.ents) {
      let prev = c.last.get(ref); if (!prev) { prev = {}; c.last.set(ref, prev); }
      let d = null;
      for (const k in fields) { if (k === '$cls') continue; if (prev[k] !== fields[k]) { (d ||= {})[k] = fields[k]; prev[k] = fields[k]; } }
      if (d) { if (fields.$cls && !prev.$cls) { d.$cls = fields.$cls; prev.$cls = fields.$cls; } out.e[ref] = d; }
    }
    // forget objects that disappeared
    for (const ref of c.last.keys()) if (!F.ents.has(ref)) c.last.delete(ref);
    for (const k in F.lists) { const key = F.lists[k].join(','); if (c['L' + k] !== key) { out.l[k] = F.lists[k]; c['L' + k] = key; } }
    for (const f in F.feeds) {
      const { arr, min } = F.feeds[f]; const last = c.feed[f] || 0; const add = [];
      for (const it of arr) { const q = this.seqOf.get(it); if (q > last) add.push([q, enc(it, reg)]); }
      if (add.length || c.full) { out.f[f] = { min, add: f === 'aiLog' ? add.slice(-20) : add }; c.feed[f] = this.feedSeq[f]; }
    }
    const fxk = F.fx.flash.toFixed(2) + F.fx.flashColor; if (c.fx !== fxk) { out.fx = F.fx; c.fx = fxk; }
    // per-player extras: input ack, prompts, killer context
    const h = this.humans.get(pid);
    if (h && !h.bot) {
      out.ack = h.seq;
      if (h.role === 'survivor') out.pr = h.ctl.prompts();
      else { const ctx = this.m.killerContext(h.ent); out.ctx = ctx ? { type: ctx.type, label: ctx.label } : null; }
    }
    // stringify sub-values (fields are already JSON strings)
    c.full = false;
    return out;
  }
  // Serialize a snapshot whose field values are pre-encoded JSON strings.
  static serialize(snap) {
    const parts = [];
    for (const ref in snap.e) { const d = snap.e[ref]; parts.push(JSON.stringify(ref) + ':{' + Object.entries(d).map(([k, v]) => JSON.stringify(k) + ':' + (k === '$cls' ? JSON.stringify(v) : v)).join(',') + '}'); }
    const rest = { ...snap }; delete rest.e;
    const j = JSON.stringify(rest);
    return j.slice(0, -1) + (j.length > 2 ? ',' : '') + '"e":{' + parts.join(',') + '}}';
  }
  results() { return this.m.results(); }
}

return { NetHost };
};
__defs["server/rooms.js"] = function(){
// Lobby rooms for custom matches (no sockets here — server.js wires them).
// A room has slots like a Dead by Daylight custom match: 1 killer + 4 survivors (1v4) or
// 2 killers + 8 survivors (2v8). Every slot is either a human or a bot; the host sets the bots'
// difficulty (and the bot killer's killer), anyone can take a bot slot, and at start every
// remaining bot slot is filled by the game's AI.
const { KILLERS, KILLER_LIST } = __req("js/killers/index.js");
const { KILLER_PERKS } = __req("js/perks/killerPerks.js");
const { SURVIVOR_PERKS } = __req("js/perks/survivorPerks.js");
const { ITEMS, ITEM_ADDONS } = __req("js/items/items.js");
const { MAPS } = __req("js/maps/maps.js");
const { SURVIVOR_ROSTER } = __req("js/ai/personality.js");
const { config2v8, SURVIVOR_CLASSES, KILLER_CLASSES } = __req("js/modes/twoVeight.js");
const { NetHost } = __req("js/net/host.js");

const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const MAX_PLAYERS = 10;

class Room {
  constructor(code, host) {
    this.code = code; this.hostId = host.id; this.players = new Map(); this.mode = '1v4'; this.mapId = 'rotwood';
    this.slots = []; this.match = null; this.chat = []; this.buildSlots();
    this.add(host);
  }
  buildSlots() {
    const old = this.slots; const nk = this.mode === '2v8' ? 2 : 1, ns = this.mode === '2v8' ? 8 : 4;
    const kOld = old.filter((s) => s.role === 'killer'), sOld = old.filter((s) => s.role === 'survivor');
    const killerPool = ['trapper', 'wraith', 'huntress', 'hillbilly'];
    this.slots = [
      ...Array.from({ length: nk }, (_, i) => ({ role: 'killer', idx: i, pid: kOld[i]?.pid || null, botDiff: kOld[i]?.botDiff ?? 5, botKiller: kOld[i]?.botKiller || killerPool[i], cls: kOld[i]?.cls || ['brute', 'enforcer'][i] })),
      ...Array.from({ length: ns }, (_, i) => ({ role: 'survivor', idx: i, pid: sOld[i]?.pid || null, botDiff: sOld[i]?.botDiff ?? 5, cls: sOld[i]?.cls || ['escapist', 'guide', 'medic', 'scout'][i % 4] })),
    ];
    // players who lost their slot become spectators
    for (const p of this.players.values()) p.slot = this.slots.findIndex((s) => s.pid === p.id);
  }
  add(p) {
    p.room = this; p.ready = false; p.slot = -1; this.players.set(p.id, p);
    // drop newcomers into the first free survivor slot, else the killer slot, else spectate
    if (!this.match) { const free = this.slots.findIndex((s) => s.role === 'survivor' && !s.pid); const fk = this.slots.findIndex((s) => s.role === 'killer' && !s.pid); this.take(p, free >= 0 ? free : fk); }
    this.say(null, `${p.name} joined`);
  }
  remove(p) {
    this.players.delete(p.id); p.room = null;
    const s = this.slots.find((q) => q.pid === p.id); if (s) s.pid = null;
    if (this.match) this.match.host.dropHuman(p.id);
    if (this.hostId === p.id) { const next = this.players.values().next().value; this.hostId = next ? next.id : null; if (next) this.say(null, `${next.name} is now the host`); }
    this.say(null, `${p.name} left`);
  }
  take(p, i) {
    const s = this.slots[i]; if (!s || (s.pid && s.pid !== p.id) || this.match) return false;
    for (const q of this.slots) if (q.pid === p.id) q.pid = null;
    s.pid = p.id; p.slot = i; p.ready = false; return true;
  }
  unslot(p) { for (const q of this.slots) if (q.pid === p.id) q.pid = null; p.slot = -1; p.ready = false; }
  say(p, text) { this.chat.push({ who: p ? p.name : '', text: String(text).slice(0, 200), t: Date.now() }); if (this.chat.length > 40) this.chat.shift(); }
  settings(p, o) {
    if (p.id !== this.hostId || this.match) return;
    if (o.mode === '1v4' || o.mode === '2v8') { if (o.mode !== this.mode) { this.mode = o.mode; this.buildSlots(); for (const q of this.players.values()) q.ready = false; } }
    if (o.mapId && MAPS[o.mapId]) this.mapId = o.mapId; else if (o.mapId === 'random') this.mapId = 'random';
  }
  editSlot(p, o) {
    const s = this.slots[o.i]; if (!s || this.match) return;
    const own = s.pid === p.id, host = p.id === this.hostId;
    if (host && o.kick && s.pid && s.pid !== p.id) { const q = this.players.get(s.pid); if (q) this.unslot(q); }
    if (host && o.botDiff >= 1 && o.botDiff <= 10) s.botDiff = Math.round(o.botDiff);
    if (host && o.botKiller && KILLERS[o.botKiller]) s.botKiller = o.botKiller;
    if ((host || own) && o.cls && (SURVIVOR_CLASSES[o.cls] || KILLER_CLASSES[o.cls])) s.cls = o.cls;
  }
  loadout(p, o) {
    const L = {};
    if (o.killer && KILLERS[o.killer]) L.killer = o.killer;
    if (Array.isArray(o.kperks)) L.kperks = o.kperks.filter((x) => KILLER_PERKS[x]).slice(0, 4);
    if (Array.isArray(o.addons) && L.killer) L.addons = o.addons.filter((x) => KILLERS[L.killer].addons[x]).slice(0, 2);
    if (Array.isArray(o.sperks)) L.sperks = o.sperks.filter((x) => SURVIVOR_PERKS[x]).slice(0, 4);
    if (o.item === null || ITEMS[o.item]) L.item = o.item || null;
    if (o.addon && ITEM_ADDONS[o.addon]) L.addon = o.addon;
    if (Number.isInteger(o.character) && SURVIVOR_ROSTER[o.character]) L.character = o.character;
    p.loadout = { ...(p.loadout || {}), ...L };
  }
  canStart() {
    const humans = this.slots.filter((s) => s.pid);
    if (!humans.length) return 'Take a slot first';
    for (const s of humans) { const q = this.players.get(s.pid); if (!q || (!q.ready && q.id !== this.hostId)) return `${q ? q.name : 'A player'} is not ready`; }
    return null;
  }
  // Match config: the game's own config, with human flags on the human slots
  buildConfig() {
    const seed = Math.floor(Math.random() * 1e9);
    const mapId = this.mapId === 'random' ? Object.keys(MAPS)[Math.floor(Math.random() * Object.keys(MAPS).length)] : this.mapId;
    const pl = (s) => (s.pid ? this.players.get(s.pid) : null);
    const ks = this.slots.filter((s) => s.role === 'killer'), ss = this.slots.filter((s) => s.role === 'survivor');
    const humans = [];
    const killerOf = (s) => { const p = pl(s); return p ? p.loadout?.killer || 'trapper' : s.botKiller || KILLER_LIST[Math.floor(Math.random() * KILLER_LIST.length)].id; };
    let cfg;
    if (this.mode === '2v8') {
      cfg = config2v8({ role: 'spectate', killerA: killerOf(ks[0]), killerB: killerOf(ks[1]), killerAClass: ks[0].cls, killerBClass: ks[1].cls, mapId, seed, killerDifficulty: ks[0].botDiff, killerBDifficulty: ks[1].botDiff });
      ks.forEach((s, i) => { const p = pl(s); if (p) { cfg.killers[i].human = true; cfg.killers[i].addons = p.loadout?.addons || []; humans.push({ pid: p.id, name: p.name, role: 'killer', index: i }); } });
      ss.forEach((s, i) => {
        const p = pl(s); const sc = cfg.survivors[i]; sc.perks = [SURVIVOR_CLASSES[s.cls]?.id || 'class_escapist']; sc.cls = s.cls; sc.difficulty = s.botDiff;
        if (p) { sc.human = true; sc.name = p.name; humans.push({ pid: p.id, name: p.name, role: 'survivor', index: i }); }
      });
    } else {
      const k = ks[0], kp = pl(k);
      cfg = { mode: '1v4', mapId, seed, perkMode: 'random', itemMode: 'random', difficulty: 5, survivorCount: 4, gensRequired: 5, gensOnMap: 7,
        killers: [kp ? { killerId: killerOf(k), perks: kp.loadout?.kperks || [], addons: kp.loadout?.addons || [], human: true } : { killerId: killerOf(k), perks: (KILLERS[killerOf(k)]?.signature || []).slice(0, 3), difficulty: k.botDiff }],
        survivors: ss.map((s) => { const p = pl(s); if (!p) return { difficulty: s.botDiff }; const L = p.loadout || {}; return { human: true, name: p.name, difficulty: 5, perks: L.sperks || [], item: L.item ?? null, itemAddons: L.addon ? [L.addon] : [] }; }),
      };
      if (kp) humans.push({ pid: kp.id, name: kp.name, role: 'killer', index: 0 });
      ss.forEach((s, i) => { const p = pl(s); if (p) humans.push({ pid: p.id, name: p.name, role: 'survivor', index: i }); });
    }
    cfg.playerRole = 'net'; cfg.killerId = cfg.killers[0].killerId;
    return { cfg, humans };
  }
  start(p) {
    if (p.id !== this.hostId || this.match) return 'Only the host can start';
    const why = this.canStart(); if (why) return why;
    const { cfg, humans } = this.buildConfig();
    const host = new NetHost(cfg, humans);
    this.match = { host, cfg, humans, acc: 0, steps: 0, t0: Date.now() };
    return null;
  }
  end() { this.match = null; for (const p of this.players.values()) p.ready = false; }
  state(forPid) {
    return {
      t: 'room', code: this.code, hostId: this.hostId, you: forPid, mode: this.mode, mapId: this.mapId, playing: !!this.match,
      players: [...this.players.values()].map((p) => ({ id: p.id, name: p.name, slot: p.slot, ready: p.ready, loadout: p.loadout || {} })),
      slots: this.slots.map((s, i) => ({ i, role: s.role, idx: s.idx, pid: s.pid, botDiff: s.botDiff, botKiller: s.botKiller, cls: s.cls })),
      chat: this.chat.slice(-20),
    };
  }
}
function newCode(rooms) { for (;;) { let c = ''; for (let i = 0; i < 4; i++) c += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]; if (!rooms.has(c)) return c; } }

return { MAX_PLAYERS, Room, newCode };
};
__defs["server/server.js"] = function(){
// Fog Trial multiplayer server: serves the game page and runs custom-match rooms.
// The server is authoritative: it runs the real simulation (bots included) at 60 Hz and sends
// each player 20 snapshots a second. Start with `npm start` (PORT env var, default 8080).


const { WebSocketServer } = __req("server/ws.js");
const { Room, newCode, MAX_PLAYERS } = __req("server/rooms.js");
const { NetHost } = __req("js/net/host.js");
const { NET_VERSION, SNAP_HZ } = __req("js/net/protocol.js");

const PORT = +process.env.PORT || 8080;
const STEP = 1 / 60;
const SPEED = Math.max(1, +process.env.FOG_SPEED || 1); // testing only: run matches faster than real time
// the bundled page lives next to the bundled server (dist/), or in ../dist when run from source
const pagePath = [new URL('./index.html', import.meta.url), new URL('../dist/index.html', import.meta.url)].find((u) => existsSync(u));
let page = pagePath ? readFileSync(pagePath) : Buffer.from('<h1>Fog Trial server</h1><p>Build the game first: npm run build</p>');

const http = createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('ok'); return; }
  if (url === '/' || url === '/index.html') { res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' }); res.end(page); return; }
  res.writeHead(404); res.end('not found');
});
const wss = new WebSocketServer({ server: http, maxPayload: 64 * 1024 });
const rooms = new Map();
let nextId = 1;

const send = (p, msg) => { if (p.ws.readyState === 1) p.ws.send(typeof msg === 'string' ? msg : JSON.stringify(msg)); };
process.on('uncaughtException', (e) => console.error('uncaught', e));
const broadcastRoom = (room) => { for (const p of room.players.values()) send(p, room.state(p.id)); };

wss.on('connection', (ws) => {
  const p = { id: 'p' + nextId++, name: 'Player', ws, room: null, loadout: {}, alive: true };
  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    try { handle(p, msg); } catch (e) { console.error('message error', e); send(p, { t: 'err', msg: 'Server error: ' + e.message }); }
  });
  ws.on('close', () => { const r = p.room; if (r) { r.remove(p); if (!r.players.size) rooms.delete(r.code); else broadcastRoom(r); } });
});

function handle(p, msg) {
  const r = p.room;
  switch (msg.t) {
    case 'hello':
      p.name = String(msg.name || 'Player').replace(/[<>]/g, '').slice(0, 16) || 'Player';
      if (msg.v !== NET_VERSION) send(p, { t: 'err', msg: 'Your game version differs from the server — reload the page.' });
      send(p, { t: 'welcome', pid: p.id, v: NET_VERSION }); return;
    case 'create': {
      if (r) r.remove(p);
      const room = new Room(newCode(rooms), p); rooms.set(room.code, room);
      if (msg.loadout) room.loadout(p, msg.loadout);
      broadcastRoom(room); return;
    }
    case 'join': {
      const room = rooms.get(String(msg.code || '').toUpperCase().trim());
      if (!room) { send(p, { t: 'err', msg: 'No room with that code.' }); return; }
      if (room.players.size >= MAX_PLAYERS + 4) { send(p, { t: 'err', msg: 'That room is full.' }); return; }
      if (r) r.remove(p);
      room.add(p); if (msg.loadout) room.loadout(p, msg.loadout);
      broadcastRoom(room);
      if (room.match) send(p, { t: 'err', msg: 'A match is running — you will join the next one (you can watch the lobby until then).' });
      return;
    }
  }
  if (!r) return;
  switch (msg.t) {
    case 'leave': r.remove(p); if (!r.players.size) rooms.delete(r.code); else broadcastRoom(r); send(p, { t: 'left' }); return;
    case 'settings': r.settings(p, msg); break;
    case 'slot': r.editSlot(p, msg); break;
    case 'take': r.take(p, msg.i); break;
    case 'unslot': r.unslot(p); break;
    case 'loadout': r.loadout(p, msg); p.ready = false; break;
    case 'ready': p.ready = !!msg.v; break;
    case 'chat': r.say(p, msg.text); break;
    case 'start': {
      const err = r.start(p); if (err) { send(p, { t: 'err', msg: err }); return; }
      for (const q of r.players.values()) {
        const h = r.match.humans.find((x) => x.pid === q.id);
        send(q, { t: 'start', cfg: r.match.cfg, role: h ? h.role : 'spectator', index: h ? h.index : -1 });
      }
      break;
    }
    case 'in': if (r.match) r.match.host.input(p.id, msg); return;
    case 'chk': if (r.match) r.match.host.check(p.id, msg.res); return;
    default: return;
  }
  broadcastRoom(r);
}

// ---------------------------------------------------------------- simulation loop
let last = performance.now();
setInterval(() => {
  const now = performance.now(); const dt = Math.min(0.25, (now - last) / 1000); last = now;
  for (const r of rooms.values()) {
    const M = r.match; if (!M) continue;
    M.acc += dt * SPEED; let n = 0;
    try {
      while (M.acc >= STEP && n < 6 * SPEED) {
        M.host.step(STEP); M.acc -= STEP; n++; M.steps++;
        if (M.steps % (60 / SNAP_HZ) === 0) {
          M.host.buildFrame();
          for (const q of r.players.values()) send(q, NetHost.serialize(M.host.snapshotFor(q.id)));
        }
        if (M.host.over) break;
      }
      if (M.acc > SPEED) M.acc = 0; // a stalled server skips ahead instead of fast-forwarding
    } catch (e) { console.error('match crashed', e); for (const q of r.players.values()) send(q, { t: 'err', msg: 'The match crashed on the server: ' + e.message }); r.end(); broadcastRoom(r); continue; }
    if (M.host.over) {
      const res = M.host.results();
      for (const q of r.players.values()) send(q, { t: 'end', results: res });
      r.end(); broadcastRoom(r);
    }
  }
}, 1000 / 60);
// drop dead connections (Render keeps idle sockets otherwise)
setInterval(() => { for (const ws of wss.clients) { if (ws._fogDead) { ws.terminate(); continue; } ws._fogDead = true; ws.ping(); } }, 30000);
wss.on('connection', (ws) => ws.on('pong', () => (ws._fogDead = false)));

http.listen(PORT, () => console.log(`Fog Trial server on http://localhost:${PORT}`));

return {  };
};
__req("server/server.js");
