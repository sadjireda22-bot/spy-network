'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const ROOT = __dirname;
const DATA_DIR = process.env.DATA_DIR || path.join(ROOT, 'data');
const DB_FILE = path.join(DATA_DIR, 'users.json');

fs.mkdirSync(DATA_DIR, { recursive: true });
let database = { users: [] };
try {
  if (fs.existsSync(DB_FILE)) {
    const parsed = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
    if (parsed && Array.isArray(parsed.users)) database = parsed;
  }
} catch (error) {
  console.error('Could not read users database; starting with an empty database:', error.message);
}
function saveDatabase() {
  const temp = DB_FILE + '.tmp';
  fs.writeFileSync(temp, JSON.stringify(database, null, 2), { mode: 0o600 });
  fs.renameSync(temp, DB_FILE);
}

const usersByName = () => new Map(database.users.map(user => [user.username, user]));
const sessions = new Map(); // Random bearer tokens are kept in memory; users log in again after a server restart.
const socketsByUser = new Map();
const rooms = new Map();
const userRoom = new Map();
const badAuthByIp = new Map();

const WORDS = [
  'بيتزا', 'مطار', 'مدرسة', 'مستشفى', 'شاطئ', 'مكتبة', 'سينما', 'ملعب', 'مخبزة', 'سفينة',
  'قمر', 'مظلة', 'حاسوب', 'هاتف', 'دراجة', 'قطار', 'حديقة', 'مطعم', 'مصعد', 'متحف',
  'ثلج', 'قهوة', 'ساعة', 'كاميرا', 'جزيرة', 'صحراء', 'مسبح', 'فندق', 'مفتاح', 'نظارات',
  'طائرة', 'غابة', 'سوق', 'مرآة', 'وسادة', 'شوكولاتة', 'كرة', 'خيمة', 'ساعي البريد', 'روبوت',
  'سندويش', 'مطر', 'شمعة', 'مسرح', 'صيدلية', 'قصر', 'مزرعة', 'نفق', 'مصباح', 'ثلاجة'
];

function safeEqual(a, b) {
  const left = Buffer.from(String(a));
  const right = Buffer.from(String(b));
  return left.length === right.length && crypto.timingSafeEqual(left, right);
}
function hashPassword(password, salt) {
  return new Promise((resolve, reject) => {
    crypto.scrypt(password, salt, 64, (err, derived) => err ? reject(err) : resolve(derived.toString('hex')));
  });
}
function normaliseWord(word) {
  return String(word || '').trim().toLocaleLowerCase('ar').replace(/[ًٌٍَُِّْـ]/g, '').replace(/[إأآ]/g, 'ا').replace(/ى/g, 'ي');
}
function publicUser(user) {
  return { username: user.username, displayName: user.displayName || user.username };
}
function online(username) {
  const sockets = socketsByUser.get(username);
  return !!sockets && [...sockets].some(socket => socket.readyState === WebSocket.OPEN && socket.authed);
}
function emitSocket(socket, event, data) {
  if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ event, data }));
}
function emitUser(username, event, data) {
  const sockets = socketsByUser.get(username);
  if (!sockets) return;
  for (const socket of sockets) emitSocket(socket, event, data);
}
function emitRoom(room, event, data) {
  if (!room) return;
  for (const player of room.players) emitUser(player.username, event, data);
}
function broadcastRooms() {
  const list = [...rooms.values()]
    .filter(room => room.phase === 'lobby' && room.players.length < room.maxPlayers)
    .map(room => ({ code: room.code, host: room.host, hostName: (usersByName().get(room.host) || {}).displayName || room.host, maxPlayers: room.maxPlayers, playerCount: room.players.length }));
  for (const username of socketsByUser.keys()) emitUser(username, 'rooms:update', list);
}
function friendPayload(username) {
  const user = usersByName().get(username);
  if (!user) return { friends: [], incoming: [], outgoing: [] };
  const index = usersByName();
  const decorate = name => ({ ...publicUser(index.get(name) || { username: name }), online: online(name) });
  return {
    friends: (user.friends || []).map(decorate),
    incoming: (user.incoming || []).map(decorate),
    outgoing: (user.outgoing || []).map(decorate)
  };
}
function updateFriends(username) {
  const user = usersByName().get(username);
  if (!user) return;
  emitUser(username, 'friends:update', friendPayload(username));
  const others = new Set([...(user.friends || []), ...(user.incoming || []), ...(user.outgoing || [])]);
  for (const other of others) emitUser(other, 'friends:update', friendPayload(other));
}
function playerDisplay(username) {
  const user = usersByName().get(username);
  return { username, displayName: user?.displayName || username, online: online(username) };
}
function publicRoomState(room, viewer) {
  const state = {
    code: room.code,
    host: room.host,
    hostName: playerDisplay(room.host).displayName,
    maxPlayers: room.maxPlayers,
    players: room.players.map(player => ({ ...playerDisplay(player.username), isHost: player.username === room.host })),
    phase: room.phase,
    youAreHost: room.host === viewer,
    currentTurn: room.phase === 'clues' ? room.clueTurnOrder[room.turnIndex] : null,
    clueTurnOrder: room.phase === 'clues' ? room.clueTurnOrder : [],
    clues: room.clues.map(clue => ({ ...clue })),
    votesSubmitted: Object.keys(room.votes).length,
    votesTotal: room.players.length,
    youVoted: !!room.votes[viewer],
    caught: room.caught ? playerDisplay(room.caught) : null,
    winner: room.winner || null,
    endMessage: room.endMessage || null
  };
  if (room.phase === 'ended') {
    state.word = room.word;
    state.roles = room.players.map(player => ({ username: player.username, displayName: playerDisplay(player.username).displayName, role: player.username === room.spy ? 'spy' : 'citizen' }));
    state.spy = room.spy;
    state.spyGuess = room.spyGuess || null;
  }
  return state;
}
function syncRoom(room, event = 'room:state') {
  for (const player of room.players) {
    emitUser(player.username, event, publicRoomState(room, player.username));
  }
}
function roomOf(username) {
  const code = userRoom.get(username);
  return code ? rooms.get(code) || null : null;
}
function shuffle(items) {
  const copy = [...items];
  for (let i = copy.length - 1; i > 0; i--) {
    const j = crypto.randomInt(i + 1);
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy;
}
function makeRoomCode() {
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code;
  do {
    code = Array.from({ length: 5 }, () => alphabet[crypto.randomInt(alphabet.length)]).join('');
  } while (rooms.has(code));
  return code;
}
function sendError(socket, id, message) {
  if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ id, ok: false, error: message || 'حدث خطأ غير متوقع.' }));
}
function sendOK(socket, id, data = {}) {
  if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ id, ok: true, data }));
}
function getUser(socket) {
  if (!socket.authed || !socket.username) throw new Error('سجّل الدخول أولًا.');
  const user = usersByName().get(socket.username);
  if (!user) throw new Error('الحساب غير موجود. سجّل الدخول من جديد.');
  return user;
}
function addSocketUser(socket, username) {
  socket.authed = true;
  socket.username = username;
  if (!socketsByUser.has(username)) socketsByUser.set(username, new Set());
  socketsByUser.get(username).add(socket);
  updateFriends(username);
  for (const friend of (usersByName().get(username)?.friends || [])) updateFriends(friend);
}
function removePlayerFromRoom(username, reason = 'غادر أحد اللاعبين الغرفة.') {
  const room = roomOf(username);
  if (!room) return;
  room.players = room.players.filter(player => player.username !== username);
  userRoom.delete(username);
  if (room.players.length === 0) {
    rooms.delete(room.code);
  } else {
    if (room.host === username) room.host = room.players[0].username;
    if (room.phase !== 'lobby') {
      room.phase = 'lobby';
      room.spy = null;
      room.word = null;
      room.clues = [];
      room.votes = {};
      room.clueTurnOrder = [];
      room.turnIndex = 0;
      room.caught = null;
      room.winner = null;
      room.endMessage = reason + ' أُعيدت الغرفة إلى الانتظار.';
      room.players.forEach(player => emitUser(player.username, 'game:role', null));
    }
    syncRoom(room);
  }
  broadcastRooms();
}
function endGame(room, winner, message, spyGuess = null) {
  room.phase = 'ended';
  room.winner = winner;
  room.endMessage = message;
  room.spyGuess = spyGuess;
  syncRoom(room);
}
function startGame(room) {
  if (room.players.length !== room.maxPlayers || ![3, 4, 5].includes(room.maxPlayers)) throw new Error('يجب امتلاء الغرفة بعدد اللاعبين المحدد قبل البدء.');
  room.spy = room.players[crypto.randomInt(room.players.length)].username;
  room.word = WORDS[crypto.randomInt(WORDS.length)];
  room.phase = 'clues';
  room.clues = [];
  room.votes = {};
  room.clueTurnOrder = shuffle(room.players.map(player => player.username));
  room.turnIndex = 0;
  room.caught = null;
  room.winner = null;
  room.endMessage = null;
  room.spyGuess = null;
  for (const player of room.players) {
    emitUser(player.username, 'game:role', player.username === room.spy
      ? { role: 'spy', word: null }
      : { role: 'citizen', word: room.word });
  }
  syncRoom(room, 'room:state');
  broadcastRooms();
}

function validUsername(value) {
  return typeof value === 'string' && /^[A-Za-z0-9_\u0600-\u06FF]{3,20}$/.test(value.trim());
}
function cleanName(value, fallback) {
  const name = String(value || '').trim().replace(/[<>\u0000-\u001f]/g, '').slice(0, 24);
  return name || fallback;
}
function ensureFriendship(a, b) {
  const userA = usersByName().get(a);
  const userB = usersByName().get(b);
  if (!userA || !userB) throw new Error('لم يتم العثور على الحساب.');
  userA.friends ||= []; userB.friends ||= [];
  userA.incoming ||= []; userB.incoming ||= [];
  userA.outgoing ||= []; userB.outgoing ||= [];
  if (a === b) throw new Error('لا يمكنك إرسال طلب صداقة لنفسك.');
  if (userA.friends.includes(b)) throw new Error('هذا اللاعب موجود في قائمة أصدقائك بالفعل.');
}

async function handleAction(socket, id, action, payload) {
  try {
    if (!action || typeof action !== 'string') throw new Error('طلب غير صالح.');
    if (action === 'auth:register') {
      if (socket.authed) throw new Error('أنت مسجّل الدخول بالفعل.');
      const username = String(payload.username || '').trim().toLowerCase();
      const password = String(payload.password || '');
      const displayName = cleanName(payload.displayName, username);
      if (!validUsername(username)) throw new Error('اسم المستخدم يجب أن يكون 3–20 حرفًا أو رقمًا، ويمكن استخدام _ أو الحروف العربية.');
      if (password.length < 8 || password.length > 128) throw new Error('كلمة المرور يجب أن تكون 8 أحرف على الأقل.');
      if (usersByName().has(username)) throw new Error('اسم المستخدم مستخدم بالفعل. اختر اسمًا آخر.');
      const salt = crypto.randomBytes(16).toString('hex');
      const passwordHash = await hashPassword(password, salt);
      const user = { username, displayName, salt, passwordHash, friends: [], incoming: [], outgoing: [], createdAt: Date.now() };
      database.users.push(user); saveDatabase();
      const token = crypto.randomBytes(32).toString('hex');
      sessions.set(token, username);
      socket.token = token;
      addSocketUser(socket, username);
      sendOK(socket, id, { token, user: publicUser(user) });
      return;
    }
    if (action === 'auth:login') {
      if (socket.authed) throw new Error('أنت مسجّل الدخول بالفعل.');
      const username = String(payload.username || '').trim().toLowerCase();
      const password = String(payload.password || '');
      const user = usersByName().get(username);
      if (!user) throw new Error('اسم المستخدم أو كلمة المرور غير صحيحة.');
      const ip = socket._socket?.remoteAddress || 'unknown';
      const attempts = badAuthByIp.get(ip) || { count: 0, at: Date.now() };
      if (attempts.count >= 12 && Date.now() - attempts.at < 60_000) throw new Error('محاولات كثيرة. انتظر دقيقة ثم حاول مجددًا.');
      const hash = await hashPassword(password, user.salt);
      if (!safeEqual(hash, user.passwordHash)) {
        const next = Date.now() - attempts.at > 60_000 ? { count: 1, at: Date.now() } : { count: attempts.count + 1, at: attempts.at };
        badAuthByIp.set(ip, next);
        throw new Error('اسم المستخدم أو كلمة المرور غير صحيحة.');
      }
      badAuthByIp.delete(ip);
      const token = crypto.randomBytes(32).toString('hex');
      sessions.set(token, username);
      socket.token = token;
      addSocketUser(socket, username);
      sendOK(socket, id, { token, user: publicUser(user) });
      return;
    }
    if (action === 'auth:restore') {
      if (socket.authed) throw new Error('أنت مسجّل الدخول بالفعل.');
      const restoredToken = String(payload.token || '');
      const username = sessions.get(restoredToken);
      if (!username || !usersByName().has(username)) throw new Error('انتهت الجلسة. سجّل الدخول مجددًا.');
      socket.token = restoredToken;
      addSocketUser(socket, username);
      const activeRoom = roomOf(username);
      const role = activeRoom && !['lobby','ended'].includes(activeRoom.phase)
        ? { role: activeRoom.spy === username ? 'spy' : 'citizen', word: activeRoom.spy === username ? null : activeRoom.word }
        : null;
      sendOK(socket, id, { user: publicUser(usersByName().get(username)), room: activeRoom ? publicRoomState(activeRoom, username) : null, role });
      return;
    }
    if (action === 'auth:logout') {
      const user = getUser(socket);
      if (socket.token) sessions.delete(socket.token);
      sendOK(socket, id, {});
      socket.authed = false; socket.username = null; socket.token = null;
      const set = socketsByUser.get(user.username);
      if (set) {
        set.delete(socket);
        if (set.size === 0) {
          socketsByUser.delete(user.username);
          removePlayerFromRoom(user.username, 'خرج اللاعب من حسابه.');
        }
      }
      updateFriends(user.username);
      return;
    }

    const user = getUser(socket);
    const username = user.username;

    switch (action) {
      case 'friends:list':
        sendOK(socket, id, friendPayload(username));
        break;
      case 'friends:request': {
        const targetName = String(payload.username || '').trim().toLowerCase();
        ensureFriendship(username, targetName);
        const target = usersByName().get(targetName);
        if ((user.outgoing || []).includes(targetName)) throw new Error('لقد أرسلت طلبًا إلى هذا اللاعب بالفعل.');
        if ((user.incoming || []).includes(targetName)) throw new Error('هذا اللاعب أرسل لك طلبًا بالفعل؛ اقبل الطلب من قائمة الوارد.');
        user.outgoing ||= []; target.incoming ||= [];
        user.outgoing.push(targetName); target.incoming.push(username); saveDatabase();
        updateFriends(username); updateFriends(targetName);
        emitUser(targetName, 'notice', { kind: 'friend', text: `لديك طلب صداقة جديد من ${user.displayName}.` });
        sendOK(socket, id, {});
        break;
      }
      case 'friends:respond': {
        const senderName = String(payload.username || '').trim().toLowerCase();
        const accept = !!payload.accept;
        if (!(user.incoming || []).includes(senderName)) throw new Error('طلب الصداقة لم يعد موجودًا.');
        const sender = usersByName().get(senderName);
        user.incoming = user.incoming.filter(name => name !== senderName);
        if (sender) sender.outgoing = (sender.outgoing || []).filter(name => name !== username);
        if (accept && sender) {
          user.friends ||= []; sender.friends ||= [];
          if (!user.friends.includes(senderName)) user.friends.push(senderName);
          if (!sender.friends.includes(username)) sender.friends.push(username);
        }
        saveDatabase(); updateFriends(username); if (sender) updateFriends(senderName);
        if (accept && sender) emitUser(senderName, 'notice', { kind: 'friend', text: `${user.displayName} قبِل طلب صداقتك.` });
        sendOK(socket, id, {});
        break;
      }
      case 'rooms:list': {
        const list = [...rooms.values()].filter(room => room.phase === 'lobby' && room.players.length < room.maxPlayers)
          .map(room => ({ code: room.code, host: room.host, hostName: (usersByName().get(room.host) || {}).displayName || room.host, maxPlayers: room.maxPlayers, playerCount: room.players.length }));
        sendOK(socket, id, list);
        break;
      }
      case 'room:create': {
        const current = roomOf(username);
        if (current) throw new Error('غادر غرفتك الحالية أولًا.');
        const maxPlayers = Number(payload.maxPlayers);
        if (![3, 4, 5].includes(maxPlayers)) throw new Error('اختر 3 أو 4 أو 5 لاعبين فقط.');
        const room = { code: makeRoomCode(), host: username, maxPlayers, players: [{ username }], phase: 'lobby', spy: null, word: null, clues: [], votes: {}, clueTurnOrder: [], turnIndex: 0, caught: null, winner: null, endMessage: null };
        rooms.set(room.code, room); userRoom.set(username, room.code);
        syncRoom(room); broadcastRooms(); sendOK(socket, id, { code: room.code });
        break;
      }
      case 'room:join': {
        if (roomOf(username)) throw new Error('غادر غرفتك الحالية أولًا.');
        const code = String(payload.code || '').trim().toUpperCase();
        const room = rooms.get(code);
        if (!room) throw new Error('لم يتم العثور على الغرفة. تحقق من الرمز.');
        if (room.phase !== 'lobby') throw new Error('هذه المباراة بدأت بالفعل.');
        if (room.players.length >= room.maxPlayers) throw new Error('الغرفة ممتلئة.');
        room.players.push({ username }); userRoom.set(username, code);
        syncRoom(room); broadcastRooms(); sendOK(socket, id, { code });
        break;
      }
      case 'room:leave': {
        const room = roomOf(username);
        if (!room) { sendOK(socket, id, {}); break; }
        removePlayerFromRoom(username, 'غادر أحد اللاعبين الغرفة.');
        emitUser(username, 'room:left', {});
        sendOK(socket, id, {});
        break;
      }
      case 'room:start': {
        const room = roomOf(username);
        if (!room) throw new Error('أنت لست داخل غرفة.');
        if (room.host !== username) throw new Error('مضيف الغرفة فقط يمكنه بدء اللعبة.');
        if (room.phase !== 'lobby' && room.phase !== 'ended') throw new Error('الجولة جارية بالفعل.');
        startGame(room); sendOK(socket, id, {});
        break;
      }
      case 'game:clue': {
        const room = roomOf(username);
        if (!room || room.phase !== 'clues') throw new Error('مرحلة التلميحات ليست نشطة.');
        if (room.clueTurnOrder[room.turnIndex] !== username) throw new Error('انتظر دورك لتقديم التلميح.');
        const clue = String(payload.text || '').trim().replace(/[<>\u0000-\u0008\u000b\u000c\u000e-\u001f]/g, '').slice(0, 100);
        if (clue.length < 1) throw new Error('اكتب تلميحًا قصيرًا أولًا.');
        room.clues.push({ username, displayName: playerDisplay(username).displayName, text: clue });
        room.turnIndex++;
        if (room.turnIndex >= room.clueTurnOrder.length) room.phase = 'voting';
        syncRoom(room); sendOK(socket, id, {});
        break;
      }
      case 'game:vote': {
        const room = roomOf(username);
        if (!room || room.phase !== 'voting') throw new Error('مرحلة التصويت ليست نشطة.');
        if (room.votes[username]) throw new Error('لقد صوّت بالفعل.');
        const target = String(payload.username || '');
        if (target === username) throw new Error('لا يمكنك التصويت لنفسك.');
        if (!room.players.some(player => player.username === target)) throw new Error('اللاعب المحدد ليس في الغرفة.');
        room.votes[username] = target;
        if (Object.keys(room.votes).length === room.players.length) {
          const tally = {};
          Object.values(room.votes).forEach(name => tally[name] = (tally[name] || 0) + 1);
          const max = Math.max(...Object.values(tally));
          const top = Object.keys(tally).filter(name => tally[name] === max);
          if (top.length !== 1) {
            room.caught = null;
            endGame(room, 'spy', 'تعادل التصويت! نجا الجاسوس وفاز بالجولة.');
          } else if (top[0] === room.spy) {
            room.caught = top[0];
            endGame(room, 'citizens', `اكتشفتم الجاسوس ${playerDisplay(top[0]).displayName}! فاز المواطنون.`);
          } else {
            room.caught = top[0];
            room.phase = 'spy_guess';
            room.endMessage = `تم التصويت على ${playerDisplay(top[0]).displayName}، لكنه ليس الجاسوس! على الجاسوس تخمين الكلمة.`;
          }
        }
        syncRoom(room); sendOK(socket, id, {});
        break;
      }
      case 'game:spy-guess': {
        const room = roomOf(username);
        if (!room || room.phase !== 'spy_guess') throw new Error('لا توجد محاولة تخمين الآن.');
        if (username !== room.spy) throw new Error('هذه المحاولة خاصة بالجاسوس.');
        const guess = String(payload.word || '').trim().slice(0, 100);
        if (!guess) throw new Error('اكتب تخمينك أولًا.');
        const correct = normaliseWord(guess) === normaliseWord(room.word);
        if (correct) endGame(room, 'spy', `خمن الجاسوس الكلمة بشكل صحيح: «${room.word}»! فاز الجاسوس.`, guess);
        else endGame(room, 'citizens', `إجابة الجاسوس غير صحيحة. الكلمة كانت «${room.word}». فاز المواطنون.`, guess);
        sendOK(socket, id, {});
        break;
      }
      case 'friends:invite': {
        const targetName = String(payload.username || '').trim().toLowerCase();
        const room = roomOf(username);
        if (!room || room.phase !== 'lobby') throw new Error('أنشئ غرفة أو ادخل غرفة انتظار أولًا.');
        if (!(user.friends || []).includes(targetName)) throw new Error('يمكنك دعوة أصدقائك فقط.');
        if (!online(targetName)) throw new Error('صديقك غير متصل حاليًا.');
        if (room.players.length >= room.maxPlayers) throw new Error('الغرفة ممتلئة.');
        emitUser(targetName, 'room:invite', { code: room.code, from: user.displayName, maxPlayers: room.maxPlayers });
        sendOK(socket, id, {});
        break;
      }
      case 'profile:update': {
        const displayName = cleanName(payload.displayName, username);
        if (displayName.length > 24) throw new Error('الاسم طويل جدًا.');
        user.displayName = displayName; saveDatabase();
        sendOK(socket, id, { user: publicUser(user) });
        const room = roomOf(username); if (room) syncRoom(room);
        updateFriends(username);
        break;
      }
      default: throw new Error('طلب غير معروف.');
    }
  } catch (error) {
    sendError(socket, id, error.message || 'تعذر تنفيذ الطلب.');
  }
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (url.pathname === '/health') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ status: 'ok', service: 'SPY NETWORK' }));
    return;
  }
  let requestPath;
  try { requestPath = decodeURIComponent(url.pathname); } catch { res.writeHead(400); res.end('Bad request'); return; }
  if (requestPath !== '/' && requestPath !== '/index.html') {
    res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); res.end('404 - الملف غير موجود'); return;
  }
  const safePath = path.join(ROOT, 'index.html');
  fs.readFile(safePath, (error, contents) => {
    if (error) { res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); res.end('404 - الملف غير موجود'); return; }
    const ext = path.extname(safePath).toLowerCase();
    const types = { '.html': 'text/html; charset=utf-8', '.css': 'text/css; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.json': 'application/json; charset=utf-8', '.svg': 'image/svg+xml' };
    res.writeHead(200, { 'content-type': types[ext] || 'application/octet-stream', 'x-content-type-options': 'nosniff', 'cache-control': ext === '.html' ? 'no-cache' : 'public, max-age=3600' });
    res.end(contents);
  });
});
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
wss.on('connection', socket => {
  socket.authed = false;
  socket.username = null;
  socket.isAlive = true;
  socket.on('pong', () => { socket.isAlive = true; });
  socket.on('message', async raw => {
    let message;
    try { message = JSON.parse(raw.toString()); } catch { sendError(socket, null, 'رسالة غير صالحة.'); return; }
    if (!message || typeof message !== 'object' || typeof message.action !== 'string') { sendError(socket, message?.id ?? null, 'طلب غير صالح.'); return; }
    await handleAction(socket, message.id ?? null, message.action, message.payload || {});
  });
  socket.on('close', () => {
    const username = socket.username;
    if (!username) return;
    const set = socketsByUser.get(username);
    if (set) {
      set.delete(socket);
      if (set.size === 0) {
        socketsByUser.delete(username);
        removePlayerFromRoom(username, 'انقطع اتصال أحد اللاعبين.');
        updateFriends(username);
        for (const friend of (usersByName().get(username)?.friends || [])) updateFriends(friend);
      }
    }
  });
});
const heartbeat = setInterval(() => {
  for (const socket of wss.clients) {
    if (!socket.isAlive) { socket.terminate(); continue; }
    socket.isAlive = false;
    socket.ping();
  }
}, 30_000);
server.on('close', () => clearInterval(heartbeat));
server.listen(PORT, HOST, () => {
  console.log(`SPY NETWORK is running at http://${HOST}:${PORT}`);
  console.log(`Users database: ${DB_FILE}`);
});
