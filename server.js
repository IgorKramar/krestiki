import http from 'node:http';
import { readFileSync } from 'node:fs';
import { randomBytes } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { applyMove, EMPTY } from './game.js';

const INDEX_HTML = readFileSync(new URL('./public/index.html', import.meta.url));
const OFFLINE_CLAIM_MS = 30_000;   // через сколько можно засчитать победу над пропавшим соперником
const WAITING_CLEANUP_MS = 5_000;  // грейс на перезагрузку страницы до удаления «ожидающей» игры
const NAME_RE = /^[\p{L}\p{N} _.-]{2,20}$/u;

const SCHEMA = `
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE COLLATE NOCASE,
  token TEXT NOT NULL UNIQUE,
  created_at INTEGER NOT NULL DEFAULT (unixepoch())
);
CREATE TABLE IF NOT EXISTS games (
  id INTEGER PRIMARY KEY,
  x_id INTEGER NOT NULL REFERENCES users(id),
  o_id INTEGER REFERENCES users(id),
  board TEXT NOT NULL DEFAULT '${EMPTY}',
  turn TEXT,
  status TEXT NOT NULL DEFAULT 'waiting' CHECK (status IN ('waiting', 'playing', 'finished')),
  winner TEXT CHECK (winner IN ('X', 'O', 'draw')),
  line TEXT,
  rematch_id INTEGER REFERENCES games(id),
  version INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  updated_at INTEGER NOT NULL DEFAULT (unixepoch())
);
CREATE INDEX IF NOT EXISTS games_status ON games(status);
`;

const GAME_SQL = `SELECT g.*, ux.name AS x_name, uo.name AS o_name FROM games g
  JOIN users ux ON ux.id = g.x_id LEFT JOIN users uo ON uo.id = g.o_id`;
const STATS_SQL = `SELECT u.id, u.name,
  COALESCE(SUM((g.winner = 'X' AND g.x_id = u.id) OR (g.winner = 'O' AND g.o_id = u.id)), 0) AS wins,
  COALESCE(SUM((g.winner = 'O' AND g.x_id = u.id) OR (g.winner = 'X' AND g.o_id = u.id)), 0) AS losses,
  COALESCE(SUM(g.winner = 'draw'), 0) AS draws,
  COUNT(g.id) AS games
  FROM users u LEFT JOIN games g ON g.status = 'finished' AND (g.x_id = u.id OR g.o_id = u.id)`;

const SQL = {
  userByToken: `SELECT id, name FROM users WHERE token = ?`,
  userByName: `SELECT id FROM users WHERE name = ?`,
  insertUser: `INSERT INTO users (name, token) VALUES (?, ?) RETURNING id, name, token`,
  game: `${GAME_SQL} WHERE g.id = ?`,
  waiting: `${GAME_SQL} WHERE g.status = 'waiting' ORDER BY g.id`,
  activeOf: `${GAME_SQL} WHERE g.status IN ('waiting', 'playing') AND (g.x_id = ? OR g.o_id = ?) ORDER BY g.id`,
  recentOf: `${GAME_SQL} WHERE g.status = 'finished' AND (g.x_id = ? OR g.o_id = ?) ORDER BY g.updated_at DESC, g.id DESC LIMIT 10`,
  recentAll: `${GAME_SQL} WHERE g.status = 'finished' ORDER BY g.updated_at DESC, g.id DESC LIMIT 10`,
  counts: `SELECT status, COUNT(*) AS n FROM games GROUP BY status`,
  leaderboard: `${STATS_SQL} GROUP BY u.id ORDER BY wins DESC, losses ASC, games DESC, u.name LIMIT 20`,
  statsOf: `${STATS_SQL} WHERE u.id = ? GROUP BY u.id`,
  insertGame: `INSERT INTO games (x_id) VALUES (?) RETURNING id`,
  insertRematch: `INSERT INTO games (x_id, o_id, status, turn, version) VALUES (?, ?, 'playing', 'X', 1) RETURNING id`,
  setRematch: `UPDATE games SET rematch_id = ? WHERE id = ?`,
  joinGame: `UPDATE games SET o_id = ?, status = 'playing', turn = 'X', version = version + 1, updated_at = unixepoch() WHERE id = ? AND status = 'waiting'`,
  moveGame: `UPDATE games SET board = ?, turn = ?, status = ?, winner = ?, line = ?, version = version + 1, updated_at = unixepoch() WHERE id = ?`,
  finishGame: `UPDATE games SET status = 'finished', turn = NULL, winner = ?, version = version + 1, updated_at = unixepoch() WHERE id = ? AND status = 'playing'`,
  deleteGame: `DELETE FROM games WHERE id = ? AND status = 'waiting' AND x_id = ?`,
  deleteWaitingOf: `DELETE FROM games WHERE status = 'waiting' AND x_id = ?`,
};

// За HTTPS-прокси хостинга cookie должна быть Secure; локально по http — нет.
const secure = (req) => (req.headers['x-forwarded-proto'] ?? '').startsWith('https') ? '; Secure' : '';

function json(res, status, body, headers = {}) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', ...headers });
  res.end(JSON.stringify(body));
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    const fail = (message) => reject(Object.assign(new Error(message), { status: 400 }));
    let raw = '';
    req.setEncoding('utf8');
    req.on('data', (chunk) => { raw += chunk; if (raw.length > 10_000) { req.destroy(); fail('Слишком большой запрос'); } });
    req.on('end', () => { try { resolve(raw ? JSON.parse(raw) : {}); } catch { fail('Некорректный JSON'); } });
    req.on('error', reject);
  });
}

export function createApp(dbPath = process.env.DB_PATH ?? './krestiki.db') {
  const db = new DatabaseSync(dbPath);
  db.exec(SCHEMA);
  const q = Object.fromEntries(Object.entries(SQL).map(([k, sql]) => [k, db.prepare(sql)]));

  // ---- присутствие: userId -> Set<res> открытых SSE-потоков
  const clients = new Map();
  const offlineSince = new Map();
  const cleanupTimers = new Map();
  let closed = false;
  const isOnline = (id) => clients.has(id);

  function send(res, event, data) {
    if (!res.destroyed) res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  }
  function sendTo(userId, event, data) { for (const res of clients.get(userId) ?? []) send(res, event, data); }
  function broadcast(event, data) { for (const set of clients.values()) for (const res of set) send(res, event, data); }

  function player(id, name) {
    return id ? { id, name, online: isOnline(id) } : null;
  }
  function gameState(row) {
    return {
      id: row.id, board: row.board, turn: row.turn, status: row.status, winner: row.winner,
      line: row.line ? JSON.parse(row.line) : null, version: row.version, rematchId: row.rematch_id,
      players: { X: player(row.x_id, row.x_name), O: player(row.o_id, row.o_name) },
      offlineSince: { X: offlineSince.get(row.x_id) ?? null, O: offlineSince.get(row.o_id) ?? null },
      offlineClaimMs: OFFLINE_CLAIM_MS, updatedAt: row.updated_at,
    };
  }
  const loadGame = (id) => { const row = q.game.get(id); return row && gameState(row); };
  function pushGame(id) {
    const g = loadGame(id);
    sendTo(g.players.X.id, 'game', g);
    if (g.players.O) sendTo(g.players.O.id, 'game', g);
    return g;
  }
  function lobbyState() {
    const counts = Object.fromEntries(q.counts.all().map((r) => [r.status, r.n]));
    return { waiting: q.waiting.all().map(gameState), online: clients.size, playing: counts.playing ?? 0, finished: counts.finished ?? 0 };
  }
  const pushLobby = () => broadcast('lobby', lobbyState());
  function gameOver(id) { const g = pushGame(id); pushLobby(); broadcast('stats', {}); return g; }
  function presenceChanged(userId) {
    for (const row of q.activeOf.all(userId, userId)) pushGame(row.id);
    pushLobby();
  }

  function auth(req) {
    const m = req.headers.cookie?.match(/(?:^|;\s*)token=([\w-]+)/);
    return m ? q.userByToken.get(m[1]) ?? null : null;
  }

  // ---- обработчики: (ctx) => [status, body, headers?]; events пишет в поток сам
  function login({ req, user, body }) {
    if (user) return [200, { user }];
    const name = String(body.name ?? '').trim();
    if (!NAME_RE.test(name)) return [400, { error: 'Имя: 2–20 символов — буквы, цифры, пробел, _ . -' }];
    if (q.userByName.get(name)) return [409, { error: 'Имя уже занято' }];
    const u = q.insertUser.get(name, randomBytes(24).toString('base64url'));
    return [200, { user: { id: u.id, name: u.name } }, { 'Set-Cookie': `token=${u.token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=31536000${secure(req)}` }];
  }
  const logout = ({ req }) => [200, { ok: true }, { 'Set-Cookie': `token=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0${secure(req)}` }];

  function events({ req, res, user }) {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
    res.write(':ok\n\n');
    let set = clients.get(user.id);
    const cameOnline = !set;
    if (cameOnline) {
      clients.set(user.id, set = new Set());
      offlineSince.delete(user.id);
      clearTimeout(cleanupTimers.get(user.id));
      cleanupTimers.delete(user.id);
    }
    set.add(res);
    const ping = setInterval(() => res.write(':ping\n\n'), 25_000);
    req.on('close', () => {
      clearInterval(ping);
      set.delete(res);
      if (set.size || closed) return;
      clients.delete(user.id);
      offlineSince.set(user.id, Date.now());
      cleanupTimers.set(user.id, setTimeout(() => {
        cleanupTimers.delete(user.id);
        if (q.deleteWaitingOf.run(user.id).changes) pushLobby();
      }, WAITING_CLEANUP_MS).unref());
      presenceChanged(user.id);
    });
    send(res, 'hello', { me: user, lobby: lobbyState(), games: q.activeOf.all(user.id, user.id).map(gameState) });
    if (cameOnline) presenceChanged(user.id);
    return null;
  }

  function dashboard({ user }) {
    const counts = Object.fromEntries(q.counts.all().map((r) => [r.status, r.n]));
    return [200, {
      totals: { online: clients.size, playing: counts.playing ?? 0, waiting: counts.waiting ?? 0, finished: counts.finished ?? 0 },
      me: q.statsOf.get(user.id),
      leaderboard: q.leaderboard.all(),
      recent: q.recentOf.all(user.id, user.id).map(gameState),
      recentAll: q.recentAll.all().map(gameState),
    }];
  }

  function createGame({ user }) {
    const mine = q.activeOf.all(user.id, user.id).find((g) => g.status === 'waiting');
    if (mine) return [200, { game: gameState(mine) }];
    const { id } = q.insertGame.get(user.id);
    pushLobby();
    return [200, { game: pushGame(id) }];
  }

  function gameAction({ user, body, params: [id, action] }) {
    const row = q.game.get(Number(id));
    if (!row) return [404, { error: 'Игра не найдена' }];
    const mark = row.x_id === user.id ? 'X' : row.o_id === user.id ? 'O' : null;
    const other = mark === 'X' ? 'O' : 'X';
    const fail = (status, error) => [status, { error, game: gameState(row) }];

    if (action === 'join') {
      if (row.status !== 'waiting') return fail(400, 'Игра уже началась');
      if (mark) return fail(400, 'Нельзя играть с самим собой');
      q.joinGame.run(user.id, row.id);
      pushLobby();
      return [200, { game: pushGame(row.id) }];
    }
    if (!mark) return fail(403, 'Вы не участник этой игры');
    switch (action) {
      case 'cancel':
        if (row.status !== 'waiting' || mark !== 'X') return fail(400, 'Отменить можно только свою неначатую игру');
        q.deleteGame.run(row.id, user.id);
        pushLobby();
        return [200, { ok: true }];
      case 'move': {
        if (row.status !== 'playing') return fail(400, 'Игра не идёт');
        if (body.version !== row.version) return fail(409, 'Доска уже изменилась, ход не принят');
        const r = applyMove(row, mark, body.cell);
        if (r.error) return fail(400, r.error);
        q.moveGame.run(r.board, r.turn, r.status, r.winner, r.line && JSON.stringify(r.line), row.id);
        return [200, { game: r.status === 'finished' ? gameOver(row.id) : pushGame(row.id) }];
      }
      case 'resign':
        if (row.status !== 'playing') return fail(400, 'Игра не идёт');
        q.finishGame.run(other, row.id);
        return [200, { game: gameOver(row.id) }];
      case 'claim': {
        if (row.status !== 'playing') return fail(400, 'Игра не идёт');
        const otherId = other === 'X' ? row.x_id : row.o_id;
        const since = offlineSince.get(otherId);
        if (isOnline(otherId) || !since || Date.now() - since < OFFLINE_CLAIM_MS) return fail(400, 'Соперник ещё может вернуться');
        q.finishGame.run(mark, row.id);
        return [200, { game: gameOver(row.id) }];
      }
      case 'rematch': {
        if (row.status !== 'finished') return fail(400, 'Игра ещё не закончена');
        if (!row.rematch_id) {
          row.rematch_id = q.insertRematch.get(row.o_id, row.x_id).id; // стороны меняются
          q.setRematch.run(row.rematch_id, row.id);
          pushGame(row.id);
          pushGame(row.rematch_id);
          pushLobby();
        }
        return [200, { game: loadGame(row.rematch_id) }];
      }
    }
    return [404, { error: 'Неизвестное действие' }];
  }

  const routes = [
    ['POST', /^\/api\/login$/, login, true],
    ['POST', /^\/api\/logout$/, logout],
    ['GET', /^\/api\/me$/, ({ user }) => [200, { user }]],
    ['GET', /^\/api\/events$/, events],
    ['GET', /^\/api\/lobby$/, () => [200, lobbyState()]],
    ['GET', /^\/api\/dashboard$/, dashboard],
    ['POST', /^\/api\/games$/, createGame],
    ['GET', /^\/api\/games\/(\d+)$/, ({ params: [id] }) => { const g = loadGame(Number(id)); return g ? [200, { game: g }] : [404, { error: 'Игра не найдена' }]; }],
    ['POST', /^\/api\/games\/(\d+)\/(join|cancel|move|resign|claim|rematch)$/, gameAction],
  ];

  const server = http.createServer(async (req, res) => {
    const { pathname } = new URL(req.url, 'http://localhost');
    try {
      if (req.method === 'GET' && pathname === '/') {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        return res.end(INDEX_HTML);
      }
      for (const [method, re, handler, isPublic] of routes) {
        const m = pathname.match(re);
        if (!m || req.method !== method) continue;
        const user = auth(req);
        if (!user && !isPublic) return json(res, 401, { error: 'Нужно войти' });
        const body = method === 'POST' ? await readJson(req) : {};
        const out = handler({ req, res, user, body, params: m.slice(1) });
        if (out) json(res, ...out);
        return;
      }
      json(res, 404, { error: 'Не найдено' });
    } catch (e) {
      if (!e.status) console.error(e);
      json(res, e.status ?? 500, { error: e.status ? e.message : 'Внутренняя ошибка' });
    }
  });

  return {
    server, db,
    async close() {
      closed = true;
      for (const t of cleanupTimers.values()) clearTimeout(t);
      server.closeAllConnections();
      await new Promise((r) => server.close(r));
      db.close();
    },
  };
}

if (import.meta.main) {
  const port = Number(process.env.PORT ?? 3000);
  createApp().server.listen(port, () => console.log(`Крестики-нолики: http://localhost:${port}`));
}
