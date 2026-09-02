import { test } from 'node:test';
import assert from 'node:assert/strict';
import { applyMove, judge, EMPTY } from './game.js';
import { createApp } from './server.js';

test('логика партии', () => {
  assert.equal(judge(EMPTY), null);
  assert.deepEqual(judge('XXX.OO...'), { winner: 'X', line: [0, 1, 2] });
  assert.deepEqual(judge('O..XO.XXO'), { winner: 'O', line: [0, 4, 8] });
  assert.deepEqual(judge('XOXXOOOXX'), { winner: 'draw', line: null });
  const g = { board: EMPTY, turn: 'X', status: 'playing' };
  assert.equal(applyMove(g, 'O', 0).error, 'Сейчас не ваш ход');
  assert.equal(applyMove(g, 'X', 9).error, 'Неверная клетка');
  assert.equal(applyMove(g, 'X', '4').error, 'Неверная клетка');
  assert.equal(applyMove({ ...g, status: 'waiting' }, 'X', 0).error, 'Игра не идёт');
  const r = applyMove(g, 'X', 4);
  assert.deepEqual(r, { board: '....X....', turn: 'O', status: 'playing', winner: null, line: null });
  assert.equal(applyMove({ ...g, board: r.board, turn: 'O' }, 'O', 4).error, 'Клетка занята');
  const w = applyMove({ board: 'XX.OO....', turn: 'X', status: 'playing' }, 'X', 2);
  assert.equal(w.status, 'finished'); assert.equal(w.winner, 'X'); assert.equal(w.turn, null);
});

test('HTTP: два игрока, версии, SSE, дашборд', async () => {
  const app = createApp(':memory:');
  await new Promise((r) => app.server.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${app.server.address().port}`;
  const client = () => {
    const c = { cookie: '' };
    c.call = async (method, path, body) => {
      const r = await fetch(base + path, { method, headers: { 'Content-Type': 'application/json', cookie: c.cookie }, body: body && JSON.stringify(body) });
      const sc = r.headers.get('set-cookie'); if (sc) c.cookie = sc.split(';')[0];
      return { status: r.status, data: await r.json() };
    };
    return c;
  };
  const a = client(), b = client();
  try {
    assert.equal((await a.call('GET', '/api/me')).status, 401);
    assert.equal((await a.call('POST', '/api/login', { name: 'x' })).status, 400);
    assert.equal((await a.call('POST', '/api/login', { name: 'Аня' })).status, 200);
    assert.equal((await b.call('POST', '/api/login', { name: 'Аня' })).status, 409);
    assert.equal((await b.call('POST', '/api/login', { name: 'Борис' })).status, 200);

    // SSE: подписчик получает hello, а затем событие game после хода
    const ac = new AbortController();
    const sse = await fetch(base + '/api/events', { headers: { cookie: b.cookie }, signal: ac.signal });
    const reader = sse.body.pipeThrough(new TextDecoderStream()).getReader();
    let buf = '';
    const waitFor = async (re) => { while (!re.test(buf)) buf += (await reader.read()).value; };
    await waitFor(/event: hello/);
    assert.match(buf, /"online":1/);

    const { data: { game } } = await a.call('POST', '/api/games');
    assert.equal(game.status, 'waiting');
    assert.equal((await a.call('POST', '/api/games')).data.game.id, game.id, 'повторное создание идемпотентно');
    assert.equal((await a.call('POST', `/api/games/${game.id}/join`)).status, 400, 'нельзя играть с собой');
    const j = await b.call('POST', `/api/games/${game.id}/join`);
    assert.equal(j.data.game.status, 'playing'); assert.equal(j.data.game.turn, 'X');
    assert.equal(j.data.game.players.O.online, true, 'у Бориса открыт SSE');
    assert.equal(j.data.game.players.X.online, false);
    const v = j.data.game.version;

    assert.equal((await b.call('POST', `/api/games/${game.id}/move`, { cell: 0, version: v })).status, 400, 'не ход O');
    const m1 = await a.call('POST', `/api/games/${game.id}/move`, { cell: 0, version: v });
    assert.equal(m1.status, 200); assert.equal(m1.data.game.version, v + 1);
    const stale = await b.call('POST', `/api/games/${game.id}/move`, { cell: 1, version: v });
    assert.equal(stale.status, 409, 'устаревшая версия отклоняется');
    assert.equal(stale.data.game.version, v + 1, 'вместе с ошибкой приходит актуальная доска');
    await waitFor(/event: game\ndata: [^\n]*"board":"X\.{8}"/);

    const play = async (c, cell) => { const r = await c.call('POST', `/api/games/${game.id}/move`, { cell, version: (await c.call('GET', `/api/games/${game.id}`)).data.game.version }); assert.equal(r.status, 200, JSON.stringify(r.data)); return r.data.game; };
    await play(b, 3); await play(a, 1); await play(b, 4);
    const fin = await play(a, 2);
    assert.equal(fin.status, 'finished'); assert.equal(fin.winner, 'X'); assert.deepEqual(fin.line, [0, 1, 2]);
    assert.equal((await b.call('POST', `/api/games/${game.id}/move`, { cell: 5, version: fin.version })).status, 400);

    const rm = await b.call('POST', `/api/games/${game.id}/rematch`);
    assert.equal(rm.data.game.players.X.name, 'Борис', 'в реванше стороны поменялись');
    assert.equal((await a.call('POST', `/api/games/${game.id}/rematch`)).data.game.id, rm.data.game.id, 'реванш один на двоих');
    assert.equal((await a.call('POST', `/api/games/${rm.data.game.id}/resign`)).data.game.winner, 'X');

    const d = (await a.call('GET', '/api/dashboard')).data;
    assert.deepEqual([d.me.wins, d.me.losses, d.me.games], [1, 1, 2]);
    assert.equal(d.leaderboard.length, 2);
    assert.equal(d.totals.finished, 2);
    assert.equal(d.recent.length, 2);
    ac.abort();
    assert.equal((await a.call('POST', '/api/logout')).status, 200);
    assert.equal((await a.call('GET', '/api/me')).status, 401);
  } finally {
    await app.close();
  }
});
