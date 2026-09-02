// Чистая логика крестиков-ноликов: без ввода-вывода, без состояния.
export const EMPTY = '.........';
export const LINES = [
  [0, 1, 2], [3, 4, 5], [6, 7, 8],
  [0, 3, 6], [1, 4, 7], [2, 5, 8],
  [0, 4, 8], [2, 4, 6],
];

// null — игра продолжается; { winner: 'X'|'O'|'draw', line } — закончена.
export function judge(board) {
  for (const line of LINES) {
    const [a, b, c] = line;
    if (board[a] !== '.' && board[a] === board[b] && board[a] === board[c]) return { winner: board[a], line };
  }
  return board.includes('.') ? null : { winner: 'draw', line: null };
}

// Возвращает { error } либо новые поля партии после хода.
export function applyMove({ board, turn, status }, mark, cell) {
  if (status !== 'playing') return { error: 'Игра не идёт' };
  if (mark !== turn) return { error: 'Сейчас не ваш ход' };
  if (!Number.isInteger(cell) || cell < 0 || cell > 8) return { error: 'Неверная клетка' };
  if (board[cell] !== '.') return { error: 'Клетка занята' };
  const next = board.slice(0, cell) + mark + board.slice(cell + 1);
  const result = judge(next);
  return result
    ? { board: next, turn: null, status: 'finished', winner: result.winner, line: result.line }
    : { board: next, turn: mark === 'X' ? 'O' : 'X', status: 'playing', winner: null, line: null };
}
