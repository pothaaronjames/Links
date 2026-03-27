// ── Game data ────────────────────────────────────────────────────────────────
// Prefer the generated puzzle pool. Keep a tiny built-in fallback so the game
// still boots if puzzle_pool.js was not generated or loaded.
const _PUZZLES_FALLBACK = [
  {
    start: { name: "Samuel L. Jackson",  qid: "Q172678" },
    end:   { name: "Cate Blanchett",     qid: "Q80966"  },
    // For demo purposes, each puzzle includes a list of valid (movie → actor) pairs
    // that form one valid solution. The game accepts ANY valid real path.
    solutionHint: [
      { movie: "Thor",           actor: "Chris Hemsworth" },
      { movie: "The Aviator",    actor: "Cate Blanchett"  },
    ],
  },
  {
    start: { name: "Meryl Streep",  qid: "Q873" },
    end:   { name: "Tom Hanks",     qid: "Q2263" },
    solutionHint: [
      { movie: "The Post",      actor: "Tom Hanks" },
    ],
  },
  {
    start: { name: "Scarlett Johansson", qid: "Q34667" },
    end:   { name: "Robert Downey Jr.",  qid: "Q318" },
    solutionHint: [
      { movie: "Avengers: Endgame", actor: "Robert Downey Jr." },
    ],
  },
  {
    start: { name: "Leonardo DiCaprio", qid: "Q38111" },
    end:   { name: "Brad Pitt",         qid: "Q35332" },
    solutionHint: [
      { movie: "Once Upon a Time in Hollywood", actor: "Brad Pitt" },
    ],
  },
  {
    start: { name: "Natalie Portman", qid: "Q37079" },
    end:   { name: "Christian Bale",  qid: "Q45772" },
    solutionHint: [
      { movie: "Thor: The Dark World", actor: "Christopher Eccleston" },
      { movie: "Batman Begins",        actor: "Christian Bale" },
    ],
  },
];

const PUZZLES = (
  typeof _GENERATED_PUZZLE_POOL !== "undefined" &&
  _GENERATED_PUZZLE_POOL &&
  Array.isArray(_GENERATED_PUZZLE_POOL.puzzles) &&
  _GENERATED_PUZZLE_POOL.puzzles.length
)
  ? _GENERATED_PUZZLE_POOL.puzzles
  : _PUZZLES_FALLBACK;

const NORMALIZED_PUZZLES = PUZZLES.map((entry, index) => ({
  ...entry,
  puzzle_id: entry.puzzle_id ?? index + 1,
  difficulty: entry.difficulty || "easy",
  shortest_hops: entry.shortest_hops ?? (Array.isArray(entry.solutionHint) ? entry.solutionHint.length : null),
  canonical_path: entry.canonical_path || null,
}));

const STORAGE_KEYS = {
  selectedDifficulty: "links:selectedDifficulty",
  selectedActorMode: "links:selectedActorMode",
  recentPuzzleIds: "links:recentPuzzleIds",
};

const RECENT_PUZZLE_LIMIT = 20;

function safeReadStorage(key, fallback) {
  try {
    const raw = window.localStorage.getItem(key);
    return raw ? JSON.parse(raw) : fallback;
  } catch {
    return fallback;
  }
}

function safeWriteStorage(key, value) {
  try {
    window.localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Ignore localStorage failures; game still works without persistence.
  }
}

function getDifficultyCounts() {
  const counts = { easy: 0, medium: 0, hard: 0 };
  NORMALIZED_PUZZLES.forEach((entry) => {
    if (counts[entry.difficulty] !== undefined) counts[entry.difficulty] += 1;
  });
  return counts;
}

const PUZZLE_COUNTS = getDifficultyCounts();

function getActorModeCounts() {
  const featured = NORMALIZED_PUZZLES.filter(
    (entry) => (entry.start?.rank ?? 9999) <= 50 && (entry.end?.rank ?? 9999) <= 50,
  ).length;
  return {
    all: NORMALIZED_PUZZLES.length,
    featured,
  };
}

const ACTOR_MODE_COUNTS = getActorModeCounts();

// Built-in fallback lookup so the game still works if movie_cast_db.js
// was not generated/loaded yet.
// Keys are lowercase movie titles; values are arrays of actor names.
const _MOVIE_CAST_DB_FALLBACK = {
  "pulp fiction":                    ["John Travolta","Samuel L. Jackson","Uma Thurman","Bruce Willis","Harvey Keitel"],
  "thor":                            ["Chris Hemsworth","Natalie Portman","Tom Hiddleston","Anthony Hopkins","Samuel L. Jackson"],
  "the aviator":                     ["Leonardo DiCaprio","Cate Blanchett","Kate Blanchett","Alec Baldwin","Alan Alda"],
  "avengers: endgame":               ["Robert Downey Jr.","Chris Evans","Scarlett Johansson","Mark Ruffalo","Chris Hemsworth","Samuel L. Jackson"],
  "avengers: infinity war":          ["Robert Downey Jr.","Chris Evans","Scarlett Johansson","Benedict Cumberbatch","Chris Hemsworth"],
  "inception":                       ["Leonardo DiCaprio","Joseph Gordon-Levitt","Elliot Page","Tom Hardy","Ken Watanabe"],
  "once upon a time in hollywood":   ["Leonardo DiCaprio","Brad Pitt","Margot Robbie","Al Pacino","Kurt Russell"],
  "the post":                        ["Meryl Streep","Tom Hanks","Sarah Paulson","Bob Odenkirk"],
  "the devil wears prada":           ["Meryl Streep","Anne Hathaway","Emily Blunt","Stanley Tucci"],
  "captain america: civil war":      ["Chris Evans","Robert Downey Jr.","Scarlett Johansson","Chadwick Boseman","Sebastian Stan"],
  "black panther":                   ["Chadwick Boseman","Michael B. Jordan","Lupita Nyong'o","Martin Freeman","Angela Bassett"],
  "batman begins":                   ["Christian Bale","Michael Caine","Gary Oldman","Liam Neeson","Cillian Murphy"],
  "the dark knight":                 ["Christian Bale","Heath Ledger","Aaron Eckhart","Michael Caine","Gary Oldman"],
  "thor: the dark world":            ["Chris Hemsworth","Natalie Portman","Tom Hiddleston","Anthony Hopkins","Christopher Eccleston"],
  "la la land":                      ["Ryan Gosling","Emma Stone","John Legend"],
  "marriage story":                  ["Scarlett Johansson","Adam Driver","Laura Dern"],
  "gone girl":                       ["Ben Affleck","Rosamund Pike","Neil Patrick Harris","Tyler Perry"],
  "fight club":                      ["Brad Pitt","Edward Norton","Helena Bonham Carter"],
  "se7en":                           ["Brad Pitt","Morgan Freeman","Kevin Spacey","Gwyneth Paltrow"],
  "the silence of the lambs":        ["Jodie Foster","Anthony Hopkins","Scott Glenn"],
  "iron man":                        ["Robert Downey Jr.","Gwyneth Paltrow","Jeff Bridges","Terrence Howard"],
  "forrest gump":                    ["Tom Hanks","Robin Wright","Gary Sinise","Sally Field"],
  "cast away":                       ["Tom Hanks","Helen Hunt"],
  "moulin rouge!":                   ["Nicole Kidman","Ewan McGregor","John Leguizamo"],
  "carol":                           ["Cate Blanchett","Rooney Mara","Kyle Chandler"],
  "notes on a scandal":              ["Cate Blanchett","Judi Dench","Bill Nighy"],
  "the lord of the rings: the fellowship of the ring": ["Cate Blanchett","Elijah Wood","Ian McKellen","Viggo Mortensen","Orlando Bloom"],
  "the lord of the rings: the return of the king":     ["Cate Blanchett","Elijah Wood","Ian McKellen","Viggo Mortensen","Andy Serkis"],
  "star wars: the force awakens":    ["Harrison Ford","Daisy Ridley","John Boyega","Oscar Isaac","Carrie Fisher","Adam Driver"],
  "jurassic park":                   ["Sam Neill","Laura Dern","Jeff Goldblum","Richard Attenborough","Ariana Richards","Samuel L. Jackson"],
  "goodfellas":                      ["Robert De Niro","Ray Liotta","Joe Pesci","Lorraine Bracco"],
  "the godfather":                   ["Marlon Brando","Al Pacino","James Caan","Robert Duvall","Diane Keaton"],
  "no country for old men":          ["Javier Bardem","Tommy Lee Jones","Josh Brolin","Kelly Macdonald"],
  "there will be blood":             ["Daniel Day-Lewis","Paul Dano","Ciarán Hinds"],
  "gravity":                         ["Sandra Bullock","George Clooney"],
  "parasite":                        ["Song Kang-ho","Lee Sun-kyun","Cho Yeo-jeong","Choi Woo-shik","Park So-dam"],
  "joker":                           ["Joaquin Phoenix","Robert De Niro","Zazie Beetz","Frances Conroy"],
  "interstellar":                    ["Matthew McConaughey","Anne Hathaway","Jessica Chastain","Michael Caine","Matt Damon"],
  "dune":                            ["Timothée Chalamet","Zendaya","Oscar Isaac","Josh Brolin","Stellan Skarsgård","Charlotte Rampling"],
};

// movie_cast_db.js defines _GENERATED_MOVIE_CAST_DB.
// Use it when available; otherwise use the fallback map above.
const MOVIE_CAST_DB = (typeof _GENERATED_MOVIE_CAST_DB !== "undefined")
  ? _GENERATED_MOVIE_CAST_DB
  : _MOVIE_CAST_DB_FALLBACK;

const MOVIE_TITLE_DISPLAY = (typeof _GENERATED_MOVIE_TITLE_CASE !== "undefined")
  ? _GENERATED_MOVIE_TITLE_CASE
  : {};

const ACTOR_PORTRAITS = (typeof _GENERATED_ACTOR_PORTRAITS !== "undefined")
  ? _GENERATED_ACTOR_PORTRAITS
  : {};

// ── State ────────────────────────────────────────────────────────────────────
let puzzle          = null;
let currentActorName = "";
let currentStep     = 0;
let selectedMovie   = "";
let selectedActor   = "";
let gameOver        = false;
let chainSteps      = [];

const MAX_STEPS = 5;
const HINT_PENALTY_POINTS = 150;
const HINT_MOVIE_COUNT = 5;
const EFFICIENCY_SCORE_MAX = 800;
const STEP_OVERAGE_PENALTY = 120;
const ONE_STEP_ONE_HINT_SCORE_CAP = 75;
const PARTIAL_CREDIT_1_AWAY = 350; // bonus pts: player's last actor was 1 hop from target
const PARTIAL_CREDIT_2_AWAY = 150; // bonus pts: player's last actor was 2 hops from target
let hintsUsed = { start: false, end: false };

// Round tracking
const ROUNDS_PER_GAME = 5;
let currentRoundNumber = 0;
let gameCumulativeScore = 0;
let gameRoundScores = [];

// ── DOM refs ─────────────────────────────────────────────────────────────────
const startNameEl        = document.getElementById("startName");
const endNameEl          = document.getElementById("endName");
const endSubEl           = document.getElementById("endSub");
const currentActorDisplay= document.getElementById("currentActorDisplay");
const chainArea          = document.getElementById("chainArea");
const pathDots           = document.getElementById("pathDots");
const stepCountLabel     = document.getElementById("stepCountLabel");
const movieInput         = document.getElementById("movieInput");
const actorInput         = document.getElementById("actorInput");
const movieSuggestions   = document.getElementById("movieSuggestions");
const actorSuggestions   = document.getElementById("actorSuggestions");
const actorFieldWrap     = document.getElementById("actorFieldWrap");
const submitStepBtn      = document.getElementById("submitStepBtn");
const undoStepBtn        = document.getElementById("undoStepBtn");
const giveUpBtn          = document.getElementById("giveUpBtn");
const errorMsg           = document.getElementById("errorMsg");
const resultBanner       = document.getElementById("resultBanner");
const resultEmoji        = document.getElementById("resultEmoji");
const resultTitle        = document.getElementById("resultTitle");
const resultSub          = document.getElementById("resultSub");
const resultMeta         = document.getElementById("resultMeta");
const resultPath         = document.getElementById("resultPath");
const resultCompletionPath = document.getElementById("resultCompletionPath");
const inputArea          = document.getElementById("inputArea");
const startHintBtn       = document.getElementById("startHintBtn");
const endHintBtn         = document.getElementById("endHintBtn");
const startHintList      = document.getElementById("startHintList");
const endHintList        = document.getElementById("endHintList");
const hintPenaltyInfo    = document.getElementById("hintPenaltyInfo");
const gameCounter        = document.getElementById("gameCounter");
const roundScoreDisplay  = document.getElementById("roundScoreDisplay");
const nextRoundBtn       = document.getElementById("nextRoundBtn");
const playAgainBtn       = document.getElementById("playAgainBtn");
const startAvatarImg     = document.getElementById("startAvatarImg");
const endAvatarImg       = document.getElementById("endAvatarImg");
const startAvatarFallback= document.getElementById("startAvatarFallback");
const endAvatarFallback  = document.getElementById("endAvatarFallback");
const mobileRoundValue   = document.getElementById("mobileRoundValue");
const mobileStepsValue   = document.getElementById("mobileStepsValue");
const mobileCurrentActor = document.getElementById("mobileCurrentActor");
const startCard          = document.getElementById("startCard");
const endCard            = document.getElementById("endCard");
const startCardToggle    = document.getElementById("startCardToggle");
const endCardToggle      = document.getElementById("endCardToggle");
const howToPlayModal      = document.getElementById("howToPlayModal");
const howToPlayBtn        = document.getElementById("howToPlayBtn");
const closeModalBtn       = document.getElementById("closeModalBtn");
const startFromModalBtn   = document.getElementById("startFromModalBtn");

function syncMobileHud() {
  if (mobileRoundValue) {
    mobileRoundValue.textContent = `${currentRoundNumber || 1} / ${ROUNDS_PER_GAME}`;
  }
  if (mobileStepsValue) {
    mobileStepsValue.textContent = String(currentStep);
  }
  if (mobileCurrentActor) {
    mobileCurrentActor.textContent = currentActorName || (puzzle?.start?.name || "Unknown Actor");
  }
}

function setActorCardCollapsed(cardEl, toggleBtnEl, collapsed) {
  if (!cardEl || !toggleBtnEl) return;
  cardEl.classList.toggle("is-collapsed", collapsed);
  toggleBtnEl.setAttribute("aria-expanded", String(!collapsed));
  toggleBtnEl.textContent = collapsed ? "Show details" : "Hide details";
}

function setupMobileCardToggles() {
  if (startCard && startCardToggle) {
    setActorCardCollapsed(startCard, startCardToggle, true);
    startCardToggle.addEventListener("click", () => {
      setActorCardCollapsed(startCard, startCardToggle, !startCard.classList.contains("is-collapsed"));
    });
  }

  if (endCard && endCardToggle) {
    setActorCardCollapsed(endCard, endCardToggle, true);
    endCardToggle.addEventListener("click", () => {
      setActorCardCollapsed(endCard, endCardToggle, !endCard.classList.contains("is-collapsed"));
    });
  }
}

function initialsForName(name) {
  const parts = String(name || "").trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return "🎭";
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return `${parts[0][0] || ""}${parts[parts.length - 1][0] || ""}`.toUpperCase();
}

function showAvatarFallback(imgEl, fallbackEl, name) {
  imgEl.classList.add("hidden");
  imgEl.src = "";
  fallbackEl.textContent = initialsForName(name);
  fallbackEl.classList.remove("hidden");
}

function normalizePortraitPath(path) {
  const raw = String(path || "").trim();
  if (!raw) return "";
  if (/^(https?:|data:|file:|\/)/i.test(raw)) return raw;
  return new URL(`./${raw.replace(/^\.?\//, "")}`, window.location.href).href;
}

function getPortraitSourcesForQid(qid) {
  const q = String(qid || "").trim();
  if (!q) return [];
  const preferred = ACTOR_PORTRAITS[q] || "";
  const candidates = [
    preferred,
    `actor_portraits/${q}.jpg`,
    `actor_portraits/${q}.jpeg`,
    `actor_portraits/${q}.png`,
    `actor_portraits/${q}.webp`,
  ].filter(Boolean);
  return Array.from(new Set(candidates)).map(normalizePortraitPath);
}

function applyAvatarImage(imgEl, fallbackEl, imageUrls, name) {
  const urls = Array.isArray(imageUrls) ? imageUrls.filter(Boolean) : [normalizePortraitPath(imageUrls)];
  if (!urls.length) {
    showAvatarFallback(imgEl, fallbackEl, name);
    return;
  }

  // Keep fallback visible while loading so we never show an empty circle.
  showAvatarFallback(imgEl, fallbackEl, name);
  imgEl.alt = `${name} portrait`;

  // Some browsers defer loading when an <img> is hidden + lazy.
  // Probe each candidate with a separate Image, then reveal the real element.
  imgEl.loading = "eager";
  imgEl.decoding = "async";

  let idx = 0;
  const tryNext = () => {
    if (idx >= urls.length) {
      showAvatarFallback(imgEl, fallbackEl, name);
      return;
    }

    const url = urls[idx];
    const probe = new Image();
    probe.onload = () => {
      imgEl.src = url;
      imgEl.classList.remove("hidden");
      fallbackEl.classList.add("hidden");
    };
    probe.onerror = () => {
      idx += 1;
      tryNext();
    };
    probe.src = url;
  };

  tryNext();
}

function updateEndpointPortraits(startQid, endQid, startName, endName) {
  showAvatarFallback(startAvatarImg, startAvatarFallback, startName);
  showAvatarFallback(endAvatarImg, endAvatarFallback, endName);

  const startPaths = getPortraitSourcesForQid(startQid);
  const endPaths = getPortraitSourcesForQid(endQid);
  applyAvatarImage(startAvatarImg, startAvatarFallback, startPaths, startName);
  applyAvatarImage(endAvatarImg, endAvatarFallback, endPaths, endName);
}

// ── Init ─────────────────────────────────────────────────────────────────────
function getSelectedDifficulty() {
  const stored = safeReadStorage(STORAGE_KEYS.selectedDifficulty, "all");
  if (stored === "easy" || stored === "medium" || stored === "hard" || stored === "all") {
    return stored;
  }
  return "all";
}

function getSelectedActorMode() {
  const stored = safeReadStorage(STORAGE_KEYS.selectedActorMode, "all");
  if (stored === "all" || stored === "featured") {
    return stored;
  }
  return "all";
}

function getPuzzlesForDifficulty(difficulty) {
  if (!difficulty || difficulty === "all") return NORMALIZED_PUZZLES;
  return NORMALIZED_PUZZLES.filter((entry) => entry.difficulty === difficulty);
}

function getPuzzlesForActorMode(mode, pool = NORMALIZED_PUZZLES) {
  if (!mode || mode === "all") return pool;
  if (mode === "featured") {
    return pool.filter((entry) => (entry.start?.rank ?? 9999) <= 50 && (entry.end?.rank ?? 9999) <= 50);
  }
  return pool;
}

function recordPuzzleUsage(puzzleId) {
  const recent = safeReadStorage(STORAGE_KEYS.recentPuzzleIds, []);
  const next = recent.filter((id) => id !== puzzleId);
  next.push(puzzleId);
  safeWriteStorage(STORAGE_KEYS.recentPuzzleIds, next.slice(-RECENT_PUZZLE_LIMIT));
}

function updateDifficultyButtons() {
  const selected = getSelectedDifficulty();
  document.querySelectorAll('.btn-option[data-difficulty]').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.difficulty === selected);
  });
}

function updateActorModeButtons() {
  const selected = getSelectedActorMode();
  document.querySelectorAll('.btn-option[data-actor-mode]').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.actorMode === selected);
  });
}

function updateSetupDifficultyButtons() {
  const selected = getSelectedDifficulty();
  document.querySelectorAll('.btn-option[data-setup-difficulty]').forEach((btn) => {
    btn.classList.toggle('active', btn.dataset.setupDifficulty === selected);
  });
}

function updateSetupActorModeButtons() {
  const selected = getSelectedActorMode();
  document.querySelectorAll('.btn-option[data-setup-actor-mode]').forEach((btn) => {
    btn.classList.toggle('active', btn.dataset.setupActorMode === selected);
  });
}

function setSelectedDifficulty(nextDifficulty, restartGame = true) {
  const allowed = new Set(["all", "easy", "medium", "hard"]);
  if (!allowed.has(nextDifficulty)) return;
  safeWriteStorage(STORAGE_KEYS.selectedDifficulty, nextDifficulty);
  updateDifficultyButtons();
  updateSetupDifficultyButtons();
  if (restartGame) startGame(true);
}

function setSelectedActorMode(nextMode, restartGame = true) {
  const allowed = new Set(["all", "featured"]);
  if (!allowed.has(nextMode)) return;
  safeWriteStorage(STORAGE_KEYS.selectedActorMode, nextMode);
  updateActorModeButtons();
  updateSetupActorModeButtons();
  if (restartGame) startGame(true);
}

function openHowToPlayModal(showSetup = true) {
  document.getElementById("modalSetupSection").classList.toggle("hidden", !showSetup);
  document.getElementById("rulesCloseBtn").classList.toggle("hidden", showSetup);
  howToPlayModal.classList.remove("hidden");
}

function closeHowToPlayModal() {
  howToPlayModal.classList.add("hidden");
  if (currentRoundNumber === 0) {
    startGame(true);
  }
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function getShortestPathNodes(entry) {
  if (Array.isArray(entry.canonical_path) && entry.canonical_path.length) {
    const nodes = [];
    entry.canonical_path.forEach((step, index) => {
      if (index > 0) {
        const previous = entry.canonical_path[index - 1];
        if (previous.movie_to_next) nodes.push({ type: "movie", label: previous.movie_to_next });
      }
      nodes.push({ type: "actor", label: step.actor_name || step.actor_qid || "Unknown Actor" });
    });
    return nodes;
  }

  if (Array.isArray(entry.solutionHint) && entry.solutionHint.length) {
    const nodes = [{ type: "actor", label: entry.start?.name || "Start" }];
    entry.solutionHint.forEach((step) => {
      nodes.push({ type: "movie", label: step.movie || "Unknown Movie" });
      nodes.push({ type: "actor", label: step.actor || "Unknown Actor" });
    });
    return nodes;
  }

  return [];
}

function renderShortestPathMarkup(entry) {
  const nodes = getShortestPathNodes(entry);
  if (!nodes.length) {
    return '<div class="result-path__empty">No shortest path data available yet.</div>';
  }

  const rows = nodes.map((node, index) => {
    const side = index % 2 === 0 ? "left" : "right";
    const emoji = node.type === "actor" ? "🎭" : "🎬";
    const cardClass = node.type === "actor" ? "path-node path-node--actor" : "path-node path-node--movie";
    const delay = (index * 140) + 150;

    return (
      `<div class="path-item path-item--${side}" style="animation-delay:${delay}ms">` +
      `<div class="${cardClass}">${emoji} ${escapeHtml(node.label)}</div>` +
      `<span class="path-item__dot"></span>` +
      `</div>`
    );
  });

  return `<div class="result-path-timeline">${rows.join("")}</div>`;
}

function pickPuzzle() {
  const selectedDifficulty = getSelectedDifficulty();
  const selectedActorMode = getSelectedActorMode();
  const difficultyPool = getPuzzlesForDifficulty(selectedDifficulty);
  const filteredPool = getPuzzlesForActorMode(selectedActorMode, difficultyPool);
  const activePool = filteredPool.length ? filteredPool : difficultyPool.length ? difficultyPool : NORMALIZED_PUZZLES;
  const recentPuzzleIds = new Set(safeReadStorage(STORAGE_KEYS.recentPuzzleIds, []));
  const freshPool = activePool.filter((entry) => !recentPuzzleIds.has(entry.puzzle_id));
  const pool = freshPool.length ? freshPool : activePool;
  const idx = Math.floor(Math.random() * pool.length);
  const chosen = pool[idx];
  recordPuzzleUsage(chosen.puzzle_id);
  return chosen;
}

function loadRound({ resetGame = false } = {}) {
  if (resetGame || currentRoundNumber === 0 || currentRoundNumber >= ROUNDS_PER_GAME) {
    currentRoundNumber = 1;
    gameCumulativeScore = 0;
    gameRoundScores = [];
  } else {
    currentRoundNumber += 1;
  }

  puzzle = pickPuzzle();
  const startActor = puzzle.start?.name || "Unknown Actor";
  const endActor = puzzle.end?.name || "Unknown Actor";
  currentActorName = startActor;
  currentStep = 0;
  selectedMovie = "";
  selectedActor = "";
  gameOver = false;
  chainSteps = [];
  hintsUsed = { start: false, end: false };

  // Update game counter display
  gameCounter.textContent = `Round ${currentRoundNumber} of ${ROUNDS_PER_GAME}`;
  roundScoreDisplay.textContent = `Game Score: ${gameCumulativeScore}`;

  // Reset UI
  chainArea.innerHTML = "";
  movieInput.value = "";
  actorInput.value = "";
  actorFieldWrap.classList.remove("hidden");
  submitStepBtn.disabled = true;
  hideError();
  resultBanner.classList.add("hidden");
  resultMeta.classList.add("hidden");
  resultMeta.innerHTML = "";
  resultPath.classList.add("hidden");
  resultPath.innerHTML = "";
  if (resultCompletionPath) {
    resultCompletionPath.classList.add("hidden");
    resultCompletionPath.innerHTML = "";
  }
  startHintList.classList.add("hidden");
  startHintList.textContent = "";
  endHintList.classList.add("hidden");
  endHintList.textContent = "";
  inputArea.classList.remove("hidden");
  nextRoundBtn.style.display = "none";
  playAgainBtn.style.display = "none";

  startNameEl.textContent = startActor;
  endNameEl.textContent   = endActor;
  const actorModeLabel = getSelectedActorMode() === "featured" ? " · featured" : "";
  endSubEl.textContent    = `Can you reach them${puzzle.difficulty ? ` · ${puzzle.difficulty}` : ""}${actorModeLabel}?`;
  currentActorDisplay.textContent = startActor;
  setActorCardCollapsed(startCard, startCardToggle, true);
  setActorCardCollapsed(endCard, endCardToggle, true);
  syncMobileHud();

  // Render starting node in chain
  renderChain();

  updateEndpointPortraits(puzzle.start?.qid || "", puzzle.end?.qid || "", startActor, endActor);

  updateDots();
  updateHintControls();
  updateUndoButton();
}

function startGame(resetGame = true) {
  loadRound({ resetGame });
}

function startNextRound() {
  loadRound({ resetGame: false });
}

// ── Chain rendering ───────────────────────────────────────────────────────────
function appendConnector() {
  const c = document.createElement("div");
  c.className = "chain-connector";
  chainArea.appendChild(c);
}

function appendMovieNode(movieTitle) {
  appendConnector();
  const step = document.createElement("div");
  step.className = "chain-step";
  const node = document.createElement("div");
  node.className = "chain-movie";
  node.textContent = movieTitle;
  step.appendChild(node);
  chainArea.appendChild(step);
}

function appendPersonNode(actorName, isEnd) {
  if (chainArea.children.length > 0) appendConnector();
  const step = document.createElement("div");
  step.className = "chain-step";
  const node = document.createElement("div");
  node.className = isEnd ? "chain-person is-end" : "chain-person";
  node.textContent = actorName;
  step.appendChild(node);
  chainArea.appendChild(step);
  // Scroll to bottom
  step.scrollIntoView({ behavior: "smooth", block: "nearest" });
}

function renderChain() {
  chainArea.innerHTML = "";
  const startActor = puzzle?.start?.name || startNameEl.textContent || "Unknown Actor";
  appendPersonNode(startActor, false);

  chainSteps.forEach((entry) => {
    appendMovieNode(entry.movie);
    const isEnd = entry.actor.toLowerCase() === endNameEl.textContent.toLowerCase();
    appendPersonNode(entry.actor, isEnd);
  });
}

function updateUndoButton() {
  undoStepBtn.disabled = gameOver || chainSteps.length === 0;
  giveUpBtn.disabled = gameOver;
}

// ── Dots indicator ────────────────────────────────────────────────────────────
function updateDots() {
  pathDots.innerHTML = "";
  const total = MAX_STEPS;
  for (let i = 0; i < total; i++) {
    const dot = document.createElement("div");
    dot.className = "dot" + (i < currentStep ? " filled" : "");
    pathDots.appendChild(dot);
  }
  stepCountLabel.textContent = `${currentStep} step${currentStep !== 1 ? "s" : ""}`;
  syncMobileHud();
}

// ── Autocomplete helpers ──────────────────────────────────────────────────────
function showSuggestions(ul, items) {
  ul.innerHTML = "";
  if (!items.length) { ul.classList.add("hidden"); return; }
  items.forEach((text) => {
    const li = document.createElement("li");
    li.textContent = text;
    li.addEventListener("mousedown", (e) => {
      e.preventDefault();
      ul.parentElement.querySelector("input").value = text;
      ul.classList.add("hidden");
      if (ul === movieSuggestions) handleMovieChosen(text);
      if (ul === actorSuggestions) handleActorChosen(text);
    });
    ul.appendChild(li);
  });
  ul.classList.remove("hidden");
}

// All known movie titles (for movie autocomplete)
const ALL_MOVIES = Object.keys(MOVIE_CAST_DB);

function buildActorMovieIndex() {
  const actorToMovies = new Map();
  Object.entries(MOVIE_CAST_DB).forEach(([movieTitle, cast]) => {
    (cast || []).forEach((actorName) => {
      const key = actorName.toLowerCase();
      if (!actorToMovies.has(key)) actorToMovies.set(key, new Set());
      actorToMovies.get(key).add(movieTitle);
    });
  });
  return actorToMovies;
}

const ACTOR_TO_MOVIES = buildActorMovieIndex();

// BFS through the actor→movie graph from startActorName to endActorName.
// Returns { distance, path: [{movie, actor}, ...] } or null if not found within maxDepth.
// Path entries lead from start up to and INCLUDING the end actor.
function bfsFromActor(startActorName, endActorName, maxDepth = 3) {
  const startKey = startActorName.toLowerCase();
  const endKey   = endActorName.toLowerCase();
  if (startKey === endKey) return { distance: 0, path: [] };

  const visited = new Set([startKey]);
  // Each queue entry: { actorKey, path[] }
  let queue = [{ actorKey: startKey, path: [] }];
  let nodesVisited = 0;
  const MAX_NODES = 10000; // safety cap to keep BFS instant in the browser

  for (let depth = 1; depth <= maxDepth; depth++) {
    const nextQueue = [];
    for (const { actorKey, path } of queue) {
      const movies = ACTOR_TO_MOVIES.get(actorKey) || new Set();
      for (const movieKey of movies) {
        const cast = MOVIE_CAST_DB[movieKey] || [];
        for (const actorName of cast) {
          const neighborKey = actorName.toLowerCase();
          if (visited.has(neighborKey)) continue;
          nodesVisited++;
          if (nodesVisited > MAX_NODES) return null;
          const newPath = [...path, { movie: movieKey, actor: actorName }];
          if (neighborKey === endKey) {
            return { distance: depth, path: newPath };
          }
          visited.add(neighborKey);
          nextQueue.push({ actorKey: neighborKey, path: newPath });
        }
      }
    }
    queue = nextQueue;
    if (!queue.length) break;
  }
  return null;
}

// Render the "how to finish your path" continuation from lastActorName → (bfsPath) → endActorName.
// Uses the same timeline markup as renderShortestPathMarkup but with a different visual class.
function renderCompletionPathMarkup(lastActorName, bfsResult) {
  if (!bfsResult || !bfsResult.path.length) {
    return '<div class="result-path__empty">No completion data available.</div>';
  }

  const nodes = [{ type: "actor", label: lastActorName }];
  bfsResult.path.forEach((step) => {
    nodes.push({ type: "movie", label: formatMovieTitle(step.movie) });
    nodes.push({ type: "actor", label: step.actor });
  });

  const rows = nodes.map((node, index) => {
    const side  = index % 2 === 0 ? "left" : "right";
    const emoji = node.type === "actor" ? "🎭" : "🎬";
    const cardClass = node.type === "actor"
      ? "path-node path-node--actor"
      : "path-node path-node--movie";
    const delay = (index * 140) + 150;
    return (
      `<div class="path-item path-item--${side}" style="animation-delay:${delay}ms">` +
      `<div class="${cardClass}">${emoji} ${escapeHtml(node.label)}</div>` +
      `<span class="path-item__dot"></span>` +
      `</div>`
    );
  });

  return `<div class="result-path-timeline">${rows.join("")}</div>`;
}

function getShortestPathMovieKeys(entry) {
  if (!entry) return [];

  if (Array.isArray(entry.canonical_path) && entry.canonical_path.length) {
    return entry.canonical_path
      .map((step) => String(step?.movie_to_next || "").toLowerCase().trim())
      .filter(Boolean);
  }

  if (Array.isArray(entry.solutionHint) && entry.solutionHint.length) {
    return entry.solutionHint
      .map((step) => String(step?.movie || "").toLowerCase().trim())
      .filter(Boolean);
  }

  return [];
}

function getHintMoviesForActor(actorName, count = HINT_MOVIE_COUNT) {
  const actorKey = (actorName || "").toLowerCase();
  const movieKeys = Array.from(ACTOR_TO_MOVIES.get(actorKey) || []);
  if (!movieKeys.length) return [];

  movieKeys.sort((a, b) => {
    const castA = (MOVIE_CAST_DB[a] || []).length;
    const castB = (MOVIE_CAST_DB[b] || []).length;
    if (castA !== castB) return castB - castA;
    return a.localeCompare(b);
  });

  // Guarantee at least one shortest-path movie appears in hint options when data overlaps.
  const shortestPathMovieKeys = getShortestPathMovieKeys(puzzle);
  const guaranteedMovie = shortestPathMovieKeys.find((movieKey) => movieKeys.includes(movieKey));

  const selected = movieKeys.slice(0, count);
  if (guaranteedMovie && !selected.includes(guaranteedMovie)) {
    if (selected.length < count) {
      selected.push(guaranteedMovie);
    } else if (selected.length > 0) {
      selected[selected.length - 1] = guaranteedMovie;
    }
  }

  // Shuffle hints so shortest-path movie isn't always presented first.
  for (let i = selected.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [selected[i], selected[j]] = [selected[j], selected[i]];
  }

  return selected.map(formatMovieTitle);
}

function renderHint(side) {
  if (gameOver) return;
  if (hintsUsed[side]) return;

  const actorName = side === "start" ? startNameEl.textContent : endNameEl.textContent;
  const hintList = side === "start" ? startHintList : endHintList;
  const hintMovies = getHintMoviesForActor(actorName);
  if (!hintMovies.length) {
    showError(`No hint movies available for ${actorName}.`);
    return;
  }

  const chips = hintMovies.map((movie, idx) => (
    `<span class="hint-chip">🎬 ${idx + 1}. ${escapeHtml(movie)}</span>`
  )).join("");
  hintList.innerHTML = `
    <div class="hint-list__title">💡 ${escapeHtml(actorName)} is known for:</div>
    <div class="hint-list__note">Any valid movie still works. These are optional ideas.</div>
    <div class="hint-list__chips">${chips}</div>
  `;
  hintList.classList.remove("hidden");
  hintsUsed[side] = true;
  hideError();
  updateHintControls();
}

function updateHintControls() {
  startHintBtn.disabled = gameOver || hintsUsed.start;
  endHintBtn.disabled = gameOver || hintsUsed.end;

  if (gameOver) {
    hintPenaltyInfo.textContent = "Hint penalties applied in final score.";
  } else {
    hintPenaltyInfo.textContent = `Hint penalty: -${HINT_PENALTY_POINTS} points each.`;
  }
}

function formatMovieTitle(rawTitle) {
  const key = rawTitle.toLowerCase();
  if (MOVIE_TITLE_DISPLAY[key]) return MOVIE_TITLE_DISPLAY[key];
  return rawTitle.replace(/\b\w/g, (c) => c.toUpperCase()).replace(/'([A-Z])/g, (m, c) => `'${c.toLowerCase()}`);
}

movieInput.addEventListener("input", () => {
  const q = movieInput.value.toLowerCase().trim();
  selectedMovie = "";
  selectedActor = "";
  actorInput.value = "";
  submitStepBtn.disabled = true;
  hideError();

  if (!q) { movieSuggestions.classList.add("hidden"); return; }
  const matches = ALL_MOVIES.filter((m) => m.includes(q)).slice(0, 8);
  showSuggestions(movieSuggestions, matches.map(formatMovieTitle));
});

movieInput.addEventListener("blur", () => {
  setTimeout(() => movieSuggestions.classList.add("hidden"), 150);
});

actorInput.addEventListener("input", () => {
  selectedActor = "";
  submitStepBtn.disabled = true;
  hideError();
  const q = actorInput.value.toLowerCase().trim();
  if (!q || !selectedMovie) { actorSuggestions.classList.add("hidden"); return; }
  const cast = MOVIE_CAST_DB[selectedMovie.toLowerCase()] || [];
  const matches = cast.filter((a) => a.toLowerCase().includes(q)).slice(0, 8);
  showSuggestions(actorSuggestions, matches);
});

actorInput.addEventListener("blur", () => {
  setTimeout(() => actorSuggestions.classList.add("hidden"), 150);
});

// ── Movie / actor selection ───────────────────────────────────────────────────
function handleMovieChosen(rawTitle) {
  const key = rawTitle.toLowerCase();
  if (!MOVIE_CAST_DB[key]) {
    showError(`"${rawTitle}" isn't in our movie database. Try a different title.`);
    return;
  }
  const cast = MOVIE_CAST_DB[key];
  const currentLower = currentActorName.toLowerCase();
  const actorInMovie = cast.some((a) => a.toLowerCase() === currentLower);
  if (!actorInMovie) {
    showError(`${currentActorName} doesn't appear in "${rawTitle}". Try another movie.`);
    return;
  }
  selectedMovie = rawTitle;
  hideError();
  actorInput.value = "";
  actorInput.focus();
}

function handleActorChosen(name) {
  if (!selectedMovie) return;
  const key = selectedMovie.toLowerCase();
  const cast = MOVIE_CAST_DB[key] || [];
  const valid = cast.some((a) => a.toLowerCase() === name.toLowerCase());
  if (!valid) {
    showError(`${name} isn't listed in "${selectedMovie}".`);
    return;
  }
  if (name.toLowerCase() === currentActorName.toLowerCase()) {
    showError("You can't pick the same actor you're currently on.");
    return;
  }
  selectedActor = name;
  hideError();
  submitStepBtn.disabled = false;
}

// Allow pressing Enter to confirm typed values
movieInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    movieSuggestions.classList.add("hidden");
    handleMovieChosen(movieInput.value.trim());
  }
});

actorInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    actorSuggestions.classList.add("hidden");
    handleActorChosen(actorInput.value.trim());
  }
});

// ── Submit step ───────────────────────────────────────────────────────────────
submitStepBtn.addEventListener("click", () => {
  if (gameOver || !selectedMovie || !selectedActor) return;

  chainSteps.push({ movie: selectedMovie, actor: selectedActor });
  currentStep = chainSteps.length;
  updateDots();

  const isWin = selectedActor.toLowerCase() === endNameEl.textContent.toLowerCase();
  renderChain();

  // Advance state
  currentActorName = selectedActor;
  currentActorDisplay.textContent = currentActorName;
  syncMobileHud();

  // Reset inputs
  movieInput.value = "";
  actorInput.value = "";
  selectedMovie = "";
  selectedActor = "";
  submitStepBtn.disabled = true;
  updateUndoButton();

  if (isWin) {
    endGame(true);
  } else if (currentStep >= MAX_STEPS) {
    endGame(false);
  }
});

undoStepBtn.addEventListener("click", () => {
  if (gameOver || chainSteps.length === 0) return;

  chainSteps.pop();
  currentStep = chainSteps.length;
  currentActorName = chainSteps.length
    ? chainSteps[chainSteps.length - 1].actor
    : (puzzle?.start?.name || startNameEl.textContent || "Unknown Actor");
  currentActorDisplay.textContent = currentActorName;
  syncMobileHud();

  selectedMovie = "";
  selectedActor = "";
  movieInput.value = "";
  actorInput.value = "";
  submitStepBtn.disabled = true;
  hideError();

  renderChain();
  updateDots();
  updateUndoButton();
});

giveUpBtn.addEventListener("click", () => {
  if (gameOver) return;
  endGame(false, true);
});

// ── Scoring ───────────────────────────────────────────────────────────────────
function calculatePlayerScore(stepsTaken) {
  const shortestPath = puzzle.shortest_hops || currentStep;
  const hintsCount = (hintsUsed.start ? 1 : 0) + (hintsUsed.end ? 1 : 0);
  const efficiencyScore = Math.max(
    0,
    Math.min(EFFICIENCY_SCORE_MAX, EFFICIENCY_SCORE_MAX - ((stepsTaken - shortestPath) * STEP_OVERAGE_PENALTY)),
  );

  const baseScore = Math.round(efficiencyScore);
  const hintPenalty = (hintsUsed.start ? HINT_PENALTY_POINTS : 0) + (hintsUsed.end ? HINT_PENALTY_POINTS : 0);
  let finalScore = Math.max(0, baseScore - hintPenalty);

  // One-hop puzzles are intentionally low scoring when hints are used.
  if (shortestPath === 1) {
    if (hintsCount >= 2) {
      finalScore = 0;
    } else if (hintsCount === 1) {
      finalScore = Math.min(finalScore, ONE_STEP_ONE_HINT_SCORE_CAP);
    }
  }

  return {
    efficiencyScore: Math.round(efficiencyScore),
    baseScore,
    hintPenalty,
    finalScore,
  };
}

// ── End game ──────────────────────────────────────────────────────────────────
function endGame(won, gaveUp = false) {
  gameOver = true;
  inputArea.classList.add("hidden");
  resultBanner.classList.remove("hidden");
  updateHintControls();
  updateUndoButton();

  let gameScore = 0;
  if (won) {
    const rating = currentStep <= 2 ? "Genius 🧠" :
                   currentStep <= 4 ? "Great job! 🌟" : "Good effort! 👍";
    
    const score = calculatePlayerScore(currentStep);
    gameScore = score.finalScore;
    gameRoundScores.push(gameScore);
    gameCumulativeScore += gameScore;

    resultEmoji.textContent = "🎉";
    resultTitle.textContent = "Round Complete!";
    const roundNum = currentRoundNumber;
    resultSub.textContent   = `Round ${roundNum}: Connected in ${currentStep} step${currentStep !== 1 ? "s" : ""}. ${rating}`;
    const shortest = puzzle.shortest_hops;
    const difficultyLabel = puzzle.difficulty ? `${puzzle.difficulty[0].toUpperCase()}${puzzle.difficulty.slice(1)}` : "Unknown";
    const hintsUsedText = [
      hintsUsed.start ? "Start" : null,
      hintsUsed.end ? "End" : null,
    ].filter(Boolean).join(" + ") || "None";
    const metaParts = [
      `<span class="result-pill">Round score: ${gameScore}</span>`,
      `<span class="result-pill">Game score: ${gameCumulativeScore}</span>`,
      `<span class="result-pill">Hints: ${hintsUsedText}</span>`,
      `<span class="result-pill">Difficulty: ${difficultyLabel}</span>`,
    ];
    if (typeof shortest === "number") {
      metaParts.push(`<span class="result-pill">Shortest: ${shortest} step${shortest !== 1 ? "s" : ""}</span>`);
    }
    resultMeta.innerHTML = metaParts.join("");
    resultMeta.classList.remove("hidden");
  } else {
    // --- Compute how close the player got ---
    const endActorName = endNameEl.textContent;
    const lastActorName = currentActorName; // where they left off
    const playerMadeProgress = chainSteps.length > 0;

    let proximityResult = null;
    let partialCredit = 0;
    let proximityLabel = "";

    if (playerMadeProgress) {
      proximityResult = bfsFromActor(lastActorName, endActorName, 3);
      if (proximityResult && (currentStep + proximityResult.distance) <= MAX_STEPS) {
        const hintPenalty = (hintsUsed.start ? HINT_PENALTY_POINTS : 0) + (hintsUsed.end ? HINT_PENALTY_POINTS : 0);
        if (proximityResult.distance === 1) {
          partialCredit = Math.max(0, PARTIAL_CREDIT_1_AWAY - hintPenalty);
          proximityLabel = "So close! 1 step away 🔥";
        } else if (proximityResult.distance === 2) {
          partialCredit = Math.max(0, PARTIAL_CREDIT_2_AWAY - hintPenalty);
          proximityLabel = "Almost there — 2 steps away";
        }
      }
    }

    const roundScore = partialCredit;
    gameRoundScores.push(roundScore);
    gameCumulativeScore += roundScore;

    resultEmoji.textContent = gaveUp ? "🏳️" : "😔";
    if (gaveUp) {
      resultTitle.textContent = "You gave up";
      resultSub.textContent   = `Round ${currentRoundNumber}: Ended after ${currentStep} step${currentStep !== 1 ? "s" : ""}.`;
    } else {
      resultTitle.textContent = "Better luck next time!";
      resultSub.textContent   = `Round ${currentRoundNumber}: You used all ${MAX_STEPS} steps without reaching ${endActorName}.`;
    }

    const metaParts = [
      `<span class="result-pill">Round score: ${roundScore}</span>`,
      `<span class="result-pill">Game score: ${gameCumulativeScore}</span>`,
    ];
    if (proximityLabel) {
      metaParts.push(`<span class="result-pill result-pill--proximity">${proximityLabel}</span>`);
    }
    if (proximityResult && proximityResult.distance > 0) {
      metaParts.push(`<span class="result-pill">${proximityResult.distance} hop${proximityResult.distance !== 1 ? "s" : ""} from ${endActorName}</span>`);
    } else if (playerMadeProgress && !proximityResult) {
      metaParts.push(`<span class="result-pill">Far from ${endActorName}</span>`);
    }
    resultMeta.innerHTML = metaParts.join("");
    resultMeta.classList.remove("hidden");
  }

  resultPath.innerHTML = `
    <div class="result-path__label">🧭 Canonical Shortest Path</div>
    ${renderShortestPathMarkup(puzzle)}
  `;
  resultPath.classList.remove("hidden");
  observePathTimeline(resultPath);

  // For non-wins where the player made at least one step: show how to finish their path.
  if (!won && resultCompletionPath) {
    const lastActorName = chainSteps.length > 0 ? chainSteps[chainSteps.length - 1].actor : null;
    if (lastActorName) {
      const completionBfs = bfsFromActor(lastActorName, endNameEl.textContent, 3);
      if (completionBfs && completionBfs.distance > 0) {
        resultCompletionPath.innerHTML = `
          <div class="result-path__label result-path__label--completion">🔗 How to finish from where you left off</div>
          ${renderCompletionPathMarkup(lastActorName, completionBfs)}
        `;
        resultCompletionPath.classList.remove("hidden");
        observePathTimeline(resultCompletionPath);
      } else {
        resultCompletionPath.classList.add("hidden");
      }
    } else {
      resultCompletionPath.classList.add("hidden");
    }
  } else if (resultCompletionPath) {
    resultCompletionPath.classList.add("hidden");
  }

  // Handle game progression
  if (currentRoundNumber < ROUNDS_PER_GAME) {
    // More rounds in this game
    nextRoundBtn.style.display = "inline-block";
    playAgainBtn.style.display = "none";
  } else {
    // Game complete
    resultTitle.textContent = "🏆 Game Complete!";
    resultSub.textContent = `You finished 5 rounds! Final game score: ${gameCumulativeScore} points.`;
    resultEmoji.textContent = "🏆";
    nextRoundBtn.style.display = "none";
    playAgainBtn.style.display = "inline-block";
    playAgainBtn.textContent = "New Game";
  }
}


// ── Helpers ───────────────────────────────────────────────────────────────────
function showError(msg) {
  errorMsg.textContent = msg;
  errorMsg.classList.remove("hidden");
}
function hideError() {
  errorMsg.classList.add("hidden");
  errorMsg.textContent = "";
}

// Trigger path animations when the timeline scrolls into view.
function observePathTimeline(containerEl) {
  const timeline = containerEl.querySelector(".result-path-timeline");
  if (!timeline) return;
  const observer = new IntersectionObserver(
    (entries, obs) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          entry.target.classList.add("is-revealed");
          obs.unobserve(entry.target);
        }
      });
    },
    { threshold: 0.05 },
  );
  observer.observe(timeline);
}


// ── Modal wiring ──────────────────────────────────────────────────────────────
howToPlayBtn.addEventListener("click", () => openHowToPlayModal(false));
closeModalBtn.addEventListener("click", closeHowToPlayModal);
document.getElementById("rulesCloseBtn").addEventListener("click", () => howToPlayModal.classList.add("hidden"));
startFromModalBtn.addEventListener("click", closeHowToPlayModal);
howToPlayModal.addEventListener("click", (e) => {
  if (e.target === howToPlayModal) {
    closeHowToPlayModal();
  }
});

document.querySelectorAll('.btn-option[data-setup-difficulty]').forEach((btn) => {
  btn.addEventListener('click', () => {
    setSelectedDifficulty(btn.dataset.setupDifficulty, false);
  });
});

document.querySelectorAll('.btn-option[data-setup-actor-mode]').forEach((btn) => {
  btn.addEventListener('click', () => {
    setSelectedActorMode(btn.dataset.setupActorMode, false);
  });
});

// ── New game / play again / button listeners ──────────────────────────────────
document.getElementById("newGameBtn").addEventListener("click", () => startGame(true));
nextRoundBtn.addEventListener("click", startNextRound);
document.getElementById("playAgainBtn").addEventListener("click", () => startGame(true));
startHintBtn.addEventListener("click", () => renderHint("start"));
endHintBtn.addEventListener("click", () => renderHint("end"));

// Difficulty buttons
document.querySelectorAll('.btn-option[data-difficulty]').forEach(btn => {
  btn.addEventListener('click', () => {
    setSelectedDifficulty(btn.dataset.difficulty, true);
  });
});

// Actor mode buttons
document.querySelectorAll('.btn-option[data-actor-mode]').forEach(btn => {
  btn.addEventListener('click', () => {
    setSelectedActorMode(btn.dataset.actorMode, true);
  });
});

// ── Boot ──────────────────────────────────────────────────────────────────────
setupMobileCardToggles();
updateDifficultyButtons();
updateActorModeButtons();
updateSetupDifficultyButtons();
updateSetupActorModeButtons();
openHowToPlayModal();
