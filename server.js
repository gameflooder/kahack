const express = require('express');
const https = require('https');
const http = require('http');
const path = require('path');
const WebSocket = require('ws');
const { Server } = require('socket.io');
const crypto = require('crypto');

// Global error handlers to prevent crashes
process.on('uncaughtException', (err) => {
    console.error('[FATAL] Uncaught exception:', err.message);
});
process.on('unhandledRejection', (reason) => {
    console.error('[FATAL] Unhandled rejection:', reason);
});

const E2E_KEY = Buffer.from('4b6168616b466c6f6f6465724145533235364743454e435259505433443b2931', 'hex');

// ==================== QUIZ ANSWER LOOKUP SYSTEM ====================
// Store linked quiz answers per game PIN
const linkedQuizzes = {};

// Search for quizzes by name
async function searchKahootQuizzes(query, limit = 15) {
    return new Promise((resolve, reject) => {
        const url = `https://create.kahoot.it/rest/kahoots/?query=${encodeURIComponent(query)}&limit=${limit}&orderBy=relevance`;
        https.get(url, { headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' } }, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    const parsed = JSON.parse(data);
                    const results = (parsed.entities || []).map(item => ({
                        uuid: item.card?.uuid,
                        title: item.card?.title,
                        description: item.card?.description,
                        questionCount: item.card?.number_of_questions,
                        creator: item.card?.creator_username
                    })).filter(r => r.uuid);
                    resolve(results);
                } catch (e) { reject(e); }
            });
        }).on('error', reject);
    });
}

// Fetch quiz answers by UUID
async function fetchQuizAnswers(quizId) {
    return new Promise((resolve, reject) => {
        const url = `https://create.kahoot.it/rest/kahoots/${quizId}`;
        https.get(url, { headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' } }, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    const quiz = JSON.parse(data);
                    const questions = (quiz.questions || []).map((q, i) => ({
                        questionIndex: i,
                        type: q.type,
                        question: (q.question || '').replace(/<[^>]*>/g, ''),
                        choices: (q.choices || []).map(c => ({
                            answer: (c.answer || '').replace(/<[^>]*>/g, ''),
                            correct: c.correct === true
                        })),
                        correctIndex: (q.choices || []).findIndex(c => c.correct === true),
                        correctIndices: (q.choices || []).map((c, idx) => c.correct ? idx : null).filter(x => x !== null)
                    }));
                    resolve({ title: quiz.title, uuid: quiz.uuid, questions });
                } catch (e) { reject(e); }
            });
        }).on('error', reject);
    });
}

// Get correct answer for a question
function getCorrectAnswer(pin, questionIndex) {
    const quiz = linkedQuizzes[pin];
    if (!quiz || !quiz.questions) return null;
    const q = quiz.questions[questionIndex];
    if (!q) return null;
    
    if (q.type === 'multiple_select_quiz') {
        return { type: 'multi', indices: q.correctIndices };
    } else if (q.type === 'open_ended' || q.type === 'type_answer') {
        const correct = q.choices.find(c => c.correct);
        return { type: 'text', answer: correct ? correct.answer : '' };
    } else {
        return { type: 'single', index: q.correctIndex };
    }
}

function decryptE2E(encryptedBase64) {
    try {
        const combined = Buffer.from(encryptedBase64, 'base64');
        const iv = combined.slice(0, 12);
        const encrypted = combined.slice(12, -16);
        const authTag = combined.slice(-16);
        
        const decipher = crypto.createDecipheriv('aes-256-gcm', E2E_KEY, iv);
        decipher.setAuthTag(authTag);
        
        let decrypted = decipher.update(encrypted, null, 'utf8');
        decrypted += decipher.final('utf8');
        
        return JSON.parse(decrypted);
    } catch (e) {
        return null;
    }
}

function encryptE2E(data) {
    try {
        const iv = crypto.randomBytes(12);
        const cipher = crypto.createCipheriv('aes-256-gcm', E2E_KEY, iv);
        
        let encrypted = cipher.update(JSON.stringify(data), 'utf8');
        encrypted = Buffer.concat([encrypted, cipher.final()]);
        const authTag = cipher.getAuthTag();
        
        const combined = Buffer.concat([iv, encrypted, authTag]);
        return combined.toString('base64');
    } catch (e) {
        return null;
    }
}

function e2eResponse(res, data, statusCode = 200) {
    const encrypted = encryptE2E(data);
    res.status(statusCode).json({ e2e: encrypted });
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST']
    }
});
const PORT = 9235;

const bots = [];
let botIdCounter = 0;
let currentAnswerMode = 'manual';

// Periodic cleanup of disconnected/failed bots to prevent memory leaks
setInterval(() => {
    const before = bots.length;
    for (let i = bots.length - 1; i >= 0; i--) {
        const bot = bots[i];
        if (bot.status === 'disconnected' || bot.status === 'failed') {
            if (bot.ws) { bot.ws.removeAllListeners(); try { bot.ws.close(); } catch(e) {} bot.ws = null; }
            bots.splice(i, 1);
        }
    }
    if (bots.length !== before) console.log(`[Cleanup] Removed ${before - bots.length} dead bots`);
}, 30000);

let pendingJoins = [];
let joinBatchTimer = null;
function emitBatchedJoins() {
    if (pendingJoins.length > 0) {
        const batch = pendingJoins.splice(0, pendingJoins.length);
        io.emit('botJoinBatch', { count: batch.length, names: batch.slice(0, 10) });
    }
    // Clear timer if nothing pending (check after potential emit)
    if (pendingJoins.length === 0 && joinBatchTimer) {
        clearInterval(joinBatchTimer);
        joinBatchTimer = null;
    }
}
function queueJoinEvent(name) {
    pendingJoins.push(name);
    if (!joinBatchTimer) {
        joinBatchTimer = setInterval(emitBatchedJoins, 500);
    }
}

const ALLOWED_ORIGINS = [
    'https://mojhehh.github.io',
    'https://gameflooder.github.io',
    'http://localhost:9235',
    'http://127.0.0.1:9235'
];

const PIN_REGEX = /^\d{4,10}$/;

function isAllowedOrigin(origin) {
    if (!origin) return true;
    if (ALLOWED_ORIGINS.includes(origin)) return true;
    if (origin.startsWith('http://localhost:')) return true;
    if (origin.startsWith('http://127.0.0.1:')) return true;
    return false;
}

app.use((req, res, next) => {
    const origin = req.headers.origin;
    if (origin && isAllowedOrigin(origin)) {
        res.setHeader('Access-Control-Allow-Origin', origin);
        res.setHeader('Vary', 'Origin');
        res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
        res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-E2E');
        res.setHeader('Access-Control-Allow-Credentials', 'true');
    }
    if (req.method === 'OPTIONS') return res.sendStatus(204);
    next();
});

app.use(express.static(__dirname));
app.use(express.json());

// Handle JSON parsing errors
app.use((err, req, res, next) => {
    if (err instanceof SyntaxError && err.status === 400 && 'body' in err) {
        return res.status(400).json({ error: 'Invalid JSON' });
    }
    next(err);
});

function parseOffset(challengeBody) {
    const match = challengeBody.match(/var offset\s*=\s*([^;]+)/);
    if (!match) return 0;
    let expr = match[1].replace(/\\t/g, '').replace(/\t/g, '').replace(/\s+/g, '');
    try {
        if (/^[\d+\-*\/\(\)\s]+$/.test(expr)) return eval(expr);
    } catch (e) {}
    return 0;
}

function kahootDecode(message, offset) {
    let result = '';
    for (let i = 0; i < message.length; i++) {
        const charCode = message.charCodeAt(i);
        result += String.fromCharCode((((charCode * i) + offset) % 77) + 48);
    }
    return result;
}

function decodeToken(headerToken, challengeBody) {
    const msgMatch = challengeBody.match(/decode\.call\(this,\s*'([^']+)'\)/);
    if (!msgMatch) return headerToken;
    
    const challengeMessage = msgMatch[1];
    const offset = parseOffset(challengeBody);
    const decodedChallenge = kahootDecode(challengeMessage, offset);
    
    const tokenBytes = Buffer.from(headerToken, 'base64');
    let result = '';
    for (let i = 0; i < tokenBytes.length; i++) {
        const challengeChar = decodedChallenge.charCodeAt(i % decodedChallenge.length);
        result += String.fromCharCode(tokenBytes[i] ^ challengeChar);
    }
    return result;
}

app.get('/api/reserve/:pin', (req, res) => {
    const pin = req.params.pin;
    const timestamp = Date.now();
    const url = `https://kahoot.it/reserve/session/${pin}/?${timestamp}`;
    
    https.get(url, (response) => {
        let data = '';
        response.on('data', chunk => data += chunk);
        response.on('end', () => {
            if (response.statusCode !== 200) {
                res.status(response.statusCode).json({ error: 'Game not found or ended' });
                return;
            }
            
            try {
                const sessionToken = response.headers['x-kahoot-session-token'];
                const body = JSON.parse(data);
                const token = decodeToken(sessionToken, body.challenge);
                
                res.json({
                    token,
                    twoFactorAuth: body.twoFactorAuth,
                    namerator: body.namerator
                });
            } catch (e) {
                res.status(500).json({ error: e.message });
            }
        });
    }).on('error', (e) => {
        res.status(500).json({ error: e.message });
    });
});

// ==================== QUIZ ANSWER API ENDPOINTS ====================

// Search for quizzes
app.get('/api/quiz/search', async (req, res) => {
    const query = req.query.q || req.query.query;
    if (!query || query.length < 2) {
        return res.status(400).json({ error: 'Query too short' });
    }
    try {
        const results = await searchKahootQuizzes(query);
        res.json({ success: true, results });
    } catch (e) {
        res.status(500).json({ error: 'Search failed', message: e.message });
    }
});

// Get quiz answers by UUID
app.get('/api/quiz/:uuid', async (req, res) => {
    const uuid = req.params.uuid;
    if (!/^[0-9a-f-]{36}$/i.test(uuid)) {
        return res.status(400).json({ error: 'Invalid UUID' });
    }
    try {
        const quiz = await fetchQuizAnswers(uuid);
        res.json({ success: true, ...quiz });
    } catch (e) {
        res.status(500).json({ error: 'Failed to fetch quiz', message: e.message });
    }
});

// Link quiz answers to a game PIN
app.post('/api/quiz/link', async (req, res) => {
    const { pin, uuid } = req.body || {};
    if (!pin || !uuid) {
        return res.status(400).json({ error: 'PIN and UUID required' });
    }
    try {
        const quiz = await fetchQuizAnswers(uuid);
        linkedQuizzes[pin] = quiz;
        console.log(`[Quiz] Linked "${quiz.title}" (${quiz.questions.length} questions) to PIN ${pin}`);
        io.emit('quizLinked', { pin, title: quiz.title, questionCount: quiz.questions.length });
        res.json({ success: true, title: quiz.title, questionCount: quiz.questions.length });
    } catch (e) {
        res.status(500).json({ error: 'Failed to link quiz', message: e.message });
    }
});

// Get correct answer for current question
app.get('/api/quiz/answer/:pin/:questionIndex', (req, res) => {
    const { pin, questionIndex } = req.params;
    const answer = getCorrectAnswer(pin, parseInt(questionIndex, 10));
    if (answer) {
        res.json({ success: true, ...answer });
    } else {
        res.json({ success: false, message: 'No quiz linked or question not found' });
    }
});

// Unlink quiz from PIN
app.post('/api/quiz/unlink', (req, res) => {
    const { pin } = req.body || {};
    if (linkedQuizzes[pin]) {
        delete linkedQuizzes[pin];
        res.json({ success: true });
    } else {
        res.json({ success: false, message: 'No quiz linked to this PIN' });
    }
});

const LOOKALIKES = {
    'a': 'а', 'A': 'Α', 'c': 'с', 'C': 'С', 'e': 'е', 'E': 'Ε',
    'i': 'і', 'I': 'Ι', 'o': 'о', 'O': 'О', 'p': 'р', 'P': 'Р',
    's': 'ѕ', 'S': 'Ѕ', 'x': 'х', 'X': 'Χ', 'y': 'у', 'Y': 'Υ',
    'j': 'ј', 'J': 'Ј', 'B': 'Β', 'H': 'Η', 'K': 'Κ', 'M': 'Μ',
    'N': 'Ν', 'T': 'Τ', 'Z': 'Ζ'
};

function bypassFilter(str, useBypass) {
    if (!useBypass) return str;
    return str.split('').map(c => LOOKALIKES[c] || c).join('');
}

function reserveSession(pin) {
    return new Promise((resolve, reject) => {
        const url = `https://kahoot.it/reserve/session/${pin}/?${Date.now()}`;
        console.log(`[Reserve] Requesting: ${url}`);
        https.get(url, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                console.log(`[Reserve] Status: ${res.statusCode}`);
                if (res.statusCode !== 200) return reject(new Error(`Game not found (${res.statusCode})`));
                try {
                    const sessionToken = res.headers['x-kahoot-session-token'];
                    const body = JSON.parse(data);
                    resolve({ sessionToken, challenge: body.challenge });
                } catch (e) { reject(e); }
            });
        }).on('error', (e) => {
            console.log(`[Reserve] Error: ${e.message}`);
            reject(e);
        });
    });
}

function handleBotMessage(bot, m) {
    if (m.channel === '/meta/handshake' && m.successful) {
        bot.clientId = m.clientId;
        const connect = [{ id: String(bot.messageId++), channel: '/meta/connect', connectionType: 'websocket', advice: { timeout: 0 }, clientId: bot.clientId, ext: { ack: 0, timesync: { tc: Date.now(), l: 0, o: 0 } } }];
        bot.ws.send(JSON.stringify(connect));
    }
    
    if (m.channel === '/meta/connect' && m.successful) {
        bot.ackCount = m.ext?.ack || bot.ackCount + 1;
        if (bot.ackCount === 1 && bot.status === 'connecting') {
            const connect2 = [{ id: String(bot.messageId++), channel: '/meta/connect', connectionType: 'websocket', clientId: bot.clientId, ext: { ack: 1, timesync: { tc: Date.now(), l: 0, o: 0 } } }];
            bot.ws.send(JSON.stringify(connect2));
            const login = [{ id: String(bot.messageId++), channel: '/service/controller', data: { type: 'login', gameid: bot.pin, host: 'kahoot.it', name: bot.name, content: '{}' }, clientId: bot.clientId, ext: {} }];
            bot.ws.send(JSON.stringify(login));
        } else {
            setTimeout(() => {
                if (bot.ws && bot.ws.readyState === WebSocket.OPEN) {
                    const keepAlive = [{ id: String(bot.messageId++), channel: '/meta/connect', connectionType: 'websocket', clientId: bot.clientId, ext: { ack: bot.ackCount, timesync: { tc: Date.now(), l: 0, o: 0 } } }];
                    try { bot.ws.send(JSON.stringify(keepAlive)); } catch (e) {}
                }
            }, 500);
        }
    }
    
    if (m.channel === '/service/controller') {
        if (m.data?.type === 'loginResponse') {
            bot.status = 'joined';
            bot.cid = m.data.cid;
            queueJoinEvent(bot.originalName); // Batched instead of immediate emit
            const namerator = [{ id: String(bot.messageId++), channel: '/service/controller', data: { gameid: bot.pin, type: 'message', host: 'kahoot.it', id: 16, content: JSON.stringify({ usingNamerator: false }) }, clientId: bot.clientId, ext: {} }];
            try { bot.ws.send(JSON.stringify(namerator)); } catch (e) {}
        }
        if (m.data?.error) {
            const err = m.data.description || m.data.error || '';
            if (/duplicate/i.test(err)) {
                bot.name = `${bot.originalName}-${Math.random().toString(36).slice(2, 5)}`;
                const loginRetry = [{ id: String(bot.messageId++), channel: '/service/controller', data: { type: 'login', gameid: bot.pin, host: 'kahoot.it', name: bot.name, content: '{}' }, clientId: bot.clientId, ext: {} }];
                try { bot.ws.send(JSON.stringify(loginRetry)); } catch (e) {}
            } else {
                bot.status = 'failed';
                io.emit('botJoinFail', { message: err });
            }
        }
    }
    
    if (m.channel === '/service/player' && m.data) {
        // Two-Factor Auth handling (id 53 = reset/show code, 51 = wrong, 52 = correct)
        if (m.data.id === 53) {
            bot.twoFactorPending = true;
            io.emit('twoFactorReset', { name: bot.originalName });
            // Auto-answer 2FA if enabled
            if (bot.autoTwoFactor) {
                setTimeout(() => answerTwoFactor(bot, [0, 1, 2, 3]), 300);
            }
        }
        if (m.data.id === 51) {
            io.emit('twoFactorWrong', { name: bot.originalName });
            // Retry with different sequence
            if (bot.autoTwoFactor) {
                setTimeout(() => answerTwoFactor(bot, [0, 1, 2, 3]), 500);
            }
        }
        if (m.data.id === 52) {
            bot.twoFactorPending = false;
            io.emit('twoFactorCorrect', { name: bot.originalName });
        }
        
        // Question start (id 2)
        if (m.data.id === 2) {
            try {
                const content = JSON.parse(m.data.content || '{}');
                const newQuestionIndex = content.questionIndex !== undefined ? content.questionIndex : (bot.questionIndex + 1);
                const numChoices = content.quizQuestionAnswers || content.numberOfChoices || 4;
                const questionType = content.type || content.gameBlockType || 'quiz';
                
                // Only process if this is actually a new question for THIS bot
                if (newQuestionIndex !== bot.questionIndex || !bot.answered) {
                    const wasNewQuestion = newQuestionIndex !== bot.questionIndex;
                    bot.questionIndex = newQuestionIndex;
                    bot.answered = false;
                    bot.currentQuestionType = questionType;
                    bot.currentChoiceCount = numChoices;
                    
                    // Get correct answer if quiz is linked
                    const correctAnswer = getCorrectAnswer(bot.pin, bot.questionIndex);
                    
                    // Only emit questionStart ONCE per question (from first bot that gets it)
                    if (!global.lastQuestionEmitted || global.lastQuestionEmitted !== newQuestionIndex) {
                        global.lastQuestionEmitted = newQuestionIndex;
                        console.log(`[Question] Q${newQuestionIndex + 1} started, mode=${currentAnswerMode}, correctAnswer=${JSON.stringify(correctAnswer)}`);
                        io.emit('questionStart', { 
                            questionIndex: newQuestionIndex, 
                            choices: numChoices, 
                            type: questionType,
                            correctAnswer: correctAnswer 
                        });
                    }
                    
                    if (currentAnswerMode !== 'manual') {
                        const delay = 800 + Math.random() * 1200; // 0.8-2 second delay
                        const currentQ = bot.questionIndex;
                        setTimeout(() => {
                            if (!bot.answered && bot.status === 'joined' && bot.questionIndex === currentQ) {
                                let choice;
                                
                                // 'correct' mode uses linked quiz answers
                                if (currentAnswerMode === 'correct') {
                                    if (correctAnswer && correctAnswer.type === 'single' && correctAnswer.index >= 0) {
                                        choice = correctAnswer.index;
                                        console.log(`[AutoAnswer] Bot ${bot.originalName} answering correct: ${choice}`);
                                    } else if (correctAnswer && correctAnswer.type === 'multi' && correctAnswer.indices?.length > 0) {
                                        choice = correctAnswer.indices[0];
                                        console.log(`[AutoAnswer] Bot ${bot.originalName} answering multi-correct: ${choice}`);
                                    } else {
                                        // No correct answer found, fallback to random
                                        choice = Math.floor(Math.random() * numChoices);
                                        console.log(`[AutoAnswer] Bot ${bot.originalName} no correct found, random: ${choice}`);
                                    }
                                } else if (currentAnswerMode === 'random') {
                                    choice = Math.floor(Math.random() * numChoices);
                                } else if (currentAnswerMode === 'first') choice = 0;
                                else if (currentAnswerMode === 'second') choice = 1;
                                else if (currentAnswerMode === 'third') choice = 2;
                                else if (currentAnswerMode === 'fourth') choice = 3;
                                else choice = Math.floor(Math.random() * numChoices);
                                
                                if (choice >= numChoices || choice < 0) choice = Math.floor(Math.random() * numChoices);
                                sendBotAnswer(bot, choice);
                            }
                        }, delay);
                    }
                }
            } catch (e) {
                console.error('[Question] Parse error:', e.message);
            }
        }
        
        if (m.data.id === 1) {
            try {
                const content = JSON.parse(m.data.content || '{}');
                if (content.questionIndex !== undefined) {
                    bot.questionIndex = content.questionIndex;
                    bot.answered = false;
                }
            } catch (e) {}
        }
        
        if (m.data.id === 8) {
            bot.answered = true;
        }
        
        // Quiz end (id 10)
        if (m.data.id === 10) {
            try {
                const content = JSON.parse(m.data.content || '{}');
                if (content.quizEnded || content.kickCode) {
                    bot.status = 'disconnected';
                    io.emit('quizEnd', { name: bot.originalName, kicked: !!content.kickCode });
                }
            } catch (e) {}
        }
    }
}

// Two-Factor Auth answer function
function answerTwoFactor(bot, sequence) {
    if (!bot || !bot.ws || bot.ws.readyState !== WebSocket.OPEN || !bot.clientId) return false;
    
    const twoFactorMsg = [{
        id: String(bot.messageId++),
        channel: '/service/controller',
        data: {
            type: 'message',
            gameid: bot.pin,
            host: 'kahoot.it',
            id: 50,
            content: JSON.stringify({ sequence: sequence })
        },
        clientId: bot.clientId,
        ext: {}
    }];
    
    try {
        bot.ws.send(JSON.stringify(twoFactorMsg));
        return true;
    } catch (e) {
        return false;
    }
}

function sendBotAnswer(bot, choice) {
    if (!bot || !bot.ws || bot.ws.readyState !== WebSocket.OPEN || !bot.clientId) return false;
    if (bot.answered) return false;
    if (bot.status !== 'joined') return false;
    
    const answerMsg = [{ 
        id: String(bot.messageId++), 
        channel: '/service/controller', 
        data: { 
            type: 'message', 
            gameid: bot.pin, 
            host: 'kahoot.it', 
            id: 45, 
            content: JSON.stringify({ 
                choice: choice, 
                questionIndex: bot.questionIndex || 0, 
                meta: { lag: Math.floor(Math.random() * 50) + 10 } 
            }) 
        }, 
        clientId: bot.clientId, 
        ext: {} 
    }];
    
    try { 
        bot.ws.send(JSON.stringify(answerMsg)); 
        bot.answered = true; // Mark as answered
        return true; 
    } catch (e) { 
        return false; 
    }
}

async function createBot(pin, name, useBypass) {
    const botId = ++botIdCounter;
    try {
        console.log(`[Bot ${botId}] Reserving session for PIN ${pin}...`);
        const { sessionToken, challenge } = await reserveSession(pin);
        console.log(`[Bot ${botId}] Got session, decoding token...`);
        const token = decodeToken(sessionToken, challenge);
        const wsUrl = `wss://kahoot.it/cometd/${pin}/${token}`;
        console.log(`[Bot ${botId}] Connecting to WebSocket...`);
        const ws = new WebSocket(wsUrl, { headers: { 'Origin': 'https://kahoot.it', 'User-Agent': 'Mozilla/5.0' } });
        
        const bot = { id: botId, name: bypassFilter(name, useBypass), originalName: name, pin, ws, clientId: null, messageId: 1, questionIndex: -1, status: 'connecting', ackCount: 0, answered: false };
        bots.push(bot);
        
        ws.on('open', () => {
            const handshake = [{ id: String(bot.messageId++), version: '1.0', minimumVersion: '1.0', channel: '/meta/handshake', supportedConnectionTypes: ['websocket', 'long-polling', 'callback-polling'], advice: { timeout: 60000, interval: 0 }, ext: { ack: true, timesync: { tc: Date.now(), l: 0, o: 0 } } }];
            ws.send(JSON.stringify(handshake));
        });
        ws.on('message', (data) => { try { JSON.parse(data.toString()).forEach(m => handleBotMessage(bot, m)); } catch (e) {} });
        ws.on('close', () => { bot.status = 'disconnected'; io.emit('botDisconnected', { name: bot.originalName }); });
        ws.on('error', () => { bot.status = 'failed'; });
        return botId;
    } catch (e) {
        io.emit('botJoinFail', { message: e.message });
        return null;
    }
}

let stopSpawning = false;

app.post('/api/spawn', async (req, res) => {
    let payload = req.body || {};
    if (req.headers['x-e2e'] === '1' && payload.e2e) {
        const decrypted = decryptE2E(payload.e2e);
        if (!decrypted) return res.status(400).json({ error: 'Decryption failed' });
        payload = decrypted;
    }
    
    const { pin, count, baseName, bypass } = payload;
    const pinStr = String(pin || '').trim();
    if (!pinStr || !PIN_REGEX.test(pinStr)) return res.status(400).json({ error: 'Invalid PIN' });
    const c = Math.max(1, Math.min(parseInt(count, 10) || 1, 500));
    const name = String(baseName || 'Bot');
    
    stopSpawning = false;
    res.json({ message: 'Spawning bots...', count: c });
    
    const BATCH_SIZE = 10;
    const BATCH_DELAY = 150;
    
    let spawned = 0;
    for (let i = 0; i < c; i += BATCH_SIZE) {
        if (stopSpawning) {
            io.emit('spawnStopped', { spawned, total: c });
            break;
        }
        
        const batchPromises = [];
        for (let j = 0; j < BATCH_SIZE && (i + j) < c; j++) {
            if (stopSpawning) break;
            const suffix = Math.random().toString(16).substring(2, 6);
            const botName = c === 1 ? name : `${name} ${suffix}`;
            batchPromises.push(createBot(pinStr, botName, bypass === true));
        }
        
        const results = await Promise.allSettled(batchPromises);
        for (const r of results) {
            if (r.status === 'fulfilled') spawned++;
        }
        
        if (i + BATCH_SIZE < c && !stopSpawning) {
            await new Promise(r => setTimeout(r, BATCH_DELAY));
        }
    }
});

app.post('/api/stop-spawn', (req, res) => {
    stopSpawning = true;
    res.json({ stopped: true });
});

app.get('/api/bots', (req, res) => {
    const total = bots.length;
    const joined = bots.filter(b => b.status === 'joined').length;
    const failed = bots.filter(b => b.status === 'failed').length;
    const connecting = bots.filter(b => b.status === 'connecting').length;
    res.json({ total, joined, failed, connecting });
});

app.get('/api/bots/debug', (req, res) => {
    const botInfo = bots.map(b => ({
        name: b.originalName,
        status: b.status,
        questionIndex: b.questionIndex,
        answered: b.answered
    }));
    res.json(botInfo);
});

const VALID_ANSWER_MODES = ['manual', 'random', 'first', 'second', 'third', 'fourth', 'correct'];

app.post('/api/answer-mode', (req, res) => {
    const { mode } = req.body || {};
    currentAnswerMode = VALID_ANSWER_MODES.includes(mode) ? mode : 'manual';
    const joined = bots.filter(b => b.status === 'joined').length;
    res.json({ mode: currentAnswerMode, sent: joined });
});

app.get('/api/answer-mode', (req, res) => {
    res.json({ mode: currentAnswerMode });
});

app.post('/api/answer', (req, res) => {
    let { answer } = req.body || {};
    if (answer === 'random') answer = Math.floor(Math.random() * 4);
    else if (answer === 'first' || answer === 'red') answer = 0;
    else if (answer === 'second' || answer === 'blue') answer = 1;
    else if (answer === 'third' || answer === 'yellow') answer = 2;
    else if (answer === 'fourth' || answer === 'green') answer = 3;
    
    answer = parseInt(answer);
    if (isNaN(answer) || answer < 0 || answer > 7) return res.status(400).json({ error: 'Invalid answer' });
    
    // Reset answered flag for all joined bots
    for (const bot of bots) {
        if (bot.status === 'joined') bot.answered = false;
    }
    
    // Multiple passes to catch stragglers
    let sent = 0;
    for (let pass = 0; pass < 3; pass++) {
        for (const bot of bots) {
            if (bot.status === 'joined' && !bot.answered && sendBotAnswer(bot, answer)) sent++;
        }
    }
    io.emit('questionAnswered', { answer, sent });
    res.json({ answer, sent });
});

// Two-Factor Auth endpoint
app.post('/api/twofactor', (req, res) => {
    const { sequence } = req.body || {};
    const seq = Array.isArray(sequence) ? sequence : [0, 1, 2, 3];
    let sent = 0;
    for (const bot of bots) {
        if (bot.status === 'joined' || bot.twoFactorPending) {
            if (answerTwoFactor(bot, seq)) sent++;
        }
    }
    res.json({ success: true, sent });
});

// Answer with correct (uses linked quiz)
app.post('/api/answer-correct', (req, res) => {
    const { pin } = req.body || {};
    const targetPin = pin || (bots.length > 0 ? bots[0].pin : null);
    
    if (!targetPin) return res.status(400).json({ error: 'No active bots or PIN provided' });
    
    // Get current question index from first joined bot
    const firstBot = bots.find(b => b.status === 'joined' && b.pin === targetPin);
    if (!firstBot) return res.status(400).json({ error: 'No joined bots for this PIN' });
    
    const correctAnswer = getCorrectAnswer(targetPin, firstBot.questionIndex);
    if (!correctAnswer) return res.status(400).json({ error: 'No quiz linked or question not found' });
    
    let choice;
    if (correctAnswer.type === 'single') {
        choice = correctAnswer.index;
    } else if (correctAnswer.type === 'multi') {
        choice = correctAnswer.indices[0];
    } else {
        return res.status(400).json({ error: 'Cannot auto-answer text questions' });
    }
    
    // Reset and send
    for (const bot of bots) {
        if (bot.status === 'joined' && bot.pin === targetPin) bot.answered = false;
    }
    
    let sent = 0;
    for (let pass = 0; pass < 3; pass++) {
        for (const bot of bots) {
            if (bot.status === 'joined' && bot.pin === targetPin && !bot.answered && sendBotAnswer(bot, choice)) sent++;
        }
    }
    
    io.emit('questionAnswered', { answer: choice, sent, correct: true });
    res.json({ answer: choice, sent, correct: true });
});

function disconnectBot(bot) {
    if (bot.ws) {
        if (bot.ws.readyState === WebSocket.OPEN && bot.clientId) {
            try {
                const disconnect = [{ id: String(bot.messageId++), channel: '/meta/disconnect', clientId: bot.clientId, ext: {} }];
                bot.ws.send(JSON.stringify(disconnect));
            } catch (e) {}
        }
        // Remove all listeners to prevent memory leaks
        bot.ws.removeAllListeners();
        try { bot.ws.close(); } catch (e) {}
        bot.ws = null;
    }
    bot.status = 'disconnected';
}

app.post('/api/kill-all', (req, res) => {
    let killed = 0;
    for (const bot of bots) {
        disconnectBot(bot);
        killed++;
    }
    bots.length = 0;
    pendingJoins = [];
    if (joinBatchTimer) {
        clearInterval(joinBatchTimer);
        joinBatchTimer = null;
    }
    res.json({ killed });
});

app.post('/api/leave', (req, res) => {
    let killed = 0;
    for (const bot of bots) {
        disconnectBot(bot);
        killed++;
    }
    bots.length = 0;
    pendingJoins = [];
    if (joinBatchTimer) {
        clearInterval(joinBatchTimer);
        joinBatchTimer = null;
    }
    res.json({ killed });
});

app.post('/api/e2e', async (req, res) => {
    if (req.headers['x-e2e'] !== '1' || !req.body?.e2e) {
        return res.status(400).json({ error: 'Invalid E2E request' });
    }
    
    const decrypted = decryptE2E(req.body.e2e);
    if (!decrypted) {
        return res.status(400).json({ error: 'Decryption failed' });
    }
    
    const path = decrypted._path;
    delete decrypted._path;
    
    try {
        if (path === '/api/spawn') {
            const { pin, count, baseName, bypass } = decrypted;
            const pinStr = String(pin || '').trim();
            if (!pinStr || !PIN_REGEX.test(pinStr)) return e2eResponse(res, { error: 'Invalid PIN' }, 400);
            const c = Math.max(1, Math.min(parseInt(count, 10) || 1, 500));
            const name = String(baseName || 'Bot');
            
            stopSpawning = false;
            e2eResponse(res, { message: 'Spawning bots...', count: c });
            
            (async () => {
                const BATCH_SIZE = 10;
                const BATCH_DELAY = 100;
                let spawned = 0;
                try {
                    for (let i = 0; i < c; i += BATCH_SIZE) {
                        if (stopSpawning) { io.emit('spawnStopped', { spawned, total: c }); break; }
                        const batchPromises = [];
                        for (let j = 0; j < BATCH_SIZE && (i + j) < c; j++) {
                            if (stopSpawning) break;
                            const suffix = Math.random().toString(16).substring(2, 6);
                            const botName = c === 1 ? name : `${name} ${suffix}`;
                            batchPromises.push(createBot(pinStr, botName, bypass === true));
                        }
                        const results = await Promise.allSettled(batchPromises);
                        for (const r of results) if (r.status === 'fulfilled') spawned++;
                        if (i + BATCH_SIZE < c && !stopSpawning) await new Promise(r => setTimeout(r, BATCH_DELAY));
                    }
                } catch (err) {
                    console.error('[E2E Spawn] Error:', err.message);
                }
            })();
        }
        else if (path === '/api/bots') {
            const total = bots.length;
            const joined = bots.filter(b => b.status === 'joined').length;
            const failed = bots.filter(b => b.status === 'failed').length;
            const connecting = bots.filter(b => b.status === 'connecting').length;
            e2eResponse(res, { total, joined, failed, connecting });
        }
        else if (path === '/api/stop-spawn') {
            stopSpawning = true;
            e2eResponse(res, { stopped: true });
        }
        else if (path === '/api/kill-all' || path === '/api/leave') {
            let killed = 0;
            for (const bot of bots) { disconnectBot(bot); killed++; }
            bots.length = 0;
            pendingJoins = [];
            if (joinBatchTimer) { clearInterval(joinBatchTimer); joinBatchTimer = null; }
            e2eResponse(res, { killed });
        }
        else if (path === '/api/answer') {
            let { answer } = decrypted;
            if (answer === 'random') answer = Math.floor(Math.random() * 4);
            else if (answer === 'first' || answer === 'red') answer = 0;
            else if (answer === 'second' || answer === 'blue') answer = 1;
            else if (answer === 'third' || answer === 'yellow') answer = 2;
            else if (answer === 'fourth' || answer === 'green') answer = 3;
            answer = parseInt(answer);
            if (isNaN(answer) || answer < 0 || answer > 7) return e2eResponse(res, { error: 'Invalid answer' }, 400);
            for (const bot of bots) if (bot.status === 'joined') bot.answered = false;
            let sent = 0;
            for (let pass = 0; pass < 3; pass++) {
                for (const bot of bots) if (bot.status === 'joined' && !bot.answered && sendBotAnswer(bot, answer)) sent++;
            }
            io.emit('questionAnswered', { answer, sent });
            e2eResponse(res, { answer, sent });
        }
        else if (path === '/api/answer-mode') {
            const { mode } = decrypted;
            currentAnswerMode = VALID_ANSWER_MODES.includes(mode) ? mode : 'manual';
            const joined = bots.filter(b => b.status === 'joined').length;
            e2eResponse(res, { mode: currentAnswerMode, sent: joined });
        }
        // Quiz search
        else if (path === '/api/quiz/search') {
            const { query } = decrypted;
            if (!query || query.length < 2) return e2eResponse(res, { error: 'Query too short' }, 400);
            try {
                const results = await searchKahootQuizzes(query);
                e2eResponse(res, { success: true, results });
            } catch (e) {
                e2eResponse(res, { error: 'Search failed' }, 500);
            }
        }
        // Quiz fetch
        else if (path === '/api/quiz/fetch') {
            const { uuid } = decrypted;
            if (!uuid || !/^[0-9a-f-]{36}$/i.test(uuid)) return e2eResponse(res, { error: 'Invalid UUID' }, 400);
            try {
                const quiz = await fetchQuizAnswers(uuid);
                e2eResponse(res, { success: true, ...quiz });
            } catch (e) {
                e2eResponse(res, { error: 'Failed to fetch quiz' }, 500);
            }
        }
        // Quiz link
        else if (path === '/api/quiz/link') {
            const { pin, uuid } = decrypted;
            if (!pin || !uuid) return e2eResponse(res, { error: 'PIN and UUID required' }, 400);
            try {
                const quiz = await fetchQuizAnswers(uuid);
                linkedQuizzes[pin] = quiz;
                console.log(`[Quiz] Linked "${quiz.title}" to PIN ${pin}`);
                io.emit('quizLinked', { pin, title: quiz.title, questionCount: quiz.questions.length });
                e2eResponse(res, { success: true, title: quiz.title, questionCount: quiz.questions.length });
            } catch (e) {
                e2eResponse(res, { error: 'Failed to link quiz' }, 500);
            }
        }
        // Two-Factor Auth
        else if (path === '/api/twofactor') {
            const { sequence } = decrypted;
            const seq = sequence || [0, 1, 2, 3];
            let sent = 0;
            for (const bot of bots) {
                if (bot.status === 'joined' || bot.twoFactorPending) {
                    if (answerTwoFactor(bot, seq)) sent++;
                }
            }
            e2eResponse(res, { success: true, sent });
        }
        else {
            e2eResponse(res, { error: 'Unknown E2E path' }, 404);
        }
    } catch (e) {
        e2eResponse(res, { error: 'Internal error' }, 500);
    }
});

io.on('connection', () => { console.log('Dashboard connected'); });

server.listen(PORT, '0.0.0.0', () => {
    console.log(`Kahoot Server listening on port ${PORT}`);
});

// Graceful shutdown
function shutdown() {
    console.log('\n[Shutdown] Cleaning up...');
    stopSpawning = true;
    if (joinBatchTimer) { clearInterval(joinBatchTimer); joinBatchTimer = null; }
    for (const bot of bots) { disconnectBot(bot); }
    bots.length = 0;
    io.close();
    server.close(() => {
        console.log('[Shutdown] Server closed');
        process.exit(0);
    });
    // Force exit after 5 seconds if graceful shutdown fails
    setTimeout(() => process.exit(1), 5000);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
