const assert = require('node:assert/strict');
const http = require('node:http');
const { once } = require('node:events');
const test = require('node:test');
const { WebSocket } = require('ws');
const { attachRealtime } = require('../realtime');

function createSharedRoomTransport() {
  const channels = new Set();

  function notifyPresenceSync() {
    for (const channel of channels) {
      if (channel.subscribed) {
        channel.emit('presence', 'sync', {});
      }
    }
  }

  return {
    createClient() {
      return {
        channel(topic, options) {
          const listeners = [];
          const channel = {
            topic,
            key: options.config.presence?.key || topic,
            subscribed: false,
            presence: null,
            on(type, filter, callback) {
              listeners.push({ type, event: filter.event, callback });
              return this;
            },
            subscribe(callback) {
              channels.add(this);
              this.subscribed = true;
              setImmediate(() => {
                callback('SUBSCRIBED');
                notifyPresenceSync();
              });
              return this;
            },
            presenceState() {
              const state = {};
              for (const peer of channels) {
                if (peer.subscribed && peer.presence) {
                  state[peer.key] = [peer.presence];
                }
              }
              return state;
            },
            async track(presence) {
              this.presence = presence;
              notifyPresenceSync();
              return 'ok';
            },
            async send(message) {
              for (const peer of channels) {
                if (peer === this || !peer.subscribed) continue;
                peer.emit('broadcast', message.event, { payload: message.payload });
              }
              return 'ok';
            },
            emit(type, event, payload) {
              for (const listener of listeners) {
                if (listener.type === type && listener.event === event) {
                  listener.callback(payload);
                }
              }
            },
          };
          return channel;
        },
        async removeChannel(channel) {
          channels.delete(channel);
          channel.subscribed = false;
          notifyPresenceSync();
          return 'ok';
        },
      };
    },
  };
}

function waitForEvent(socket, type, predicate = () => true) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out waiting for WebSocket event "${type}"`));
    }, 3000);
    const onMessage = (data) => {
      const event = JSON.parse(data.toString());
      if (event.type === type && predicate(event)) {
        cleanup();
        resolve(event);
      }
    };
    const onError = (error) => {
      cleanup();
      reject(error);
    };
    function cleanup() {
      clearTimeout(timer);
      socket.removeListener('message', onMessage);
      socket.removeListener('error', onError);
    }
    socket.on('message', onMessage);
    socket.on('error', onError);
  });
}

function assertNoEvent(socket, type, predicate, duration = 100) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.removeListener('message', onMessage);
      resolve();
    }, duration);
    const onMessage = (data) => {
      const event = JSON.parse(data.toString());
      if (event.type === type && predicate(event)) {
        clearTimeout(timer);
        socket.removeListener('message', onMessage);
        reject(new Error(`Unexpected WebSocket event "${type}"`));
      }
    };
    socket.on('message', onMessage);
  });
}

async function connect(port) {
  const socket = new WebSocket(`ws://127.0.0.1:${port}/ws`, {
    headers: { Origin: 'http://localhost:5173' },
  });
  await once(socket, 'open');
  return socket;
}

async function authenticate(socket, token) {
  const authenticated = waitForEvent(socket, 'authenticated');
  const history = waitForEvent(socket, 'chat:history');
  const presence = waitForEvent(socket, 'chat:presence');
  socket.send(JSON.stringify({ type: 'auth', token }));
  const [, , presenceEvent] = await Promise.all([authenticated, history, presence]);
  return presenceEvent;
}

function createTestFleet() {
  return [
    { type: 'seriousGirl', cells: [208, 209] },
    { type: 'vanguard', cells: [15, 16, 17] },
    { type: 'coordinator', cells: [30, 31, 32] },
    { type: 'battleship', cells: [90, 91, 92, 93] },
    { type: 'carrier', cells: [60, 61, 62, 63] },
  ];
}

test('authenticated players and spectators can chat and exchange match actions', async (t) => {
  const server = http.createServer();
  const realtime = attachRealtime(server, {
    allowedOrigins: new Set(['http://localhost:5173']),
    authenticate: async (token) => ({
      id: token,
      user_metadata: { nombre_usuario: token },
    }),
  });
  const sockets = [];
  server.listen(0);
  await once(server, 'listening');
  t.after(async () => {
    await realtime.close();
    if (server.listening) await new Promise((resolve) => server.close(resolve));
    for (const socket of sockets) {
      if (socket.readyState === WebSocket.OPEN) socket.terminate();
    }
  });

  const alice = await connect(server.address().port);
  sockets.push(alice);
  await authenticate(alice, 'alice');
  const generalMessage = waitForEvent(alice, 'chat:message', (event) => event.scope === 'general');
  alice.send(JSON.stringify({ type: 'chat:send', scope: 'general', text: 'Hola a todos' }));
  assert.equal((await generalMessage).message.text, 'Hola a todos');

  const createdEvent = waitForEvent(alice, 'match:created');
  alice.send(JSON.stringify({ type: 'match:create', visibility: 'public' }));
  const matchId = (await createdEvent).match.id;

  const bob = await connect(server.address().port);
  sockets.push(bob);
  const aliceSeesBob = waitForEvent(
    alice,
    'chat:presence',
    (event) => event.users.some((user) => user.id === 'bob'),
  );
  const bobPresence = await authenticate(bob, 'bob');
  assert.deepEqual(
    bobPresence.users.map((user) => user.id).sort(),
    ['alice', 'bob'],
  );
  assert.ok((await aliceSeesBob).users.some((user) => user.username === 'bob'));
  assert.deepEqual(
    realtime.getOnlineUsers().map((user) => user.id).sort(),
    ['alice', 'bob'],
  );
  const aliceHttpMessage = waitForEvent(
    alice,
    'chat:message',
    (event) => event.message.text === 'Enviado por HTTP',
  );
  const bobHttpMessage = waitForEvent(
    bob,
    'chat:message',
    (event) => event.message.text === 'Enviado por HTTP',
  );
  const httpMessage = realtime.publishGeneralMessage(
    { id: 'alice', username: 'alice' },
    'Enviado por HTTP',
  );
  assert.equal(httpMessage.text, 'Enviado por HTTP');
  await Promise.all([aliceHttpMessage, bobHttpMessage]);
  assert.equal(realtime.getGeneralHistory().at(-1).id, httpMessage.id);

  const bobJoined = waitForEvent(bob, 'match:joined');
  const aliceUpdated = waitForEvent(alice, 'match:updated', (event) => event.match.status === 'playing');
  bob.send(JSON.stringify({ type: 'match:join', matchId }));
  await Promise.all([bobJoined, aliceUpdated]);

  const spectator = await connect(server.address().port);
  sockets.push(spectator);
  await authenticate(spectator, 'spectator');
  const spectating = waitForEvent(spectator, 'match:spectating');
  spectator.send(JSON.stringify({ type: 'match:spectate', matchId }));
  await spectating;

  const spectatorError = waitForEvent(
    spectator,
    'error',
    (event) => event.code === 'SPECTATOR_READ_ONLY',
  );
  spectator.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'fire', x: 1, y: 2 },
  }));
  await spectatorError;

  const playerAction = waitForEvent(bob, 'match:action');
  const spectatorAction = waitForEvent(spectator, 'match:action');
  alice.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'fire', x: 1, y: 2 },
  }));
  assert.equal((await playerAction).playerId, 'alice');
  assert.equal((await spectatorAction).action.type, 'fire');

  const matchChat = waitForEvent(spectator, 'chat:message', (event) => event.scope === 'match');
  bob.send(JSON.stringify({
    type: 'chat:send',
    scope: 'match',
    matchId,
    text: 'Tu turno',
  }));
  assert.equal((await matchChat).message.text, 'Tu turno');

  const ended = waitForEvent(alice, 'match:ended');
  bob.send(JSON.stringify({ type: 'match:leave', matchId }));
  assert.equal((await ended).reason, 'player_left');

  const aliceSeesBobLeave = waitForEvent(
    alice,
    'chat:presence',
    (event) => !event.users.some((user) => user.id === 'bob'),
  );
  const bobClosed = once(bob, 'close');
  bob.close();
  await Promise.all([aliceSeesBobLeave, bobClosed]);
});

test('keeps match chats isolated and allows players and spectators to participate', async (t) => {
  const server = http.createServer();
  const realtime = attachRealtime(server, {
    allowedOrigins: new Set(['http://localhost:5173']),
    authenticate: async (token) => ({
      id: token,
      user_metadata: { username: token },
    }),
  });
  const sockets = [];
  server.listen(0);
  await once(server, 'listening');
  t.after(async () => {
    await realtime.close();
    if (server.listening) await new Promise((resolve) => server.close(resolve));
    for (const socket of sockets) {
      if (socket.readyState === WebSocket.OPEN) socket.terminate();
    }
  });

  const connectUser = async (username) => {
    const socket = await connect(server.address().port);
    sockets.push(socket);
    await authenticate(socket, username);
    return socket;
  };

  const alice = await connectUser('alice');
  const matchACreated = waitForEvent(alice, 'match:created');
  alice.send(JSON.stringify({ type: 'match:create', visibility: 'public' }));
  const matchAId = (await matchACreated).match.id;

  const bob = await connectUser('bob');
  const bobJoined = waitForEvent(bob, 'match:joined');
  bob.send(JSON.stringify({ type: 'match:join', matchId: matchAId }));
  await bobJoined;

  const spectator = await connectUser('spectator');
  const spectatorJoined = waitForEvent(spectator, 'match:spectating');
  spectator.send(JSON.stringify({ type: 'match:spectate', matchId: matchAId }));
  await spectatorJoined;

  const charlie = await connectUser('charlie');
  const matchBCreated = waitForEvent(charlie, 'match:created');
  charlie.send(JSON.stringify({ type: 'match:create', visibility: 'public' }));
  const matchBId = (await matchBCreated).match.id;

  const dave = await connectUser('dave');
  const daveJoined = waitForEvent(dave, 'match:joined');
  dave.send(JSON.stringify({ type: 'match:join', matchId: matchBId }));
  await daveJoined;

  const aliceReceives = waitForEvent(
    alice,
    'chat:message',
    (event) => event.scope === 'match' && event.matchId === matchAId,
  );
  const spectatorReceives = waitForEvent(
    spectator,
    'chat:message',
    (event) => event.scope === 'match' && event.matchId === matchAId,
  );
  const noLeakToOtherMatch = Promise.all([
    assertNoEvent(
      charlie,
      'chat:message',
      (event) => event.scope === 'match' && event.matchId === matchAId,
    ),
    assertNoEvent(
      dave,
      'chat:message',
      (event) => event.scope === 'match' && event.matchId === matchAId,
    ),
  ]);
  bob.send(JSON.stringify({
    type: 'chat:send',
    scope: 'match',
    matchId: matchAId,
    text: 'Solo en partida A',
  }));
  assert.equal((await aliceReceives).message.text, 'Solo en partida A');
  assert.equal((await spectatorReceives).message.text, 'Solo en partida A');
  await noLeakToOtherMatch;
  assert.deepEqual(realtime.getGeneralHistory(), []);

  const aliceReceivesSpectator = waitForEvent(
    alice,
    'chat:message',
    (event) => event.scope === 'match'
      && event.matchId === matchAId
      && event.message.text === 'Respuesta del espectador',
  );
  const bobReceivesSpectator = waitForEvent(
    bob,
    'chat:message',
    (event) => event.scope === 'match'
      && event.matchId === matchAId
      && event.message.text === 'Respuesta del espectador',
  );
  spectator.send(JSON.stringify({
    type: 'chat:send',
    scope: 'match',
    matchId: matchAId,
    text: 'Respuesta del espectador',
  }));
  await Promise.all([aliceReceivesSpectator, bobReceivesSpectator]);

  const lateSpectator = await connectUser('late-spectator');
  const lateSpectatorJoined = waitForEvent(lateSpectator, 'match:spectating');
  lateSpectator.send(JSON.stringify({ type: 'match:spectate', matchId: matchAId }));
  const joinedMatch = await lateSpectatorJoined;
  assert.deepEqual(
    joinedMatch.messages.map((message) => message.text),
    ['Solo en partida A', 'Respuesta del espectador'],
  );

  const unauthorizedError = waitForEvent(
    charlie,
    'error',
    (event) => event.code === 'NOT_IN_MATCH',
  );
  charlie.send(JSON.stringify({
    type: 'chat:send',
    scope: 'match',
    matchId: matchAId,
    text: 'No autorizado',
  }));
  await unauthorizedError;
});

test('rejects invalid tokens and disallowed browser origins', async (t) => {
  const server = http.createServer();
  const realtime = attachRealtime(server, {
    allowedOrigins: new Set(['http://localhost:5173']),
    authenticate: async () => null,
  });
  server.listen(0);
  await once(server, 'listening');
  t.after(async () => {
    await realtime.close();
    if (server.listening) await new Promise((resolve) => server.close(resolve));
  });

  const socket = await connect(server.address().port);
  const authError = waitForEvent(
    socket,
    'error',
    (event) => event.code === 'AUTHENTICATION_FAILED',
  );
  const closed = once(socket, 'close');
  socket.send(JSON.stringify({ type: 'auth', token: 'invalid' }));
  await authError;
  await closed;

  const disallowed = new WebSocket(`ws://127.0.0.1:${server.address().port}/ws`, {
    headers: { Origin: 'https://untrusted.example' },
  });
  const rejectedResponse = new Promise((resolve, reject) => {
    disallowed.once('unexpected-response', (_request, response) => {
      response.resume();
      resolve(response.statusCode);
    });
    disallowed.once('error', reject);
  });
  assert.equal(await rejectedResponse, 403);
});

test('allows time for Supabase to validate a token after the auth message arrives', async (t) => {
  const server = http.createServer();
  const realtime = attachRealtime(server, {
    allowedOrigins: new Set(['http://localhost:5173']),
    authMessageTimeoutMs: 20,
    authValidationTimeoutMs: 500,
    authenticate: async (token) => {
      await new Promise((resolve) => setTimeout(resolve, 60));
      return { id: token, user_metadata: { username: token } };
    },
  });
  server.listen(0);
  await once(server, 'listening');
  t.after(async () => {
    await realtime.close();
    if (server.listening) await new Promise((resolve) => server.close(resolve));
  });

  const socket = await connect(server.address().port);
  const authenticated = waitForEvent(socket, 'authenticated');
  socket.send(JSON.stringify({ type: 'auth', token: 'delayed-user' }));
  assert.equal((await authenticated).user.id, 'delayed-user');
  socket.close();
});

test('retries authentication once after a transient Supabase fetch failure', async (t) => {
  const server = http.createServer();
  let attempts = 0;
  const realtime = attachRealtime(server, {
    allowedOrigins: new Set(['http://localhost:5173']),
    authenticate: async (token) => {
      attempts += 1;
      if (attempts === 1) {
        const error = new Error('fetch failed');
        error.name = 'AuthRetryableFetchError';
        error.status = 0;
        throw error;
      }
      return { id: token, user_metadata: { username: token } };
    },
  });
  server.listen(0);
  await once(server, 'listening');
  t.after(async () => {
    await realtime.close();
    if (server.listening) await new Promise((resolve) => server.close(resolve));
  });

  const socket = await connect(server.address().port);
  const authenticated = waitForEvent(socket, 'authenticated');
  const history = waitForEvent(socket, 'chat:history');
  const presence = waitForEvent(socket, 'chat:presence');
  socket.send(JSON.stringify({ type: 'auth', token: 'retry-user' }));

  const [, , presenceEvent] = await Promise.all([authenticated, history, presence]);
  assert.equal(presenceEvent.users[0].id, 'retry-user');
  assert.equal(attempts, 2);
  socket.close();
  await once(socket, 'close');
});

test('routes a private chat addressed by username to the two connected users', async (t) => {
  const server = http.createServer();
  const users = {
    alice: { id: 'alice-id', user_metadata: { nombre_usuario: 'Alice' } },
    bob: { id: 'bob-id', user_metadata: { nombre_usuario: 'Bob' } },
  };
  const realtime = attachRealtime(server, {
    allowedOrigins: new Set(['http://localhost:5173']),
    authenticate: async (token) => users[token] || null,
  });
  const sockets = [];
  server.listen(0);
  await once(server, 'listening');
  t.after(async () => {
    await realtime.close();
    if (server.listening) await new Promise((resolve) => server.close(resolve));
    for (const socket of sockets) {
      if (socket.readyState === WebSocket.OPEN) socket.terminate();
    }
  });

  const alice = await connect(server.address().port);
  const bob = await connect(server.address().port);
  sockets.push(alice, bob);
  await authenticate(alice, 'alice');
  await authenticate(bob, 'bob');

  const aliceMessage = waitForEvent(alice, 'chat:private:message');
  const bobMessage = waitForEvent(bob, 'chat:private:message');
  alice.send(JSON.stringify({
    type: 'chat:private:send',
    recipientId: 'Bob',
    text: 'Hola, Bob',
  }));

  const [aliceEvent, bobEvent] = await Promise.all([aliceMessage, bobMessage]);
  assert.equal(aliceEvent.recipientId, 'bob-id');
  assert.equal(bobEvent.senderId, 'alice-id');
  assert.equal(aliceEvent.message.text, 'Hola, Bob');
});

test('expires general chat history after one hour', async (t) => {
  const server = http.createServer();
  const realtime = attachRealtime(server, {
    allowedOrigins: new Set(['http://localhost:5173']),
    authenticate: async () => null,
  });
  t.after(async () => {
    await realtime.close();
    if (server.listening) await new Promise((resolve) => server.close(resolve));
  });

  realtime.broadcastGeneralMessage({
    id: 'expired',
    text: 'Vencido',
    createdAt: new Date(Date.now() - 60 * 60 * 1000 - 1000).toISOString(),
  });
  realtime.broadcastGeneralMessage({
    id: 'recent',
    text: 'Vigente',
    createdAt: new Date().toISOString(),
  });

  assert.deepEqual(realtime.getGeneralHistory().map((message) => message.id), ['recent']);
});

test('shares the global room presence and chat across backend instances', async (t) => {
  const transport = createSharedRoomTransport();
  const serverA = http.createServer();
  const serverB = http.createServer();
  const serverC = http.createServer();
  const options = {
    allowedOrigins: new Set(['http://localhost:5173']),
    authenticate: async (token) => ({
      id: token,
      user_metadata: { nombre_usuario: token },
    }),
  };
  const realtimeA = attachRealtime(serverA, {
    ...options,
    roomClient: transport.createClient(),
  });
  const realtimeB = attachRealtime(serverB, {
    ...options,
    roomClient: transport.createClient(),
  });
  const realtimeC = attachRealtime(serverC, {
    ...options,
    roomClient: transport.createClient(),
  });
  const sockets = [];
  serverA.listen(0);
  serverB.listen(0);
  serverC.listen(0);
  await Promise.all([
    once(serverA, 'listening'),
    once(serverB, 'listening'),
    once(serverC, 'listening'),
  ]);
  t.after(async () => {
    await Promise.all([realtimeA.close(), realtimeB.close(), realtimeC.close()]);
    await Promise.all([
      new Promise((resolve) => serverA.close(resolve)),
      new Promise((resolve) => serverB.close(resolve)),
      new Promise((resolve) => serverC.close(resolve)),
    ]);
    for (const socket of sockets) {
      if (socket.readyState === WebSocket.OPEN) socket.terminate();
    }
  });

  const alice = await connect(serverA.address().port);
  const bob = await connect(serverB.address().port);
  const charlie = await connect(serverC.address().port);
  sockets.push(alice, bob, charlie);
  const alicePresence = waitForEvent(
    alice,
    'chat:presence',
    (event) => event.users.some((user) => user.id === 'bob'),
  );
  await authenticate(alice, 'alice');
  const bobPresence = waitForEvent(
    bob,
    'chat:presence',
    (event) =>
      event.users.some((user) => user.id === 'alice') &&
      event.users.some((user) => user.id === 'bob'),
  );
  await authenticate(bob, 'bob');
  await Promise.all([alicePresence, bobPresence]);
  const aliceSeesCharlie = waitForEvent(
    alice,
    'chat:presence',
    (event) => event.users.some((user) => user.id === 'charlie'),
  );
  const bobSeesCharlie = waitForEvent(
    bob,
    'chat:presence',
    (event) => event.users.some((user) => user.id === 'charlie'),
  );
  await authenticate(charlie, 'charlie');
  await Promise.all([aliceSeesCharlie, bobSeesCharlie]);
  assert.deepEqual(
    realtimeA.getOnlineUsers().map((user) => user.id).sort(),
    ['alice', 'bob', 'charlie'],
  );
  assert.deepEqual(realtimeA.getPresenceDiagnostics(), {
    room: 'chat:general',
    realtime: 'SUBSCRIBED',
    privateRealtime: 'SUBSCRIBED',
    localUsers: 1,
    remoteUsers: 2,
    onlineUsers: 3,
  });

  const messageOnBob = waitForEvent(
    bob,
    'chat:message',
    (event) => event.message.text === 'Hola desde otra instancia',
  );
  alice.send(JSON.stringify({
    type: 'chat:send',
    scope: 'general',
    text: 'Hola desde otra instancia',
  }));
  assert.equal((await messageOnBob).message.userId, 'alice');

  const privateOnAlice = waitForEvent(
    alice,
    'chat:private:message',
    (event) => event.message.text === 'Solo para Bob',
  );
  const privateOnBob = waitForEvent(
    bob,
    'chat:private:message',
    (event) => event.message.text === 'Solo para Bob',
  );
  let privateLeakedToCharlie = false;
  let publicLeakedToCharlie = false;
  const observeCharlie = (raw) => {
    const event = JSON.parse(raw.toString());
    if (event.type === 'chat:private:message') privateLeakedToCharlie = true;
    if (event.type === 'chat:message' && event.message?.text === 'Solo para Bob') {
      publicLeakedToCharlie = true;
    }
  };
  charlie.on('message', observeCharlie);
  alice.send(JSON.stringify({
    type: 'chat:private:send',
    recipientId: 'bob',
    text: 'Solo para Bob',
  }));
  const [aliceEvent, bobEvent] = await Promise.all([privateOnAlice, privateOnBob]);
  await new Promise((resolve) => setTimeout(resolve, 50));
  charlie.removeListener('message', observeCharlie);
  assert.equal(aliceEvent.recipientId, 'bob');
  assert.equal(bobEvent.senderId, 'alice');
  assert.equal(privateLeakedToCharlie, false);
  assert.equal(publicLeakedToCharlie, false);

  const offlineError = waitForEvent(
    alice,
    'error',
    (event) => event.code === 'RECIPIENT_OFFLINE',
  );
  alice.send(JSON.stringify({
    type: 'chat:private:send',
    recipientId: 'offline-user',
    text: 'No debe entregarse',
  }));
  await offlineError;
});

test('direct challenges join a private match and alternate server-enforced turns', async (t) => {
  const server = http.createServer();
  const realtime = attachRealtime(server, {
    allowedOrigins: new Set(['http://localhost:5173']),
    authenticate: async (token) => ({
      id: `${token}-id`,
      user_metadata: { nombre_usuario: token },
    }),
  });
  const sockets = [];
  server.listen(0);
  await once(server, 'listening');
  t.after(async () => {
    await realtime.close();
    if (server.listening) await new Promise((resolve) => server.close(resolve));
    for (const socket of sockets) {
      if (socket.readyState === WebSocket.OPEN) socket.terminate();
    }
  });

  const alice = await connect(server.address().port);
  const bob = await connect(server.address().port);
  sockets.push(alice, bob);
  await authenticate(alice, 'alice');
  await authenticate(bob, 'bob');

  const sentChallenge = waitForEvent(alice, 'game:challenge:sent');
  const invitation = waitForEvent(bob, 'game:challenge:invite');
  alice.send(JSON.stringify({
    type: 'game:challenge:invite',
    recipientId: 'bob-id',
  }));
  const [sentEvent, invitationEvent] = await Promise.all([sentChallenge, invitation]);
  const matchId = sentEvent.matchId;
  assert.equal(invitationEvent.invitation.matchId, matchId);
  assert.equal(invitationEvent.invitation.from, 'alice');

  const acceptedByAlice = waitForEvent(alice, 'game:challenge:accepted');
  const acceptedByBob = waitForEvent(bob, 'game:challenge:accepted');
  const joinedByBob = waitForEvent(bob, 'match:joined');
  bob.send(JSON.stringify({
    type: 'game:challenge:respond',
    matchId,
    accepted: true,
  }));
  await Promise.all([acceptedByAlice, acceptedByBob, joinedByBob]);

  const fleetA = createTestFleet();
  const fleetB = createTestFleet();
  let readyByAlice = waitForEvent(
    alice,
    'match:action',
    (event) => event.action?.type === 'fleet:ready' && event.playerId === 'alice-id',
  );
  let readyByBob = waitForEvent(
    bob,
    'match:action',
    (event) => event.action?.type === 'fleet:ready' && event.playerId === 'alice-id',
  );
  alice.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'fleet:ready', ships: fleetA },
  }));
  await Promise.all([readyByAlice, readyByBob]);

  const aliceStarted = waitForEvent(
    alice,
    'match:updated',
    (event) => event.match.game?.phase === 'active',
  );
  const bobStarted = waitForEvent(
    bob,
    'match:updated',
    (event) => event.match.game?.phase === 'active',
  );
  readyByAlice = waitForEvent(
    alice,
    'match:action',
    (event) => event.action?.type === 'fleet:ready' && event.playerId === 'bob-id',
  );
  readyByBob = waitForEvent(
    bob,
    'match:action',
    (event) => event.action?.type === 'fleet:ready' && event.playerId === 'bob-id',
  );
  bob.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'fleet:ready', ships: fleetB },
  }));
  const [aliceMatch, bobMatch] = await Promise.all([aliceStarted, bobStarted]);
  await Promise.all([readyByAlice, readyByBob]);
  const firstPlayerId = aliceMatch.match.game.currentTurnId;
  assert.ok(['alice-id', 'bob-id'].includes(firstPlayerId));
  assert.equal(bobMatch.match.game.currentTurnId, firstPlayerId);
  assert.equal(aliceMatch.match.game.turnNumber, 1);
  assert.deepEqual(aliceMatch.match.game.ammo, {
    cross: 6,
    bombardment: 3,
    radar: 2,
    normal: 'infinite',
  });
  assert.deepEqual(bobMatch.match.game.ammo, aliceMatch.match.game.ammo);

  const first = firstPlayerId === 'alice-id' ? alice : bob;
  const second = firstPlayerId === 'alice-id' ? bob : alice;
  const secondPlayerId = firstPlayerId === 'alice-id' ? 'bob-id' : 'alice-id';
  const rejectedAttack = waitForEvent(first === alice ? bob : alice, 'error',
    (event) => event.code === 'NOT_YOUR_TURN');
  (first === alice ? bob : alice).send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'attack', cell: 200, attackType: 'normal' },
  }));
  await rejectedAttack;

  const firstTurnEnds = waitForEvent(
    second,
    'match:action',
    (event) => event.action?.type === 'attack',
  );
  const firstTurnEcho = waitForEvent(first, 'match:action',
    (event) => event.action?.type === 'attack');
  first.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'attack', cell: 15, attackType: 'normal' },
  }));
  const interceptedAttack = await firstTurnEnds;
  await firstTurnEcho;
  assert.ok(interceptedAttack.action.results.some((result) =>
    result.cell === 15 && result.result === 'intercepted'));
  assert.equal(interceptedAttack.game.currentTurnId, firstPlayerId);
  assert.equal(interceptedAttack.game.extraNormalAvailable, true);

  const vanguardHitBySecondShot = waitForEvent(first, 'match:action',
    (event) => event.action?.type === 'attack');
  const vanguardHitBySecondPlayer = waitForEvent(second, 'match:action',
    (event) => event.action?.type === 'attack');
  first.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'attack', cell: 16, attackType: 'normal' },
  }));
  const secondVanguardAttack = await vanguardHitBySecondShot;
  await vanguardHitBySecondPlayer;
  assert.ok(secondVanguardAttack.action.results.some((result) =>
    result.cell === 16 && result.result === 'hit'));
  assert.equal(secondVanguardAttack.game.currentTurnId, secondPlayerId);
  assert.equal(secondVanguardAttack.game.turnNumber, 2);

  const radarBySecond = waitForEvent(second, 'match:radar:result');
  const radarActionBySecond = waitForEvent(second, 'match:action',
    (event) => event.action?.type === 'radar');
  second.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'radar', cell: 208 },
  }));
  const [radarResult, radarAction] = await Promise.all([radarBySecond, radarActionBySecond]);
  assert.ok(radarResult.results.some((result) => result.cell === 208 && !result.detected));
  assert.ok(radarResult.results.some((result) => result.cell === 209 && !result.detected));
  assert.equal(radarAction.game.currentTurnId, firstPlayerId);
  assert.equal(radarAction.game.turnNumber, 3);

  const bombardmentBySecond = waitForEvent(second, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.attackType === 'bombardment');
  const bombardmentByFirst = waitForEvent(first, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.attackType === 'bombardment');
  first.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'attack', cell: 208, attackType: 'bombardment' },
  }));
  const bombardmentResult = await bombardmentBySecond;
  await bombardmentByFirst;
  assert.ok(bombardmentResult.action.results.some((result) =>
    result.cell === 208 && result.result === 'evaded'));
  assert.equal(bombardmentResult.game.currentTurnId, firstPlayerId);
  assert.equal(bombardmentResult.game.extraNormalAvailable, true);

  const carrierHitBySecond = waitForEvent(second, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.attackType === 'normal');
  const carrierHitByFirst = waitForEvent(first, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.attackType === 'normal');
  const ammoAfterCarrierHit = waitForEvent(second, 'match:updated',
    (event) => event.match.game.ammo?.radar === 1);
  first.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'attack', cell: 63, attackType: 'normal' },
  }));
  const carrierHit = await carrierHitBySecond;
  await carrierHitByFirst;
  assert.ok(carrierHit.action.results.some((result) => result.cell === 63 && result.result === 'hit'));
  assert.deepEqual((await ammoAfterCarrierHit).match.game.ammo, {
    cross: 5,
    bombardment: 2,
    radar: 1,
    normal: 'infinite',
  });
  assert.equal(carrierHit.game.currentTurnId, secondPlayerId);
  assert.equal(carrierHit.game.turnNumber, 4);

  const armoredHitsByFirst = waitForEvent(first, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.attackType === 'cross');
  const armoredHitsBySecond = waitForEvent(second, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.attackType === 'cross');
  second.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'attack', cell: 91, attackType: 'cross' },
  }));
  const armorFirstHit = await armoredHitsByFirst;
  await armoredHitsBySecond;
  assert.ok(armorFirstHit.action.results.some((result) =>
    result.cell === 91 && result.result === 'hit' && result.damage === 1 && !result.cellSunk));
  assert.equal(armorFirstHit.game.currentTurnId, secondPlayerId);
  assert.equal(armorFirstHit.game.extraNormalAvailable, true);

  const armoredSecondHitByFirst = waitForEvent(first, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.attackType === 'normal');
  const armoredSecondHitBySecond = waitForEvent(second, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.attackType === 'normal');
  second.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'attack', cell: 91, attackType: 'normal' },
  }));
  const armorSecondHit = await armoredSecondHitByFirst;
  await armoredSecondHitBySecond;
  assert.ok(armorSecondHit.action.results.some((result) =>
    result.cell === 91 && result.damage === 2 && result.cellSunk));
  assert.equal(armorSecondHit.game.currentTurnId, firstPlayerId);
  assert.equal(armorSecondHit.game.turnNumber, 5);

  let firstExtra = waitForEvent(first, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.cell === 30);
  let secondExtra = waitForEvent(second, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.cell === 30);
  first.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'attack', cell: 30, attackType: 'normal' },
  }));
  await Promise.all([firstExtra, secondExtra]);
  firstExtra = waitForEvent(first, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.cell === 31);
  secondExtra = waitForEvent(second, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.cell === 31);
  first.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'attack', cell: 31, attackType: 'normal' },
  }));
  await Promise.all([firstExtra, secondExtra]);

  const firstCoordinatorHit = waitForEvent(first, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.cell === 30);
  const secondCoordinatorHit = waitForEvent(second, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.cell === 30);
  second.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'attack', cell: 30, attackType: 'normal' },
  }));
  await Promise.all([firstCoordinatorHit, secondCoordinatorHit]);
  const firstCoordinatorHit2 = waitForEvent(first, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.cell === 31);
  const secondCoordinatorHit2 = waitForEvent(second, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.cell === 31);
  second.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'attack', cell: 31, attackType: 'normal' },
  }));
  await Promise.all([firstCoordinatorHit2, secondCoordinatorHit2]);

  const finalCoordinatorHit = waitForEvent(first, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.cell === 32);
  const finalCoordinatorHitBySecond = waitForEvent(second, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.cell === 32);
  first.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'attack', cell: 32, attackType: 'normal' },
  }));
  await Promise.all([finalCoordinatorHit, finalCoordinatorHitBySecond]);
  const finishExtraByFirst = waitForEvent(first, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.cell === 33);
  const finishExtraBySecond = waitForEvent(second, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.cell === 33);
  first.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'attack', cell: 33, attackType: 'normal' },
  }));
  const [lastExtraAttack] = await Promise.all([finishExtraByFirst, finishExtraBySecond]);
  assert.equal(lastExtraAttack.game.currentTurnId, secondPlayerId);
  assert.equal(lastExtraAttack.game.extraNormalAvailable, false);

  const chicaHitByFirst = waitForEvent(first, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.cell === 208);
  const chicaHitBySecond = waitForEvent(second, 'match:action',
    (event) => event.action?.type === 'attack' && event.action.cell === 208);
  second.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'attack', cell: 208, attackType: 'normal' },
  }));
  const chicaAttack = await chicaHitByFirst;
  await chicaHitBySecond;
  assert.ok(chicaAttack.action.results.some((result) =>
    result.cell === 208 && result.result === 'hit'));

  const repeatedCellError = waitForEvent(first, 'error',
    (event) => event.code === 'CELL_ALREADY_ATTACKED');
  first.send(JSON.stringify({
    type: 'match:action',
    matchId,
    action: { type: 'attack', cell: 63, attackType: 'normal' },
  }));
  await repeatedCellError;
});
