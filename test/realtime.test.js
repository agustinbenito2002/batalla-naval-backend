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
            key: options.config.presence.key,
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
  await assert.rejects(once(disallowed, 'open'));
});

test('shares the global room presence and chat across backend instances', async (t) => {
  const transport = createSharedRoomTransport();
  const serverA = http.createServer();
  const serverB = http.createServer();
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
  const sockets = [];
  serverA.listen(0);
  serverB.listen(0);
  await Promise.all([once(serverA, 'listening'), once(serverB, 'listening')]);
  t.after(async () => {
    await Promise.all([realtimeA.close(), realtimeB.close()]);
    await Promise.all([
      new Promise((resolve) => serverA.close(resolve)),
      new Promise((resolve) => serverB.close(resolve)),
    ]);
    for (const socket of sockets) {
      if (socket.readyState === WebSocket.OPEN) socket.terminate();
    }
  });

  const alice = await connect(serverA.address().port);
  const bob = await connect(serverB.address().port);
  sockets.push(alice, bob);
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
  assert.deepEqual(
    realtimeA.getOnlineUsers().map((user) => user.id).sort(),
    ['alice', 'bob'],
  );

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
});
