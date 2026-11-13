const { randomInt, randomUUID } = require('node:crypto');
const { WebSocket, WebSocketServer } = require('ws');
const { formatChatMessage } = require('./chat');

const MAX_MESSAGE_BYTES = 16 * 1024;
const MAX_CHAT_LENGTH = 500;
const MAX_CHAT_HISTORY = 50;
const GENERAL_MESSAGE_TTL_MS = 60 * 60 * 1000;
const MAX_MATCHES = 500;
const MATCH_TTL_MS = 2 * 60 * 60 * 1000;
const BOARD_SIZE = 15;
const BOARD_CELLS = BOARD_SIZE * BOARD_SIZE;
const AUTH_MESSAGE_TIMEOUT_MS = 5000;
const AUTH_VALIDATION_TIMEOUT_MS = 20000;
const AUTH_RETRY_DELAY_MS = 500;
const SHIP_LENGTHS = Object.freeze({
  seriousGirl: 2,
  vanguard: 3,
  coordinator: 3,
  battleship: 4,
  carrier: 4,
});
const INITIAL_AMMO = Object.freeze({ cross: 5, bombardment: 2, radar: 1 });
const CARRIER_BONUS_AMMO = Object.freeze({ cross: 1, bombardment: 1, radar: 1 });
const SHIP_HIT_POINTS = Object.freeze({
  seriousGirl: 1,
  vanguard: 1,
  coordinator: 1,
  battleship: 2,
  carrier: 1,
});

function attachRealtime(server, {
  authenticate,
  allowedOrigins,
  roomClient,
  roomName = 'chat:general',
  authMessageTimeoutMs = AUTH_MESSAGE_TIMEOUT_MS,
  authValidationTimeoutMs = AUTH_VALIDATION_TIMEOUT_MS,
}) {
  const wss = new WebSocketServer({
    noServer: true,
    maxPayload: MAX_MESSAGE_BYTES,
    perMessageDeflate: false,
  });
  const connections = new Map();
  const matches = new Map();
  const generalHistory = [];
  const instanceId = randomUUID();
  const remoteUsers = new Map();
  let roomChannel;
  let roomReady = false;
  let roomStatus = roomClient ? 'CONNECTING' : 'DISABLED';
  let privateChannel;
  let privateRoomReady = false;
  let privateRoomStatus = roomClient ? 'CONNECTING' : 'DISABLED';
  let presenceUpdates = Promise.resolve();

  function send(socket, message) {
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify(message));
    }
  }

  function sendError(socket, message, code = 'INVALID_MESSAGE') {
    send(socket, { type: 'error', code, message });
  }

  function getLocalOnlineUsers() {
    return [...connections.values()]
      .filter((socket) => socket.readyState === WebSocket.OPEN)
      .map((socket) => ({
        id: socket.user.id,
        username: socket.user.username,
      }))
      .sort((left, right) => left.username.localeCompare(right.username));
  }

  function getOnlineUsers() {
    const users = new Map();
    for (const user of remoteUsers.values()) users.set(user.id, user);
    for (const user of getLocalOnlineUsers()) users.set(user.id, user);
    return [...users.values()]
      .sort((left, right) => left.username.localeCompare(right.username));
  }

  function broadcastPresence() {
    const event = { type: 'chat:presence', users: getOnlineUsers() };
    for (const connection of connections.values()) send(connection, event);
  }

  function pruneGeneralHistory() {
    const now = Date.now();
    for (let index = generalHistory.length - 1; index >= 0; index -= 1) {
      const timestamp = Date.parse(
        generalHistory[index].fecha_envio ?? generalHistory[index].createdAt ?? '',
      );
      if (!Number.isFinite(timestamp) || now - timestamp >= GENERAL_MESSAGE_TTL_MS) {
        generalHistory.splice(index, 1);
      }
    }
  }

  function refreshRoomPresence() {
    if (!roomChannel) return;

    const users = new Map();
    const presenceState = roomChannel.presenceState();
    for (const presences of Object.values(presenceState)) {
      for (const presence of presences) {
        if (presence.instanceId === instanceId || !Array.isArray(presence.users)) continue;
        for (const user of presence.users) {
          if (typeof user?.id === 'string' && typeof user.username === 'string') {
            users.set(user.id, user);
          }
        }
      }
    }

    remoteUsers.clear();
    for (const [userId, user] of users) remoteUsers.set(userId, user);
    broadcastPresence();
  }

  function syncRoomPresence() {
    if (!roomChannel || !roomReady) return;

    const users = getLocalOnlineUsers();
    presenceUpdates = presenceUpdates
      .then(async () => {
        const status = await roomChannel.track({ instanceId, users });
        if (status !== 'ok') {
          throw new Error(`Supabase Realtime presence update returned "${status}".`);
        }
      })
      .catch((error) => {
        console.error('No se pudo sincronizar la presencia del chat:', error);
      });
  }

  function deliverGeneralMessage(message) {
    pruneGeneralHistory();
    generalHistory.push(message);
    if (generalHistory.length > MAX_CHAT_HISTORY) generalHistory.shift();
    const event = { type: 'chat:message', scope: 'general', message };
    for (const connection of connections.values()) send(connection, event);
  }

  function relayGeneralMessage(message) {
    if (!roomChannel || !roomReady) return;

    roomChannel.send({
      type: 'broadcast',
      event: 'general-chat-message',
      payload: { message },
    }).then((status) => {
      if (status !== 'ok') {
        console.error(`Supabase Realtime no pudo retransmitir el mensaje: ${status}.`);
      }
    }).catch((error) => {
      console.error('Error retransmitiendo mensaje entre instancias:', error);
    });
  }

  function relayPrivateMessage(message) {
    if (!privateChannel || !privateRoomReady) {
      return false;
    }

    privateChannel.send({
      type: 'broadcast',
      event: 'private-chat-message',
      payload: { message },
    }).then((status) => {
      if (status !== 'ok') {
        console.error(`Supabase Realtime no pudo retransmitir el mensaje privado: ${status}.`);
      }
    }).catch((error) => {
      console.error('Error retransmitiendo mensaje privado entre instancias:', error);
    });
    return true;
  }

  function deliverPrivateMessage(message) {
    const recipients = new Set([message.senderId, message.recipientId]);
    for (const userId of recipients) {
      const connection = connections.get(userId);
      if (connection) {
        send(connection, {
          type: 'chat:private:message',
          scope: 'private',
          conversationId: message.conversationId,
          senderId: message.senderId,
          recipientId: message.recipientId,
          message,
        });
      }
    }
  }

  function sendPrivateMessage(socket, message) {
    const recipientValue = message.recipientId ??
      message.recipient_id ??
      message.toUserId ??
      message.to_user_id ??
      message.to ??
      message.recipient?.id ??
      message.recipient;
    const text = message.text ?? message.contenido ?? message.content ?? message.message;
    if (typeof recipientValue !== 'string' ||
        !recipientValue.trim() ||
        recipientValue.length > 128) {
      sendError(socket, 'recipientId debe identificar al destinatario.', 'INVALID_RECIPIENT');
      return;
    }
    const recipientKey = recipientValue.trim();
    const recipient = getOnlineUsers().find((user) =>
      user.id === recipientKey ||
      user.username.toLocaleLowerCase() === recipientKey.toLocaleLowerCase(),
    );
    const recipientId = recipient?.id ?? recipientKey;
    if (recipientId === socket.user.id) {
      sendError(socket, 'No puedes enviarte un mensaje privado.', 'INVALID_RECIPIENT');
      return;
    }
    if (typeof text !== 'string' || !text.trim() || text.trim().length > MAX_CHAT_LENGTH) {
      sendError(socket, `text debe tener entre 1 y ${MAX_CHAT_LENGTH} caracteres.`, 'INVALID_MESSAGE');
      return;
    }

    const localRecipient = connections.get(recipientId);
    if (!recipient) {
      sendError(socket, 'El destinatario no está conectado al chat.', 'RECIPIENT_OFFLINE');
      return;
    }

    if (localRecipient?.readyState !== WebSocket.OPEN && !privateRoomReady) {
      sendError(socket, 'La sala privada entre servidores no está disponible.', 'PRIVATE_CHAT_UNAVAILABLE');
      return;
    }

    const privateMessage = {
      id: randomUUID(),
      senderId: socket.user.id,
      sender_id: socket.user.id,
      sender: socket.user.username,
      senderUsername: socket.user.username,
      recipientId,
      recipient_id: recipientId,
      recipientUsername: recipient?.username,
      text: text.trim(),
      contenido: text.trim(),
      createdAt: new Date().toISOString(),
    };

    deliverPrivateMessage(privateMessage);
    if (localRecipient?.readyState !== WebSocket.OPEN) {
      relayPrivateMessage(privateMessage);
    }
  }

  if (roomClient) {
    roomChannel = roomClient.channel(roomName, {
      config: {
        presence: { key: instanceId },
        broadcast: { self: false },
      },
    });
    roomChannel
      .on('presence', { event: 'sync' }, refreshRoomPresence)
      .on('broadcast', { event: 'general-chat-message' }, ({ payload }) => {
        if (payload?.message && typeof payload.message === 'object') {
          deliverGeneralMessage(payload.message);
        }
      })
      .subscribe((status, error) => {
        roomStatus = status;
        if (status === 'SUBSCRIBED') {
          roomReady = true;
          syncRoomPresence();
          refreshRoomPresence();
          return;
        }
        if (status === 'CHANNEL_ERROR' || status === 'TIMED_OUT') {
          roomReady = false;
          console.error(`No se pudo conectar a la sala global de Supabase Realtime: ${status}.`, error);
        }
        if (status === 'CLOSED') roomReady = false;
      });

    privateChannel = roomClient.channel('chat:private', {
      config: {
        private: true,
        broadcast: { self: false },
      },
    });
    privateChannel
      .on('broadcast', { event: 'private-chat-message' }, ({ payload }) => {
        const message = payload?.message;
        if (message &&
            typeof message.senderId === 'string' &&
            typeof message.recipientId === 'string') {
          deliverPrivateMessage(message);
        }
      })
      .subscribe((status, error) => {
        privateRoomStatus = status;
        privateRoomReady = status === 'SUBSCRIBED';
        if (status === 'CHANNEL_ERROR' || status === 'TIMED_OUT') {
          console.error(`No se pudo conectar a la sala privada de Supabase Realtime: ${status}.`, error);
        }
      });
  }

  function liveMembers(match) {
    return [...match.members.values()]
      .map((member) => member.socket)
      .filter((socket) => socket && socket.readyState === WebSocket.OPEN);
  }

  function matchSnapshot(match, userId) {
    return {
      id: match.id,
      visibility: match.visibility,
      status: match.status,
      players: match.players.map((player) => ({
        id: player.id,
        username: player.username,
        online: match.members.get(player.id)?.socket?.readyState === WebSocket.OPEN,
      })),
      spectators: [...match.members.values()]
        .filter((member) => member.role === 'spectator')
        .length,
      yourRole: match.members.get(userId)?.role,
      createdAt: match.createdAt,
      game: {
        phase: match.game.phase,
        currentTurnId: match.game.currentTurnId,
        turnNumber: match.game.turnNumber,
        winnerId: match.game.winnerId,
        readyPlayerIds: [...match.game.fleets.keys()],
        primaryActionAvailable: match.game.primaryActionAvailable,
        extraNormalAvailable: match.game.extraNormalAvailable,
        ...(userId && match.game.ammo.has(userId)
          ? { ammo: getAmmoSnapshot(match.game, userId) }
          : {}),
      },
    };
  }

  function createMatch(socket, visibility, invitedUserId = null) {
    const now = Date.now();
    const match = {
      id: randomUUID(),
      visibility,
      status: 'waiting',
      players: [{ id: socket.user.id, username: socket.user.username }],
      members: new Map([[socket.user.id, {
        role: 'player',
        socket,
        username: socket.user.username,
      }]]),
      chat: [],
      actions: [],
      invitedUserId,
      game: {
        phase: 'placing',
        fleets: new Map(),
        attackedCells: new Map(),
        ammo: new Map(),
        currentTurnId: null,
        turnNumber: 0,
        winnerId: null,
        primaryActionAvailable: false,
        extraNormalAvailable: false,
      },
      createdAt: new Date(now).toISOString(),
      lastActivity: now,
    };
    socket.matchId = match.id;
    matches.set(match.id, match);
    return match;
  }

  function validateFleet(ships) {
    if (!Array.isArray(ships) || ships.length !== Object.keys(SHIP_LENGTHS).length) return null;
    const occupied = new Set();
    const fleet = [];
    const seenTypes = new Set();

    for (const ship of ships) {
      if (!ship || typeof ship !== 'object' || Array.isArray(ship) ||
          !Object.prototype.hasOwnProperty.call(SHIP_LENGTHS, ship.type) ||
          seenTypes.has(ship.type) ||
          !Array.isArray(ship.cells) || ship.cells.length !== SHIP_LENGTHS[ship.type]) return null;
      const cells = new Set(ship.cells);
      if (cells.size !== ship.cells.length ||
          [...cells].some((cell) => !Number.isInteger(cell) || cell < 0 || cell >= BOARD_CELLS)) {
        return null;
      }
      if ([...cells].some((cell) => occupied.has(cell))) return null;

      const rows = [...cells].map((cell) => Math.floor(cell / BOARD_SIZE));
      const columns = [...cells].map((cell) => cell % BOARD_SIZE);
      const sameRow = rows.every((row) => row === rows[0]);
      const sameColumn = columns.every((column) => column === columns[0]);
      const sorted = [...cells].sort((left, right) => left - right);
      const contiguous = sameRow
        ? sorted.every((cell, index) => index === 0 || cell === sorted[index - 1] + 1)
        : sameColumn && sorted.every((cell, index) =>
          index === 0 || cell === sorted[index - 1] + BOARD_SIZE);
      if (!contiguous) return null;

      cells.forEach((cell) => occupied.add(cell));
      seenTypes.add(ship.type);
      fleet.push({
        type: ship.type,
        cells: [...cells],
        damage: new Map([...cells].map((cell) => [cell, 0])),
        interceptAvailable: ship.type === 'vanguard',
      });
    }

    if (seenTypes.size !== Object.keys(SHIP_LENGTHS).length) return null;
    return fleet;
  }

  function getAttackTargets(center, attackType) {
    if (!Number.isInteger(center) || center < 0 || center >= BOARD_CELLS) return null;
    if (!['normal', 'cross', 'bombardment', 'radar'].includes(attackType)) return null;

    const centerRow = Math.floor(center / BOARD_SIZE);
    const centerColumn = center % BOARD_SIZE;
    const targets = [];
    for (let rowOffset = -2; rowOffset <= 2; rowOffset += 1) {
      for (let columnOffset = -2; columnOffset <= 2; columnOffset += 1) {
        const isTarget = attackType === 'normal'
          ? rowOffset === 0 && columnOffset === 0
          : attackType === 'cross'
            ? (rowOffset === 0 && columnOffset === 0)
              || (rowOffset === 0 && Math.abs(columnOffset) === 1)
              || (columnOffset === 0 && Math.abs(rowOffset) === 1)
            : attackType === 'radar'
              ? (rowOffset === 0 && Math.abs(columnOffset) <= 1)
                || (columnOffset === 0 && Math.abs(rowOffset) <= 2)
              : (rowOffset + columnOffset) % 2 === 0
        if (!isTarget) continue;

        const row = centerRow + rowOffset;
        const column = centerColumn + columnOffset;
        if (row >= 0 && row < BOARD_SIZE && column >= 0 && column < BOARD_SIZE) {
          targets.push(row * BOARD_SIZE + column)
        }
      }
    }
    return targets;
  }

  function getAmmoSnapshot(game, userId) {
    const ammo = game.ammo.get(userId);
    if (!ammo) return null;
    return {
      cross: ammo.cross.base + ammo.cross.carrierBonus,
      bombardment: ammo.bombardment.base + ammo.bombardment.carrierBonus,
      radar: ammo.radar.base + ammo.radar.carrierBonus,
      normal: 'infinite',
    };
  }

  function createPlayerAmmo() {
    return Object.fromEntries(Object.keys(INITIAL_AMMO).map((type) => [type, {
      base: INITIAL_AMMO[type],
      carrierBonus: CARRIER_BONUS_AMMO[type],
    }]));
  }

  function consumeAmmo(game, userId, type) {
    const ammo = game.ammo.get(userId)?.[type];
    if (!ammo || ammo.base + ammo.carrierBonus <= 0) return false;
    if (ammo.carrierBonus > 0) ammo.carrierBonus -= 1;
    else ammo.base -= 1;
    return true;
  }

  function removeCarrierBonus(game, userId) {
    const ammo = game.ammo.get(userId);
    if (!ammo) return;
    for (const type of Object.keys(CARRIER_BONUS_AMMO)) {
      ammo[type].carrierBonus = 0;
    }
  }

  function coordinatorCanGrantExtra(fleet) {
    const coordinator = fleet.find((ship) => ship.type === 'coordinator');
    return Boolean(coordinator && coordinator.cells.some((cell) =>
      coordinator.damage.get(cell) < SHIP_HIT_POINTS.coordinator));
  }

  function beginTurn(match, userId, incrementTurn = true) {
    const game = match.game;
    game.currentTurnId = userId;
    if (incrementTurn) game.turnNumber += 1;
    game.primaryActionAvailable = true;
    game.extraNormalAvailable = coordinatorCanGrantExtra(game.fleets.get(userId));
  }

  function advanceTurn(match) {
    const nextPlayer = match.players.find((player) => player.id !== match.game.currentTurnId);
    beginTurn(match, nextPlayer.id);
  }

  function shipIsSunk(ship) {
    return ship.cells.every((cell) =>
      ship.damage.get(cell) >= SHIP_HIT_POINTS[ship.type]);
  }

  function publishMatchAction(match, socket, action) {
    const event = {
      type: 'match:action',
      actionId: randomUUID(),
      matchId: match.id,
      playerId: socket.user.id,
      username: socket.user.username,
      action,
      game: matchSnapshot(match).game,
      sentAt: new Date().toISOString(),
    };
    match.actions.push(event);
    match.lastActivity = Date.now();
    for (const [userId, member] of match.members) {
      if (!member.socket || member.socket.readyState !== WebSocket.OPEN) continue;
      send(member.socket, {
        ...event,
        game: matchSnapshot(match, userId).game,
      });
    }
    updateMatch(match);
  }

  function sendChallenge(socket, message) {
    if (!requireNoMatch(socket)) return;
    const recipientValue = message.recipientId ?? message.recipient_id
      ?? message.toUserId ?? message.to_user_id ?? message.to ?? message.recipient;
    if (typeof recipientValue !== 'string' || !recipientValue.trim()) {
      sendError(socket, 'Indica el ID o nombre del usuario que recibirá el desafío.', 'INVALID_RECIPIENT');
      return;
    }
    const recipient = getOnlineUsers().find((user) =>
      user.id === recipientValue.trim()
      || user.username.toLocaleLowerCase() === recipientValue.trim().toLocaleLowerCase(),
    );
    if (!recipient) {
      sendError(socket, 'El usuario desafiado no está conectado.', 'RECIPIENT_OFFLINE');
      return;
    }
    if (recipient.id === socket.user.id) {
      sendError(socket, 'No puedes desafiarte a ti mismo.', 'INVALID_RECIPIENT');
      return;
    }
    const recipientSocket = connections.get(recipient.id);
    if (!recipientSocket || recipientSocket.readyState !== WebSocket.OPEN) {
      sendError(socket, 'El usuario está conectado a otra instancia y no puede recibir partidas en memoria.', 'MATCH_UNAVAILABLE');
      return;
    }
    if (recipientSocket.matchId) {
      sendError(socket, 'El usuario ya está en una partida o tiene un desafío pendiente.', 'RECIPIENT_IN_MATCH');
      return;
    }
    if (recipientSocket.pendingMatchId) {
      sendError(socket, 'El usuario ya está en una partida o tiene un desafío pendiente.', 'RECIPIENT_IN_MATCH');
      return;
    }
    if (matches.size >= MAX_MATCHES) pruneMatches();
    if (matches.size >= MAX_MATCHES) {
      sendError(socket, 'No se pueden crear más partidas por el momento.', 'SERVER_BUSY');
      return;
    }

    const match = createMatch(socket, 'private', recipient.id);
    recipientSocket.pendingMatchId = match.id;
    send(socket, {
      type: 'game:challenge:sent',
      matchId: match.id,
      recipientId: recipient.id,
      recipientUsername: recipient.username,
    });
    send(recipientSocket, {
      type: 'game:challenge:invite',
      invitation: {
        id: match.id,
        matchId: match.id,
        fromId: socket.user.id,
        from: socket.user.username,
        username: socket.user.username,
        toId: recipient.id,
      },
    });
  }

  function respondToChallenge(socket, message) {
    const matchId = message.matchId ?? message.invitationId;
    const match = matches.get(matchId);
    if (!match || match.visibility !== 'private' || match.invitedUserId !== socket.user.id) {
      sendError(socket, 'La invitación ya no está disponible.', 'INVITATION_NOT_FOUND');
      return;
    }
    if (message.accepted !== true && message.accepted !== false) {
      sendError(socket, 'accepted debe ser true o false.', 'INVALID_RESPONSE');
      return;
    }

    const sender = match.players[0];
    if (!message.accepted) {
      socket.pendingMatchId = null;
      removeMatch(match, {
        type: 'game:challenge:declined',
        matchId: match.id,
        fromId: socket.user.id,
        from: socket.user.username,
      });
      return;
    }
    if (socket.matchId) {
      requireNoMatch(socket);
      return;
    }

    socket.pendingMatchId = null;
    match.players.push({ id: socket.user.id, username: socket.user.username });
    match.members.set(socket.user.id, {
      role: 'player',
      socket,
      username: socket.user.username,
    });
    match.status = 'playing';
    match.invitedUserId = null;
    socket.matchId = match.id;
    match.lastActivity = Date.now();
    const acceptedEvent = {
      type: 'game:challenge:accepted',
      gameId: match.id,
      matchId: match.id,
      fromId: sender.id,
      from: sender.username,
      toId: socket.user.id,
      to: socket.user.username,
      players: match.players,
    };
    broadcastMatch(match, acceptedEvent);
    send(socket, {
      type: 'match:joined',
      match: matchSnapshot(match, socket.user.id),
      messages: match.chat,
      actions: match.actions,
    });
    updateMatch(match);
  }

  function broadcastMatch(match, message) {
    for (const socket of liveMembers(match)) {
      send(socket, message);
    }
  }

  function updateMatch(match) {
    match.lastActivity = Date.now();
    for (const [userId, member] of match.members) {
      if (!member.socket || member.socket.readyState !== WebSocket.OPEN) continue;
      send(member.socket, {
        type: 'match:updated',
        match: matchSnapshot(match, userId),
      });
    }
  }

  function removeMatch(match, message) {
    broadcastMatch(match, message);
    for (const member of match.members.values()) {
      if (member.socket) member.socket.matchId = null;
    }
    const invitedSocket = match.invitedUserId
      ? connections.get(match.invitedUserId)
      : null;
    if (invitedSocket?.pendingMatchId === match.id) {
      invitedSocket.pendingMatchId = null;
      if (message.type !== 'game:challenge:declined') {
        send(invitedSocket, {
          type: 'game:challenge:declined',
          matchId: match.id,
          reason: 'cancelled',
        });
      }
    }
    matches.delete(match.id);
  }

  function leaveMatch(socket, { endMatch = false } = {}) {
    if (!socket.matchId) return;
    const match = matches.get(socket.matchId);
    socket.matchId = null;
    if (!match) return;

    const member = match.members.get(socket.user.id);
    if (!member || member.socket !== socket) return;

    if (endMatch && member.role === 'player') {
      removeMatch(match, {
        type: 'match:ended',
        matchId: match.id,
        reason: 'player_left',
        playerId: socket.user.id,
      });
      return;
    }

    if (member.role === 'spectator') {
      match.members.delete(socket.user.id);
    } else {
      member.socket = null;
    }
    updateMatch(match);
  }

  function cleanupSocket(socket) {
    clearTimeout(socket.authTimer);
    if (socket.user && connections.get(socket.user.id) === socket) {
      connections.delete(socket.user.id);
      syncRoomPresence();
      broadcastPresence();
    }
    if (socket.pendingMatchId) {
      const pendingMatch = matches.get(socket.pendingMatchId);
      if (pendingMatch) {
        removeMatch(pendingMatch, {
          type: 'game:challenge:declined',
          matchId: pendingMatch.id,
          reason: 'recipient_offline',
        });
      }
      socket.pendingMatchId = null;
    }
    leaveMatch(socket);
  }

  function pruneMatches() {
    const now = Date.now();
    for (const match of matches.values()) {
      if (match.invitedUserId && now - Date.parse(match.createdAt) > MATCH_TTL_MS) {
        removeMatch(match, {
          type: 'game:challenge:declined',
          matchId: match.id,
          reason: 'expired',
        });
        continue;
      }
      if (now - match.lastActivity > MATCH_TTL_MS && liveMembers(match).length === 0) {
        matches.delete(match.id);
      }
    }
  }

  function requireNoMatch(socket) {
    if (!socket.matchId && !socket.pendingMatchId) return true;
    sendError(
      socket,
      socket.pendingMatchId
        ? 'Responde la invitación pendiente antes de iniciar otra partida.'
        : 'Primero debes salir de tu partida actual.',
      socket.pendingMatchId ? 'INVITATION_PENDING' : 'ALREADY_IN_MATCH',
    );
    return false;
  }

  function getCurrentMatch(socket, matchId) {
    if (typeof matchId !== 'string' || socket.matchId !== matchId) {
      sendError(socket, 'No perteneces a esta partida.', 'NOT_IN_MATCH');
      return null;
    }
    const match = matches.get(matchId);
    if (!match || !match.members.has(socket.user.id)) {
      sendError(socket, 'La partida ya no está disponible.', 'MATCH_NOT_FOUND');
      return null;
    }
    return match;
  }

  function rateLimit(socket, isChat) {
    const now = Date.now();
    socket.messageTimes = socket.messageTimes.filter((time) => now - time < 10000);
    if (socket.messageTimes.length >= 60) {
      socket.close(4029, 'Demasiados mensajes');
      return false;
    }
    socket.messageTimes.push(now);

    if (isChat) {
      socket.chatTimes = socket.chatTimes.filter((time) => now - time < 10000);
      if (socket.chatTimes.length >= 5) {
        sendError(socket, 'Espera antes de enviar más mensajes.', 'RATE_LIMITED');
        return false;
      }
      socket.chatTimes.push(now);
    }
    return true;
  }

  function authenticateSocket(socket, token, retryCount = 0) {
    if (typeof token !== 'string' || token.length < 1 || token.length > 4096) {
      clearTimeout(socket.authTimer);
      socket.authenticating = false;
      sendError(socket, 'Token de sesión no válido.', 'AUTHENTICATION_FAILED');
      socket.close(4003, 'Autenticación requerida');
      return;
    }

    Promise.resolve()
      .then(() => authenticate(token))
      .then((user) => {
        if (socket.readyState !== WebSocket.OPEN) return;
        clearTimeout(socket.authTimer);
        socket.authenticating = false;
        if (!user || typeof user.id !== 'string') {
          sendError(socket, 'La sesión no es válida o expiró.', 'AUTHENTICATION_FAILED');
          socket.close(4003, 'Autenticación requerida');
          return;
        }

        const priorConnection = connections.get(user.id);
        const metadataUsername =
          user.user_metadata?.nombre_usuario || user.user_metadata?.username;
        socket.user = {
          id: user.id,
          username: typeof metadataUsername === 'string' &&
              metadataUsername.trim() &&
              metadataUsername.length <= 100
            ? metadataUsername.trim()
            : `Jugador-${user.id.slice(0, 8)}`,
        };
        socket.messageTimes = [];
        socket.chatTimes = [];
        socket.matchId = null;
        socket.pendingMatchId = null;
        connections.set(user.id, socket);
        if (priorConnection && priorConnection !== socket) {
          priorConnection.close(4001, 'Sesión reemplazada');
        }

        send(socket, { type: 'authenticated', user: socket.user });
        pruneGeneralHistory();
        send(socket, { type: 'chat:history', scope: 'general', messages: generalHistory });
        syncRoomPresence();
        broadcastPresence();
      })
      .catch((error) => {
        if (error?.name === 'AuthRetryableFetchError' &&
            retryCount === 0 &&
            socket.readyState === WebSocket.OPEN &&
            socket.authenticating) {
          setTimeout(() => {
            if (socket.readyState === WebSocket.OPEN && socket.authenticating) {
              authenticateSocket(socket, token, retryCount + 1);
            }
          }, AUTH_RETRY_DELAY_MS);
          return;
        }

        clearTimeout(socket.authTimer);
        socket.authenticating = false;
        if (socket.readyState !== WebSocket.OPEN) return;
        console.error('Error autenticando conexión WebSocket:', error);
        sendError(socket, 'No se pudo validar la sesión.', 'AUTHENTICATION_FAILED');
        socket.close(1011, 'Error al validar la sesión');
      });
  }

  function publishChat(socket, scope, text, match) {
    const message = formatChatMessage({
      id: randomUUID(),
      userId: socket.user.id,
      username: socket.user.username,
      text: text.trim(),
      createdAt: new Date().toISOString(),
    }, socket.user.username);
    const event = {
      type: 'chat:message',
      scope,
      ...(match ? { matchId: match.id } : {}),
      message,
    };

    if (match) {
      match.chat.push(message);
      if (match.chat.length > MAX_CHAT_HISTORY) match.chat.shift();
      match.lastActivity = Date.now();
      broadcastMatch(match, event);
    } else {
      publishGeneralMessage(socket.user, message.text, message);
    }
  }

  function publishGeneralMessage(user, text, existingMessage) {
    const message = formatChatMessage(existingMessage || {
      id: randomUUID(),
      userId: user.id,
      username: user.username,
      text: text.trim(),
      createdAt: new Date().toISOString(),
    }, user.username);
    deliverGeneralMessage(message);
    relayGeneralMessage(message);
    return message;
  }

  function broadcastGeneralMessage(message) {
    deliverGeneralMessage(message);
    relayGeneralMessage(message);
  }

  function handleMessage(socket, message) {
    if (!socket.user) {
      if (socket.authenticating) {
        sendError(socket, 'Espera a que termine la autenticación.', 'AUTHENTICATION_PENDING');
        return;
      }
      if (message?.type !== 'auth') {
        sendError(socket, 'Autentícate antes de enviar otros mensajes.', 'AUTHENTICATION_REQUIRED');
        socket.close(4003, 'Autenticación requerida');
        return;
      }
      socket.authenticating = true;
      clearTimeout(socket.authTimer);
      socket.authTimer = setTimeout(() => {
        if (!socket.user && socket.authenticating) {
          socket.authenticating = false;
          sendError(socket, 'Supabase tardó demasiado en validar la sesión.', 'AUTHENTICATION_TIMEOUT');
          socket.close(4003, 'Tiempo de autenticación agotado');
        }
      }, authValidationTimeoutMs);
      authenticateSocket(socket, message.token);
      return;
    }

    if (!message || typeof message !== 'object' || Array.isArray(message) ||
        typeof message.type !== 'string') {
      sendError(socket, 'El mensaje debe ser un objeto JSON con un campo type.');
      return;
    }

    const isChat = message.type === 'chat:send' ||
      message.type === 'chat:private:send';
    if (!rateLimit(socket, isChat)) return;

    switch (message.type) {
      case 'ping':
        send(socket, { type: 'pong' });
        break;

      case 'match:list': {
        pruneMatches();
        const lobbies = [...matches.values()]
          .filter((match) =>
            match.visibility === 'public' &&
            match.status === 'waiting' &&
            match.players.length < 2)
          .map((match) => matchSnapshot(match, socket.user.id));
        send(socket, { type: 'match:list', matches: lobbies });
        break;
      }

      case 'match:create': {
        if (!requireNoMatch(socket)) break;
        if (matches.size >= MAX_MATCHES) pruneMatches();
        if (matches.size >= MAX_MATCHES) {
          sendError(socket, 'No se pueden crear más partidas por el momento.', 'SERVER_BUSY');
          break;
        }
        const visibility = message.visibility ?? 'public';
        if (!['public', 'private'].includes(visibility)) {
          sendError(socket, 'visibility debe ser public o private.');
          break;
        }

        const match = createMatch(socket, visibility);
        send(socket, {
          type: 'match:created',
          match: matchSnapshot(match, socket.user.id),
        });
        break;
      }

      case 'game:challenge:invite':
        sendChallenge(socket, message);
        break;

      case 'game:challenge:respond':
        respondToChallenge(socket, message);
        break;

      case 'match:join': {
        if (!requireNoMatch(socket)) break;
        const match = matches.get(message.matchId);
        if (!match) {
          sendError(socket, 'La partida no existe o ya terminó.', 'MATCH_NOT_FOUND');
          break;
        }
        const existingMember = match.members.get(socket.user.id);
        if (match.visibility === 'private' &&
            !existingMember &&
            match.invitedUserId !== socket.user.id) {
          sendError(socket, 'No recibiste una invitación a esta partida.', 'NOT_INVITED');
          break;
        }
        if (existingMember?.role === 'player') {
          existingMember.socket = socket;
        } else if (match.status !== 'waiting' || match.players.length >= 2) {
          sendError(socket, 'La partida ya está completa.', 'MATCH_FULL');
          break;
        } else {
          match.players.push({ id: socket.user.id, username: socket.user.username });
          match.members.set(socket.user.id, {
            role: 'player',
            socket,
            username: socket.user.username,
          });
          match.status = 'playing';
        }
        socket.matchId = match.id;
        match.lastActivity = Date.now();
        send(socket, {
          type: 'match:joined',
          match: matchSnapshot(match, socket.user.id),
          messages: match.chat,
          actions: match.actions,
        });
        broadcastMatch(match, {
          type: 'match:updated',
          match: matchSnapshot(match),
        });
        break;
      }

      case 'match:spectate': {
        if (!requireNoMatch(socket)) break;
        const match = matches.get(message.matchId);
        if (!match) {
          sendError(socket, 'La partida no existe o ya terminó.', 'MATCH_NOT_FOUND');
          break;
        }
        if (!match.members.has(socket.user.id)) {
          match.members.set(socket.user.id, {
            role: 'spectator',
            socket,
            username: socket.user.username,
          });
        } else {
          match.members.get(socket.user.id).socket = socket;
        }
        socket.matchId = match.id;
        match.lastActivity = Date.now();
        send(socket, {
          type: 'match:spectating',
          match: matchSnapshot(match, socket.user.id),
          messages: match.chat,
          actions: match.actions,
        });
        broadcastMatch(match, {
          type: 'match:updated',
          match: matchSnapshot(match),
        });
        break;
      }

      case 'match:leave': {
        const match = getCurrentMatch(socket, message.matchId);
        if (!match) break;
        leaveMatch(socket, { endMatch: true });
        break;
      }

      case 'match:action': {
        const match = getCurrentMatch(socket, message.matchId);
        if (!match) break;
        if (match.members.get(socket.user.id).role !== 'player') {
          sendError(socket, 'Solo los jugadores pueden enviar acciones.', 'SPECTATOR_READ_ONLY');
          break;
        }
        if (match.status !== 'playing') {
          sendError(socket, 'La partida todavía no ha comenzado.', 'MATCH_NOT_STARTED');
          break;
        }
        if (!message.action || typeof message.action !== 'object' || Array.isArray(message.action)) {
          sendError(socket, 'action debe ser un objeto JSON.');
          break;
        }
        if (message.action.type === 'fleet:ready') {
          if (match.game.phase !== 'placing') {
            sendError(socket, 'La fase de colocación ya terminó.', 'FLEET_PHASE_CLOSED');
            break;
          }
          if (match.game.fleets.has(socket.user.id)) {
            sendError(socket, 'Ya confirmaste tu flota.', 'FLEET_ALREADY_READY');
            break;
          }
          const fleet = validateFleet(message.action.ships);
          if (!fleet) {
            sendError(
              socket,
              'La flota debe incluir una nave de cada clase, rectas, contiguas y sin superposición.',
              'INVALID_FLEET',
            );
            break;
          }
          match.game.fleets.set(socket.user.id, fleet);
          match.game.ammo.set(socket.user.id, createPlayerAmmo());
          if (match.game.fleets.size === 2) {
            match.game.phase = 'active';
            beginTurn(match, match.players[randomInt(0, match.players.length)].id);
          }
          publishMatchAction(match, socket, { type: 'fleet:ready' });
          break;
        }
        if (message.action.type === 'attack' || message.action.type === 'radar') {
          const game = match.game;
          if (game.phase !== 'active') {
            sendError(socket, 'La partida todavía no está lista para atacar.', 'GAME_NOT_ACTIVE');
            break;
          }
          if (game.currentTurnId !== socket.user.id) {
            sendError(socket, 'Espera a que el otro jugador termine su turno.', 'NOT_YOUR_TURN');
            break;
          }
          if (!game.primaryActionAvailable && !game.extraNormalAvailable) {
            sendError(socket, 'Ya usaste tus acciones de este turno.', 'NO_ACTIONS_REMAINING');
            break;
          }
          if (message.action.type === 'radar' && !game.primaryActionAvailable) {
            sendError(socket, 'El radar consume la acción principal y no puede usarse como ataque extra.', 'RADAR_REQUIRES_PRIMARY_ACTION');
            break;
          }

          const opponent = match.players.find((player) => player.id !== socket.user.id);
          const targetFleet = game.fleets.get(opponent.id);
          if (!targetFleet) {
            sendError(socket, 'El rival todavía no confirmó su flota.', 'FLEET_NOT_READY');
            break;
          }

          if (message.action.type === 'radar') {
            const targets = getAttackTargets(message.action.cell, 'radar');
            if (!targets) {
              sendError(socket, 'La casilla del radar no es válida.', 'INVALID_ATTACK');
              break;
            }
            if (!consumeAmmo(game, socket.user.id, 'radar')) {
              sendError(socket, 'No te quedan cargas de radar.', 'OUT_OF_AMMO');
              break;
            }
            const results = targets.map((cell) => {
              const ship = targetFleet.find((candidate) =>
                candidate.type !== 'seriousGirl' && candidate.cells.includes(cell));
              return { cell, detected: Boolean(ship) };
            });
            game.primaryActionAvailable = false;
            game.extraNormalAvailable = false;
            advanceTurn(match);
            const actionId = randomUUID();
            publishMatchAction(match, socket, {
              type: 'radar',
              cell: message.action.cell,
              actionId,
            });
            send(socket, {
              type: 'match:radar:result',
              matchId: match.id,
              actionId,
              results,
            });
            break;
          }

          const attackType = message.action.attackType ?? 'normal';
          const isExtraAttack = !game.primaryActionAvailable;
          if (isExtraAttack && attackType !== 'normal') {
            sendError(socket, 'El ataque extra de la Coordinadora debe ser normal.', 'EXTRA_ATTACK_MUST_BE_NORMAL');
            break;
          }
          if (attackType !== 'normal' && attackType !== 'cross' && attackType !== 'bombardment') {
            sendError(socket, 'El tipo de ataque no es válido.', 'INVALID_ATTACK');
            break;
          }
          const ammo = game.ammo.get(socket.user.id);
          if (attackType !== 'normal' &&
              (!ammo?.[attackType] ||
               ammo[attackType].base + ammo[attackType].carrierBonus <= 0)) {
            sendError(socket, 'No te queda munición de ese tipo.', 'OUT_OF_AMMO');
            break;
          }
          const targets = getAttackTargets(message.action.cell, attackType);
          if (!targets) {
            sendError(socket, 'El ataque o la casilla no son válidos.', 'INVALID_ATTACK');
            break;
          }
          const attackedCells = game.attackedCells.get(socket.user.id) || new Set();
          if (targets.some((cell) => {
            if (!attackedCells.has(cell)) return false;
            const ship = targetFleet.find((candidate) => candidate.cells.includes(cell));
            return ship?.type !== 'battleship'
              || ship.damage.get(cell) >= SHIP_HIT_POINTS.battleship;
          })) {
            sendError(socket, 'El ataque incluye casillas que ya fueron atacadas.', 'CELL_ALREADY_ATTACKED');
            break;
          }
          if (attackType !== 'normal') consumeAmmo(game, socket.user.id, attackType);
          const results = targets.map((cell) => {
            const ship = targetFleet.find((candidate) => candidate.cells.includes(cell));
            if (!ship) return { cell, result: 'miss' };
            if (ship.type === 'seriousGirl' && attackType === 'bombardment') {
              return { cell, result: 'evaded' };
            }
            if (ship.type === 'vanguard' && ship.interceptAvailable) {
              ship.interceptAvailable = false;
              return { cell, result: 'intercepted' };
            }
            ship.damage.set(cell, ship.damage.get(cell) + 1);
            if (ship.type === 'carrier') removeCarrierBonus(game, opponent.id);
            const cellSunk = ship.damage.get(cell) >= SHIP_HIT_POINTS[ship.type];
            return {
              cell,
              result: 'hit',
              damage: ship.damage.get(cell),
              cellSunk,
              shipSunk: shipIsSunk(ship),
            };
          });
          const nextAttackedCells = new Set(attackedCells);
          results
            .filter((result) => result.result !== 'evaded' && result.result !== 'intercepted')
            .forEach((result) => nextAttackedCells.add(result.cell));
          game.attackedCells.set(socket.user.id, nextAttackedCells);
          const hasWon = targetFleet.every(shipIsSunk);
          if (hasWon) {
            game.phase = 'finished';
            game.winnerId = socket.user.id;
            game.currentTurnId = null;
            game.primaryActionAvailable = false;
            game.extraNormalAvailable = false;
          } else {
            if (isExtraAttack) {
              game.extraNormalAvailable = false;
              advanceTurn(match);
            } else {
              game.primaryActionAvailable = false;
              if (!game.extraNormalAvailable) advanceTurn(match);
            }
          }
          publishMatchAction(match, socket, {
            type: 'attack',
            cell: message.action.cell,
            attackType,
            results,
            winnerId: game.winnerId,
          });
          break;
        }
        match.lastActivity = Date.now();
        const actionEvent = {
          type: 'match:action',
          actionId: randomUUID(),
          matchId: match.id,
          playerId: socket.user.id,
          username: socket.user.username,
          action: message.action,
          game: matchSnapshot(match).game,
          sentAt: new Date().toISOString(),
        };
        broadcastMatch(match, actionEvent);
        break;
      }

      case 'chat:send': {
        if (typeof message.text !== 'string' ||
            !message.text.trim() ||
            message.text.trim().length > MAX_CHAT_LENGTH) {
          sendError(socket, `text debe tener entre 1 y ${MAX_CHAT_LENGTH} caracteres.`);
          break;
        }
        if (message.scope === 'general') {
          publishChat(socket, 'general', message.text);
          break;
        }
        if (message.scope === 'match') {
          const match = getCurrentMatch(socket, message.matchId);
          if (match) publishChat(socket, 'match', message.text, match);
          break;
        }
        sendError(socket, 'scope debe ser general o match.');
        break;
      }

      case 'chat:private:send':
        sendPrivateMessage(socket, message);
        break;

      default:
        sendError(socket, `Tipo de mensaje no reconocido: ${message.type}.`, 'UNKNOWN_MESSAGE_TYPE');
    }
  }

  function onUpgrade(request, socket, head) {
    let url;
    try {
      url = new URL(request.url, 'http://localhost');
    } catch {
      socket.destroy();
      return;
    }

    if (url.pathname !== '/ws') {
      socket.destroy();
      return;
    }
    const origin = request.headers.origin;
    if (origin && !allowedOrigins.has(origin)) {
      console.error(`Conexión WebSocket rechazada: el origen "${origin}" no está incluido en CORS_ORIGINS.`);
      const body = 'WebSocket origin is not allowed by CORS.';
      socket.end(
        'HTTP/1.1 403 Forbidden\r\n' +
        'Connection: close\r\n' +
        'Content-Type: text/plain; charset=utf-8\r\n' +
        `Content-Length: ${Buffer.byteLength(body)}\r\n` +
        '\r\n' +
        body,
      );
      return;
    }

    wss.handleUpgrade(request, socket, head, (webSocket) => {
      wss.emit('connection', webSocket, request);
    });
  }

  server.on('upgrade', onUpgrade);
  wss.on('connection', (socket) => {
    socket.authTimer = setTimeout(() => {
      if (!socket.user) {
        sendError(socket, 'Debes autenticarte en los 5 segundos siguientes a la conexión.', 'AUTHENTICATION_REQUIRED');
        socket.close(4003, 'Autenticación requerida');
      }
    }, authMessageTimeoutMs);
    socket.on('message', (data) => {
      let message;
      try {
        message = JSON.parse(data.toString());
      } catch {
        sendError(socket, 'El mensaje debe contener JSON válido.');
        return;
      }
      handleMessage(socket, message);
    });
    socket.on('close', () => {
      clearTimeout(socket.authTimer);
      cleanupSocket(socket);
    });
    socket.on('error', (error) => {
      console.error('Error en conexión WebSocket:', error.message);
    });
  });

  const cleanupTimer = setInterval(() => {
    pruneMatches();
    pruneGeneralHistory();
  }, 5 * 60 * 1000);
  cleanupTimer.unref();

  return {
    wss,
    getGeneralHistory(limit = MAX_CHAT_HISTORY) {
      pruneGeneralHistory();
      return generalHistory.slice(-limit);
    },
    getOnlineUsers,
    getPresenceDiagnostics() {
      return {
        room: roomName,
        realtime: roomStatus,
        privateRealtime: privateRoomStatus,
        localUsers: getLocalOnlineUsers().length,
        remoteUsers: remoteUsers.size,
        onlineUsers: getOnlineUsers().length,
      };
    },
    closeRoom() {
      roomReady = false;
      roomStatus = 'CLOSED';
      privateRoomReady = false;
      privateRoomStatus = 'CLOSED';
      const channels = [roomChannel, privateChannel].filter(Boolean);
      if (channels.length === 0) return Promise.resolve();
      return Promise.all(channels.map((channel) => roomClient.removeChannel(channel))).then(() => {
        roomChannel = null;
        privateChannel = null;
        remoteUsers.clear();
      });
    },
    broadcastGeneralMessage,
    publishGeneralMessage,
    close() {
      clearInterval(cleanupTimer);
      server.removeListener('upgrade', onUpgrade);
      for (const socket of wss.clients) socket.terminate();
      return Promise.all([
        new Promise((resolve) => wss.close(resolve)),
        this.closeRoom(),
      ]);
    },
  };
}

module.exports = { attachRealtime };
