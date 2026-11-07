const { randomUUID } = require('node:crypto');
const { WebSocket, WebSocketServer } = require('ws');
const { formatChatMessage } = require('./chat');

const MAX_MESSAGE_BYTES = 16 * 1024;
const MAX_CHAT_LENGTH = 500;
const MAX_CHAT_HISTORY = 50;
const MAX_MATCHES = 500;
const MATCH_TTL_MS = 2 * 60 * 60 * 1000;

function attachRealtime(server, {
  authenticate,
  allowedOrigins,
  roomClient,
  roomName = 'chat:general',
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
    const recipientId = message.recipientId ??
      message.recipient_id ??
      message.toUserId ??
      message.to_user_id ??
      message.to;
    const text = message.text ?? message.contenido ?? message.content ?? message.message;
    if (typeof recipientId !== 'string' || !recipientId.trim() || recipientId.length > 128) {
      sendError(socket, 'recipientId debe identificar al destinatario.', 'INVALID_RECIPIENT');
      return;
    }
    if (recipientId === socket.user.id) {
      sendError(socket, 'No puedes enviarte un mensaje privado.', 'INVALID_RECIPIENT');
      return;
    }
    if (typeof text !== 'string' || !text.trim() || text.trim().length > MAX_CHAT_LENGTH) {
      sendError(socket, `text debe tener entre 1 y ${MAX_CHAT_LENGTH} caracteres.`, 'INVALID_MESSAGE');
      return;
    }

    const localRecipient = connections.get(recipientId);
    const recipient = localRecipient?.readyState === WebSocket.OPEN
      ? localRecipient.user
      : remoteUsers.get(recipientId);
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
    };
  }

  function broadcastMatch(match, message) {
    for (const socket of liveMembers(match)) {
      send(socket, message);
    }
  }

  function updateMatch(match) {
    match.lastActivity = Date.now();
    broadcastMatch(match, {
      type: 'match:updated',
      match: matchSnapshot(match),
    });
  }

  function removeMatch(match, message) {
    broadcastMatch(match, message);
    for (const member of match.members.values()) {
      if (member.socket) member.socket.matchId = null;
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
    leaveMatch(socket);
  }

  function pruneMatches() {
    const now = Date.now();
    for (const match of matches.values()) {
      if (now - match.lastActivity > MATCH_TTL_MS && liveMembers(match).length === 0) {
        matches.delete(match.id);
      }
    }
  }

  function requireNoMatch(socket) {
    if (!socket.matchId) return true;
    sendError(socket, 'Primero debes salir de tu partida actual.', 'ALREADY_IN_MATCH');
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

  function authenticateSocket(socket, token) {
    if (typeof token !== 'string' || token.length < 1 || token.length > 4096) {
      sendError(socket, 'Token de sesión no válido.', 'AUTHENTICATION_FAILED');
      socket.close(4003, 'Autenticación requerida');
      return;
    }

    Promise.resolve()
      .then(() => authenticate(token))
      .then((user) => {
        if (socket.readyState !== WebSocket.OPEN) return;
        if (!user || typeof user.id !== 'string') {
          sendError(socket, 'La sesión no es válida o expiró.', 'AUTHENTICATION_FAILED');
          socket.close(4003, 'Autenticación requerida');
          return;
        }

        clearTimeout(socket.authTimer);
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
        connections.set(user.id, socket);
        if (priorConnection && priorConnection !== socket) {
          priorConnection.close(4001, 'Sesión reemplazada');
        }

        send(socket, { type: 'authenticated', user: socket.user });
        send(socket, { type: 'chat:history', scope: 'general', messages: generalHistory });
        syncRoomPresence();
        broadcastPresence();
      })
      .catch((error) => {
        console.error('Error autenticando conexión WebSocket:', error);
        if (socket.readyState === WebSocket.OPEN) {
          sendError(socket, 'No se pudo validar la sesión.', 'AUTHENTICATION_FAILED');
          socket.close(1011, 'Error al validar la sesión');
        }
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
          createdAt: new Date(now).toISOString(),
          lastActivity: now,
        };
        socket.matchId = match.id;
        matches.set(match.id, match);
        send(socket, {
          type: 'match:created',
          match: matchSnapshot(match, socket.user.id),
        });
        break;
      }

      case 'match:join': {
        if (!requireNoMatch(socket)) break;
        const match = matches.get(message.matchId);
        if (!match) {
          sendError(socket, 'La partida no existe o ya terminó.', 'MATCH_NOT_FOUND');
          break;
        }
        const existingMember = match.members.get(socket.user.id);
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
        match.lastActivity = Date.now();
        broadcastMatch(match, {
          type: 'match:action',
          matchId: match.id,
          playerId: socket.user.id,
          username: socket.user.username,
          action: message.action,
          sentAt: new Date().toISOString(),
        });
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
    }, 5000);
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
    socket.on('close', () => cleanupSocket(socket));
    socket.on('error', (error) => {
      console.error('Error en conexión WebSocket:', error.message);
    });
  });

  const cleanupTimer = setInterval(pruneMatches, 10 * 60 * 1000);
  cleanupTimer.unref();

  return {
    wss,
    getGeneralHistory(limit = MAX_CHAT_HISTORY) {
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
