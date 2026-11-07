function parseConversationId(value) {
  if (typeof value !== 'string' && typeof value !== 'number') return null;
  if (typeof value === 'string' && !/^\d+$/.test(value)) return null;

  const id = Number(value);
  return Number.isSafeInteger(id) && id > 0 ? id : null;
}

function parseMessageContent(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) return null;
  const value = body.contenido ?? body.content ?? body.text ?? body.message ?? body.mensaje;
  if (typeof value !== 'string' || !value.trim() || value.trim().length > 500) return null;
  return value.trim();
}

function formatChatMessage(message, username) {
  const senderId = message.remitente_id ?? message.userId ?? message.user_id ?? message.sender_id;
  const text = message.contenido ?? message.text ?? message.content ?? message.message;
  const senderName = username || message.username || message.sender || senderId;

  return {
    ...message,
    remitente_id: senderId,
    user_id: senderId,
    userId: senderId,
    sender_id: senderId,
    senderId,
    remitente: senderName,
    sender: senderName,
    username: senderName,
    contenido: text,
    text,
    content: text,
    message: text,
    fecha_envio: message.fecha_envio ?? message.createdAt,
    createdAt: message.createdAt ?? message.fecha_envio,
  };
}

function createChatService(adminClient) {
  let generalConversationPromise;

  async function getGeneralConversation() {
    if (!generalConversationPromise) {
      generalConversationPromise = (async () => {
        const { data, error } = await adminClient
          .from('conversaciones')
          .select('id, tipo, partida_id')
          .eq('tipo', 'general')
          .is('partida_id', null)
          .order('id', { ascending: true })
          .limit(1)
          .maybeSingle();
        if (error) throw error;
        if (data) return data;

        const { data: created, error: createError } = await adminClient
          .from('conversaciones')
          .insert({ tipo: 'general' })
          .select('id, tipo, partida_id')
          .single();
        if (createError) throw createError;
        return created;
      })().catch((error) => {
        generalConversationPromise = null;
        throw error;
      });
    }

    return generalConversationPromise;
  }

  async function getConversation(id) {
    return adminClient
      .from('conversaciones')
      .select('id, tipo, partida_id')
      .eq('id', id)
      .maybeSingle();
  }

  async function hasMembership(conversationId, userId) {
    const { data, error } = await adminClient
      .from('miembros_conversacion')
      .select('usuario_id')
      .eq('conversacion_id', conversationId)
      .eq('usuario_id', userId)
      .maybeSingle();
    return { isMember: Boolean(data), error };
  }

  async function addGeneralMember(conversationId, userId) {
    const { error } = await adminClient
      .from('miembros_conversacion')
      .insert({ conversacion_id: conversationId, usuario_id: userId });
    if (error && error.code !== '23505') return { error };
    return { error: null };
  }

  return {
    getGeneralConversation,
    getConversation,
    hasMembership,
    addGeneralMember,
    getProfiles(userIds) {
      if (userIds.length === 0) return Promise.resolve({ data: [], error: null });
      return adminClient
        .from('perfiles')
        .select('id, nombre_usuario')
        .in('id', userIds);
    },
    getMessages(conversationId, limit = 100) {
      return adminClient
        .from('mensajes')
        .select('id, conversacion_id, remitente_id, contenido, fecha_envio, leido')
        .eq('conversacion_id', conversationId)
        .order('fecha_envio', { ascending: false })
        .limit(limit);
    },
    createMessage(conversationId, userId, content) {
      return adminClient
        .from('mensajes')
        .insert({
          conversacion_id: conversationId,
          remitente_id: userId,
          contenido: content,
        })
        .select('id, conversacion_id, remitente_id, contenido, fecha_envio, leido')
        .single();
    },
  };
}

module.exports = {
  createChatService,
  formatChatMessage,
  parseConversationId,
  parseMessageContent,
};
