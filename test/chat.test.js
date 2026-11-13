const assert = require('node:assert/strict');
const test = require('node:test');
const {
  createChatService,
  formatChatMessage,
  parseConversationId,
  parseMessageContent,
} = require('../chat');

test('parses positive integer conversation IDs from route payloads', () => {
  assert.equal(parseConversationId(12), 12);
  assert.equal(parseConversationId('12'), 12);
  assert.equal(parseConversationId('12abc'), null);
  assert.equal(parseConversationId('0'), null);
  assert.equal(parseConversationId('9007199254740992'), null);
  assert.equal(parseConversationId(undefined), null);
});

test('accepts message field aliases while validating and trimming content', () => {
  assert.equal(parseMessageContent({ contenido: ' hola ' }), 'hola');
  assert.equal(parseMessageContent({ text: 'hola' }), 'hola');
  assert.equal(parseMessageContent({ message: 'hola' }), 'hola');
  assert.equal(parseMessageContent({ mensaje: 'hola' }), 'hola');
  assert.equal(parseMessageContent({ contenido: '   ' }), null);
  assert.equal(parseMessageContent({ contenido: 'x'.repeat(501) }), null);
  assert.equal(parseMessageContent([]), null);
});

test('formats database messages with sender and text fields for chat clients', () => {
  const formatted = formatChatMessage({
    id: 7,
    conversacion_id: 3,
    remitente_id: 'user-123',
    contenido: 'Hola',
    fecha_envio: '2026-11-06T12:00:00Z',
  }, 'Navegante');

  assert.equal(formatted.remitente_id, 'user-123');
  assert.equal(formatted.sender_id, 'user-123');
  assert.equal(formatted.userId, 'user-123');
  assert.equal(formatted.sender, 'Navegante');
  assert.equal(formatted.username, 'Navegante');
  assert.equal(formatted.text, 'Hola');
  assert.equal(formatted.content, 'Hola');
  assert.equal(formatted.message, 'Hola');
  assert.equal(formatted.contenido, 'Hola');
});

test('deletes only general chat messages older than the one-hour cutoff', async () => {
  const expectedCutoff = '2026-11-06T12:00:00.000Z';
  const query = {};
  const adminClient = {
    from(table) {
      assert.equal(table, 'mensajes');
      return {
        delete() {
          return {
            eq(column, value) {
              query.conversation = [column, value];
              return {
                lt(timestampColumn, cutoff) {
                  query.timestamp = [timestampColumn, cutoff];
                  return Promise.resolve({ error: null });
                },
              };
            },
          };
        },
      };
    },
  };

  const service = createChatService(adminClient);
  const result = await service.deleteExpiredGeneralMessages(42, expectedCutoff);

  assert.deepEqual(result, { error: null });
  assert.deepEqual(query, {
    conversation: ['conversacion_id', 42],
    timestamp: ['fecha_envio', expectedCutoff],
  });
});
