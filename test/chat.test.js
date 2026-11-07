const assert = require('node:assert/strict');
const test = require('node:test');
const {
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
