const assert = require('node:assert/strict');
const test = require('node:test');
const { parseConversationId, parseMessageContent } = require('../chat');

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
