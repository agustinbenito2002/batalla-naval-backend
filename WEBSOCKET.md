# WebSocket en tiempo real

El backend expone el WebSocket en `ws://localhost:3000/ws` (usa `wss://` en
producción). Cada conexión debe autenticarse con un access token de Supabase.
El estado de partidas y el chat por partida viven en memoria; no se conservan
al reiniciar el proceso ni se comparten entre varias instancias del backend.
Todos los clientes del chat general se unen a la sala global `chat:general`.
Su presencia y mensajes se sincronizan entre instancias del backend mediante
Supabase Realtime; los mensajes enviados por HTTP se persisten en las tablas
`conversaciones`, `miembros_conversacion` y `mensajes` de Supabase.

## Chat general por HTTP

El chat general está disponible en estos endpoints autenticados con
`Authorization: Bearer <token>`:

```http
GET /chat/messages
POST /chat/messages
Content-Type: application/json

{ "contenido": "Hola a todos" }
```

El servidor crea la conversación de tipo `general` si todavía no existe y
agrega al usuario autenticado a `miembros_conversacion`. También acepta
`content`, `text`, `message` o `mensaje` como nombre del campo de texto, y
`conversacion_id`, `conversation_id` o `conversationId` para indicar una
conversación existente. Los mensajes se guardan con los campos
`conversacion_id`, `remitente_id` y `contenido` de la tabla `mensajes`.
También están disponibles los alias `/api/chat/messages`, `/api/chat` y
`/api/chat/general`. La respuesta exitosa tiene la forma `{ "data": ... }`;
los mensajes generales enviados por HTTP también se retransmiten como
`chat:message` a las conexiones WebSocket.

La lista de usuarios conectados también se puede consultar por HTTP con
`GET /chat/online` (alias: `GET /api/chat/online`) y el mismo encabezado
`Authorization`. La respuesta es directamente un arreglo JSON:
`[{ "id": "...", "username": "..." }]`.

## Conexión desde el navegador

```js
const socket = new WebSocket('ws://localhost:3000/ws');

socket.addEventListener('open', () => {
  socket.send(JSON.stringify({ type: 'auth', token: accessToken }));
});

socket.addEventListener('message', ({ data }) => {
  const event = JSON.parse(data);
  console.log(event);
});
```

La conexión debe proceder de un origen incluido en `CORS_ORIGINS` (en
desarrollo también se permiten los orígenes locales configurados por el
backend). El servidor valida el token contra Supabase. Solo se permite una
conexión activa por usuario; una conexión nueva reemplaza la anterior.
Al autenticarse, el servidor envía `chat:presence` con la lista de usuarios
conectados y vuelve a transmitirla cuando alguien se conecta o desconecta.
La presencia es en memoria, solo incluye usuarios conectados por WebSocket a
esta instancia del backend y no representa a usuarios conectados únicamente
por HTTP.

## Mensajes del cliente

Todos los mensajes son objetos JSON con un campo `type`.

| Mensaje | Campos | Descripción |
| --- | --- | --- |
| `auth` | `token` | Autentica la conexión; debe ser el primer mensaje. |
| `ping` | — | Solicita una respuesta `pong`. |
| `match:list` | — | Lista las partidas públicas esperando un jugador. |
| `match:create` | `visibility` (`public` o `private`, opcional) | Crea una partida y asigna al creador el primer puesto de jugador. |
| `match:join` | `matchId` | Se une como jugador a una partida disponible; el segundo jugador la inicia. |
| `match:spectate` | `matchId` | Se une como espectador, sin permiso para enviar acciones del juego. |
| `match:leave` | `matchId` | Sale de la partida; si sale un jugador, la partida termina. |
| `match:action` | `matchId`, `action` (objeto JSON) | Retransmite la acción de un jugador al resto de la sala. |
| `chat:send` | `scope` (`general` o `match`), `text`; `matchId` para `match` | Envía un mensaje al chat general o al chat de la partida actual. |

El backend retransmite acciones y no implementa ni valida las reglas de
Batalla Naval, los turnos o el tablero. La página debe interpretar las
acciones y aplicar las reglas del juego.

## Eventos del servidor

- `authenticated`: la autenticación terminó correctamente.
- `error`: incluye `code` y `message` cuando una operación no es válida.
- `chat:history`: historial reciente del chat general al autenticarse.
- `chat:presence`: estado actual de presencia, con `users` (objetos con `id` y
  `username`); se retransmite al cambiar la lista de conexiones.
- `chat:message`: nuevo mensaje de chat; incluye `scope`, `message` y, para el
  chat de partida, `matchId`.
- `match:created`: partida creada; incluye el objeto `match`.
- `match:list`: respuesta de la solicitud de lista, con `matches`.
- `match:joined` / `match:spectating`: confirma el ingreso e incluye `match`
  y los últimos mensajes de esa partida.
- `match:updated`: estado y jugadores actuales de la partida.
- `match:action`: acción retransmitida con `playerId`, `username` y `sentAt`.
- `match:ended`: partida finalizada, por ejemplo cuando un jugador sale.
- `pong`: respuesta al mensaje `ping`.

El objeto `match` incluye `id`, `visibility`, `status` (`waiting` o `playing`),
`players` (con estado `online`), cantidad de `spectators`, `yourRole` y
`createdAt`. Los mensajes de chat admiten hasta 500 caracteres. Los marcos
WebSocket están limitados a 16 KiB y las conexiones se limitan para evitar
envíos excesivos.
