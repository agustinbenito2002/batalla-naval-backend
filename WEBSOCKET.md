# WebSocket en tiempo real

El backend expone el WebSocket en `ws://localhost:3000/ws` en desarrollo. En
producción, usa `wss://<dominio-del-backend>/ws` cuando la plataforma o el
proxy inverso termina HTTPS. Si Node termina TLS directamente, el WebSocket
seguro está disponible en `wss://<dominio-del-backend>:<PORT>/ws`. Cada
conexión debe autenticarse con un access token de Supabase.
El estado de partidas y el chat por partida viven en memoria; no se conservan
al reiniciar el proceso ni se comparten entre varias instancias del backend.
Todos los clientes del chat general se unen a la sala global `chat:general`.
Su presencia y mensajes se sincronizan entre instancias del backend mediante
Supabase Realtime; los mensajes enviados por HTTP se persisten en las tablas
`conversaciones`, `miembros_conversacion` y `mensajes` de Supabase durante una
hora. Los mensajes vencidos se eliminan periódicamente y al consultar o enviar
mensajes en el chat general.

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
`GET /chat/messages` devuelve directamente un arreglo JSON con los mensajes
(no un objeto que envuelve el arreglo). El ID de conversación se devuelve en
el encabezado `X-Conversation-Id`. Cada mensaje incluye los campos de Supabase
y alias compatibles (`remitente_id`, `sender_id`, `userId`, `sender`,
`username`, `contenido`, `text`, `content` y `message`) para identificar
remitente y texto en la página. También están disponibles los alias
`/api/chat/messages`, `/api/chat` y `/api/chat/general`. `POST` devuelve el
mensaje guardado dentro de `{ "data": ... }`; los mensajes generales enviados
por HTTP también se retransmiten como `chat:message` a las conexiones
WebSocket.

La lista pública de usuarios conectados también se puede consultar por HTTP
con `GET /chat/online` (alias: `GET /api/chat/online`); no requiere token.
La respuesta es directamente un arreglo JSON:
`[{ "id": "...", "username": "..." }]`.

Para diagnosticar la conexión, consulta `GET /chat/online/status` (alias:
`GET /api/chat/online/status`); no requiere token y devuelve el nombre de sala
y los estados de Supabase Realtime, conexiones WebSocket autenticadas
localmente y usuarios de otras instancias. Los mensajes y las partidas siguen
requiriendo autenticación.

## Conexión desde el navegador

```js
const socket = new WebSocket('ws://localhost:3000/ws');

socket.addEventListener('open', () => {
  socket.send(JSON.stringify({ type: 'auth', token: accessToken }));
});

socket.addEventListener('message', ({ data }) => {
  const event = JSON.parse(data);
  if (event.type === 'chat:presence') {
    setOnlineUsers(event.users);
  }
  console.log(event);
});
```

Mantén esta conexión abierta mientras el usuario esté en el chat. La lista se
actualiza con cada evento `chat:presence`; consultar `/chat/online` una sola
vez no mantiene la interfaz sincronizada. Usa `wss://` cuando la página esté
cargada por HTTPS y conecta todas las sesiones a la misma URL pública del
backend.

Para una página publicada por HTTPS, el frontend debe conectar mediante
`wss://`; los navegadores bloquean `ws://` desde una página HTTPS. En
alojamientos con HTTPS integrado (por ejemplo, un proxy inverso o una
plataforma de hosting), deja que la plataforma termine TLS y reenvíe la
conexión HTTP/WebSocket a Node. No actives TLS en Node en ese caso.

Si administras el certificado y Node termina TLS directamente, configura
`TLS_KEY_PATH` y `TLS_CERT_PATH` con las rutas de la clave privada y el
certificado (incluida la cadena completa). El servidor escucha entonces por
HTTPS/WSS. Debes configurar ambas variables; una sola provoca un error al
iniciar.

En desarrollo se permite `http://localhost:5173` y
`http://127.0.0.1:5173`. Para producción, configura `CORS_ORIGINS` con el
origen exacto de la página, incluido `https://`, por ejemplo
`CORS_ORIGINS=https://mi-juego.example.com`. Se pueden separar varios
orígenes con comas. En el WebSocket el servidor
rechaza con HTTP 403 los orígenes que no estén permitidos y registra el
origen rechazado en la terminal.

La conexión debe proceder de un origen incluido en `CORS_ORIGINS` (en
desarrollo también se permiten los orígenes locales configurados por el
backend). El cliente debe enviar el mensaje `auth` en los primeros cinco
segundos. Desde que lo recibe, el servidor puede tardar hasta 20 segundos en
validar el token contra Supabase; si se supera ese plazo, envía el error
`AUTHENTICATION_TIMEOUT` y cierra la conexión. Solo se permite una conexión
activa por usuario; una conexión nueva reemplaza la anterior.
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
| `game:challenge:invite` | `recipientId` (ID o nombre de usuario) | Crea una partida privada e invita al usuario conectado. |
| `game:challenge:respond` | `matchId`, `accepted` (booleano) | Acepta o rechaza una invitación privada. Solo el usuario invitado puede responder. |
| `match:action` | `matchId`, `action` (objeto JSON) | Procesa la flota tipada, ataques y radar; valida munición y turnos, y retransmite los resultados. |
| `chat:send` | `scope` (`general` o `match`), `text`; `matchId` para `match` | Envía un mensaje al chat general o al chat de la partida actual. |
| `chat:private:send` | `recipientId` (o `recipient_id`), `text` | Envía un mensaje privado únicamente al emisor y al destinatario conectados. El destinatario puede identificarse por ID o nombre de usuario; también acepta `recipient` (ID, nombre u objeto con `id`), `toUserId`, `to_user_id`, `to` y los campos de texto `contenido`, `content` o `message`. El destinatario debe estar en línea; de lo contrario devuelve el error `RECIPIENT_OFFLINE`. |

Los mensajes privados no se guardan en Supabase ni en el historial del servidor.
El cliente debe mantenerlos solo en el estado de la pestaña de chat privado y
descartarlos al cerrarla. Cada partida mantiene su propio historial en memoria
(hasta 50 mensajes); solo pueden leerlo y escribir quienes se hayan unido a esa
partida como jugador o espectador. Los mensajes de una partida nunca se envían
al chat general ni a otras partidas. Los mensajes generales se conservan como
máximo una hora.

`fleet:ready` debe enviar cinco objetos `{ type, cells }`, uno de cada tipo:
`seriousGirl` (2 casillas), `vanguard` (3), `coordinator` (3), `battleship` (4)
y `carrier` (4). Las naves deben ser rectas, contiguas y no superponerse.

Al comenzar, cada jugador recibe 5 municiones `cross`, 2 `bombardment` y 1
`radar`; el ataque `normal` es ilimitado. El portaaviones intacto agrega una
munición de cada tipo y pierde las que queden cuando recibe su primer impacto.
El ataque en cruz alcanza cinco casillas, el bombardeo usa el patrón 5×5
existente y el radar revela las naves en una cruz de siete casillas (cinco
verticales y tres horizontales, compartiendo el centro). El radar no causa daño,
no detecta a la Chica seria, consume la acción principal y termina el turno.
Solo el jugador que lo usó recibe `match:radar:result`.

El servidor sortea al azar quién empieza cuando ambas flotas están listas. La
Coordinadora concede un ataque normal adicional en el turno mientras quede al
menos una de sus tres casillas sin impacto. El Acorazado necesita dos impactos
en cada casilla; la Vanguardia intercepta el primer impacto que recibe; la
Chica seria evade el bombardeo. El servidor calcula impactos, hundimientos,
munición y victoria, y rechaza ataques fuera de turno, munición agotada y
repeticiones que no sean el segundo impacto de una casilla del Acorazado.

## Eventos del servidor

- `authenticated`: la autenticación terminó correctamente.
- `error`: incluye `code` y `message` cuando una operación no es válida.
- `chat:history`: historial reciente del chat general al autenticarse.
- `chat:presence`: estado actual de presencia, con `users` (objetos con `id` y
  `username`); se retransmite al cambiar la lista de conexiones.
- `chat:message`: nuevo mensaje de chat; incluye `scope`, `message` y, para el
  chat de partida, `matchId`.
- `chat:private:message`: mensaje privado dirigido solo al emisor y al
  destinatario; contiene `senderId`, `recipientId` y `message`.
- `match:created`: partida creada; incluye el objeto `match`.
- `match:list`: respuesta de la solicitud de lista, con `matches`.
- `match:joined` / `match:spectating`: confirma el ingreso e incluye `match`
  y los últimos mensajes de esa partida.
- `match:updated`: estado y jugadores actuales de la partida.
- `game:challenge:sent`: confirma al emisor que se envió el desafío e incluye
  `matchId` y destinatario.
- `game:challenge:invite`: invitación dirigida al destinatario, con `matchId`
  y datos del emisor dentro de `invitation`.
- `game:challenge:accepted` / `game:challenge:declined`: resultado del desafío;
  el rechazo puede incluir `reason` (`recipient_offline`, `expired` o
  `cancelled`).
- `match:action`: acción procesada con `actionId`, `playerId`, `username`,
  `sentAt` y estado público `game`; ataques también incluyen `results` y
  `winnerId` dentro de `action`. `game.ammo` contiene solo la munición del
  usuario que recibe el evento.
- `match:radar:result`: resultado privado del radar, con las casillas
  `{ cell, detected }`; solo lo recibe quien usó el radar.
- `match:ended`: partida finalizada, por ejemplo cuando un jugador sale.
- `pong`: respuesta al mensaje `ping`.

El objeto `match` incluye `id`, `visibility`, `status` (`waiting` o `playing`),
`players` (con estado `online`), cantidad de `spectators`, `yourRole` y
`createdAt`. También contiene `game` (`phase`, `currentTurnId`, `turnNumber`,
`winnerId`, `readyPlayerIds`, `primaryActionAvailable`, `extraNormalAvailable`
y la munición del receptor); nunca incluye la posición ni el daño detallado de
las flotas. Las partidas siguen en memoria y solo pueden emparejar usuarios
conectados a la misma instancia del backend. Los mensajes de chat admiten hasta
500 caracteres. Los marcos WebSocket están limitados a 16 KiB y las conexiones
se limitan para evitar envíos excesivos.
