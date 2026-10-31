const path = require('node:path');
const express = require('express');
const dotenv = require('dotenv');

const { error: envError } = dotenv.config({ path: path.join(__dirname, '.env') });
if (envError) {
  throw envError;
}

const { supabase, createAdminClient, createUserClient } = require('./db');

const app = express();
const port = Number(process.env.PORT) || 3000;

app.use(express.json());
app.locals.supabase = supabase;

const localDevelopmentOrigins = process.env.NODE_ENV === 'production'
  ? []
  : ['http://localhost:5173', 'http://127.0.0.1:5173'];
const configuredCorsOrigins = (process.env.CORS_ORIGINS || '')
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);
const corsOrigins = new Set([...localDevelopmentOrigins, ...configuredCorsOrigins]);

app.use((req, res, next) => {
  const origin = req.get('origin');
  if (origin && corsOrigins.has(origin)) {
    res.set({
      'Access-Control-Allow-Origin': origin,
      'Access-Control-Allow-Headers': 'Authorization, Content-Type',
      'Access-Control-Allow-Methods': 'GET, POST, PATCH, OPTIONS',
      Vary: 'Origin',
    });
  }

  if (req.method === 'OPTIONS') {
    return res.sendStatus(!origin || corsOrigins.has(origin) ? 204 : 403);
  }

  return next();
});

app.get('/', (_req, res) => {
  res.json({ status: 'ok', message: 'Backend en funcionamiento. Consulta /health para verificar Supabase.' });
});

function esErrorTimeout(error) {
  const mensaje = typeof error?.message === 'string' ? error.message : '';
  const detalles = typeof error?.details === 'string' ? error.details : '';
  const codigoCausa = error?.cause?.code;
  const detallesTimeout = `${mensaje}\n${detalles}\n${error?.cause?.message || ''}`;

  return error?.name === 'TimeoutError' ||
    error?.name === 'AbortError' ||
    error?.code === 'UND_ERR_CONNECT_TIMEOUT' ||
    codigoCausa === 'UND_ERR_CONNECT_TIMEOUT' ||
    /^(TimeoutError|AbortError):/.test(mensaje) ||
    /UND_ERR_CONNECT_TIMEOUT|Connect Timeout Error/i.test(detallesTimeout);
}

async function consultarTabla(req, res, tabla) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(tabla)) {
    return res.status(400).json({ error: 'Nombre de tabla no válido.' });
  }

  const limite = Number(req.query.limite ?? 50);
  if (!Number.isInteger(limite) || limite < 1 || limite > 100) {
    return res.status(400).json({ error: 'El límite debe ser un entero entre 1 y 100.' });
  }

  try {
    const { data, error } = await supabase
      .from(tabla)
      .select('*')
      .limit(limite)
      .retry(false);

    if (error) {
      console.error(`Error de Supabase al consultar "${tabla}":`, {
        code: error.code,
        message: error.message,
        details: error.details,
        hint: error.hint,
      });

      const esTimeout = esErrorTimeout(error);
      const status = esTimeout ? 504 : error.code ? 502 : 503;
      return res.status(status).json({
        error: error.message,
        code: esTimeout ? 'SUPABASE_TIMEOUT' : error.code,
        details: error.details,
        hint: error.hint,
      });
    }

    return res.json({ data });
  } catch (error) {
    console.error('Error consultando Supabase:', error);
    const esTimeout = esErrorTimeout(error);
    return res.status(esTimeout ? 504 : 503).json({
      error: esTimeout
        ? 'Supabase no respondió dentro del tiempo límite.'
        : 'No se pudo completar la consulta a Supabase.',
      code: esTimeout ? 'SUPABASE_TIMEOUT' : undefined,
      details: error.message,
    });
  }
}

function responderErrorSupabase(res, operacion, error) {
  console.error(`Error de Supabase al ${operacion}:`, {
    code: error.code,
    message: error.message,
    details: error.details,
    hint: error.hint,
  });

  const esTimeout = esErrorTimeout(error);
  const status = esTimeout
    ? 504
    : error.code === '23505'
      ? 409
      : error.code === '42501'
        ? 403
        : error.code
          ? 502
          : 503;

  return res.status(status).json({
    error: error.message,
    code: esTimeout ? 'SUPABASE_TIMEOUT' : error.code,
    details: error.details,
    hint: error.hint,
  });
}

function validarCredenciales(body, requiereUsuario = false) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return { error: 'El cuerpo de la solicitud debe ser un objeto JSON.' };
  }

  const { password, username } = body;
  if (typeof password !== 'string' || !password ||
      (requiereUsuario && password.length < 8)) {
    return {
      error: requiereUsuario
        ? 'La contraseña debe tener al menos 8 caracteres.'
        : 'La contraseña es obligatoria.',
    };
  }
  if (requiereUsuario && (typeof username !== 'string' || !username.trim())) {
    return { error: 'nombre_usuario es obligatorio.' };
  }
  if (requiereUsuario && username.trim().length > 100) {
    return { error: 'nombre_usuario no puede superar los 100 caracteres.' };
  }
  if (requiereUsuario && !/^[A-Za-z0-9_]{3,20}$/.test(username.trim())) {
    return { error: 'nombre_usuario debe tener entre 3 y 20 caracteres: letras, números o guion bajo.' };
  }

  if (!requiereUsuario &&
      (typeof username !== 'string' || !/^[A-Za-z0-9_]{3,20}$/.test(username.trim()))) {
    return { error: 'Ingresa un nombre de usuario válido.' };
  }

  const cleanUsername = username.trim();
  return {
    credentials: {
      email: `${cleanUsername.toLowerCase()}@users.example.com`,
      password,
      username: cleanUsername,
    },
  };
}

async function guardarPerfilUsuario(user, accessToken, username) {
  const { error } = await createUserClient(accessToken)
    .from('perfiles')
    .upsert(
      { id: user.id, nombre_usuario: username, avatar_url: null },
      { onConflict: 'id' },
    );
  return error;
}

app.post('/api/auth/register', async (req, res) => {
  const validacion = validarCredenciales(req.body, true);
  if (validacion.error) {
    return res.status(400).json({ error: validacion.error });
  }

  const admin = createAdminClient();
  if (!admin) {
    return res.status(503).json({
      error: 'El backend necesita SUPABASE_SERVICE_ROLE_KEY para registrar usuarios sin correo.',
    });
  }

  const { email, password, username } = validacion.credentials;
  try {
    const { data, error } = await admin.auth.admin.createUser({
      email,
      password,
      email_confirm: true,
      user_metadata: { nombre_usuario: username },
    });

    if (error) {
      const esTimeout = esErrorTimeout(error);
      console.error('Error de Supabase al registrar usuario:', {
        code: error.code,
        message: error.message,
      });
      return res.status(esTimeout ? 504 : error.status || 400).json({
        error: esTimeout
          ? 'Supabase no respondió al registrar el usuario.'
          : error.code === 'email_exists' || error.code === 'user_already_exists'
            ? 'Ese nombre de usuario ya está registrado.'
            : 'No se pudo registrar el usuario. Comprueba los datos e inténtalo nuevamente.',
      });
    }

    const { error: profileError } = await admin
      .from('perfiles')
      .upsert(
        { id: data.user.id, nombre_usuario: username, avatar_url: null },
        { onConflict: 'id' },
      );
    if (profileError) {
      console.error('No se pudo guardar el perfil del usuario registrado:', {
        code: profileError.code,
        message: profileError.message,
      });
      return res.status(502).json({
        error: 'La cuenta se creó, pero no se pudo guardar el perfil. Intenta iniciar sesión.',
      });
    }

    const { data: sessionData, error: sessionError } = await supabase.auth.signInWithPassword({
      email,
      password,
    });
    if (sessionError) {
      console.error('La cuenta se creó, pero no se pudo iniciar su sesión:', {
        code: sessionError.code,
        message: sessionError.message,
      });
      return res.status(502).json({
        error: 'La cuenta se creó, pero no se pudo iniciar sesión. Intenta ingresar con tus datos.',
      });
    }

    return res.status(201).json({
      user: { id: data.user.id, username },
      token: sessionData.session.access_token,
      confirmationRequired: false,
    });
  } catch (error) {
    const esTimeout = esErrorTimeout(error);
    console.error('Error registrando usuario:', error);
    return res.status(esTimeout ? 504 : 503).json({
      error: esTimeout
        ? 'Supabase no respondió al registrar el usuario.'
        : 'No se pudo completar el registro. Inténtalo nuevamente.',
    });
  }
});

app.post('/api/auth/login', async (req, res) => {
  const validacion = validarCredenciales(req.body);
  if (validacion.error) {
    return res.status(400).json({ error: validacion.error });
  }

  const { email, password, username } = validacion.credentials;
  try {
    const { data, error } = await supabase.auth.signInWithPassword({ email, password });
    if (error) {
      const esTimeout = esErrorTimeout(error);
      if (esTimeout) {
        return responderErrorSupabase(res, 'iniciar sesión', error);
      }
      if (error.code === 'email_not_confirmed') {
        return res.status(403).json({
          error: 'La cuenta todavía no está habilitada. Revisa la configuración de autenticación del backend.',
        });
      }
      return res.status(401).json({ error: 'Usuario o contraseña incorrectos.' });
    }

    const profileUsername = data.user.user_metadata?.nombre_usuario || username;
    if (username) {
      const profileError = await guardarPerfilUsuario(
        data.user,
        data.session.access_token,
        profileUsername,
      );
      if (profileError) {
        console.error('No se pudo sincronizar el perfil durante el inicio de sesión:', {
          code: profileError.code,
          message: profileError.message,
        });
        return res.status(502).json({
          error: 'La sesión es válida, pero no se pudo sincronizar el perfil. Inténtalo nuevamente.',
        });
      }
    }

    return res.json({
      user: {
        id: data.user.id,
        username: profileUsername,
      },
      token: data.session.access_token,
    });
  } catch (error) {
    const esTimeout = esErrorTimeout(error);
    console.error('Error iniciando sesión:', error);
    return res.status(esTimeout ? 504 : 503).json({
      error: esTimeout
        ? 'Supabase no respondió al iniciar sesión.'
        : 'No se pudo completar el inicio de sesión. Inténtalo nuevamente.',
    });
  }
});

async function obtenerUsuarioAutenticado(req, res) {
  const authorization = req.get('authorization') || '';
  const tokenMatch = authorization.match(/^Bearer\s+(.+)$/i);
  if (!tokenMatch) {
    res.status(401).json({ error: 'Debes iniciar sesión para modificar tu perfil.' });
    return null;
  }

  try {
    const { data, error } = await supabase.auth.getUser(tokenMatch[1]);
    if (error || !data.user) {
      if (error && esErrorTimeout(error)) {
        responderErrorSupabase(res, 'validar la sesión', error);
      } else {
        res.status(401).json({ error: 'La sesión no es válida o expiró.' });
      }
      return null;
    }

    return { user: data.user, client: createUserClient(tokenMatch[1]) };
  } catch (error) {
    const esTimeout = esErrorTimeout(error);
    console.error('Error validando la sesión de Supabase:', error);
    res.status(esTimeout ? 504 : 503).json({
      error: esTimeout
        ? 'Supabase no respondió al validar la sesión.'
        : 'No se pudo validar la sesión.',
      code: esTimeout ? 'SUPABASE_TIMEOUT' : undefined,
    });
    return null;
  }
}

function validarDatosPerfil(body, parcial = false) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return { error: 'El cuerpo de la solicitud debe ser un objeto JSON.' };
  }

  const campos = Object.keys(body);
  if (campos.some((campo) => !['nombre_usuario', 'avatar_url'].includes(campo))) {
    return { error: 'Solo se permiten los campos nombre_usuario y avatar_url.' };
  }
  if (parcial && campos.length === 0) {
    return { error: 'Indica al menos un campo para actualizar.' };
  }

  const datos = {};
  if ('nombre_usuario' in body || !parcial) {
    if (typeof body.nombre_usuario !== 'string' || !body.nombre_usuario.trim()) {
      return { error: 'nombre_usuario es obligatorio y debe ser texto no vacío.' };
    }
    if (body.nombre_usuario.trim().length > 100) {
      return { error: 'nombre_usuario no puede superar los 100 caracteres.' };
    }
    datos.nombre_usuario = body.nombre_usuario.trim();
  }

  if ('avatar_url' in body || !parcial) {
    if (body.avatar_url !== undefined && body.avatar_url !== null &&
        typeof body.avatar_url !== 'string') {
      return { error: 'avatar_url debe ser texto o null.' };
    }
    if (typeof body.avatar_url === 'string' && body.avatar_url.length > 2048) {
      return { error: 'avatar_url no puede superar los 2048 caracteres.' };
    }
    datos.avatar_url = body.avatar_url ?? null;
  }

  return { datos };
}

app.post('/api/perfiles', async (req, res) => {
  const validacion = validarDatosPerfil(req.body);
  if (validacion.error) {
    return res.status(400).json({ error: validacion.error });
  }

  const sesion = await obtenerUsuarioAutenticado(req, res);
  if (!sesion) return;

  try {
    const { data, error } = await sesion.client
      .from('perfiles')
      .insert({ id: sesion.user.id, ...validacion.datos })
      .select('id, nombre_usuario, avatar_url, fecha_registro')
      .single();

    if (error) return responderErrorSupabase(res, 'crear el perfil', error);
    return res.status(201).json({ data });
  } catch (error) {
    const esTimeout = esErrorTimeout(error);
    console.error('Error creando el perfil:', error);
    return res.status(esTimeout ? 504 : 503).json({
      error: esTimeout ? 'Supabase no respondió a tiempo.' : 'No se pudo crear el perfil.',
      code: esTimeout ? 'SUPABASE_TIMEOUT' : undefined,
    });
  }
});

app.patch('/api/perfiles', async (req, res) => {
  const validacion = validarDatosPerfil(req.body, true);
  if (validacion.error) {
    return res.status(400).json({ error: validacion.error });
  }

  const sesion = await obtenerUsuarioAutenticado(req, res);
  if (!sesion) return;

  try {
    const { data, error } = await sesion.client
      .from('perfiles')
      .update(validacion.datos)
      .eq('id', sesion.user.id)
      .select('id, nombre_usuario, avatar_url, fecha_registro')
      .maybeSingle();

    if (error) return responderErrorSupabase(res, 'actualizar el perfil', error);
    if (!data) {
      return res.status(404).json({ error: 'No existe un perfil para este usuario.' });
    }
    return res.json({ data });
  } catch (error) {
    const esTimeout = esErrorTimeout(error);
    console.error('Error actualizando el perfil:', error);
    return res.status(esTimeout ? 504 : 503).json({
      error: esTimeout ? 'Supabase no respondió a tiempo.' : 'No se pudo actualizar el perfil.',
      code: esTimeout ? 'SUPABASE_TIMEOUT' : undefined,
    });
  }
});

app.get('/api/tablas', (req, res) => {
  const tabla = req.query.tabla;
  if (typeof tabla !== 'string' || !tabla) {
    return res.status(400).json({
      error: 'Indica el nombre de la tabla con el parámetro ?tabla=nombre.',
    });
  }

  return consultarTabla(req, res, tabla);
});

app.get('/api/tablas/:tabla', (req, res) => {
  return consultarTabla(req, res, req.params.tabla);
});

app.get('/health', async (_req, res) => {
  const supabaseUrl =
    (process.env.VITE_SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || '').trim();
  const supabaseKey =
    (
      process.env.VITE_SUPABASE_PUBLISHABLE_KEY ||
      process.env.NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY ||
      ''
    ).trim();

  if (!supabaseUrl || !supabaseKey) {
    return res.status(503).json({
      status: 'error',
      supabase: 'disconnected',
      message: 'Faltan las variables de conexión de Supabase en el archivo .env.',
    });
  }

  try {
    const response = await fetch(`${supabaseUrl.replace(/\/+$/, '')}/auth/v1/settings`, {
      headers: {
        apikey: supabaseKey,
      },
      signal: AbortSignal.timeout(5000),
    });

    if (!response.ok) {
      return res.status(503).json({
        status: 'error',
        supabase: 'disconnected',
        message: `Supabase respondió con HTTP ${response.status}. Verifica la URL, la clave y el acceso al proyecto.`,
      });
    }

    return res.json({ status: 'ok', supabase: 'connected' });
  } catch (error) {
    console.error('No se pudo verificar la conexión con Supabase:', error.message);
    return res.status(503).json({
      status: 'error',
      supabase: 'disconnected',
      message: 'No se pudo alcanzar Supabase. Verifica la conexión a internet, la URL y el estado del proyecto.',
    });
  }
});

app.listen(port, () => {
  console.log(`Backend escuchando en el puerto ${port}`);
});