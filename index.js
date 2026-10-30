const path = require('node:path');
const express = require('express');
const dotenv = require('dotenv');

const { error: envError } = dotenv.config({ path: path.join(__dirname, '.env') });
if (envError) {
  throw envError;
}

const supabase = require('./db');

const app = express();
const port = Number(process.env.PORT) || 3000;

app.use(express.json());
app.locals.supabase = supabase;

app.get('/', (_req, res) => {
  res.json({ status: 'ok', message: 'Backend en funcionamiento. Consulta /health para verificar Supabase.' });
});

async function consultarTabla(req, res, tabla) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(tabla)) {
    return res.status(400).json({ error: 'Nombre de tabla no válido.' });
  }

  const limite = Number(req.query.limite ?? 50);
  if (!Number.isInteger(limite) || limite < 1 || limite > 100) {
    return res.status(400).json({ error: 'El límite debe ser un entero entre 1 y 100.' });
  }

  try {
    const { data, error } = await supabase.from(tabla).select('*').limit(limite);

    if (error) {
      console.error(`Error de Supabase al consultar "${tabla}":`, {
        code: error.code,
        message: error.message,
        details: error.details,
        hint: error.hint,
      });

      const esTimeout = error.name === 'TimeoutError' ||
        error.name === 'AbortError' ||
        /^(TimeoutError|AbortError):/.test(error.message || '');
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
    const esTimeout = error.name === 'TimeoutError' || error.name === 'AbortError';
    return res.status(esTimeout ? 504 : 503).json({
      error: esTimeout
        ? 'Supabase no respondió dentro del tiempo límite.'
        : 'No se pudo completar la consulta a Supabase.',
      code: esTimeout ? 'SUPABASE_TIMEOUT' : undefined,
      details: error.message,
    });
  }
}

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
    const response = await fetch(`${supabaseUrl.replace(/\/+$/, '')}/rest/v1/`, {
      headers: {
        apikey: supabaseKey,
        Authorization: `Bearer ${supabaseKey}`,
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