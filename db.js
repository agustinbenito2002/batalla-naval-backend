const path = require('node:path');
const dotenv = require('dotenv');
const { createClient } = require('@supabase/supabase-js');

const { error } = dotenv.config({ path: path.join(__dirname, '.env') });
if (error) {
  throw error;
}

const supabaseUrl = (
  process.env.VITE_SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || ''
).trim();
const supabaseKey = (
  process.env.VITE_SUPABASE_PUBLISHABLE_KEY ||
  process.env.NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY ||
  ''
).trim();

const supabaseTimeoutMs = 25000;

if (!supabaseUrl || !supabaseKey) {
  throw new Error(
    'Configura VITE_SUPABASE_URL y VITE_SUPABASE_PUBLISHABLE_KEY en el archivo .env.'
  );
}

const fetchWithTimeout = (input, options = {}) => {
  const timeoutSignal = AbortSignal.timeout(supabaseTimeoutMs);
  const signal = options.signal
    ? AbortSignal.any([options.signal, timeoutSignal])
    : timeoutSignal;

  return fetch(input, { ...options, signal });
};

const supabase = createClient(supabaseUrl, supabaseKey, {
  global: { fetch: fetchWithTimeout },
});

module.exports = supabase;