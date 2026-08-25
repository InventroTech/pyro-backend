import http from 'k6/http';
import { check } from 'k6';

/**
 * Multi-API load test: hot GETs (JWT) + optional entity writes (X-Secret-Pyro).
 *
 * JWT GETs:
 *   ACCESS_TOKEN, TENANT_SLUG
 *
 * Entity writes (Praja-style):
 *   INCLUDE_ENTITY_WRITES=true
 *   PYRO_SECRET          — X-Secret-Pyro header (NOT the user JWT)
 *   ENTITY_PRAJA_ID      — for PUT /entity/
 *   ENTITY_TICKET_ID     — for PATCH /entity/support_ticket/ (records.id)
 */

const API_BASE_URL = (__ENV.API_BASE_URL || 'https://pyro-backend-ea2j.onrender.com').replace(/\/$/, '');
const ACCESS_TOKEN = __ENV.ACCESS_TOKEN || '';
const TENANT_SLUG = (__ENV.TENANT_SLUG || 'praja').trim();
const RATE = Number(__ENV.RATE || 20);
const DURATION_SECONDS = Number(__ENV.DURATION_SECONDS || 60);
const PREALLOCATED_VUS = Number(__ENV.PREALLOCATED_VUS || 20);

const INCLUDE_ENTITY_WRITES = (__ENV.INCLUDE_ENTITY_WRITES || '').toLowerCase() === 'true';
const PYRO_SECRET = (__ENV.PYRO_SECRET || '').trim();
const ENTITY_PRAJA_ID = (__ENV.ENTITY_PRAJA_ID || '').trim();
const ENTITY_TICKET_ID = (__ENV.ENTITY_TICKET_ID || '').trim();

const ENTITY_PUT_BODY =
  __ENV.ENTITY_PUT_BODY ||
  JSON.stringify({
    praja_id: ENTITY_PRAJA_ID,
    data: {
      k6_loadtest: true,
      latest_remarks: 'k6 load-test PUT /entity/',
    },
  });

const ENTITY_PATCH_TICKET_BODY =
  __ENV.ENTITY_PATCH_TICKET_BODY ||
  JSON.stringify({
    ticket_id: ENTITY_TICKET_ID ? Number(ENTITY_TICKET_ID) : undefined,
    data: {
      cse_remarks: 'k6 load-test PATCH /entity/support_ticket/',
    },
  });

const ENDPOINTS = [
  { method: 'GET', path: '/accounts/users/assignees-by-role/?role=RM', auth: 'jwt' },
  { method: 'GET', path: '/analytics/get-ticket-status/', auth: 'jwt' },
  { method: 'GET', path: '/analytics/support-ticket/', auth: 'jwt' },
  { method: 'GET', path: '/analytics/support-tickets/filter-options/', auth: 'jwt' },
  { method: 'GET', path: '/crm-records/events/count/', auth: 'jwt' },
  { method: 'GET', path: '/crm-records/leads/current/', auth: 'jwt' },
  { method: 'GET', path: '/crm-records/records/?entity_type=lead', auth: 'jwt' },
  { method: 'GET', path: '/crm-records/records/events/', auth: 'jwt' },
];

if (INCLUDE_ENTITY_WRITES && PYRO_SECRET && ENTITY_PRAJA_ID) {
  ENDPOINTS.push({
    method: 'PUT',
    path: '/entity/',
    auth: 'secret',
    body: ENTITY_PUT_BODY,
  });
}

if (INCLUDE_ENTITY_WRITES && PYRO_SECRET && ENTITY_TICKET_ID) {
  ENDPOINTS.push({
    method: 'PATCH',
    path: '/entity/support_ticket/',
    auth: 'secret',
    body: ENTITY_PATCH_TICKET_BODY,
  });
}

export const options = {
  scenarios: {
    traffic: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: `${DURATION_SECONDS}s`,
      preAllocatedVUs: PREALLOCATED_VUS,
    },
  },
};

function headersFor(endpoint) {
  const headers = { Accept: 'application/json' };
  if (endpoint.body != null) {
    headers['Content-Type'] = 'application/json';
  }
  if (endpoint.auth === 'secret') {
    headers['X-Secret-Pyro'] = PYRO_SECRET;
  } else {
    if (ACCESS_TOKEN) {
      headers.Authorization = `Bearer ${ACCESS_TOKEN}`;
    }
    if (TENANT_SLUG) {
      headers['X-Tenant-Slug'] = TENANT_SLUG;
    }
  }
  return headers;
}

export function setup() {
  console.log(`API: ${API_BASE_URL} | tenant: ${TENANT_SLUG}`);
  console.log(`ACCESS_TOKEN length: ${ACCESS_TOKEN.length || 0}`);
  if (INCLUDE_ENTITY_WRITES) {
    console.log(
      `Entity writes ON | secret set: ${Boolean(PYRO_SECRET)} | praja_id: ${ENTITY_PRAJA_ID || '(missing)'} | ticket_id: ${ENTITY_TICKET_ID || '(missing)'}`,
    );
    if (!PYRO_SECRET) {
      console.warn('INCLUDE_ENTITY_WRITES=true but PYRO_SECRET empty — entity PUT/PATCH skipped.');
    }
  } else {
    console.log('Entity writes OFF (set INCLUDE_ENTITY_WRITES=true to enable PUT /entity/ + PATCH /entity/support_ticket/)');
  }
  console.log(`${ENDPOINTS.length} endpoints @ ${RATE}/s for ${DURATION_SECONDS}s`);
  ENDPOINTS.forEach((e, i) => console.log(`  [${i}] ${e.method} ${e.path} (${e.auth})`));
}

export default function () {
  const endpoint = ENDPOINTS[__ITER % ENDPOINTS.length];
  const url = `${API_BASE_URL}${endpoint.path}`;
  const res = http.request(endpoint.method, url, endpoint.body || null, {
    headers: headersFor(endpoint),
  });

  check(res, {
    'status is 2xx': (r) => r.status >= 200 && r.status < 300,
  });

  if (res.status >= 400) {
    console.warn(`${endpoint.method} ${endpoint.path} -> ${res.status}`);
  }
}
