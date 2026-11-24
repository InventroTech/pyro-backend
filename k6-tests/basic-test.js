import http from 'k6/http';
import { check } from 'k6';

/**
 * Multi-API GET load test.
 *
 * The workflow logs in first and passes a fresh ACCESS_TOKEN.
 * Do not print that token.
 *
 * Env:
 *   API_BASE_URL, ACCESS_TOKEN, TENANT_SLUG
 *   RATE / DURATION_SECONDS / PREALLOCATED_VUS
 */

const API_BASE_URL = (__ENV.API_BASE_URL || 'https://pyro-backend-ea2j.onrender.com').replace(/\/$/, '');
const ACCESS_TOKEN = __ENV.ACCESS_TOKEN || '';
const TENANT_SLUG = (__ENV.TENANT_SLUG || 'praja').trim();
const RATE = Number(__ENV.RATE || 20);
const DURATION_SECONDS = Number(__ENV.DURATION_SECONDS || 60);
const PREALLOCATED_VUS = Number(__ENV.PREALLOCATED_VUS || 20);

const ENDPOINTS = [
  '/accounts/users/assignees-by-role/?role=RM',
  '/analytics/get-ticket-status/',
  '/analytics/support-ticket/',
  '/analytics/support-tickets/filter-options/',
  '/crm-records/events/',
  '/crm-records/events/count/',
  '/crm-records/leads/current/',
  '/crm-records/records/?entity_type=lead',
];

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
  thresholds: {
    checks: ['rate>0.99'],
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<5000'],
    dropped_iterations: ['rate<1'],
  },
};

function authHeaders() {
  const headers = { Accept: 'application/json' };
  if (ACCESS_TOKEN) {
    headers.Authorization = `Bearer ${ACCESS_TOKEN}`;
  }
  if (TENANT_SLUG) {
    headers['X-Tenant-Slug'] = TENANT_SLUG;
  }
  return headers;
}

export function setup() {
  if (!ACCESS_TOKEN) {
    throw new Error('ACCESS_TOKEN is empty. Log in before starting k6.');
  }
  console.log(`ACCESS_TOKEN length: ${ACCESS_TOKEN.length} | TENANT_SLUG: ${TENANT_SLUG || '(none)'}`);
  console.log(
    `GET-only load: ${ENDPOINTS.length} endpoints @ ${RATE}/s for ${DURATION_SECONDS}s → ${API_BASE_URL}`,
  );
  ENDPOINTS.forEach((path, i) => console.log(`  [${i}] GET ${path}`));
}

export default function () {
  const path = ENDPOINTS[__ITER % ENDPOINTS.length];
  const url = `${API_BASE_URL}${path}`;
  const res = http.get(url, { headers: authHeaders() });

  check(res, {
    'status is 2xx': (r) => r.status >= 200 && r.status < 300,
  });

  if (res.status >= 400) {
    console.warn(`GET ${path} -> ${res.status}`);
  }
}
