import http from 'k6/http';
import { check } from 'k6';

/**
 * Read-only load test of the screens that scan the database.
 *
 * The workflow logs in once and passes ACCESS_TOKEN. This script never
 * prints that token, and it never calls a route that writes, assigns a
 * lead or ticket, or talks to OpenAI, Zoho, or a courier.
 *
 * Weights are shares of the traffic. Lead and ticket lists get more of it
 * because that is what the app does, and that is what loads Postgres.
 *
 * Env:
 *   API_BASE_URL, ACCESS_TOKEN, TENANT_SLUG
 *   RATE / DURATION_SECONDS / PREALLOCATED_VUS
 */

const API_BASE_URL = (__ENV.API_BASE_URL || 'https://pyro-backend-ea2j.onrender.com').replace(/\/$/, '');
const ACCESS_TOKEN = __ENV.ACCESS_TOKEN || '';
const TENANT_SLUG = (__ENV.TENANT_SLUG || 'praja').trim();
// Production HTTP API (pyro-prod-backend) on 24 Nov 2026, from Render:
// peak 89 requests/s across 3 instances, so one instance sees about 30/s.
// Preprod is a single instance, so the default matches that one instance.
const RATE = Number(__ENV.RATE || 30);
const DURATION_SECONDS = Number(__ENV.DURATION_SECONDS || 60);
const PREALLOCATED_VUS = Number(__ENV.PREALLOCATED_VUS || 40);

function istDay(daysAgo) {
  const shifted = new Date(Date.now() + 5.5 * 60 * 60 * 1000 - daysAgo * 24 * 60 * 60 * 1000);
  const year = shifted.getUTCFullYear();
  const month = String(shifted.getUTCMonth() + 1).padStart(2, '0');
  const day = String(shifted.getUTCDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

const TODAY = istDay(0);
const WEEK_AGO = istDay(6);

const ENDPOINTS = [
  { name: 'lead_list', weight: 18, path: '/crm-records/records/?entity_type=lead' },
  { name: 'lead_stats', weight: 6, path: '/crm-records/leads/stats/' },
  { name: 'current_lead', weight: 8, path: '/crm-records/leads/current/' },
  { name: 'events', weight: 6, path: '/crm-records/events/' },
  { name: 'events_count', weight: 4, path: '/crm-records/events/count/' },
  { name: 'ticket_list', weight: 12, path: '/analytics/support-ticket/' },
  { name: 'ticket_status', weight: 6, path: '/analytics/get-ticket-status/' },
  { name: 'ticket_filters', weight: 3, path: '/analytics/support-tickets/filter-options/' },
  { name: 'wip_tickets', weight: 4, path: '/support-ticket/get-wip-tickets/' },
  { name: 'not_connected_tickets', weight: 3, path: '/support-ticket/get-not-connected-tickets/' },
  { name: 'daily_progress', weight: 3, path: '/support-ticket/daily-progress/' },
  { name: 'team_overview', weight: 5, path: `/analytics/team/overview/?date=${TODAY}` },
  { name: 'team_members', weight: 3, path: `/analytics/team/members/?date=${TODAY}` },
  { name: 'team_events', weight: 3, path: `/analytics/team/events/?date=${TODAY}` },
  { name: 'team_timeseries', weight: 3, path: `/analytics/team/time-series/?from=${WEEK_AGO}&to=${TODAY}` },
  { name: 'unassigned_leads', weight: 4, path: '/analytics/team/unassigned-leads/' },
  { name: 'cse_overview', weight: 4, path: `/analytics/cse/overview/?date=${TODAY}` },
  { name: 'cse_members', weight: 2, path: `/analytics/cse/members/?date=${TODAY}` },
  { name: 'cse_filters', weight: 2, path: '/analytics/cse/filter-options/' },
  { name: 'rm_sessions', weight: 2, path: '/analytics/rm-active-sessions/' },
  { name: 'rm_targets', weight: 2, path: `/analytics/rm-daily-targets/?from=${WEEK_AGO}&to=${TODAY}` },
  { name: 'rm_filters', weight: 1, path: '/analytics/rm-filter-options/' },
  { name: 'analytics_board', weight: 2, path: '/analytics/board/' },
  { name: 'assignees_rm', weight: 2, path: '/accounts/users/assignees-by-role/?role=RM' },
  { name: 'lead_filter_options', weight: 2, path: '/user-settings/lead-filter-options/' },
  { name: 'groups', weight: 2, path: '/user-settings/groups/' },
  { name: 'me', weight: 2, path: '/auth/me/' },
  { name: 'my_role', weight: 2, path: '/membership/me/role/' },
];

const DECK = [];
ENDPOINTS.forEach((endpoint) => {
  for (let i = 0; i < endpoint.weight; i += 1) {
    DECK.push(endpoint);
  }
});

export const options = {
  scenarios: {
    app_reads: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: `${DURATION_SECONDS}s`,
      preAllocatedVUs: PREALLOCATED_VUS,
      // Extra workers so a few slow queries do not drop the rest of the traffic.
      maxVUs: Math.max(PREALLOCATED_VUS * 2, 50),
    },
  },
  thresholds: {
    checks: ['rate>0.99'],
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<5000'],
    'http_req_duration{name:lead_list}': ['p(95)<5000'],
    'http_req_duration{name:ticket_list}': ['p(95)<5000'],
    'http_req_duration{name:unassigned_leads}': ['p(95)<5000'],
    dropped_iterations: ['rate<1'],
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
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
  console.log(
    `Read-only load: ${ENDPOINTS.length} endpoints @ ${RATE}/s for ${DURATION_SECONDS}s → ${API_BASE_URL}`,
  );
  console.log(`Dates ${WEEK_AGO} .. ${TODAY} | token length ${ACCESS_TOKEN.length} | tenant ${TENANT_SLUG}`);
  ENDPOINTS.forEach((endpoint) => {
    console.log(`  ${endpoint.weight}\t${endpoint.name}\tGET ${endpoint.path}`);
  });
}

export default function () {
  const endpoint = DECK[__ITER % DECK.length];
  const res = http.get(`${API_BASE_URL}${endpoint.path}`, {
    headers: authHeaders(),
    tags: { name: endpoint.name },
    timeout: '30s',
    responseType: 'none',
  });

  check(res, {
    'status is 2xx': (r) => r.status >= 200 && r.status < 300,
  });

  if (res.status < 200 || res.status >= 300) {
    console.warn(`${endpoint.name} -> ${res.status || 'timeout'}`);
  }
}
