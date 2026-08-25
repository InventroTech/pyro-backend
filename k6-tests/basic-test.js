import http from 'k6/http';

const BASE_URL = __ENV.BASE_URL || 'https://pyro-dev.vercel.app/auth';
const RATE = Number(__ENV.RATE || 42);
const DURATION_SECONDS = Number(__ENV.DURATION_SECONDS || 300);
const PREALLOCATED_VUS = Number(__ENV.PREALLOCATED_VUS || 20);
const HTTP_METHOD = (__ENV.HTTP_METHOD || 'GET').toUpperCase();
const ACCESS_TOKEN = __ENV.ACCESS_TOKEN || '';
const REQUEST_BODY = __ENV.REQUEST_BODY || '{}';

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

function buildHeaders() {
  const headers = {};
  if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(HTTP_METHOD)) {
    headers['Content-Type'] = 'application/json';
  }
  if (ACCESS_TOKEN) {
    headers.Authorization = `Bearer ${ACCESS_TOKEN}`;
  }
  return headers;
}

function sendRequest() {
  const params = { headers: buildHeaders() };
  const body = ['POST', 'PUT', 'PATCH'].includes(HTTP_METHOD) ? REQUEST_BODY : null;

  switch (HTTP_METHOD) {
    case 'GET':
      return http.get(BASE_URL, params);
    case 'POST':
      return http.post(BASE_URL, body, params);
    case 'PUT':
      return http.put(BASE_URL, body, params);
    case 'PATCH':
      return http.patch(BASE_URL, body, params);
    case 'DELETE':
      return http.del(BASE_URL, null, params);
    default:
      throw new Error(`Unsupported HTTP method: ${HTTP_METHOD}`);
  }
}

export function setup() {
  console.log(
    `Starting load test: ${HTTP_METHOD} ${BASE_URL} @ ${RATE}/s for ${DURATION_SECONDS}s` +
      (ACCESS_TOKEN ? ' (authenticated)' : ' (no token)'),
  );
}

export default function () {
  const response = sendRequest();
  if (response.status >= 400) {
    console.warn(`${HTTP_METHOD} ${BASE_URL} -> ${response.status}`);
  }
}
