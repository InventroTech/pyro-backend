import http from 'k6/http';

const BASE_URL = 'https://pyro-dev.vercel.app/auth';

let requestCount = 0;

export const options = {
  scenarios: {
    traffic: {
      executor: 'constant-arrival-rate',
      rate: 42,
      timeUnit: '1s',
      duration: '300s',
      preAllocatedVUs: 20,
    },
  },
};

export default function () {
  const response = http.get(BASE_URL);

  requestCount++;

  console.log(
    `Request sent: ${requestCount} | Status: ${response.status}`
  );
}