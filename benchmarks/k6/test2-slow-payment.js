import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    slow_payment_workload: {
      executor: 'shared-iterations',
      vus: 50,
      iterations: 1000,
      maxDuration: '2m',
    },
  },
};

export default function () {
  const baseUrl = __ENV.BASE_URL || 'http://host.docker.internal:3000';
  const url = `${baseUrl}/orders`;
  const payload = JSON.stringify({
    orderId: `ORD-${__VU}-${__ITER}-${Date.now()}`,
    userId: `USER-${__VU}`,
    amount: 100000,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
    timeout: '10s', // 10s timeout to capture blocking failures in sync baseline
  };

  const res = http.post(url, payload, params);

  check(res, {
    'request succeeded (200 or 202)': (r) => r.status === 200 || r.status === 202,
  });
}
