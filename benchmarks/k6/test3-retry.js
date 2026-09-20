import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    retry_workload: {
      executor: 'shared-iterations',
      vus: 10,
      iterations: 100,
      maxDuration: '1m',
    },
  },
};

export default function () {
  const baseUrl = __ENV.BASE_URL || 'http://host.docker.internal:3000';
  const url = `${baseUrl}/orders`;
  const payload = JSON.stringify({
    orderId: `ORD-RETRY-${__VU}-${__ITER}-${Date.now()}`,
    userId: `USER-${__VU}`,
    amount: 100000,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);

  check(res, {
    'order accepted with 202': (r) => r.status === 202,
  });
}
