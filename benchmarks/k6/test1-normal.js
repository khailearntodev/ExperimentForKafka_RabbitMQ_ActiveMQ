import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    normal_workload: {
      executor: 'shared-iterations',
      vus: 50,
      iterations: 1000,
      maxDuration: '1m',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'], // error rate < 1%
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
  };

  const res = http.post(url, payload, params);

  check(res, {
    'status is 202': (r) => r.status === 202,
    'status is PROCESSING': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.status === 'PROCESSING';
      } catch (e) {
        return false;
      }
    },
  });
}
