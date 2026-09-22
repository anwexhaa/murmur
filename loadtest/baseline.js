// Steady mixed read/write load against the gateway.
//
// This is the control the viral scenario is compared against: ordinary readers
// reading ordinary timelines, with a trickle of writes so the fanout worker and
// the cache invalidation path are not idle while the reads are measured.
//
//   make loadtest SCENARIO=baseline
//
// VIEWERS is a comma-separated list of user IDs, supplied by the make target
// from the seeded graph. Auth is the X-Murmur-User header until phase 7.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const GATEWAY = __ENV.GATEWAY || 'http://localhost:8080';
const VIEWERS = (__ENV.VIEWERS || '').split(',').filter(Boolean);

const timelineDuration = new Trend('timeline_duration', true);
const timelineErrors = new Rate('timeline_errors');

export const options = {
  scenarios: {
    readers: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '15s', target: 50 },
        { duration: '30s', target: 50 },
        { duration: '10s', target: 0 },
      ],
      exec: 'readTimeline',
    },
    writers: {
      executor: 'constant-arrival-rate',
      rate: 5,
      timeUnit: '1s',
      duration: '55s',
      preAllocatedVUs: 5,
      exec: 'writePost',
    },
  },
  thresholds: {
    // A budget rather than a hope. If a change makes the read path slower than
    // this, the load test fails rather than quietly recording it.
    timeline_duration: ['p(95)<250'],
    timeline_errors: ['rate<0.01'],
  },
};

const TIMELINE_QUERY = `query HomeTimeline {
  timeline(first: 50) {
    edges { node { id body createdAt author { id handle displayName followerCount viewerFollows } } }
    pageInfo { hasNextPage endCursor }
  }
}`;

function viewer() {
  return VIEWERS[Math.floor(Math.random() * VIEWERS.length)];
}

function post(query, user) {
  return http.post(
    `${GATEWAY}/query`,
    JSON.stringify({ operationName: 'HomeTimeline', query }),
    { headers: { 'Content-Type': 'application/json', 'X-Murmur-User': user } },
  );
}

export function readTimeline() {
  const res = post(TIMELINE_QUERY, viewer());

  const ok = check(res, {
    'status is 200': (r) => r.status === 200,
    'no graphql errors': (r) => {
      try {
        return !JSON.parse(r.body).errors;
      } catch {
        return false;
      }
    },
  });

  timelineDuration.add(res.timings.duration);
  timelineErrors.add(!ok);
  sleep(0.2);
}

export function writePost() {
  const user = viewer();
  http.post(
    `${GATEWAY}/query`,
    JSON.stringify({
      operationName: 'Write',
      query: 'mutation Write($body: String!) { createPost(body: $body) { id } }',
      variables: { body: `load test post ${Date.now()}` },
    }),
    { headers: { 'Content-Type': 'application/json', 'X-Murmur-User': user } },
  );
}
