// Every reader converging on the same posts.
//
// This is the scenario the three-tier cache and singleflight exist for. A post
// that suddenly becomes popular sits in a great many timelines at once, so
// every read wants the same rows. Without a cache the number of identical
// downstream fetches is the number of concurrent readers; without singleflight
// it is the number who miss in the same instant, which is worst exactly when
// the post is hottest.
//
//   make loadtest SCENARIO=viral
//
// Every virtual user reads the *same* viewer's timeline, so all of them
// hydrate the same fifty post IDs. That is what makes the measurement legible:
//
//   murmur_postcache_lookups_total{tier="source"}
//
// on the timeline service should stay flat while VUs climb from 200 to 1000.
// Latency is the symptom; that counter is the mechanism, and the make target
// prints it before and after.
import http from 'k6/http';
import { check } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const GATEWAY = __ENV.GATEWAY || 'http://localhost:8080';
const TOKEN = __ENV.TOKEN || '';
// PEAK is how many concurrent readers to converge on the same timeline.
//
// The cache claim holds at any value — the source-tier counter stays at the
// number of distinct posts regardless. What changes with PEAK is whether the
// machine under the test can still answer within its deadlines, which is a
// capacity question rather than a caching one, so it is a knob rather than a
// constant.
const PEAK = Number(__ENV.PEAK || 400);

const readDuration = new Trend('viral_read_duration', true);
const readErrors = new Rate('viral_read_errors');

export const options = {
  scenarios: {
    stampede: {
      executor: 'ramping-vus',
      startVUs: 0,
      // A steep ramp on purpose. A gentle one lets the cache warm ahead of the
      // load and measures nothing; the interesting moment is the instant cold
      // posts become popular.
      stages: [
        { duration: '5s', target: 50 },
        { duration: '10s', target: PEAK },
        { duration: '20s', target: PEAK },
        { duration: '5s', target: 0 },
      ],
      exec: 'readHotTimeline',
    },
  },
  thresholds: {
    // Budgets for a single laptop running the whole stack plus k6. They are
    // about this machine's capacity, not about the cache — the cache is
    // measured by the source-tier counter the make target prints, which is
    // unaffected by how hard the machine is pushed.
    viral_read_duration: ['p(95)<2000'],
    viral_read_errors: ['rate<0.01'],
  },
};

const TIMELINE_QUERY = `query ViralTimeline {
  timeline(first: 50) {
    edges { node { id body createdAt author { id handle displayName followerCount } } }
  }
}`;

export function readHotTimeline() {
  const res = http.post(
    `${GATEWAY}/query`,
    JSON.stringify({ operationName: 'ViralTimeline', query: TIMELINE_QUERY }),
    { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${TOKEN}` } },
  );

  const ok = check(res, {
    'status is 200': (r) => r.status === 200,
    'timeline returned': (r) => {
      try {
        const body = JSON.parse(r.body);
        return !body.errors && body.data && body.data.timeline;
      } catch {
        return false;
      }
    },
  });

  readDuration.add(res.timings.duration);
  readErrors.add(!ok);
}
