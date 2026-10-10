-- Wasserstein-1 distance with a permutation p-value, for step 02.
--
-- Replaces compute_wasserstein_p_value in the union path (legacy keeps the old one):
-- * The RNG seed is passed in from SQL (a hash of group + day + metric) instead of
--   being derived from JSON.stringify of both whole arrays, a multi-MB string for
--   the largest groups and a likely contributor to "UDF out of memory".
-- * p = (c + 1) / (B + 1). The old count / B allowed p = 0 and, at B = 50, made the
--   real false-positive rate of "p < 0.05" 3/51 = 5.9%. With B = 199 it is 4.5% and
--   the resolution 0.005.
-- Callers cap both samples (see stats_cap() in 02), so memory is bounded.
CREATE TEMP FUNCTION wasserstein_permutation_test(sample_a ARRAY<FLOAT64>, sample_b ARRAY<FLOAT64>, num_permutations INT64, seed INT64) RETURNS STRUCT<distance FLOAT64, p_value FLOAT64> LANGUAGE js
AS
r"""
'use strict';

function wassersteinDistanceCDF(arrA, arrB) {
  if (!arrA.length && !arrB.length) {
    return 0.0;
  } else if (!arrA.length) {
    // Distance = integral of cdf difference = sum of all mass in arrB times distances,
    // but if arrA is truly empty, a typical interpretation is 0 mass in A at all x,
    // meaning you'd effectively be “moving” all points in B from their position to nowhere.
    // A stricter approach is to use the maximum distance from 0. Usually for 1D EMD with an
    // empty distribution, it is somewhat undefined. We'll just return the distance
    // as if we sum up the absolute differences. One approach is to treat it as the average
    // absolute value. But typically you'd want to handle the “empty distribution” case
    // upstream. For safety:
    return arrB.map(Math.abs).reduce((s, x) => s + x, 0) / arrB.length;
  } else if (!arrB.length) {
    // Similarly handle if B is empty:
    return arrA.map(Math.abs).reduce((s, x) => s + x, 0) / arrA.length;
  }

  // Sort both arrays
  const A = arrA.slice().sort((x, y) => x - y);
  const B = arrB.slice().sort((x, y) => x - y);

  const nA = A.length;
  const nB = B.length;
  
  // Indices, cumulative distribution “heights,” and total distance
  let i = 0;
  let j = 0;
  let cdfA = 0.0;
  let cdfB = 0.0;
  let dist = 0.0;

  // Start from the leftmost point; track the previous x to measure intervals.
  // We will step through all unique points in A and B in ascending order.
  let prevX = Math.min(A[0], B[0]);

  while (i < nA || j < nB) {
    // Next candidate points from each distribution, or +∞ if done
    const xA = (i < nA) ? A[i] : Infinity;
    const xB = (j < nB) ? B[j] : Infinity;

    // Move to whichever is smaller (or handle tie)
    const xNext = Math.min(xA, xB);

    // Distance contribution from the interval [prevX, xNext]:
    //  integral over this interval of |CDF_A - CDF_B| dx
    //  is simply |cdfA - cdfB| * (xNext - prevX) because in [prevX, xNext]
    //  the CDFs are constant (no new point triggered).
    dist += Math.abs(cdfA - cdfB) * (xNext - prevX);

    // Now update the CDF(s) that have this point
    if (xA === xNext && i < nA) {
      cdfA += 1.0 / nA;
      i++;
    }
    if (xB === xNext && j < nB) {
      cdfB += 1.0 / nB;
      j++;
    }
    // Advance prevX
    prevX = xNext;
  }

  return dist;
}

/**
 * Shuffles the array in place using Fisher-Yates and a deterministic PRNG.
 */
function shuffleInPlace(array, rng) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    const temp = array[i];
    array[i] = array[j];
    array[j] = temp;
  }
}

/** Mulberry32: a compact deterministic generator with a 32-bit state. */
function seededRng(seed) {
  let state = seed >>> 0;
  return function() {
    state = (state + 0x6D2B79F5) >>> 0;
    let z = state;
    z = Math.imul(z ^ (z >>> 15), z | 1);
    z ^= z + Math.imul(z ^ (z >>> 7), z | 61);
    return ((z ^ (z >>> 14)) >>> 0) / 4294967296;
  };
}

function permutationTest(a, b, numPerms, seed) {
  if (!a || !b || a.length < 1 || b.length < 1) {
    return { distance: 0.0, p_value: 1.0 };
  }
  const observed = wassersteinDistanceCDF(a, b);
  // Sorting first makes the permutation stream independent of input order.
  const combined = a.slice().sort((x, y) => x - y).concat(b.slice().sort((x, y) => x - y));
  const nA = a.length;
  const rng = seededRng(Number(seed));
  let countGe = 0;
  for (let i = 0; i < numPerms; i++) {
    shuffleInPlace(combined, rng);
    if (wassersteinDistanceCDF(combined.slice(0, nA), combined.slice(nA)) >= observed) {
      countGe++;
    }
  }
  return { distance: observed, p_value: (countGe + 1) / (numPerms + 1) };
}

return permutationTest(sample_a, sample_b, Number(num_permutations), seed);
""";
