CREATE TEMP FUNCTION compute_wasserstein_p_value(weekly_data ARRAY<FLOAT64>, daily_data ARRAY<FLOAT64>, num_permutations INT64) RETURNS STRUCT<distance FLOAT64, p_value FLOAT64> LANGUAGE js
AS
r"""
'use strict';

/**
 * Computes the 1D Wasserstein distance (Earth Mover's Distance) 
 * using the 'partial sums' (CDF difference) approach for discrete samples.
 *
 * Interpretation: each array a/b is viewed as an empirical distribution:
 *    - Sort the data.
 *    - Each point has mass = 1/length.
 *    - We sweep from left to right over the union of sample points,
 *      and integrate the absolute difference of CDFs.
 */
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

/**
 * FNV-1a hashes canonical input data to a stable unsigned 32-bit seed.
 */
function fnv1a(text) {
  let hash = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
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

/**
 * Main function that computes:
 *  1) Observed Wasserstein distance
 *  2) A null distribution via permutations
 *  3) P-value = fraction of null distances >= observed
 */
function computeWassersteinPValue(weeklyData, dailyData, numPerms) {
  // Handle trivial cases
  if (!weeklyData || !dailyData || weeklyData.length < 1 || dailyData.length < 1) {
    return { distance: 0.0, p_value: 1.0 };
  }

  // 1) Observed Wasserstein distance using the partial sums approach
  const observedDistance = wassersteinDistanceCDF(weeklyData, dailyData);

  // 2) Build combined array and do permutations
  // Canonicalize before seeding and shuffling. BigQuery ARRAY_AGG order is not
  // guaranteed, so equal empirical distributions must seed equal permutations.
  const canonicalWeekly = weeklyData.slice().sort((a, b) => a - b);
  const canonicalDaily = dailyData.slice().sort((a, b) => a - b);
  const combined = canonicalWeekly.concat(canonicalDaily);
  const nWeekly = weeklyData.length;
  const seedMaterial = JSON.stringify([canonicalWeekly, canonicalDaily, numPerms]);
  const rng = seededRng(fnv1a(seedMaterial));

  let countGeObserved = 0;
  for (let i = 0; i < numPerms; i++) {
    // Shuffle in place
    shuffleInPlace(combined, rng);
    const pseudoWeekly = combined.slice(0, nWeekly);
    const pseudoDaily = combined.slice(nWeekly);
    
    const dist = wassersteinDistanceCDF(pseudoWeekly, pseudoDaily);
    if (dist >= observedDistance) {
      countGeObserved++;
    }
  }

  // 3) P-value
  const pValue = (numPerms > 0)
    ? countGeObserved / numPerms
    : 1.0;

  return {
    distance: observedDistance,
    p_value: pValue
  };
}

// Invoke our main function
return computeWassersteinPValue(weekly_data, daily_data, num_permutations);
""";
