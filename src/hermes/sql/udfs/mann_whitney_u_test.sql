CREATE FUNCTION `mlab-collaboration`.hermes.mann_whitney_u_test(baseline ARRAY<FLOAT64>, current_rtt ARRAY<FLOAT64>) RETURNS STRUCT<U FLOAT64, Z FLOAT64, p_value FLOAT64, meanU FLOAT64, stdU FLOAT64> LANGUAGE js
AS
r"""
function mannWhitneyUTest(baseline, current_rtt) {
  // Validate inputs
  if (!baseline || !current_rtt || baseline.length < 1 || current_rtt.length < 1) {
    return {
      U: null,
      Z: null,
      p_value: null,
      meanU: null,
      stdU: null
    };
  }

  const n1 = baseline.length;
  const n2 = current_rtt.length;
  const N = n1 + n2;

  // Check if either sample exceeds the size threshold
  if (n1 > 20000 || n2 > 20000) {
    return {
      U: 0.0,
      Z: 0.0,
      p_value: 1e-10, // Small p-value indicating significance
      meanU: 0.0,
      stdU: 0.0
    };
  }

  // Combine samples with group labels
  const combined = [];
  for (let i = 0; i < n1; i++) {
    combined.push({ value: baseline[i], group: 1 });
  }
  for (let i = 0; i < n2; i++) {
    combined.push({ value: current_rtt[i], group: 2 });
  }

  // Sort combined samples
  combined.sort((a, b) => a.value - b.value);

  // Assign ranks, handling ties
  let i = 0;
  const tieGroups = {};
  while (i < combined.length) {
    const tieValue = combined[i].value;
    const tieStart = i;
    let tieSumRanks = 0;
    let tieCount = 0;
    while (i < combined.length && combined[i].value === tieValue) {
      tieSumRanks += i + 1; // Rank starts from 1
      i++;
      tieCount++;
    }
    const tieAverageRank = tieSumRanks / tieCount;
    for (let j = tieStart; j < i; j++) {
      combined[j].rank = tieAverageRank;
    }
    if (tieCount > 1) {
      tieGroups[tieValue] = tieCount;
    }
  }

  // Calculate U statistic
  let R1 = 0;
  for (let k = 0; k < combined.length; k++) {
    if (combined[k].group === 1) {
      R1 += combined[k].rank;
    }
  }
  const U1 = n1 * n2 + (n1 * (n1 + 1)) / 2 - R1;
  const U2 = n1 * n2 - U1;
  const U = Math.min(U1, U2);

  // Calculate mean U
  const meanU = (n1 * n2) / 2;

  // Calculate variance with tie correction
  let tieCorrection = 0;
  for (const key in tieGroups) {
    const t = tieGroups[key];
    tieCorrection += t * (t * t - 1);
  }
  const stdU = Math.sqrt(
    (n1 * n2 / 12) * ((N + 1) - (tieCorrection / (N * (N - 1))))
  );

  // Continuity correction
  const correction = 0.5;

  // Calculate Z-score
  const Z = (U - meanU - correction) / stdU;

  // Calculate p-value
  const p_value = 2 * (1 - normalCdf(Math.abs(Z)));

  return {
    U: U,
    Z: Z,
    p_value: p_value,
    meanU: meanU,
    stdU: stdU
  };
}

// Standard normal cumulative distribution function
function normalCdf(x) {
  return (1 + erf(x / Math.SQRT2)) / 2;
}

// Error function approximation
function erf(x) {
  const a1 = 0.254829592;
  const a2 = -0.284496736;
  const a3 = 1.421413741;
  const a4 = -1.453152027;
  const a5 = 1.061405429;
  const p = 0.3275911;

  const sign = x >= 0 ? 1 : -1;
  x = Math.abs(x);

  const t = 1 / (1 + p * x);
  const y =
    1 -
    (((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t * Math.exp(-x * x));

  return sign * y;
}

return mannWhitneyUTest(baseline, current_rtt);
""";
