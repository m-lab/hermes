CREATE FUNCTION `mlab-collaboration`.hermes.welchs_t_test(baseline ARRAY<FLOAT64>, current_rtt ARRAY<FLOAT64>) RETURNS STRUCT<t_stat FLOAT64, degrees_of_freedom FLOAT64, p_value FLOAT64, mean1 FLOAT64, mean2 FLOAT64, se1 FLOAT64, se2 FLOAT64> LANGUAGE js
AS
r"""

function welchsTTest(baseline, current_rtt) {
 if (!baseline || !current_rtt || baseline.length < 2 || current_rtt.length < 2) {
   return {
     t_stat: null,
     degrees_of_freedom: null,
     p_value: null,
     mean1: null,
     mean2: null,
     se1: null,
     se2: null
   };
 }


 if (baseline.length > 20000 || current_rtt.length > 20000) {
   return {
     t_stat: 0.0,
     degrees_of_freedom: 0.0,
     p_value: 1e-10,
     mean1: 0.0,
     mean2: 0.0,
     se1: 0.0,
     se2: 0.0
   };
 }


 const n1 = baseline.length;
 const n2 = current_rtt.length;


 const mean1 = baseline.reduce((a, b) => a + b, 0) / n1;
 const mean2 = current_rtt.reduce((a, b) => a + b, 0) / n2;


 const var1 = baseline.reduce((a, b) => a + Math.pow(b - mean1, 2), 0) / (n1 - 1);
 const var2 = current_rtt.reduce((a, b) => a + Math.pow(b - mean2, 2), 0) / (n2 - 1);


 const se1 = var1 / n1;
 const se2 = var2 / n2;


 const t_stat = (mean1 - mean2) / Math.sqrt(se1 + se2);


 const numerator = Math.pow(se1 + se2, 2);
 const denominator = (Math.pow(se1, 2) / (n1 - 1)) + (Math.pow(se2, 2) / (n2 - 1));
 const df = numerator / denominator;


 const p_value = 2 * (1 - studentTCDF(Math.abs(t_stat), df));


 return {
   t_stat: t_stat,
   degrees_of_freedom: df,
   p_value: p_value,
   mean1: mean1,
   mean2: mean2,
   se1: Math.sqrt(se1),
   se2: Math.sqrt(se2)
 };


 // Corrected implementation of the cumulative distribution function for the t-distribution
 function studentTCDF(t, df) {
   const x = df / (df + t * t);
   const a = df / 2;
   const b = 0.5;
   const ibeta = regularizedIncompleteBeta(x, a, b);
   return 1 - 0.5 * ibeta;
 }


 function regularizedIncompleteBeta(x, a, b) {
   const lnB = lnBeta(a, b);
   const bt = Math.exp(a * Math.log(x) + b * Math.log(1 - x) - lnB);
   if (x < (a + 1) / (a + b + 2)) {
     return bt * betacf(x, a, b) / a;
   } else {
     return 1 - bt * betacf(1 - x, b, a) / b;
   }
 }


 function lnGamma(z) {
   const g = 7;
   const p = [
     0.99999999999980993,
     676.5203681218851,
     -1259.1392167224028,
     771.32342877765313,
     -176.61502916214059,
     12.507343278686905,
     -0.13857109526572012,
     9.9843695780195716e-6,
     1.5056327351493116e-7
   ];
   if (z < 0.5) {
     return Math.log(Math.PI) - Math.log(Math.sin(Math.PI * z)) - lnGamma(1 - z);
   } else {
     z -= 1;
     let x = p[0];
     for (let i = 1; i < g + 2; i++) {
       x += p[i] / (z + i);
     }
     const t = z + g + 0.5;
     return 0.5 * Math.log(2 * Math.PI) + (z + 0.5) * Math.log(t) - t + Math.log(x);
   }
 }


 function lnBeta(a, b) {
   return lnGamma(a) + lnGamma(b) - lnGamma(a + b);
 }


 function betacf(x, a, b) {
   const MAX_ITER = 100;
   const EPS = 1e-12;
   let m = 1;
   let aa, del;
   let qab = a + b;
   let qap = a + 1;
   let qam = a - 1;
   let c = 1;
   let d = 1 - qab * x / qap;
   if (Math.abs(d) < EPS) d = EPS;
   d = 1 / d;
   let h = d;


   for (; m <= MAX_ITER; m++) {
     let m2 = 2 * m;


     // Even iteration
     aa = m * (b - m) * x / ((qam + m2) * (a + m2 - 1));
     d = 1 + aa * d;
     if (Math.abs(d) < EPS) d = EPS;
     c = 1 + aa / c;
     if (Math.abs(c) < EPS) c = EPS;
     d = 1 / d;
     h *= d * c;


     // Odd iteration
     aa = -(a + m - 1) * (qab + m - 1) * x / ((a + m2 - 1) * (qap + m2 - 1));
     d = 1 + aa * d;
     if (Math.abs(d) < EPS) d = EPS;
     c = 1 + aa / c;
     if (Math.abs(c) < EPS) c = EPS;
     d = 1 / d;
     del = d * c;
     h *= del;


     if (Math.abs(del - 1.0) < EPS) break;
   }


   return h;
 }
}
return welchsTTest(baseline, current_rtt);
""";
