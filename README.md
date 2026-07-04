<p align="center">
  <img src="docs/hermes-logo.png" alt="HERMES" width="520">
</p>

# HERMES

HERMES is a public, path-aware Internet performance observatory that repurposes user-initiated M-Lab NDT speed tests to detect and localize user-facing performance degradations at Internet scale.

HERMES starts from the user perspective rather than from control-plane events, operator telemetry, or outage reports. It combines NDT latency, throughput, and packet-loss measurements with forward and inferred reverse paths to identify when groups of users in the same network and metro experience a statistically significant degradation and to localize the network entities most likely associated with the event.

The system targets degradations that affect users but may remain invisible to existing public observatories, including persistent congestion, routing detours, degraded interconnections, metro-level disruptions, and reverse-path problems that do not necessarily produce a visible BGP event or complete outage.

## Run HERMES with Docker

The whole pipeline ships as a container — this is the fastest way to run it. Full setup (service-account keys, cache volume, cron) is in **[README-docker.md](README-docker.md)**; the short version:

```bash
# 1. Build the image (from the repo root)
docker build -t hermes-pipeline:latest .

# 2. Provide credentials + tokens
mkdir -p /etc/hermes /data/hermes-cache
cp /path/to/service-account-key.json /etc/hermes/
cat > /etc/hermes/hermes.env <<'EOF'
IPINFO_TOKEN=your-token-here
HOIHO_TOKEN=your-token-here
EOF

# 3. Run (defaults to the last 2 days — ideal for a daily cron)
docker run --rm \
  --env-file /etc/hermes/hermes.env \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
  -v /etc/hermes/service-account-key.json:/app/credentials.json:ro \
  -v /data/hermes-cache:/app/cache \
  hermes-pipeline
```

On a GCE VM with an attached service account, drop the key mount and the `GOOGLE_APPLICATION_CREDENTIALS` line. To process a specific range, append pipeline flags, e.g. `hermes-pipeline --start-date 2026-05-17 --end-date 2026-05-23`. See **[README-docker.md](README-docker.md)** for the persistent cache, first-run behavior, and cron examples.

## What HERMES Is

HERMES is both a research system and an operational measurement pipeline.

- As a performance-monitoring system, it turns noisy, irregular, user-initiated speed tests into statistically supported evidence of user-facing degradation.
- As a path-measurement system, it associates performance observations with forward and reverse path context whenever those measurements are available.
- As a localization system, it applies complementary tomography methods to identify the network entities and links most strongly associated with a degradation.
- As a public observatory, it exposes detected events, affected user groups, supporting measurements, paths, and inferred sources through public datasets and dashboards that researchers and operators can inspect.

HERMES does not claim definitive causal attribution. Instead, it narrows each degradation to a transparent, evidence-backed set of likely responsible entities and reports whether the available evidence supports a localized, ambiguous, or unresolved conclusion.

## Core Ideas

### User Groups

HERMES groups measurements into user groups defined by the source AS, source metro, and destination M-Lab site. Including the destination site ensures that HERMES compares measurements that are likely to traverse similar paths, while grouping users by AS and metro captures operationally meaningful regional degradations without treating problems affecting a single household, device, or connection as wider Internet events.

HERMES only analyzes a user group when it has sufficient measurement density to support robust inference. Its filtering criteria include:

- At least 25 speed tests during the baseline window.
- At least 5 distinct client IP addresses.
- Controls that prevent a single IP address from dominating the group.
- Geographic consistency checks that remove implausibly located measurements.

These requirements reduce the likelihood that HERMES mistakes individual-user artifacts for network-wide degradations.

### Performance Signals

HERMES detects degradations using latency, throughput, and packet loss relative to a recent historical baseline for each user group.

For latency, HERMES identifies statistically significant increases that also exceed a minimum effect-size threshold, preventing small but statistically detectable changes from being classified as meaningful events.

For throughput, HERMES compares the full baseline and event-day distributions using Wasserstein distance and the Mann–Whitney U test. It additionally requires a meaningful decrease in throughput, accounting for the multimodal and variable nature of user-initiated speed-test measurements.

For packet loss, HERMES identifies unusually high loss relative to the user group's baseline. Because packet loss measurements can be sensitive to local wireless conditions and other end-host effects, HERMES interprets them alongside the other performance and path signals rather than treating every isolated loss observation as a network event.

### Bidirectional Path Context

For localization, HERMES constructs path-level context around each affected user group using:

- Forward traceroutes from M-Lab servers toward clients.
- Inferred reverse paths from clients back toward M-Lab servers, when available.
- IP-to-AS, IP-to-metro, organization, and IXP metadata.

This bidirectional view is central to HERMES. Forward and reverse Internet paths are frequently asymmetric, and many user-facing degradations involve entities that appear only on one direction of the path. HERMES therefore analyzes the two directions separately rather than assuming that one direction represents the other.

### Tomography-Based Localization

Once HERMES detects an anomalous user group, it uses two complementary tomography methods to identify likely sources.

Temporal tomography compares paths observed during the event with paths observed during the baseline period. It identifies entities whose path involvement changes during the event, making it particularly useful for degradations involving rerouting, link disappearance, or avoidance of previously used infrastructure.

Correlation tomography compares affected and unaffected user groups and identifies entities whose presence is strongly associated with degraded performance. It is particularly useful when the responsible entity remains on the path throughout both the baseline and event periods, as can occur during congestion or other performance degradations that do not trigger a routing change.

HERMES combines candidates identified across path directions and network granularities, including ASes, metros, IXPs, and AS–metro links. Based on the strength and consistency of the available evidence, it classifies each event as localized, ambiguous, or unresolved.
