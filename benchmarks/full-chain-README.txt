Current full-chain demonstration, 2026-10-17
=========================================

Scope: the approved OFFLINE A2A + ACP scenario. One current integration test
executes every stage against disposable storage. No production data, vendor
credentials, paid model calls, downloads, host security changes or UI enablement.

Executable: tests/test_full_chain_demo.py
Real-encoder evidence: full-chain-real-encoder-2026-10-17.json
Validation matrix: full-chain-validation-2026-10-17.json
Final regressions: Windows 118 passed; Linux 81 passed. Ruff and scoped Git
whitespace checks passed. These platform regressions ran concurrently, not as
isolated timing measurements. The passing Windows suite printed a loopback
fixture request-handler exception header; its exit code was zero.

What the single scenario proves
------------------------------
1. Capture: instrument a real requirements.txt write with tool.started,
   file.changed and tool.completed events; assert canonical run tool state.
   Publish the observed bytes as a board finding and a run-linked artifact.
2. Memory: create explicit project memory pinned to immutable board/artifact
   revisions, then verify it through the operator API. This is an explicit
   fixture assertion, not autonomous extraction or independent factual review.
3. Retrieval: real cached pinned Gemma finds that memory using a differently
   worded semantic query. A bounded context packet contains its resource ID.
   Drain actual vector/graph jobs and assert graph provenance reaches the
   source board post and artifact. The default no-model test uses lexical search.
4. Assignment: production task-plan scheduling assigns separate scoped,
   capability-matched build/review tasks to distinct agents and runs. The review
   worker first remains idle because its build dependency is not ready.
5. Execution: AgentWorker calls production A2ARuntime over real loopback HTTP
   and production ACPRuntime over a real child-process stdio session. The local
   peers assert the memory is in each received, budget-bounded context packet.
   A2A exclusively creates answer.py; ACP reads it and exclusively creates
   review.txt. Both canonical tasks complete once under the intended runs.
6. Verification: the producer's completion remains self-reported. A separate
   reviewer identity cannot verify it before a trusted check. An allowlisted,
   real Python subprocess checks BOTH output files, records their hashes and
   permits the distinct fixture reviewer's verification decision.
7. Recovery: deliberately drop a task-create response AFTER canonical commit;
   close its durable worker journal and the application. Open a new application
   against the same canonical directory and reopen the journal. Receipt-based
   recovery proves application using GET requests only. Exactly three tasks
   remain (two completed tasks plus one recovery follow-up); the second sweep
   is a no-op. Memory, verified decision and retrieval survive the restart.
   A freshly constructed A2A adapter refuses replay of completed remote work.
   Exactly one A2A file write occurred, and the ACP child has exited.

The JSON includes source hashes, resource/run IDs, trusted artifact hashes,
context counts, graph edge kinds and recovery assertions. It is emitted only
after all assertions pass; existing reports are never overwritten.

Reproduce (PowerShell, repository root)
--------------------------------------
Fast, no embedding model:
  .venv/Scripts/python.exe -m pytest tests/test_full_chain_demo.py -q

Real embedding model, existing local cache ONLY:
  $env:HF_HOME='<repo>/.hivemind-data/models'
  $env:HF_HUB_OFFLINE='1'
  $env:TRANSFORMERS_OFFLINE='1'
  $env:HIVEMIND_DEMO_REAL_EMBEDDING='1'
  $env:HIVEMIND_DEMO_REPORT='<repo>/benchmarks/full-chain-new-proof.json'
  .venv/Scripts/python.exe -m pytest tests/test_full_chain_demo.py -q

Choose a NEW report path. Clear the two HIVEMIND_DEMO_* variables before later
ordinary test runs. Pytest allocates a fresh temporary store/workspace each time.
The cached-model run passed in 20.09 seconds; this is functional evidence,
not a throughput, latency-SLO or production indexing benchmark.

Linux, existing local test image, no external network:
  docker run --rm --network none --mount 'type=bind,source=<repo>/hivemind,target=/opt/hivemind/hivemind,readonly' --mount 'type=bind,source=<repo>/tests,target=/opt/hivemind/tests,readonly' --tmpfs /tmp:rw,nosuid,nodev,size=512m hivemind-runtime-tests:2026-10-17 python -m pytest -p no:cacheprovider tests/test_full_chain_demo.py tests/test_protocol_runtimes.py tests/test_worker_recovery_integration.py tests/test_task_verification.py tests/test_team_access.py -q --tb=short

This mounts only code/tests, not the production store or model cache. The Linux
variant uses lexical retrieval and a conservative local context counter; the
real Gemma run was Windows. Tmpfs correctness is not disk durability evidence.

Failures retained and corrected
------------------------------
- Initial fixture memory also cited a mutable run. Worker heartbeat revised
  that source and correctly made the memory unavailable. Replaced that fixture
  evidence with immutable board/artifact snapshots, not weaker validation.
- Initial Windows peer output used platform CRLF translation while the trusted
  check required exact LF bytes. Made peer output explicitly LF on both OSes.
- First focused regression: 98 passed, one older verification test failed.
  Source-gated permissions now hide verifications when supporting evidence is
  deleted (404 detail, omitted list), rather than returning a redacted record.
  Updated only that test's expectations, retaining revision-drift downgrade
  checks. No production access control was loosened.

Explicit limits / remaining gates
--------------------------------
- Canonical API is the real service through in-process ASGI TestClient, not a
  separately deployed HTTP server. The A2A peer is real loopback HTTP and the
  ACP peer a real stdio subprocess. Identity credentials are disposable fixture
  principals, not a demonstration of production credential provisioning.
- Both peers are deterministic controlled fixtures, not generative or live
  vendor sessions. An ACP fixture identity's distinct review does not establish
  independent human judgment. The trusted subprocess check is real.
- Restart means graceful application shutdown/reopen, not an external process
  crash, host reboot, power loss, whole-system restore or uncertain tool-effect
  reconciliation. Completed work is refused, never automatically replayed.
- Protocol peers have no OS-sandbox claim. Background projection jobs are
  explicitly drained; this does not measure capture/projection timing.
- Context fits the reported local counter; no exact future vendor tokenizer
  guarantee. No retrieval-quality or 10k-load claim follows from this tiny case.
- This closes the requested core chain only, NOT all section 27 requirements.
  Nexus epochs/steering, coordinated messaging, unauthorized third-worker
  behavior, evidence-deletion propagation and the UI remain separate coverage.
- The separate broad 10k preparation timed out. See
  retrieval-mixed-preparation-timeout-2026-10-17.json; load and human review
  remain pending. The small real-encoder demonstration does not unblock them.
