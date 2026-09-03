# Query task configuration ownership

This document records design guidance for configuration that crosses the query request,
coordinator, Spider task, and execution-binary boundaries. It distinguishes desired ownership from
current implementation behavior where they differ.

## Configuration ownership across layers

When a request passes through a top-level service, a coordinator, a distributed task, and an execution binary, each layer should own a distinct kind of decision. Defaults and policy should not be copied into every layer merely to preserve the behavior of the current implementation.

### Top-level service: product policy

The top-level service should enforce user-facing or deployment-wide policy. If the system requires a default value, limit, timeout, or feature choice, this layer should materialize that decision explicitly in the request or job configuration. This layer sees the original request, can apply deployment settings and access controls, and exposes behavior that can be documented as part of the public API.

A default enforced here should be passed downstream as an explicit value. The coordinator and task should not need to know whether it came from the user or from a service-level default.

### Coordinator: orchestration

The coordinator should translate the resolved job configuration into an execution plan. It selects work units, constructs task inputs, submits the task graph, observes its outcome, and manages job-level state.

The coordinator should not duplicate defaults owned by the top-level service or the execution binary. It may validate cross-task or job-wide invariants and convert resolved values into their wire representation, but it should otherwise preserve the configuration's meaning when constructing task inputs. In particular, it must not reinterpret a sentinel value unless that conversion is part of an explicit compatibility contract.

### Task: mechanical translation

The distributed task should translate each input into execution arguments, environment variables, or side-effect destinations without silently introducing policy. For an optional command-line setting, a useful contract is:

- `Some(value)` means pass the corresponding argument with `value`.
- `None` means omit the argument.

`None` should describe what the task does, not predict the execution binary's current behavior. The task should not reinterpret `None` as a hard-coded value unless that conversion is explicitly part of its contract.

### Execution binary: local behavior

The execution binary owns the behavior of an omitted command-line option. It may currently apply an internal default, and that default may change as the binary evolves.

If the top-level service requires stable behavior independent of such changes, it should pass an explicit value. If no higher layer specifies a value, the system deliberately accepts the execution binary's current no-argument behavior.

### Specification guideline

Specifications should describe each layer's observable action. For example, write "the task omits the flag" instead of "the option is unlimited" when unlimited behavior is merely a consequence of the binary's current default.

This separation keeps product policy at the system boundary, orchestration in the coordinator, argument translation in the task, and local execution behavior in the binary. It avoids duplicated defaults and allows the binary's behavior to change without requiring synchronized coordinator and task-package changes.

## Example: query result limit

Query result-limit behavior spans the API server, query coordinator, query task, and clp-s. Each layer has a distinct responsibility so that the task contract does not duplicate policy owned elsewhere.

The query task receives `max_num_results: Option<NonZeroU32>`. Its behavior is deliberately mechanical:

- `Some(max_num_results)` causes the task to pass `--max-num-results <max_num_results>` to the clp-s results-cache output handler.
- `None` causes the task to omit `--max-num-results` entirely. The task does not replace `None` with a hard-coded value.

The layers therefore have the following responsibilities:

1. **API server:** For API-originated queries, the API server owns the product default. The current request type deserializes an omitted `max_num_results` as `0`, and `submit_query` replaces `0` with `default_max_num_query_results` before persisting `SearchJobConfig`. The configured default is currently `1000`, so API-originated jobs normally contain an explicit positive limit.
2. **Other job producers:** Code that inserts `SearchJobConfig` directly into `query_jobs` is itself responsible for resolving product-level semantics. It cannot assume that a legacy `0` sentinel will retain its meaning after conversion to the query task's optional nonzero wire type.
3. **Query coordinator:** The coordinator converts an already-resolved positive job value to `Some(max_num_results)`. It does not choose the API server's default. A persisted `0` needs an explicitly documented compatibility rule; mapping it to `None` means "omit the flag," not "unlimited."
4. **Query task:** The task preserves the wire type's optional semantics. `Some` produces the clp-s flag and `None` produces no flag. It neither chooses a default limit nor interprets `None` as a particular number.
5. **clp-s:** clp-s determines what happens when `--max-num-results` is absent. It currently defaults to `1000` results and rejects an explicit value of `0`. Enforcing a service-level default is not clp-s's responsibility.

There is a current interface discrepancy: the API schema describes `0` as "no limit," while the API implementation replaces `0` with the configured default. In addition, legacy direct job producers may still use `0` as an unlimited sentinel. The coordinator implementation must not silently guess between these meanings. The API contract and any supported direct-producer compatibility behavior must be reconciled before `SearchJobConfig.max_num_results` is converted into `ClpSQueryOption.max_num_results`.

In the future, clp-s may change its no-flag behavior to mean that the number of results is unlimited. The query task would continue to omit the flag for `None`. A deployment that requires a stable bounded default would continue to have the API server persist an explicit positive value.

This separation keeps mechanism in the task layer and policy in the top-level service. It also prevents the API server, query coordinator, and task package from each maintaining a duplicate copy of clp-s's current default value.
