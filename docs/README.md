# docs/

- [`architecture.md`](architecture.md) — system design, medallion data flow,
  and the rationale behind every major decision (storage format, object
  store, orchestrator, why two Gold implementations exist, cloud
  portability).
- [`pipelines.md`](pipelines.md) — "follow the data": which DAG runs what,
  in what order, calling which code. Start here if you want to trace a
  batch through the whole system.
- [`developer_guide.md`](developer_guide.md) — local setup, running the
  full platform, adding a new source/Silver table/Gold table, code
  conventions.
- [`deployment_guide.md`](deployment_guide.md) — moving from local Docker
  Compose to a real cloud environment (AWS/Azure/GCP), component by
  component.
- [`data_dictionary.md`](data_dictionary.md) — table/column definitions for
  the Silver and Gold layers.
- [`interview_notes.md`](interview_notes.md) — recruiter talking points,
  resume bullets, and (the most useful section) a list of real bugs found
  and fixed while building this, with root causes and fixes.

Every folder in the repo also has its own `README.md` with implementation
detail specific to that layer — the docs here are cross-cutting; the
per-folder READMEs are the deep reference.
