# Matchtv

Elite-grade exporter of public IPTV M3U/M3U8 GitHub repositories.

## Files included
- `.github/workflows/export-iptv.yml` - GitHub Actions workflow (daily + manual)
- `tools/export_agent.py` - main exporter script (clones, scans, HEAD-checks, archives)
- `tools/requirements.txt` - Python dependencies
- `dags/iptv_export_dag.py` - Airflow DAG for on-prem/cloud scheduling

## Quickstart
1. Adjust repository list in `.github/workflows/export-iptv.yml` or pass `--repos` on the CLI.
2. Push this repo to GitHub; the workflow runs daily at 03:00 UTC, or manually via Actions -> Run workflow.

### Run locally
```bash
pip install -r tools/requirements.txt
export GITHUB_TOKEN=ghp_xxx   # optional
python tools/export_agent.py --repos "iptv-org/iptv,Guovin/iptv-api" --output-dir ./exports
```

### Airflow
Place the repo's `tools/` directory inside the Airflow DAGs folder and the package environment.
