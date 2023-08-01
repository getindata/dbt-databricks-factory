import json

from click.testing import CliRunner

from dbt_databricks_factory.cli import create_job


def test_cli() -> None:
    runner = CliRunner()
    result = runner.invoke(
        create_job,
        [
            "--project-dir",
            "/project/dir",
            "--profiles-dir",
            "/profiles/dir",
            "--job-name",
            "job-name",
            "--job-cluster",
            "my-cluster",
            '{"new_cluster": "config"}',
            "--task-cluster",
            "model.pipeline_example.report",
            "my-cluster",
            "--task-cluster",
            "model.pipeline_example.orders",
            "my-cluster",
            "--task-cluster",
            "model.pipeline_example.supplier_parts",
            "my-cluster",
            "--task-cluster",
            "model.pipeline_example.all_europe_region_countries",
            "my-cluster",
            "--library",
            "my-library==1.0.0",
            "--cron-schedule",
            "0 0 0 1 1 ? 2099",
            "--git-url",
            "https://my-git.url.com",
            "--git-provider",
            "gitHub",
            "--git-branch",
            "my-branch",
            "--pretty",
            "tests/unit/dbt_databricks_factory/test_data/manifest.json",
        ],
    )
    expected = {
        "name": "job-name",
        "tasks": [
            {
                "task_key": "model_pipeline_example_orders-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select orders"],
                },
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_orders-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select orders"],
                },
                "depends_on": [{"task_key": "model_pipeline_example_orders-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_supplier_parts-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select supplier_parts"],
                },
                "depends_on": [{"task_key": "model_pipeline_example_orders-test"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_supplier_parts-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select supplier_parts"],
                },
                "depends_on": [{"task_key": "model_pipeline_example_supplier_parts-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_all_europe_region_countries-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": [
                        "dbt deps",
                        "dbt run --profiles-dir /profiles/dir --select all_europe_region_countries",
                    ],
                },
                "depends_on": [{"task_key": "model_pipeline_example_orders-test"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_all_europe_region_countries-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": [
                        "dbt deps",
                        "dbt test --profiles-dir /profiles/dir --select all_europe_region_countries",
                    ],
                },
                "depends_on": [{"task_key": "model_pipeline_example_all_europe_region_countries-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_report-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select report"],
                },
                "depends_on": [
                    {"task_key": "model_pipeline_example_all_europe_region_countries-test"},
                    {"task_key": "model_pipeline_example_supplier_parts-test"},
                ],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_report-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select report"],
                },
                "depends_on": [{"task_key": "model_pipeline_example_report-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
        ],
        "job_clusters": [{"job_cluster_key": "my-cluster", "new_cluster": {"new_cluster": "config"}}],
        "git_source": {
            "git_url": "https://my-git.url.com",
            "git_provider": "gitHub",
            "git_branch": "my-branch",
        },
        "format": "MULTI_TASK",
        "schedule": {"quartz_cron_expression": "0 0 0 1 1 ? 2099", "timezone_id": "UTC"},
    }
    assert result.output == json.dumps(expected, indent=2) + "\n"
    assert result.exit_code == 0


def test_cli_2() -> None:
    runner = CliRunner()
    result = runner.invoke(
        create_job,
        [
            "--project-dir",
            "/project/dir",
            "--profiles-dir",
            "/profiles/dir",
            "--job-name",
            "job-name",
            "--job-cluster",
            "my-cluster",
            "@tests/unit/dbt_databricks_factory/test_data/cluster_config.json",
            "--task-cluster",
            "model.pipeline_example.report",
            "my-cluster",
            "--task-cluster",
            "model.pipeline_example.orders",
            "my-cluster",
            "--task-cluster",
            "model.pipeline_example.supplier_parts",
            "my-cluster",
            "--task-cluster",
            "model.pipeline_example.all_europe_region_countries",
            "my-cluster",
            "--library",
            "my-library==1.0.0",
            "--cron-schedule",
            "0 0 0 1 1 ? 2099",
            "--git-url",
            "https://my-git.url.com",
            "--git-provider",
            "gitHub",
            "--git-branch",
            "my-branch",
            "--pretty",
            "tests/unit/dbt_databricks_factory/test_data/manifest.json",
        ],
    )
    expected = {
        "name": "job-name",
        "tasks": [
            {
                "task_key": "model_pipeline_example_orders-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select orders"],
                },
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_orders-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select orders"],
                },
                "depends_on": [{"task_key": "model_pipeline_example_orders-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_supplier_parts-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select supplier_parts"],
                },
                "depends_on": [{"task_key": "model_pipeline_example_orders-test"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_supplier_parts-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select supplier_parts"],
                },
                "depends_on": [{"task_key": "model_pipeline_example_supplier_parts-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_all_europe_region_countries-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": [
                        "dbt deps",
                        "dbt run --profiles-dir /profiles/dir --select all_europe_region_countries",
                    ],
                },
                "depends_on": [{"task_key": "model_pipeline_example_orders-test"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_all_europe_region_countries-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": [
                        "dbt deps",
                        "dbt test --profiles-dir /profiles/dir --select all_europe_region_countries",
                    ],
                },
                "depends_on": [{"task_key": "model_pipeline_example_all_europe_region_countries-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_report-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select report"],
                },
                "depends_on": [
                    {"task_key": "model_pipeline_example_all_europe_region_countries-test"},
                    {
                        "task_key": "model_pipeline_example_supplier_parts-test",
                    },
                ],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_report-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select report"],
                },
                "depends_on": [{"task_key": "model_pipeline_example_report-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
        ],
        "job_clusters": [
            {
                "job_cluster_key": "my-cluster",
                "new_cluster": {
                    "runtime_engine": "PHOTON",
                    "spark_version": "13.2.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "autoscale": {"min_workers": 2, "max_workers": 8},
                    "init_scripts": [{"workspace": {"destination": "/Shared/init-scripts/init_config.sh"}}],
                    "spark_env_vars": {"GCP_KEY": "{{secrets/dataops-labs/gcp-pipeline-example-key}}"},
                    "cluster_log_conf": {"dbfs": {"destination": "dbfs:/cluster-logs"}},
                },
            }
        ],
        "git_source": {
            "git_url": "https://my-git.url.com",
            "git_provider": "gitHub",
            "git_branch": "my-branch",
        },
        "format": "MULTI_TASK",
        "schedule": {"quartz_cron_expression": "0 0 0 1 1 ? 2099", "timezone_id": "UTC"},
    }
    assert result.output == json.dumps(expected, indent=2) + "\n"
    assert result.exit_code == 0
