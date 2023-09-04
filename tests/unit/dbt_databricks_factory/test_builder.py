from dbt_databricks_factory.builder import DatabricksGraphBuilder
from dbt_databricks_factory.config import (
    ClusterConfig,
    DatabricksJobConfig,
    DbtProjectConfig,
    GitProvider,
    LibrariesConfig,
    ScheduleConfig,
)


def test_builder():
    graph_builder = DatabricksGraphBuilder(
        DbtProjectConfig(
            "/project/dir",
            "/profiles/dir",
            "https://my-git.url.com",
            GitProvider.GITHUB,
            "my-branch",
        ),
        DatabricksJobConfig(
            "job-name",
            [ClusterConfig(job_cluster_key="my-cluster")],
            {
                "model.pipeline_example.report": ClusterConfig(job_cluster_key="my-cluster"),
                "model.pipeline_example.orders": ClusterConfig(job_cluster_key="my-cluster"),
                "model.pipeline_example.supplier_parts": ClusterConfig(job_cluster_key="my-cluster"),
                "model.pipeline_example.all_europe_region_countries": ClusterConfig(job_cluster_key="my-cluster"),
            },
            LibrariesConfig(["my-library==1.0.0"]),
        ),
        schedule_config=ScheduleConfig(
            "0 0 0 1 1 ? 2099",
            "UTC",
        ),
    )
    assert graph_builder.build("tests/unit/dbt_databricks_factory/test_data/manifest.json") == {
        "name": "job-name",
        "format": "MULTI_TASK",
        "git_source": {"git_branch": "my-branch", "git_provider": "gitHub", "git_url": "https://my-git.url.com"},
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
        "schedule": {
            "quartz_cron_expression": "0 0 0 1 1 ? 2099",
            "timezone_id": "UTC",
        },
        "job_clusters": [{"job_cluster_key": "my-cluster"}],
    }
