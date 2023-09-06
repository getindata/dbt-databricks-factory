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
                "model.pipeline_example.suppliers": ClusterConfig(job_cluster_key="my-cluster"),
                "model.pipeline_example.not_shiped_by_rail": ClusterConfig(job_cluster_key="my-cluster"),
                "model.pipeline_example.automibile_customers_from_europe": ClusterConfig(job_cluster_key="my-cluster"),
                "model.pipeline_example.supplier_with_nation": ClusterConfig(job_cluster_key="my-cluster"),
                "model.pipeline_example.customer_nation_region": ClusterConfig(job_cluster_key="my-cluster"),
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
        "tasks": [
            {
                "task_key": "model_pipeline_example_suppliers-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select suppliers"],
                },
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_suppliers-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select suppliers"],
                },
                "depends_on": [{"task_key": "model_pipeline_example_suppliers-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_not_shiped_by_rail-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select not_shiped_by_rail"],
                },
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_not_shiped_by_rail-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select not_shiped_by_rail"],
                },
                "depends_on": [{"task_key": "model_pipeline_example_not_shiped_by_rail-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_automibile_customers_from_europe-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": [
                        "dbt deps",
                        "dbt run --profiles-dir /profiles/dir --select automibile_customers_from_europe",
                    ],
                },
                "depends_on": [
                    {"task_key": "model_pipeline_example_all_europe_region_countries-test"},
                ],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_automibile_customers_from_europe-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": [
                        "dbt deps",
                        "dbt test --profiles-dir /profiles/dir --select automibile_customers_from_europe",
                    ],
                },
                "depends_on": [{"task_key": "model_pipeline_example_automibile_customers_from_europe-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_supplier_with_nation-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select supplier_with_nation"],
                },
                "depends_on": [
                    {"task_key": "model_pipeline_example_suppliers-test"},
                ],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_supplier_with_nation-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select supplier_with_nation"],
                },
                "depends_on": [{"task_key": "model_pipeline_example_supplier_with_nation-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_report-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select report"],
                },
                "depends_on": [{"task_key": "model_pipeline_example_orders-test"}],
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
            {
                "task_key": "model_pipeline_example_all_europe_region_countries-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": [
                        "dbt deps",
                        "dbt run --profiles-dir /profiles/dir --select all_europe_region_countries",
                    ],
                },
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
                "task_key": "model_pipeline_example_supplier_parts-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select supplier_parts"],
                },
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
                "task_key": "model_pipeline_example_customer_nation_region-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select customer_nation_region"],
                },
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_customer_nation_region-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select customer_nation_region"],
                },
                "depends_on": [{"task_key": "model_pipeline_example_customer_nation_region-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_pipeline_example_orders-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select orders"],
                },
                "depends_on": [
                    {"task_key": "model_pipeline_example_automibile_customers_from_europe-test"},
                    {"task_key": "model_pipeline_example_not_shiped_by_rail-test"},
                    {"task_key": "model_pipeline_example_supplier_with_nation-test"},
                ],
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
        ],
        "job_clusters": [{"job_cluster_key": "my-cluster"}],
        "git_source": {"git_url": "https://my-git.url.com", "git_provider": "gitHub", "git_branch": "my-branch"},
        "format": "MULTI_TASK",
        "schedule": {"quartz_cron_expression": "0 0 0 1 1 ? 2099", "timezone_id": "UTC"},
    }


def test_builder_2():
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
            {},
            LibrariesConfig(["my-library==1.0.0"]),
            default_task_cluster=ClusterConfig(job_cluster_key="my-cluster"),
        ),
        schedule_config=ScheduleConfig(
            "0 0 0 1 1 ? 2099",
            "UTC",
        ),
    )
    assert graph_builder.build("tests/unit/dbt_databricks_factory/test_data/manifest_2.json") == {
        "name": "job-name",
        "tasks": [
            {
                "task_key": "model_dbt_test_model1-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select model1"],
                },
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model1-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select model1"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model1-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model2-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select model2"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model1-test"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model2-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select model2"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model2-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model3-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select model3"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model2-test"}, {"task_key": "model_dbt_test_model5-test"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model3-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select model3"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model3-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model4-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select model4"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model10-test"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model4-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select model4"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model4-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model5-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select model5"],
                },
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model5-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select model5"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model5-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model6-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select model6"],
                },
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model6-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select model6"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model6-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model7-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select model7"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model6-test"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model7-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select model7"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model7-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model8-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select model8"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model6-test"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model8-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select model8"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model8-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model9-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select model9"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model7-test"}, {"task_key": "model_dbt_test_model8-test"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model9-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select model9"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model9-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model10-run",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt run --profiles-dir /profiles/dir --select model10"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model3-test"}, {"task_key": "model_dbt_test_model9-test"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
            {
                "task_key": "model_dbt_test_model10-test",
                "dbt_task": {
                    "project_directory": "/project/dir",
                    "commands": ["dbt deps", "dbt test --profiles-dir /profiles/dir --select model10"],
                },
                "depends_on": [{"task_key": "model_dbt_test_model10-run"}],
                "libraries": [{"pypi": {"package": "my-library==1.0.0"}}],
                "job_cluster_key": "my-cluster",
            },
        ],
        "job_clusters": [{"job_cluster_key": "my-cluster"}],
        "git_source": {"git_url": "https://my-git.url.com", "git_provider": "gitHub", "git_branch": "my-branch"},
        "format": "MULTI_TASK",
        "schedule": {"quartz_cron_expression": "0 0 0 1 1 ? 2099", "timezone_id": "UTC"},
    }
