import pytest

from dbt_databricks_factory.config import (
    ClusterConfig,
    DatabricksJobConfig,
    DbtProjectConfig,
    LibrariesConfig,
    ScheduleConfig,
)


def test_library_config():
    libraries_config = LibrariesConfig(["dbt-databricks>=1.0.0,<2.0.0", "dbt-bigquery==1.3.0"])
    assert libraries_config.to_json() == {
        "libraries": [
            {"pypi": {"package": "dbt-databricks>=1.0.0,<2.0.0"}},
            {"pypi": {"package": "dbt-bigquery==1.3.0"}},
        ]
    }


def test_cluster_config():
    cluster_config = ClusterConfig(
        existing_cluster_id="existing-cluster-id",
        job_cluster_key=None,
        new_cluster=None,
    )
    assert cluster_config.to_json() == {"existing_cluster_id": "existing-cluster-id"}

    cluster_config = ClusterConfig(
        existing_cluster_id=None,
        job_cluster_key="job-cluster-key",
        new_cluster=None,
    )
    assert cluster_config.to_json() == {"job_cluster_key": "job-cluster-key"}

    cluster_config = ClusterConfig(
        existing_cluster_id=None,
        job_cluster_key=None,
        new_cluster={"new_cluster": "config"},
    )
    assert cluster_config.to_json() == {"new_cluster": {"new_cluster": "config"}}


def test_databricks_job_config():
    DatabricksJobConfig(
        job_name="job-name",
        job_clusters=[ClusterConfig(job_cluster_key="my-cluster")],
        task_clusters={
            "model.pipeline_example.report": ClusterConfig(job_cluster_key="my-cluster"),
        },
        libraries_config=LibrariesConfig(["my-library==1.0.0"]),
    )


def test_dbt_project_config():
    DbtProjectConfig(
        project_dir="/project/dir",
        profiles_dir="/profiles/dir",
        git_url="https://my-git.url.com",
        git_provider="gitHub",
        git_branch="branch",
    )
    with pytest.raises(ValueError):
        DbtProjectConfig(
            project_dir="/project/dir",
            profiles_dir="/profiles/dir",
            git_url="https://my-git.url.com",
            git_provider="gitHub",
            git_branch="branch",
            git_commit="commit",
        )


def test_cluster_config_invalid():
    with pytest.raises(ValueError):
        ClusterConfig(
            existing_cluster_id="existing-cluster-id",
            job_cluster_key="job-cluster-key",
            new_cluster=None,
        )

    with pytest.raises(ValueError):
        ClusterConfig(
            existing_cluster_id="existing-cluster-id",
            job_cluster_key=None,
            new_cluster={"new_cluster": "config"},
        )

    with pytest.raises(ValueError):
        ClusterConfig(
            existing_cluster_id="existing-cluster-id",
            job_cluster_key="job-cluster-key",
            new_cluster={"new_cluster": "config"},
        )

    with pytest.raises(ValueError):
        ClusterConfig(
            existing_cluster_id=None,
            job_cluster_key=None,
            new_cluster=None,
        )


def test_schedule_config():
    config = ScheduleConfig(
        quartz_cron_expression="0 0 12 * * ?",
        timezone_id="UTC",
    )
    assert config.to_json() == {
        "schedule": {
            "quartz_cron_expression": "0 0 12 * * ?",
            "timezone_id": "UTC",
        }
    }
