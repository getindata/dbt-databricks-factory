from __future__ import annotations

import json

import click

from .builder import DatabricksGraphBuilder
from .config import (
    ClusterConfig,
    DatabricksJobConfig,
    DbtProjectConfig,
    GitProvider,
    LibrariesConfig,
    ScheduleConfig,
)


@click.group()
def cli() -> None:
    """CLI entrypoint."""


@cli.command()
@click.argument(
    "manifest-file",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True),
)
@click.option("--job-name", required=True, help="Name of the job to create.")
@click.option("--project-dir", required=True, help="Path to dbt project directory.")
@click.option("--profiles-dir", required=True, help="Path to dbt profiles directory.")
@click.option("--cron-schedule", help="Cron schedule for the job.")
@click.option("--job-cluster", multiple=True, type=click.Tuple([str, str]), help="Job cluster config.")
@click.option(
    "--task-cluster",
    multiple=True,
    type=click.Tuple([str, str]),
    help="Job cluster name or existing cluster id.",
)
@click.option("--default-task-cluster", help="Default task cluster name or existing cluster id.")
@click.option("--library", multiple=True, type=str, help="Libraries config.")
@click.option("--git-url", required=True, help="Git url.")
@click.option("--git-branch", help="Git branch.")
@click.option("--git-commit", help="Git commit.")
@click.option("--git-tag", help="Git tag.")
@click.option(
    "--git-provider",
    required=True,
    help="Git provider.",
    type=click.Choice([provider.value for provider in GitProvider]),
)
@click.option("--pretty", is_flag=True, help="Pretty print the output.")
def create_job(
    job_name: str,
    manifest_file: str,
    project_dir: str,
    profiles_dir: str,
    cron_schedule: str | None,
    job_cluster: list[tuple[str, str]],
    task_cluster: list[tuple[str, str]],
    default_task_cluster: str | None,
    library: list[str],
    git_url: str,
    git_branch: str | None,
    git_commit: str | None,
    git_tag: str | None,
    git_provider: str,
    pretty: bool,
) -> None:
    """Create a job."""  # noqa: DCO020, DCO050
    if len(task_cluster) == 0 and default_task_cluster is None:
        raise click.BadParameter("Either task cluster or default task cluster must be provided")

    job_clusters: list[ClusterConfig] = []
    for cluster_key, new_cluster in job_cluster:
        if new_cluster is not None and new_cluster.startswith("@"):
            with open(new_cluster[1:]) as file:
                new_cluster = file.read()
        job_clusters.append(ClusterConfig(job_cluster_key=cluster_key, new_cluster=json.loads(new_cluster)))

    if default_task_cluster is not None:
        default_task_cluster_config = (
            ClusterConfig(job_cluster_key=default_task_cluster)
            if len(job_clusters)
            else ClusterConfig(existing_cluster_id=default_task_cluster)
        )
    else:
        default_task_cluster_config = None

    builder = DatabricksGraphBuilder(
        manifest_file,
        DbtProjectConfig(
            project_dir, profiles_dir, git_url, GitProvider(git_provider), git_branch, git_commit, git_tag
        ),
        DatabricksJobConfig(
            job_name,
            job_clusters=job_clusters,
            task_clusters={
                task: ClusterConfig(job_cluster_key=cluster)
                if len(job_clusters)
                else ClusterConfig(existing_cluster_id=cluster)
                for task, cluster in task_cluster
            },
            libraries_config=LibrariesConfig(library),
            default_task_cluster=default_task_cluster_config,
        ),
        schedule_config=ScheduleConfig(cron_schedule, "UTC") if cron_schedule is not None else None,
    )
    click.echo(json.dumps(builder.build(), indent=2 if pretty else None))


if __name__ == "__main__":
    cli()
