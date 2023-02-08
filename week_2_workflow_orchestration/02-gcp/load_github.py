# from prefect.filesystems import GitHub
# from parameterized_flow import etl_parent_flow
# from prefect.deployments import Deployment

# github_block = GitHub.load("justine-git-block")

# github_dep = Deployment.build_from_flow(
#     flow=etl_parent_flow,
#     name="github-flow",
#     infrastructure=github_block 
# )

# if __name__=="__main":
#     github_dep.apply()

from parameterized_flow import etl_parent_flow
from prefect import flow
from prefect_github import GitHubCredentials
from prefect_github.repository import query_repository
from prefect_github.mutations import add_star_starrable


@flow()
def github_add_star_flow():
    github_credentials = GitHubCredentials.load("justine-git-block")
    repository_id = query_repository(
        "PrefectHQ",
        "Prefect",
        github_credentials=github_credentials,
        return_fields="id"
    )["id"]
    starrable = add_star_starrable(
        repository_id,
        github_credentials
    )
    return starrable


github_add_star_flow()