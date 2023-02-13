# Databricks notebook source
import os

# from mlflow.utils import databricks_utils
from typing import Tuple

try:
    databricks_token = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .apiToken()
        .getOrElse(None)
    )
    # api_url = databricks_utils._get_command_context().extraContext().get("api_url").get()
    os.environ["DATABRICKS_HOST"] = "https://e2-demo-field-eng.cloud.databricks.com/"
    #os.environ["DATABRICKS_TOKEN"] = "dapi8425262def00bee4ea48ab80cf211dfb"
    os.environ["DATABRICKS_TOKEN"] ="dapi2d21d28cfe89aa097e09352ce6212e57"
    
except:
    pass

# COMMAND ----------

import sys
import json
import time
import yaml
import pathlib
from argparse import ArgumentParser
from random import random
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import EnvironmentVariableConfigProvider
from databricks_cli.sdk import JobsService, ApiClient
from databricks_cli.sdk import DeltaPipelinesService, ReposService
from jinja2 import Environment, PackageLoader, select_autoescape


def get_branch() -> str:
    p = ArgumentParser()
    p.add_argument("--branch_name", required=False, type=str)
    namespace = p.parse_known_args(sys.argv + ["", ""])[0]
    branch_name = namespace.branch_name
    print("Branch Name: ", branch_name)
    return branch_name


def get_repo_path(repos_path_prefix: str, branch: str) -> Tuple[str, str]:
    prefix = str(int(random() * 1000))
    _b = branch.replace("/", "_")
    repo_path = f"{repos_path_prefix}_{_b}_{prefix}"
    return repo_path, prefix


def prepare_dlt_conf(prefix: str) -> str:
    env = Environment(autoescape=select_autoescape())
    templ = env.from_string(pathlib.Path("dlt_config.json").read_text())
    dlt_def = templ.render(
        path=repo_path, prefix=prefix, raw_input="/Users/msh/demo/dlt_loan"
    )
    dlt_def = json.loads(dlt_def)
    return dlt_def


def create_and_update_dlt(api_client: ApiClient, dlt_def: str) -> Tuple[str, str]:
    res = api_client.perform_query("POST", "/pipelines", data=dlt_def)
    pipeline_id = res["pipeline_id"]
    try:
        res = api_client.perform_query(
            "POST", f"/pipelines/{pipeline_id}/updates", data={"full_refresh": "true"}
        )
        update_id = res["update_id"]
    except Exception as e:
        api_client.perform_query("DELETE", f"/pipelines/{pipeline_id}")
        raise e
    return pipeline_id, update_id


conf = yaml.safe_load(pathlib.Path("conf.yaml").read_text())
repos_path_prefix = conf.get("repos_path_prefix")
git_url = conf.get("git_url")
provider = conf.get("provider", "gitHub")

config = EnvironmentVariableConfigProvider().get_config()
api_client = _get_api_client(config, command_name="cicdtemplates-")
dlt_service = DeltaPipelinesService(api_client)
repos_service = ReposService(api_client)
branch = get_branch()
if branch is None:
    branch = conf.get("branch", "master")


def wait_while_dlt_updating(api_client, pipeline_id, update_id, wait_time=5) -> str:
    while True:
        res = api_client.perform_query(
            "GET", f"/pipelines/{pipeline_id}/updates/{update_id}"
        )
        status = res["update"]["state"]
        print(status)
        if status in ["FAILED", "CANCELED"]:
            print(res)
            raise Exception("Pipeline failed!")
        elif status in ["COMPLETED"]:
            return status
        else:
            time.sleep(wait_time)


repo_path, prefix = get_repo_path(repos_path_prefix, branch)
print("Checking out the following repo: ", repo_path)

repo = repos_service.create_repo(url=git_url, provider=provider, path=repo_path)
try:
    repos_service.update_repo(id=repo["id"], branch=branch)

    dlt_def = prepare_dlt_conf(prefix)
    pipeline_id, update_id = create_and_update_dlt(api_client, dlt_def)
    try:
        wait_while_dlt_updating(api_client, pipeline_id, update_id, wait_time=15)
    finally:
        api_client.perform_query("DELETE", f"/pipelines/{pipeline_id}")

finally:
    repos_service.delete_repo(id=repo["id"])
