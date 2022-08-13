from datetime import datetime
from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.decorators import task
import jinja2
from airflow.configuration import conf
from typing import List, Union, Dict


__dag_properties = {'dag_id': 8, 'description': None}
__tasks = [{'task_id': 32, 'task_name': 'read_file_str_32_32', 'pipeline_options': {'config': '{"ports": {"inputs": [], "outputs": [{"id": 2, "name": "string", "type": "string", "config": {"file_path": "opt/airflow/data/test_output"}, "priority": 0, "required": true, "port_status": "Port Status 1"}]}, "configuration": {"file_path": "/opt/airflow/data/test_input.csv"}}'}, 'py_file': 'lagrange_workflow.module.io.read_file'}, {'task_id': 33, 'task_name': 'fimder_33_33', 'pipeline_options': {'config': '{"ports": {"inputs": [], "outputs": [{"id": 2, "name": "string", "type": "string", "config": {"file_path": "opt/airflow/data/test_output"}, "priority": 0, "required": true, "port_status": "Port Status 1"}]}, "configuration": {"file_path": "/opt/airflow/data/test_input.csv"}}'}, 'py_file': 'lagrange_workflow.module.io.read_file'}, {'task_id': 34, 'task_name': 'defind_34_34', 'pipeline_options': {'config': '{"ports": {"inputs": [], "outputs": [{"id": 2, "name": "string", "type": "string", "config": {"file_path": "opt/airflow/data/test_output"}, "priority": 0, "required": true, "port_status": "Port Status 1"}]}, "configuration": {"file_path": "/opt/airflow/data/test_input.csv"}}'}, 'py_file': 'lagrange_workflow.module.io.read_file'}, {'task_id': 35, 'task_name': 'remap_35_35', 'pipeline_options': {'config': '{"ports": {"inputs": [], "outputs": [{"id": 2, "name": "string", "type": "string", "config": {"file_path": "opt/airflow/data/test_output"}, "priority": 0, "required": true, "port_status": "Port Status 1"}]}, "configuration": {"file_path": "/opt/airflow/data/test_input.csv"}}'}, 'py_file': 'lagrange_workflow.module.io.read_file'}]
__dependencies = {34: [33, 32], 33: [32], 35: [32, 34], 32: []}


@task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
def watcher():
    raise AirflowException("Failing task because one or more upstream tasks failed.")


def _check_arg(dag_properties) -> Dict:
    params = {
        "dag_id": None, "description": None, "timetable": None, "schedule_interval": "@daily",
        "start_date": datetime(2021, 1, 1), "end_date": None, "full_filepath": None, "template_searchpath": None,
        "template_undefined": jinja2.StrictUndefined, "user_defined_macros": None, "user_defined_filters": None,
        "default_args": None, "concurrency": None,
        "max_active_tasks": conf.getint('core', 'max_active_tasks_per_dag'),
        "max_active_runs": conf.getint('core', 'max_active_runs_per_dag'), "dagrun_timeout": None,
        "sla_miss_callback": None,
        "default_view": conf.get_mandatory_value('webserver', 'dag_default_view').lower(),
        "orientation": conf.get_mandatory_value('webserver', 'dag_orientation'), "catchup": False,
        "on_success_callback": None, "on_failure_callback": None, "doc_md": None, "params": None,
        "access_control": None, "is_paused_upon_creation": None, "jinja_environment_kwargs": None,
        "render_template_as_native_obj": False, "tags": None
    }
    for dag_property in dag_properties:
        try:
            params[dag_property] = dag_properties[dag_property]
        except KeyError:
            continue
    return params


def _check_arg_beam(task_properties) -> Dict:
    params = {
        "task_id": None, "task_name": None, "type": "BeamRunPythonPipelineOperator",
        "py_file": "apache_beam.examples.wordcount",
        "py_options": ['-m'], "py_requirements": ['git+https://szainalie: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@github.com/buildwithlagrangeai/lagrange-workflow.git@develop'],
        "py_interpreter": "python3", "py_system_site_packages": False, "pipeline_options": None,
        "runner": 'DirectRunner', "gcp_conn_id": 'google_cloud_default', "delegate_to": None, "dataflow_config": None
    }
    for task_property in task_properties:
        try:
            params[task_property] = task_properties[task_property]
        except KeyError:
            continue
    return params


def _find_task_object(task_id: int, task_object: List) -> Union[BeamRunPythonPipelineOperator, None]:
    for i in range(0, len(task_object)):
        if task_id == task_object[i]['task_id']:
            return task_object[i]['object']
    return None


__dag_pram = _check_arg(__dag_properties)

try:
    with DAG(
            dag_id=str(__dag_pram['dag_id']), description=__dag_pram['description'], timetable=__dag_pram['timetable'],
            schedule_interval=__dag_pram['schedule_interval'], start_date=__dag_pram['start_date'],
            end_date=__dag_pram['end_date'], full_filepath=__dag_pram['full_filepath'],
            template_searchpath=__dag_pram['template_searchpath'],
            template_undefined=__dag_pram['template_undefined'], user_defined_macros=__dag_pram['user_defined_macros'],
            user_defined_filters=__dag_pram['user_defined_filters'], default_args=__dag_pram['default_args'],
            concurrency=__dag_pram['concurrency'], max_active_tasks=__dag_pram['max_active_tasks'],
            max_active_runs=__dag_pram['max_active_runs'], dagrun_timeout=__dag_pram['dagrun_timeout'],
            sla_miss_callback=__dag_pram['sla_miss_callback'], default_view=__dag_pram['default_view'],
            orientation=__dag_pram['orientation'], on_success_callback=__dag_pram['on_success_callback'],
            on_failure_callback=__dag_pram['on_failure_callback'], doc_md=__dag_pram['doc_md'],
            params=__dag_pram['params'], access_control=__dag_pram['access_control'],
            is_paused_upon_creation=__dag_pram['is_paused_upon_creation'],
            jinja_environment_kwargs=__dag_pram['jinja_environment_kwargs'],
            render_template_as_native_obj=__dag_pram['render_template_as_native_obj'],
            tags=__dag_pram['tags'], catchup=bool(__dag_pram['catchup'])
    ) as dag:
        task_objects = __tasks
        objects = []
        for task in task_objects:
            param = _check_arg_beam(task)  # add default beam python operator
            objects.append(
                {
                    'task_id': param['task_id'],
                    'object': BeamRunPythonPipelineOperator(
                        task_id=param['task_name'],
                        py_file=param['py_file'],
                        py_options=param['py_options'],
                        py_requirements=param['py_requirements'],
                        py_interpreter=param['py_interpreter'],
                        py_system_site_packages=bool(param['py_system_site_packages']),
                        pipeline_options = param['pipeline_options'],
                        runner = param['runner'],
                        gcp_conn_id = param['gcp_conn_id'],
                        delegate_to = param['delegate_to'],
                        dataflow_config = param['dataflow_config']
                    )
                })

        depends = []
        for dep in __dependencies:
            dependencies = _find_task_object(task_id=dep, task_object=objects)
            if dependencies:
                for dep_2 in __dependencies[dep]:
                    dep_obj2 = _find_task_object(task_id=dep_2, task_object=objects)
                    if dep_obj2:
                        depends.append(dependencies << dep_obj2)
        list(dag.tasks) >> watcher()

except Exception as e:
    print(e)