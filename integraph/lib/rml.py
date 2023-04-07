from airflow.decorators import task, task_group
from ..util.path import ensure_path


class RMLParsingException(Exception):
    pass


class RMLMappingException(Exception):
    pass


@task
def check_rml_exists(rml_filepath):
    rml_filepath = ensure_path(str(rml_filepath) + ".rml.ttl")
    if not rml_filepath.exists():
        raise RMLParsingException(
            f"RML rules creation failed. File {rml_filepath} not found."
        )
    return str(rml_filepath)


@task
def check_graph_exists(graph_filepath):
    graph_filepath = ensure_path(graph_filepath)
    if not graph_filepath.exists():
        raise RMLMappingException(
            "RDF graph generation failed. File {graph_filepath} not found."
        )
    return str(graph_filepath)


@task(task_id="prepare")
def prepare_for_rml_execution(filepaths, config_filepath, rml_filepath, output_dir):
    from ..util.csv import write, read
    import configparser

    output_dir.mkdir(parents=True, exist_ok=True)
    config = configparser.ConfigParser()
    config.read(config_filepath)
    config.add_section("DataSource1")
    config.set("DataSource1", "mappings", ensure_path(rml_filepath).name)
    fill_config_filepath = output_dir / ensure_path(config_filepath).name
    with open(fill_config_filepath, "w") as f:
        config.write(f)

    for filepath in filepaths:
        copy_filepath = output_dir / ensure_path(filepath).name
        df = read(filepath)
        df["integraph_internal_id"] = df.index
        write(df, copy_filepath)

    return str(fill_config_filepath)


@task_group(group_id="generate_rml")
def generate_rml(mapping_filepath, rml_filepath):
    from airflow.operators.bash import BashOperator

    mapping_filepath = ensure_path(mapping_filepath)
    rml_filepath = ensure_path(rml_filepath)
    cmd = f"python3 -m mapeathor -i {mapping_filepath.absolute()} -l RML -o {rml_filepath}"
    run_mapeathor = BashOperator(task_id="run_mapeathor", bash_command=cmd)
    return run_mapeathor >> check_rml_exists(rml_filepath)


@task_group(group_id="execute_rml")
def execute_rml(filepaths, morph_config_filepath, rml_filepath, output_dir):
    from airflow.operators.bash import BashOperator

    prepare = prepare_for_rml_execution(
        filepaths=filepaths,
        config_filepath=morph_config_filepath,
        rml_filepath=rml_filepath,
        output_dir=output_dir,
    )
    cmd = f"cd {output_dir} ; python3  -m morph_kgc {prepare}"
    run_morpk_kgc = BashOperator(task_id="run_morpk_kgc", bash_command=cmd)
    graph_filepath = output_dir / "result.nq"
    graph_task = check_graph_exists(graph_filepath=graph_filepath)
    return prepare >> run_morpk_kgc >> graph_task


# def generate_rml(filepath, output_dir):
#     output_dir.mkdir(parents=True, exist_ok=True)
#     rml_filepath = output_dir / ensure_path(filepath).stem
#     # result = run_mapeathor(filepath.absolute(), rml_filepath.absolute())
#     cmd = f"python3 -m mapeathor -i {filepath.absolute()} -l RML -o {rml_filepath.absolute()}"
#     run_mapeathor = BashOperator(task_id="run_mapeathor", bash_command=cmd)
#     return run_mapeathor
# print(result)
# if result["StatusCode"] != 0:
#     raise RMLParsingException(result["logs"])
# rml_filepath = ensure_path(str(rml_filepath) + ".rml.ttl")
# if not os.path.exists(rml_filepath):
#     raise RMLParsingException(
#         f"RML rules creation failed. File {rml_filepath} not found."
#     )
# return str(rml_filepath)


# def execute_rml(filepaths, config_filepath, rml_filepath, output_dir):
#     output_dir.mkdir(parents=True, exist_ok=True)
#     print(config_filepath)
#     config = configparser.ConfigParser()
#     config.read(config_filepath)
#     config.add_section("DataSource1")
#     config.set("DataSource1", "mappings", ensure_path(rml_filepath).name)
#     fill_config_filepath = output_dir / ensure_path(config_filepath).name
#     with open(fill_config_filepath, "w") as f:
#         config.write(f)
#     output_file = config.get("CONFIGURATION", "output_file")

#     for filepath in filepaths:
#         copy_filepath = output_dir / ensure_path(filepath).name
#         df = read(filepath)
#         df["integraph_internal_id"] = df.index
#         write(df, copy_filepath)

#     result = run_morph_kgc(fill_config_filepath.absolute())
#     if result["StatusCode"] != 0:
#         raise RMLMappingException(result["logs"])
#     output_file = output_dir / output_file
#     if not os.path.exists(output_file):
#         raise RMLMappingException(
#             "RDF graph generation failed. File {output_file} not found."
#         )
#     return str(output_file)
