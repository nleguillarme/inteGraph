from airflow.decorators import task, task_group
from ..util.path import ensure_path


class RMLParsingException(Exception):
    pass


class RMLMappingException(Exception):
    pass


@task
def check_rml_exists(rml_filepath):
    rml_filepath = ensure_path(str(rml_filepath) + ".yml")
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
def prepare_for_rml_execution(filepaths, rml_filepath, output_dir):
    from ..util.csv import write, read

    def create_morph_kgc_config(rml_filepath, output_filepath):
        import configparser

        config = configparser.ConfigParser()
        config.add_section("CONFIGURATION")
        config.set("CONFIGURATION", "output_file", "result.nt")
        config.set("CONFIGURATION", "output_format", "N-TRIPLES")
        config.add_section("DataSource1")
        config.set("DataSource1", "mappings", ensure_path(rml_filepath).name)
        with open(output_filepath, "w") as f:
            config.write(f)

    output_dir = ensure_path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    morph_config_filepath = output_dir / "config.ini"
    create_morph_kgc_config(rml_filepath, morph_config_filepath)

    # print(filepaths)
    for filepath in filepaths:
        copy_filepath = output_dir / ensure_path(filepath).name
        df = read(filepath)
        df["integraph_internal_id"] = df.index
        write(df, copy_filepath)

    return str(morph_config_filepath)


@task_group(group_id="generate_rml")
def generate_rml(mapping_filepath, rml_filepath):
    from airflow.operators.bash import BashOperator

    mapping_filepath = ensure_path(mapping_filepath)
    rml_filepath = ensure_path(rml_filepath)
    rml_filepath.mkdir(parents=True, exist_ok=True)
    cmd = f"python3 -m mapeathor -i {mapping_filepath.absolute()} -l YARRRML -o {rml_filepath}"
    run_mapeathor = BashOperator(task_id="run_mapeathor", bash_command=cmd)
    return run_mapeathor >> check_rml_exists(rml_filepath)


@task_group(group_id="execute_rml")
def execute_rml(filepaths, rml_filepath, output_dir):
    from airflow.operators.bash import BashOperator

    prepare = prepare_for_rml_execution(
        filepaths=filepaths,
        rml_filepath=rml_filepath,
        output_dir=output_dir,
    )
    cmd = f"cd {output_dir} ; python3  -m morph_kgc {prepare}"
    run_morpk_kgc = BashOperator(task_id="run_morpk_kgc", bash_command=cmd)
    graph_filepath = output_dir / "result.nt"
    graph_task = check_graph_exists(graph_filepath=graph_filepath)
    return prepare >> run_morpk_kgc >> graph_task
