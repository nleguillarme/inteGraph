from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from ..lib.csv import *
from ..lib.rml import *
from ..lib.annotator import *
from ..util.staging import StagingHelper
from ..util.path import ensure_path


class TransformCSV:
    def __init__(self, root_dir, config, graph_id):
        self.tg_id = "transform"
        self.root_dir = ensure_path(root_dir)
        self.staging = StagingHelper(root_dir / "staging")
        self.graph_id = graph_id
        self.cfg = config

    def transform(self, filepath):
        with TaskGroup(group_id=self.tg_id):
            cleansed = filepath

            if "cleanse" in self.cfg:
                # Cleanse data using script provided by user
                self.staging.register("cleansed")
                output_dir = self.staging["cleansed"]
                user_script = self.root_dir / self.cfg["cleanse"]["script"]
                cmd = f"python {user_script} --integraph_filepath={filepath} --integraph_outputdir={output_dir}"
                cleanse_task = BashOperator(task_id="cleanse", bash_command=cmd)
                filepath >> cleanse_task
                cleansed = cleanse_task.output

            with TaskGroup(group_id="annotate"):  # Semantic annotation
                self.staging.register("chunked")

                # Split dataset in chunks
                chunks = split(
                    filepath=cleansed,
                    chunksize=self.cfg["chunksize"],
                    delimiter=self.cfg["delimiter"],
                    output_dir=self.staging["chunked"],
                )

                concat_chunks = task(task_id="concat_chunks")(concat)(
                    filepaths=chunks,
                    output_filepath=self.staging["chunked"] / "all.tsv",
                    drop=False,
                    index_col=0,
                )

                self.staging.register("annotated")
                ann_filepaths = []
                entity_ann_last_task = {}

                for entity in self.cfg[
                    "annotate"
                ]:  # Each entity is annotated separately
                    entity_ann = task_group(group_id=f"annotate_{entity}")(
                        annotate_entity
                    )(
                        filepath=chunks,
                        root_dir=self.root_dir,
                        entity=entity,
                        entity_cfg=self.cfg["annotate"][entity],
                        output_dir=self.staging["annotated"],
                    )

                    # Keep a pointer to the entity-aware concat task for concatenating all entity annotations
                    ann_filepaths.append(entity_ann["filepath"])

                    # Keep a pointer to the last task for each entity to chain with taxonomic mapping
                    entity_ann_last_task[entity] = entity_ann["task"]

                with TaskGroup(
                    group_id="map_taxa"
                ):  # Map taxonomic entities to other taxonomies
                    self.staging.register("mapped")
                    mapping_filepaths = []

                    for entity in self.cfg["annotate"]:
                        if needs_mapping(self.cfg["annotate"][entity]):
                        # if self.cfg["annotate"][entity].get("with_mapping"):
                            entity_mapping = task_group(group_id=f"map_{entity}")(
                                map_taxo_entity
                            )(
                                filepath=entity_ann_last_task[entity],
                                entity=entity,
                                output_dir=self.staging["mapped"],
                            )
                            mapping_filepaths.append(entity_mapping)

                    # Concat taxonomic mappings across entities
                    taxo_ann = task(task_id="concat")(concat)(
                        filepaths=mapping_filepaths,
                        output_filepath=self.staging["mapped"] / "taxa.tsv",
                        drop=True,
                    )

                # Concat annotations across entities and original data
                data_ann = task(task_id="add_annotations")(concat)(
                    filepaths=[concat_chunks] + ann_filepaths,
                    output_filepath=self.staging["annotated"] / "data.tsv",
                    drop=False,
                    index_col=0,
                    axis=1,
                )

            with TaskGroup(group_id="generate_graph"):
                self.staging.register("triplified")

                mapping_filepath = self.root_dir / self.cfg["triplify"]["mapping"]

                rml_filepath = generate_rml(
                    mapping_filepath, self.staging["triplified"] / "mapping"
                )

                data_graph_filepath = execute_rml(
                    filepaths=[data_ann, taxo_ann],
                    rml_filepath=rml_filepath,
                    output_dir=self.staging["triplified"],
                )

                taxa_graph_filepath = task(task_id="generate_taxa_graph")(triplify)(
                    filepath=taxo_ann,
                    output_filepath=self.staging["triplified"] / "taxa.nt",
                )

                graph_id = "<" + self.graph_id.replace("/", "\/") + ">"

                taxa_nq = self.staging["triplified"] / "taxa.nq"
                cmd = f"sed 's/.$/{graph_id} &/' {taxa_graph_filepath} > {taxa_nq}"
                taxa_to_nquads = BashOperator(
                    task_id="taxa_to_nquads", bash_command=cmd
                )

                taxa_graph_filepath >> taxa_to_nquads

                data_nq = self.staging["triplified"] / "result.nq"
                cmd = f"sed 's/.$/{graph_id} &/' {data_graph_filepath} > {data_nq}"
                data_to_nquads = BashOperator(
                    task_id="data_to_nquads", bash_command=cmd
                )

                data_graph_filepath >> data_to_nquads

                graph_nq = self.staging["triplified"] / "graph.nq"
                cmd = f"cat {data_nq} {taxa_nq} > {graph_nq} ; echo {graph_nq}"
                graph_filepath = BashOperator(task_id="merge_graphs", bash_command=cmd)

                [data_to_nquads, taxa_to_nquads] >> graph_filepath

                # graph_filepath = task(task_id="merge_graphs")(merge)(
                #     filepaths=[data_graph_filepath, taxa_graph_filepath],
                #     graph_id=self.graph_id,
                #     output_filepath=self.staging["triplified"] / "graph.nq",
                # )
            return graph_filepath.output
            # return graph_filepath
