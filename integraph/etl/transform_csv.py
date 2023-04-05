from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from ..lib.csv import *
from ..lib.rml import *
from ..util.staging import StagingHelper
from ..util.path import ensure_path


class TransformCSV:
    def __init__(self, root_dir, config, graph_id, morph_config_filepath):
        self.tg_id = "transform"
        self.root_dir = ensure_path(root_dir)
        self.staging = StagingHelper(root_dir / "staging")
        self.graph_id = graph_id
        self.morph_config_filepath = morph_config_filepath
        self.cfg = config

    def transform(self, filepath):
        with TaskGroup(group_id=self.tg_id):
            cleansed = filepath

            with_cleansing = "cleanse" in self.cfg or self.cfg["delimiter"] != "\t"

            if with_cleansing:  # Cleanse data
                with TaskGroup(group_id="cleanse"):
                    self.staging.register("cleansed")
                    output_dir = self.staging["cleansed"]

                    if (
                        "cleanse" in self.cfg
                    ):  # Cleanse data using script provided by user
                        user_script = self.root_dir / self.cfg["cleanse"]["script"]
                        cmd = f"python {user_script} --integraph_filepath={filepath} --integraph_outputdir={output_dir}"
                        cleanse_task = BashOperator(
                            task_id="run_user_script", bash_command=cmd
                        )
                        filepath >> cleanse_task
                        cleansed = cleanse_task.output

            with TaskGroup(group_id="annotate"):  # Semantic annotation
                self.staging.register("chunked")
                split_task = task(split)(  # Split dataset in chunks
                    filepath=cleansed,
                    chunksize=self.cfg["chunksize"],
                    delimiter=self.cfg["delimiter"],
                    output_dir=self.staging["chunked"],
                )

                concat_chunks_task = task(task_id="concat_chunks")(concat)(
                    filepaths=split_task,
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
                    ent_cfg = self.cfg["annotate"][entity]
                    staging_area = f"annotated/{entity}"
                    self.staging.register(staging_area)

                    prev_task = split_task
                    with TaskGroup(group_id="annotate_" + entity):
                        first_target = True

                        for target in ent_cfg.get("target"):
                            task_id = target
                            if ensure_path(target).suffix == ".yml":
                                target = self.root_dir / target
                                task_id = target.name.replace(".", "_")

                            prev_task = (
                                task(task_id=task_id)(annotate)
                                .partial(
                                    id_col=ent_cfg.get("id"),
                                    label_col=ent_cfg.get("label"),
                                    iri_col=entity + "_iri",
                                    source=ent_cfg.get("source"),
                                    target=target,
                                    output_dir=self.staging[staging_area] / task_id,
                                    replace=first_target,
                                )
                                .expand(filepath=prev_task)
                            )
                            first_target = False

                        # Concat chunks for the current entity
                        concat_ann_chunks_task = task(task_id=f"concat")(concat)(
                            filepaths=prev_task,
                            output_filepath=self.staging[staging_area] / "all.tsv",
                            drop=False,
                            index_col=0,
                        )

                        # Keep a pointer to the entity-aware concat task for concatenating all entity annotations
                        ann_filepaths.append(concat_ann_chunks_task)

                        # Keep a pointer to the last task for each entity to chain with taxonomic mapping
                        entity_ann_last_task[entity] = prev_task

                with TaskGroup(
                    group_id="map_taxa"
                ):  # Map taxonomic entities to other taxonomies
                    self.staging.register("mapped")
                    map_filepaths = []
                    for entity in self.cfg["annotate"]:
                        ent_cfg = self.cfg["annotate"][entity]
                        if ent_cfg.get("with_mapping"):
                            staging_area = f"mapped/{entity}"
                            self.staging.register(staging_area)

                            with TaskGroup(group_id="map_" + entity):
                                map_task = (
                                    task(map)
                                    .partial(output_dir=self.staging[staging_area])
                                    .expand(filepath=entity_ann_last_task[entity])
                                )

                                # Concat chunks for the current entity
                                concat_map_chunks_task = task(task_id="concat")(concat)(
                                    filepaths=map_task,
                                    output_filepath=self.staging[staging_area]
                                    / "all.tsv",
                                    drop=False,
                                    index_col=0,
                                )

                                map_filepaths.append(concat_map_chunks_task)

                    # Concat taxonomic mappings across entities
                    concat_taxa_task = task(task_id="concat")(concat)(
                        filepaths=map_filepaths,
                        output_filepath=self.staging["mapped"] / "taxa.tsv",
                        drop=True,
                    )

                # Concat annotations across entities and original data
                concat_ann_data_task = task(task_id="add_annotations")(concat)(
                    filepaths=[concat_chunks_task] + ann_filepaths,
                    output_filepath=self.staging["annotated"] / "data.tsv",
                    drop=False,
                    index_col=0,
                    axis=1,
                )

            with TaskGroup(group_id="generate_graph"):
                self.staging.register("triplified")

                mapping_filepath = self.root_dir / self.cfg["triplify"]["mapping"]

                rml_task = task(generate_rml)(
                    mapping_filepath, self.staging["triplified"]
                )

                graph_task = task(execute_rml)(
                    filepaths=[concat_ann_data_task, concat_taxa_task],
                    config_filepath=self.morph_config_filepath,
                    rml_filepath=rml_task,
                    output_dir=self.staging["triplified"],
                )

                taxa_task = task(task_id="generate_taxa_graph")(triplify)(
                    filepath=concat_taxa_task,
                    output_filepath=self.staging["triplified"] / "taxa.nq",
                )

                merge_task = task(task_id="merge_graphs")(merge)(
                    filepaths=[graph_task, taxa_task],
                    graph_id=self.graph_id,
                    output_filepath=self.staging["triplified"] / "graph.nq",
                )

            return merge_task
