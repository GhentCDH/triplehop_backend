import asyncio
import time
import typing
import uuid

import asyncpg
import elasticsearch
import fastapi
import rich.progress
import starlette
import typer

from app.config import DATABASE, ELASTICSEARCH
from app.es.base import BaseElasticsearch
from app.mgmt.config import ConfigManager
from app.mgmt.data import DataManager
from app.models.auth import UserWithPermissions
from app.utils import BATCH_SIZE


async def reindex(project_name: str, entity_type_names: typing.List[str] = None):
    app = fastapi.FastAPI()
    app.state.pool = await asyncpg.create_pool(**DATABASE)
    app.state.es = elasticsearch.AsyncElasticsearch(**ELASTICSEARCH)

    try:
        request = starlette.requests.Request(
            {
                "type": "http",
                "app": app,
                "path_params": {
                    "project_name": project_name,
                },
            }
        )
        user = UserWithPermissions(
            id=uuid.uuid4(),
            username="cmd",
            permissions={},
        )
        config_manager = ConfigManager(request, user)
        entity_types_config = await config_manager.get_entity_types_config(project_name)

        if entity_type_names:
            for entity_type_name in entity_type_names:
                if entity_type_name not in entity_types_config:
                    raise Exception("Entity type name not found.")
        else:
            entity_type_names = entity_types_config.keys()

        user = UserWithPermissions(
            id=uuid.uuid4(),
            username="cmd",
            permissions={
                project_name: {
                    "entities": {
                        entity_type_name: {"es_data": {"index": []}}
                        for entity_type_name in entity_types_config.keys()
                    }
                }
            },
        )

        es = BaseElasticsearch(app.state.es)
        data_manager = DataManager(request, user)

        for entity_type_name in entity_type_names:
            entity_ids = await data_manager.get_entity_ids_by_type_name(
                entity_type_name
            )
            entity_type_config = entity_types_config[entity_type_name]

            # Add title to display on edit pages when creating relations
            # For now, [id] display.title is being used
            # If required, a more specific configuration option can be added later on
            es_data_config = [
                {
                    "system_name": "edit_relation_title",
                    "selector_value": " $||$ ".join(
                        [
                            f"[$id] {title_part}"
                            for title_part in entity_type_config["config"]["display"][
                                "title"
                            ].split(" $||$ ")
                        ]
                    ),
                    "type": "text",
                    "display_not_available": True,
                }
            ]
            if (
                "es_data" in entity_type_config["config"]
                and "fields" in entity_type_config["config"]["es_data"]
            ):
                es_data_config.extend(entity_type_config["config"]["es_data"]["fields"])
            triplehop_query = BaseElasticsearch.extract_query_from_es_data_config(
                es_data_config
            )
            new_index_name = await es.create_new_index(es_data_config)

            async def index(entity_ids):
                batch_entities = await data_manager.get_entity_data(
                    batch_ids,
                    triplehop_query,
                    entity_type_name=entity_type_name,
                )

                batch_docs = BaseElasticsearch.convert_entities_to_docs(
                    entity_types_config, es_data_config, batch_entities
                )

                await es.add_bulk(new_index_name, batch_docs)

            batch_counter = 0
            batch_ids = []
            for entity_id in rich.progress.track(
                entity_ids, f"Indexing {entity_type_name}"
            ):
                batch_counter += 1
                batch_ids.append(entity_id)
                if not batch_counter % BATCH_SIZE:
                    await index(batch_ids)
                    batch_ids = []
            if len(batch_ids):
                await index(batch_ids)

            await es.switch_to_new_index(new_index_name, entity_type_config["id"])
    finally:
        await app.state.pool.close()
        await app.state.es.close()


app = typer.Typer(pretty_exceptions_show_locals=False)


@app.command()
def main(
    project_name: str,
    entity_type_names: typing.List[str] = typer.Option(
        None, help="Names of entity types to be reindexed"
    ),
):
    start_time = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(reindex(project_name, entity_type_names))
    loop.close()
    print(f"Total time: {time.time() - start_time}")


if __name__ == "__main__":
    app()
