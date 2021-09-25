import logging
import sys

sys.path.append("../")
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Union

import fire
import pandas as pd

from internal.graph_database_builder import GraphDatabaseBuilder
from store.graph_output_handlers.neo4j_output_handlers.nodes_file import NodesFile
from store.graph_output_handlers.neo4j_output_handlers.relations_file import (
    RelationsFile,
)
from utils.logger import get_logger


def process_all_files_for_neo4j_import(
    matchMetadataFilepath: Union[str, Path],
    matchEventsFilepath: Union[str, Path],
    outputDirectory: Union[str, Path],
) -> None:
    Path(outputDirectory).mkdir(parents=True, exist_ok=True)
    logOutputFilename = f"{outputDirectory}/football_graph_{datetime.now().date()}.log"
    logger = get_logger(logOutputFilename=logOutputFilename, overwriteExistingFile=True)
    try:
        nodeOutputHandler = NodesFile(
            fileName=f"{outputDirectory}/football_event_graph_nodes.csv.gz"
        )
        relationOutputHandler = RelationsFile(
            fileName=f"{outputDirectory}/football_event_graph_relations.csv.gz"
        )
        databaseBuilder = GraphDatabaseBuilder(
            nodeOutputHandler=nodeOutputHandler,
            relationsOutputHandler=relationOutputHandler,
        )
        teamToIdMap = process_match_metadata_file(
            matchMetadataFilepath=matchMetadataFilepath,
            databaseBuilder=databaseBuilder,
            logger=logger,
        )
        process_match_events_file(
            matchEventsFilepath=matchEventsFilepath,
            databaseBuilder=databaseBuilder,
            teamToIdMap=teamToIdMap,
            logger=logger,
        )
        logger.info(msg="Finished processing all files")
    except Exception as ex:
        logger.exception(msg=ex)
        raise ex


def process_match_metadata_file(
    matchMetadataFilepath: Union[str, Path],
    databaseBuilder: GraphDatabaseBuilder,
    logger: logging.Logger,
) -> Dict[str, int]:
    matchMetadataDataframe = pd.read_csv(filepath_or_buffer=matchMetadataFilepath)
    logger.info(msg="Building maps for categorical variables")
    leagueToIdMap = {
        league: index
        for index, league in enumerate(set(matchMetadataDataframe["league"]))
    }
    countryToIdMap = {
        country: index
        for index, country in enumerate(set(matchMetadataDataframe["country"]))
    }
    seasonToIdMap = {
        season: index
        for index, season in enumerate(set(matchMetadataDataframe["season"]))
    }
    teamToIdMap = {
        team: index
        for index, team in enumerate(
            set(matchMetadataDataframe["ht"]).union(matchMetadataDataframe["at"])
        )
    }
    logger.info(msg="Adding league nodes")
    databaseBuilder.add_leagues(leagueToIdMap=leagueToIdMap)
    logger.info(msg="Adding country nodes")
    databaseBuilder.add_countries(countryToIdMap=countryToIdMap)
    logger.info(msg="Adding season nodes")
    databaseBuilder.add_seasons(seasonToIdMap=seasonToIdMap)
    logger.info(msg="Adding team nodes")
    databaseBuilder.add_teams(teamToIdMap=teamToIdMap)
    logger.info(msg="Adding date nodes")
    databaseBuilder.add_dates(allDates=set(matchMetadataDataframe["date"]))

    logger.info(msg="Remapping categorical metadata columns")
    remappedMetadata = matchMetadataDataframe.replace(
        {
            "league": leagueToIdMap,
            "country": countryToIdMap,
            "season": seasonToIdMap,
            "ht": teamToIdMap,
            "at": teamToIdMap,
        }
    )
    logger.info(msg="Adding football match nodes and relations")
    databaseBuilder.add_football_matches(remappedMetadata=remappedMetadata)
    logger.info(msg="Finished processing match metadata file")
    return teamToIdMap


def process_match_events_file(
    matchEventsFilepath: Union[str, Path],
    databaseBuilder: GraphDatabaseBuilder,
    teamToIdMap: Dict[str, int],
    logger: logging.Logger,
) -> None:
    matchEventsDataframe = pd.read_csv(filepath_or_buffer=matchEventsFilepath)
    logger.info(msg="Adding assist method nodes")
    databaseBuilder.add_assist_methods()
    logger.info(msg="Adding event body part nodes")
    databaseBuilder.add_event_body_parts()
    logger.info(msg="Adding event situation nodes")
    databaseBuilder.add_event_situations()
    logger.info(msg="Adding pitch location nodes")
    databaseBuilder.add_pitch_locations()
    logger.info(msg="Adding shot outcome nodes")
    databaseBuilder.add_shot_outcomes()
    logger.info(msg="Adding shot placement nodes")
    databaseBuilder.add_shot_placements()

    def _get_unique_names(names: Iterable[str]):
        return {v for v in names if v is not None}

    playerNames = _get_unique_names(names=matchEventsDataframe["player"])
    player2Names = _get_unique_names(names=matchEventsDataframe["player2"])
    playerInNames = _get_unique_names(names=matchEventsDataframe["player_in"])
    playerOutNames = _get_unique_names(names=matchEventsDataframe["player_out"])
    allPlayerNames = set.union(
        *(playerNames, player2Names, playerInNames, playerOutNames)
    )
    playerToIdMap = {player: index for index, player in enumerate(allPlayerNames)}
    logger.info(msg="Adding player nodes")
    databaseBuilder.add_players(playerToIdMap=playerToIdMap)

    # remapping is done inside the DB builder with dicts, because remapping the whole DF with pandas can be memory-intensive
    logger.info(msg="Adding football event nodes and relations")
    databaseBuilder.add_football_events(
        remappedEventData=matchEventsDataframe,
        teamToIdMap=teamToIdMap,
        playerToIdMap=playerToIdMap,
    )


if __name__ == "__main__":
    fire.Fire(process_all_files_for_neo4j_import)
