from typing import Dict, Iterable

import pandas as pd
import math
from tqdm.auto import tqdm

from datamodel.existing_data_maps.assist_method_map import idToAssistMethodMap
from datamodel.existing_data_maps.body_part_map import idToBodyPartMap
from datamodel.existing_data_maps.event_situation import idToEventSituationMap
from datamodel.existing_data_maps.event_types import idToEventTypeMap
from datamodel.existing_data_maps.pitch_location_map import idToPitchLocationMap
from datamodel.existing_data_maps.shot_outcome import idToShotOutcomeMap
from datamodel.existing_data_maps.shot_placement import idToShotPlacementMap
from datamodel.node_field import NodeField
from datamodel.node_ids import CountryId
from datamodel.node_ids import DateId
from datamodel.node_ids import EventContextId
from datamodel.node_ids import LeagueId
from datamodel.node_ids import MatchId
from datamodel.node_ids import MatchEventId
from datamodel.node_ids import MonthId
from datamodel.node_ids import PlayerId
from datamodel.node_ids import SeasonId
from datamodel.node_ids import TeamId
from datamodel.node_ids import YearId
from datamodel.node_labels import NodeLabel
from datamodel.relations import EventRelationType
from datamodel.relations import GeneralRelationType
from store.graph_output_handlers.output_handler_base import NodeOutputHandlerBase
from store.graph_output_handlers.output_handler_base import RelationOutputHandlerBase
from utils.dataframe_functions import replace_nan_with_none_in_dataframe


class GraphDatabaseBuilder:
    def __init__(
        self,
        nodeOutputHandler: NodeOutputHandlerBase,
        relationsOutputHandler: RelationOutputHandlerBase,
    ):
        self.nodeOutputHandler = nodeOutputHandler
        self.relationsOutputHandler = relationsOutputHandler

    def close(self) -> None:
        self.nodeOutputHandler.close()
        self.relationsOutputHandler.close()

    def add_leagues(self, leagueToIdMap: Dict[str, int]) -> None:
        for leagueName, i in leagueToIdMap.items():
            self.nodeOutputHandler.add(
                nodeId=LeagueId(leagueId=i),
                nodeLabels=[NodeLabel.LEAGUE],
                nodeProperties={NodeField.TEXT: leagueName},
            )

    def add_countries(self, countryToIdMap: Dict[str, int]) -> None:
        for countryName, i in countryToIdMap.items():
            self.nodeOutputHandler.add(
                nodeId=CountryId(countryId=i),
                nodeLabels=[NodeLabel.COUNTRY],
                nodeProperties={NodeField.TEXT: countryName},
            )

    def add_seasons(self, seasonToIdMap: Dict[str, int]) -> None:
        for season, i in seasonToIdMap.items():
            self.nodeOutputHandler.add(
                nodeId=SeasonId(seasonId=i),
                nodeLabels=[NodeLabel.SEASON],
                nodeProperties={NodeField.TEXT: season},
            )

    def add_dates(self, allDates: Iterable[str]) -> None:
        # dates are split into sets to try and avoid node collisions if they've already been added.
        # Time-tree method for temporal graphs: https://graphaware.com/neo4j/2014/08/20/graphaware-neo4j-timetree.html
        splitDates = {tuple(date.split("-")) for date in allDates}
        allYears = {d[0] for d in splitDates}
        allMonths = {(d[0], d[1]) for d in splitDates}
        for year in allYears:
            self.nodeOutputHandler.add(
                nodeId=YearId(year=year), nodeLabels=[NodeLabel.YEAR], nodeProperties={NodeField.TEXT: f"{year}"}
            )
        for year, month in allMonths:
            self.nodeOutputHandler.add(
                nodeId=MonthId(year=year, month=month),
                nodeLabels=[NodeLabel.MONTH],
                nodeProperties={NodeField.TEXT: f"{year}-{month}"},
            )
            self.relationsOutputHandler.add(
                    startNodeId=MonthId(year=year, month=month),
                    endNodeId=YearId(year=year),
                    relationType=GeneralRelationType.IN_YEAR,
                )
        for year, month, day in splitDates:
            self.nodeOutputHandler.add(
                nodeId=DateId(year=year, month=month, day=day),
                nodeLabels=[NodeLabel.DATE],
                nodeProperties={NodeField.TEXT: f"{year}-{month}-{day}"},
            )
            self.relationsOutputHandler.add(
                startNodeId=DateId(year=year, month=month, day=day),
                endNodeId=MonthId(year=year, month=month),
                relationType=GeneralRelationType.IN_MONTH,
            )

    def add_teams(self, teamToIdMap: Dict[str, int]) -> None:
        for team, i in teamToIdMap.items():
            self.nodeOutputHandler.add(
                nodeId=TeamId(teamId=i),
                nodeLabels=[NodeLabel.ENTITY, NodeLabel.TEAM],
                nodeProperties={NodeField.TEXT: team},
            )

    def add_football_matches(self, remappedMetadata: pd.DataFrame) -> None:
        remappedMetadata = replace_nan_with_none_in_dataframe(
            dataframe=remappedMetadata
        )
        lastMatchForTeam = {}
        for _, metadataRow in tqdm(
            remappedMetadata.iterrows(), total=len(remappedMetadata)
        ):
            nodeProperties = {
                NodeField.FULLTIME_HOME_GOALS: metadataRow["fthg"],
                NodeField.FULLTIME_AWAY_GOALS: metadataRow["ftag"],
                NodeField.HOME_ODDS: metadataRow["odd_h"],
                NodeField.AWAY_ODDS: metadataRow["odd_a"],
                NodeField.DRAW_ODDS: metadataRow["odd_d"],
                NodeField.OVER_25_GOAL_ODDS: metadataRow["odd_over"],
                NodeField.UNDER_25_GOAL_ODDS: metadataRow["odd_under"],
                NodeField.BOTH_TEAMS_TO_SCORE_ODDS: metadataRow["odd_bts"],
                NodeField.NOT_BOTH_TEAMS_TO_SCORE_ODDS: metadataRow["odd_bts_n"],
            }
            matchId = MatchId(matchId=metadataRow["id_odsp"])
            self.nodeOutputHandler.add(
                nodeId=matchId,
                nodeLabels=[NodeLabel.MATCH],
                nodeProperties=nodeProperties,
            )

            year, month, day = metadataRow["date"].split("-")
            dateId = DateId(year=year, month=month, day=day)
            self.relationsOutputHandler.add(
                startNodeId=dateId,
                endNodeId=matchId,
                relationType=GeneralRelationType.ON_DATE,
            )
            seasonId = SeasonId(seasonId=metadataRow["season"])
            self.relationsOutputHandler.add(
                startNodeId=seasonId,
                endNodeId=matchId,
                relationType=GeneralRelationType.IN_SEASON,
            )

            homeTeamId = TeamId(teamId=metadataRow["ht"])
            awayTeamId = TeamId(teamId=metadataRow["at"])
            self.relationsOutputHandler.add(
                startNodeId=matchId,
                endNodeId=homeTeamId,
                relationType=GeneralRelationType.HOME_TEAM,
            )
            self.relationsOutputHandler.add(
                startNodeId=matchId,
                endNodeId=awayTeamId,
                relationType=GeneralRelationType.AWAY_TEAM,
            )
            previousHomeMatch = lastMatchForTeam.get(homeTeamId)
            if previousHomeMatch is not None:
                self.relationsOutputHandler.add(
                    startNodeId=previousHomeMatch,
                    endNodeId=matchId,
                    relationType=GeneralRelationType.NEXT,
                )
            previousAwayMatch = lastMatchForTeam.get(awayTeamId)
            if previousAwayMatch is not None:
                self.relationsOutputHandler.add(
                    startNodeId=previousAwayMatch,
                    endNodeId=matchId,
                    relationType=GeneralRelationType.NEXT,
                )

            leagueId = LeagueId(leagueId=metadataRow["league"])
            countryId = CountryId(countryId=metadataRow["country"])
            self.relationsOutputHandler.add(
                startNodeId=matchId,
                endNodeId=leagueId,
                relationType=GeneralRelationType.IN_LEAGUE,
            )
            self.relationsOutputHandler.add(
                startNodeId=leagueId,
                endNodeId=countryId,
                relationType=GeneralRelationType.IN_COUNTRY,
            )

    def add_assist_methods(self) -> None:
        for i, assistMethod in idToAssistMethodMap.items():
            self.nodeOutputHandler.add(
                nodeId=EventContextId(eventType=assistMethod, eventId=i),
                nodeLabels=[NodeLabel.MATCH_EVENT_CONTEXT],
                nodeProperties={NodeField.TEXT: assistMethod},
            )

    def add_event_body_parts(self) -> None:
        for i, bodyPart in idToBodyPartMap.items():
            self.nodeOutputHandler.add(
                nodeId=EventContextId(eventType=bodyPart, eventId=i),
                nodeLabels=[NodeLabel.MATCH_EVENT_CONTEXT],
                nodeProperties={NodeField.TEXT: bodyPart},
            )

    def add_event_situations(self) -> None:
        for i, eventSituation in idToEventSituationMap.items():
            self.nodeOutputHandler.add(
                nodeId=EventContextId(eventType=eventSituation, eventId=i),
                nodeLabels=[NodeLabel.MATCH_EVENT_CONTEXT],
                nodeProperties={NodeField.TEXT: eventSituation},
            )

    def add_pitch_locations(self) -> None:
        for i, pitchLocation in idToPitchLocationMap.items():
            self.nodeOutputHandler.add(
                nodeId=EventContextId(eventType=pitchLocation, eventId=i),
                nodeLabels=[NodeLabel.MATCH_EVENT_CONTEXT],
                nodeProperties={NodeField.TEXT: pitchLocation},
            )

    def add_players(self, playerToIdMap: Dict[str, int]) -> None:
        for player, i in playerToIdMap.items():
            self.nodeOutputHandler.add(
                nodeId=PlayerId(playerId=i),
                nodeLabels=[NodeLabel.ENTITY, NodeLabel.PLAYER],
                nodeProperties={NodeField.TEXT: player},
            )

    def add_shot_outcomes(self) -> None:
        for i, shotOutcome in idToShotOutcomeMap.items():
            self.nodeOutputHandler.add(
                nodeId=EventContextId(eventType=shotOutcome, eventId=i),
                nodeLabels=[NodeLabel.MATCH_EVENT_CONTEXT],
                nodeProperties={NodeField.TEXT: shotOutcome},
            )

    def add_shot_placements(self) -> None:
        for i, shotPlacement in idToShotPlacementMap.items():
            self.nodeOutputHandler.add(
                nodeId=EventContextId(eventType=shotPlacement, eventId=i),
                nodeLabels=[NodeLabel.MATCH_EVENT_CONTEXT],
                nodeProperties={NodeField.TEXT: shotPlacement},
            )

    def add_football_events(
        self,
        remappedEventData: pd.DataFrame,
        teamToIdMap: Dict[str, int],
        playerToIdMap: Dict[str, int],
    ) -> None:
        remappedEventData = replace_nan_with_none_in_dataframe(
            dataframe=remappedEventData
        )
        for _, row in tqdm(remappedEventData.iterrows(), total=len(remappedEventData)):
            matchId = MatchId(matchId=row["id_odsp"])
            matchEventId = MatchEventId(matchEventId=row["id_event"])
            eventType1 = idToEventTypeMap.get(row["event_type"], None)
            eventType2 = idToEventTypeMap.get(row["event_type2"], None)
            matchEventNodeLabels = [NodeLabel.MATCH_EVENT, eventType1, eventType2]
            self.nodeOutputHandler.add(
                nodeId=matchEventId,
                nodeLabels=[v for v in matchEventNodeLabels if v is not None],
                nodeProperties={
                    NodeField.IS_FAST_BREAK: bool(row["fast_break"]),
                    NodeField.IS_GOAL: bool(row["is_goal"]),
                    NodeField.TEXT: row["text"],
                    NodeField.MATCH_EVENT_TIME: row["time"],
                    NodeField.SORT_ORDER: row["sort_order"],
                },
            )
            self.relationsOutputHandler.add(
                startNodeId=matchId,
                endNodeId=matchEventId,
                relationType=GeneralRelationType.HAS_MATCH_EVENT,
            )
            self.relationsOutputHandler.add(
                startNodeId=matchEventId,
                endNodeId=TeamId(teamId=int(teamToIdMap[row["event_team"]])),
                relationType=EventRelationType.EVENT_TEAM,
            )
            self.relationsOutputHandler.add(
                startNodeId=matchEventId,
                endNodeId=TeamId(teamId=int(teamToIdMap[row["opponent"]])),
                relationType=EventRelationType.OPPONENT_TEAM,
            )
            # player + player2 pairs are always distinct from playerIn + playerOut pairs
            if row["player"] is not None:
                self.relationsOutputHandler.add(
                    startNodeId=matchEventId,
                    endNodeId=PlayerId(playerId=int(playerToIdMap[row["player"]])),
                    relationType=EventRelationType.PLAYER_1,
                )
            if row["player2"] is not None:
                self.relationsOutputHandler.add(
                    startNodeId=matchEventId,
                    endNodeId=PlayerId(playerId=int(playerToIdMap[row["player2"]])),
                    relationType=EventRelationType.PLAYER_2,
                )
            if row["player_in"] is not None:
                self.relationsOutputHandler.add(
                    startNodeId=matchEventId,
                    endNodeId=PlayerId(playerId=int(playerToIdMap[row["player_in"]])),
                    relationType=EventRelationType.PLAYER_1,
                )
            if row["player_out"] is not None:
                self.relationsOutputHandler.add(
                    startNodeId=matchEventId,
                    endNodeId=PlayerId(playerId=int(playerToIdMap[row["player_out"]])),
                    relationType=EventRelationType.PLAYER_2,
                )

            if (row["shot_place"] is not None) and not(math.isnan(row["shot_place"])):
                shotPlacementId = int(row["shot_place"])
                self.relationsOutputHandler.add(
                    startNodeId=matchEventId,
                    endNodeId=EventContextId(
                        eventType=idToShotPlacementMap[shotPlacementId],
                        eventId=shotPlacementId,
                    ),
                    relationType=EventRelationType.SHOT_PLACEMENT,
                )

            if (row["shot_outcome"] is not None) and not(math.isnan(row["shot_outcome"])):
                shotOutcomeId = int(row["shot_outcome"])
                self.relationsOutputHandler.add(
                    startNodeId=matchEventId,
                    endNodeId=EventContextId(
                        eventType=idToShotOutcomeMap[shotOutcomeId],
                        eventId=shotOutcomeId,
                    ),
                    relationType=EventRelationType.SHOT_OUTCOME,
                )

            if (row["location"] is not None) and not(math.isnan(row["location"])):
                pitchLocationId = int(row["location"])
                self.relationsOutputHandler.add(
                    startNodeId=matchEventId,
                    endNodeId=EventContextId(
                        eventType=idToPitchLocationMap[pitchLocationId],
                        eventId=pitchLocationId,
                    ),
                    relationType=EventRelationType.PITCH_LOCATION,
                )

            if (row["bodypart"] is not None) and not(math.isnan(row["bodypart"])):
                bodyPartId = int(row["bodypart"])
                self.relationsOutputHandler.add(
                    startNodeId=matchEventId,
                    endNodeId=EventContextId(
                        eventType=idToBodyPartMap[bodyPartId], eventId=bodyPartId
                    ),
                    relationType=EventRelationType.BODY_PART,
                )

            if row["assist_method"] is not None:
                assistMethodId = int(row["assist_method"])
                self.relationsOutputHandler.add(
                    startNodeId=matchEventId,
                    endNodeId=EventContextId(
                        eventType=idToAssistMethodMap[assistMethodId],
                        eventId=assistMethodId,
                    ),
                    relationType=EventRelationType.ASSIST_METHOD,
                )

            if (row["situation"] is not None) and not(math.isnan(row["situation"])):
                eventSituationId = int(row["situation"])
                self.relationsOutputHandler.add(
                    startNodeId=matchEventId,
                    endNodeId=EventContextId(
                        eventType=idToEventSituationMap[eventSituationId],
                        eventId=eventSituationId,
                    ),
                    relationType=EventRelationType.EVENT_SITUATION,
                )
