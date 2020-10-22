import pandas as pd

from datamodel.fields_and_labels import NodeField
from datamodel.fields_and_labels import NodeLabel
from datamodel.fields_and_labels import RelationType
from datamodel.node_ids import CountryId
from datamodel.node_ids import DateId
from datamodel.node_ids import LeagueId
from datamodel.node_ids import MatchId
from datamodel.node_ids import MatchEventId
from datamodel.node_ids import MonthId
from datamodel.node_ids import PlayerId
from datamodel.node_ids import SeasonId
from datamodel.node_ids import TeamId
from datamodel.node_ids import YearId
from store.graph_output_handlers.output_handler_base import NodeOutputHandlerBase
from store.graph_output_handlers.output_handler_base import RelationOutputHandlerBase


class GraphDatabaseBuilder:

    def __init__(self, nodeOutputHandler: NodeOutputHandlerBase, relationsOutputHandler: RelationOutputHandlerBase):
        self.nodeOutputHandler = nodeOutputHandler
        self.relationsOutputHandler = relationsOutputHandler

    def close(self):
        self.nodeOutputHandler.close()
        self.relationsOutputHandler.close()

    def add_leagues(self, leagueToIdMap: dict):
        for leagueName, i in leagueToIdMap.items():
            self.nodeOutputHandler.add(nodeId=LeagueId(leagueId=i), nodeLabels=[NodeLabel.LEAGUE], nodeProperties={NodeField.TEXT: leagueName})

    def add_countries(self, countryToIdMap: dict):
        for countryName, i in countryToIdMap.items():
            self.nodeOutputHandler.add(nodeId=CountryId(countryId=i), nodeLabels=[NodeLabel.COUNTRY], nodeProperties={NodeField.TEXT: countryName})

    def add_seasons(self, seasonToIdMap: dict):
        for season, i in seasonToIdMap.items():
            self.nodeOutputHandler.add(nodeId=SeasonId(seasonId=i), nodeLabels=[NodeLabel.SEASON], nodeProperties={NodeField.TEXT: season})

    def add_dates(self, allDates: iter):
        # dates are split into sets to try and avoid node collisions if they've already been added.
        # Time-tree method for temporal graphs: https://graphaware.com/neo4j/2014/08/20/graphaware-neo4j-timetree.html
        splitDates = {date.split('-') for date in allDates}
        allYears = {d[0] for d in splitDates}
        allMonths = {(d[0], d[1]) for d in splitDates}
        for year in allYears:
            self.nodeOutputHandler.add(nodeId=YearId(year=year), nodeLabels=[NodeLabel.YEAR], nodeProperties={})
        for year, month in allMonths:
            self.nodeOutputHandler.add(nodeId=MonthId(year=year, month=month), nodeLabels=[NodeLabel.MONTH], nodeProperties={})
        for year, month, day in splitDates:
            self.nodeOutputHandler.add(nodeId=DateId(year=year, month=month, day=day), nodeLabels=[NodeLabel.DATE], nodeProperties={})
            self.relationsOutputHandler.add(startNodeId=MonthId(year=year, month=month), endNodeId=YearId(year=year), relationType=RelationType.IN_YEAR)
            self.relationsOutputHandler.add(startNodeId=DateId(year=year, month=month, day=day), endNodeId=MonthId(year=year, month=month), relationType=RelationType.IN_MONTH)

    def add_teams(self, teamToIdMap: dict):
        for team, i in teamToIdMap.items():
            self.nodeOutputHandler.add(nodeId=TeamId(teamId=i), nodeLabels=[NodeLabel.ENTITY, NodeLabel.TEAM], nodeProperties={NodeField.TEXT: team})

    def add_players(self, playerToIdMap: dict):
        for player, i in playerToIdMap.items():
            self.nodeOutputHandler.add(nodeId=PlayerId(playerId=i), nodeLabels=[NodeLabel.ENTITY, NodeLabel.PLAYER], nodeProperties={NodeField.TEXT: player})

    def add_football_matches(self, remappedMetadata: pd.DataFrame):
        lastMatchForTeam = {}
        for _, metadataRow in remappedMetadata.iterrows():
            nodeProperties = {
                NodeField.FULLTIME_HOME_GOALS: metadataRow['fthg'],
                NodeField.FULLTIME_AWAY_GOALS: metadataRow['ftag'],
                NodeField.HOME_ODDS: metadataRow['odd_h'],
                NodeField.AWAY_ODDS: metadataRow['odd_a'],
                NodeField.DRAW_ODDS: metadataRow['odd_d'],
                NodeField.OVER_25_GOAL_ODDS: metadataRow['odd_over'],
                NodeField.UNDER_25_GOAL_ODDS: metadataRow['odd_under'],
                NodeField.BOTH_TEAMS_TO_SCORE_ODDS: metadataRow['odd_bts'],
                NodeField.NOT_BOTH_TEAMS_TO_SCORE_ODDS: metadataRow['odd_bts_n']
            }
            matchId = MatchId(matchId=metadataRow['id_odsp'])
            self.nodeOutputHandler.add(nodeId=matchId, nodeLabels=[NodeLabel.MATCH], nodeProperties=nodeProperties)

            year, month, day = metadataRow.split(metadataRow['date'])
            dateId = DateId(year=year, month=month, day=day)
            self.relationsOutputHandler.add(startNodeId=dateId, endNodeId=matchId, relationType=RelationType.ON_DATE)
            seasonId = SeasonId(seasonId=metadataRow['seasonId'])
            self.relationsOutputHandler.add(startNodeId=seasonId, endNodeId=matchId, relationType=RelationType.IN_SEASON)

            homeTeamId = TeamId(teamId=metadataRow['homeTeamId'])
            awayTeamId = TeamId(teamId=metadataRow['awayTeamId'])
            self.relationsOutputHandler.add(startNodeId=matchId, endNodeId=homeTeamId, relationType=RelationType.HOME_TEAM)
            self.relationsOutputHandler.add(startNodeId=matchId, endNodeId=awayTeamId, relationType=RelationType.AWAY_TEAM)
            previousHomeMatch = lastMatchForTeam.get(homeTeamId)
            if previousHomeMatch is not None:
                self.relationsOutputHandler.add(startNodeId=previousHomeMatch, endNodeId=matchId, relationType=RelationType.NEXT)
            previousAwayMatch = lastMatchForTeam.get(awayTeamId)
            if previousAwayMatch is not None:
                self.relationsOutputHandler.add(startNodeId=previousAwayMatch, endNodeId=matchId, relationType=RelationType.NEXT)

            leagueId = LeagueId(leagueId=metadataRow['leagueId'])
            countryId = CountryId(countryId=metadataRow['countryId'])
            self.relationsOutputHandler.add(startNodeId=matchId, endNodeId=leagueId, relationType=RelationType.IN_LEAGUE)
            self.relationsOutputHandler.add(startNodeId=leagueId, endNodeId=countryId, relationType=RelationType.IN_COUNTRY)

    def add_football_events(self):
        raise NotImplementedError('TODO!')
