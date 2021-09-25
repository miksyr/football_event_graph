class BaseNodeId:
    class Letters:

        COUNTRY = "C"
        LEAGUE = "L"
        EVENT_CONTEXT = "EC"
        MATCH = "M"
        MATCH_EVENT = "MEV"
        PLAYER = "P"
        SEASON = "S"
        TEAM = "TEAM"
        TIME_DIVISION = "T"

    def __init__(self, letter, *args):
        self.letter = letter
        self.ids = args

    def __str__(self):
        return f'{self.letter}{"_".join([str(value) for value in self.ids])}'


class CountryId(BaseNodeId):
    def __init__(self, countryId):
        super().__init__(BaseNodeId.Letters.COUNTRY, countryId)


class EventContextId(BaseNodeId):
    def __init__(self, eventType, eventId):
        super().__init__(BaseNodeId.Letters.EVENT_CONTEXT, eventType, eventId)


class LeagueId(BaseNodeId):
    def __init__(self, leagueId):
        super().__init__(BaseNodeId.Letters.LEAGUE, leagueId)


class MatchId(BaseNodeId):
    def __init__(self, matchId):
        super().__init__(BaseNodeId.Letters.MATCH, matchId)


class MatchEventId(BaseNodeId):
    def __init__(self, matchEventId):
        super().__init__(BaseNodeId.Letters.MATCH_EVENT, matchEventId)


class PlayerId(BaseNodeId):
    def __init__(self, playerId):
        super().__init__(BaseNodeId.Letters.PLAYER, playerId)


class SeasonId(BaseNodeId):
    def __init__(self, seasonId):
        super().__init__(BaseNodeId.Letters.SEASON, seasonId)


class TeamId(BaseNodeId):
    def __init__(self, teamId):
        super().__init__(BaseNodeId.Letters.TEAM, teamId)


class TimeType:

    YEAR = "Y"
    MONTH = "M"
    DATE = "D"


class YearId(BaseNodeId):
    def __init__(self, year):
        super().__init__(BaseNodeId.Letters.TIME_DIVISION, TimeType.YEAR, year)


class MonthId(BaseNodeId):
    def __init__(self, year, month):
        super().__init__(BaseNodeId.Letters.TIME_DIVISION, TimeType.MONTH, year, month)


class DateId(BaseNodeId):
    def __init__(self, year, month, day):
        super().__init__(
            BaseNodeId.Letters.TIME_DIVISION, TimeType.DATE, year, month, day
        )
