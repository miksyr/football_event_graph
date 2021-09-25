from datamodel.node_labels import NodeLabel

# TODO: turn into standardized handler

idToEventTypeMap = {
    0: NodeLabel.ANNOUNCEMENT,
    1: NodeLabel.SHOT_ATTEMPT,
    2: NodeLabel.CORNER,
    3: NodeLabel.FOUL,
    4: NodeLabel.YELLOW_CARD,
    5: NodeLabel.SECOND_YELLOW_CARD,
    6: NodeLabel.RED_CARD,
    7: NodeLabel.SUBSTITUTION,
    8: NodeLabel.FREE_KICK_WON,
    9: NodeLabel.OFFSIDE,
    10: NodeLabel.HANDBALL,
    11: NodeLabel.PENALTY_CONCEDED,
    12: NodeLabel.KEY_PASS,
    13: NodeLabel.FAILED_THROUGH_BALL,
    14: NodeLabel.SENDING_OFF,
    15: NodeLabel.OWN_GOAL,
}
