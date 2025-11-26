PERIOD_STARTS = {
    1: 0,
    2: 45,
    3: 90,
    4: 105,
}

# settings event type
PASS_EVENT = 'pass'
SHOT_EVENT = 'shot'
GENERIC_EVENT = 'generic'

FPS = 10

# idx in formatted_data
IDX_SPEED = 0
IDX_VX = 1
IDX_VY = 2
IDX_ACC = 3
IDX_ACC_TO_REFINE = 4

IMPOSSIBLE_SPEED_TH = 10.5  # in m/s


# SPEED / ACC INFO
SMOOTHING_SPEED = 2
SMOOTHING_ACC = 8
SMOOTHING_ACC_TO_REFINE = 2

# OFFSET MANAGER INFO
DEFAULT_START = {1: 0, 2: 27000, 3: 54000, 4: 63000}
# TH to determine if a player is close to the ball
TH_DIST_PLY_BALL = 2.5  # in meters
# Minimum pass per period by player to determine an offset
MIN_PASS_PER_PERIOD = {1: 10, 2: 10, 3: 5, 4: 5}

# TH to set the is_matched flag
TH_IS_MATCHED = 3.5  # in meters
