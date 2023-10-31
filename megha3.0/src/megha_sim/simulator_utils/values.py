"""
File for constants and name constants.

This file contains the various constants and named constants used throughout\
 the project.
"""

from typing_extensions import Final, Literal
from enum import Enum, unique
import sys
import os


@unique
class InconsistencyType(Enum):
    """
    Enum for expressing the type of inconsistency event.

    This enum contains the named constants for identifying the type of \
    inconsistency event.

    Args:
        Enum (Enum): The `Enum` parent class
    """

    INTERNAL_INCONSISTENCY = Literal[0]
    EXTERNAL_INCONSISTENCY = Literal[1]

###############################################################################


LM_HEARTBEAT_INTERVAL = 5
NETWORK_DELAY = 0.0005  # Same as the Sparrow simulator

TASK_FILE="task_stats_"+(os.path.basename(sys.argv[1])
                                      .split("_")[-1])+".csv"
###############################################################################


class TaskDurationDistributions:
    """
    Named constants for selecting the Task Duration Distributions.

    Named constants for expressing the type of distribution to consider for \
    the task duration.
    """

    CONSTANT: Final[int] = 0
    MEAN: Final[int] = 1
    FROM_FILE: Final[int] = 2


class EstimationErrorDistribution:
    """Named constants for expressing the distribution of estimated error."""

    CONSTANT: Final[int] = 0
    RANDOM: Final[int] = 1
    MEAN: Final[int] = 2
