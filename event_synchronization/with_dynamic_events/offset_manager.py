import numpy as np
import pandas as pd
from scipy import signal

from event_synchronization.constants import DEFAULT_START, MIN_PASS_PER_PERIOD

SEARCH_OFFSET = 25


class OffsetSyncManager:
    """
    A manager class responsible for handling the period start offset synchronization process.
    """

    def __init__(self, provider_passes: pd.DataFrame, provider_events: pd.DataFrame, skc_events: pd.DataFrame) -> None:
        self.provider_passes = provider_passes
        self.provider_events = provider_events
        self.skc_events = skc_events.rename(columns={'player_id': 'skc_player_id'})
        self.skc_passes = self.get_skc_passes()

    def get_skc_passes(self) -> pd.DataFrame:
        skc_passes = self.skc_events.query('event_type_id==8 and end_type in ["pass", "clearance"]').copy()
        return skc_passes[['event_id', 'frame_start', 'frame_end', 'skc_player_id', 'period']]

    def get_period_start_estimation(self, period: int) -> int:
        """
        Estimate the period start by applying a convolution between provider and SKC passes

        Args:
            period (int): period to sync
        """

        provider_passes_period = self.provider_passes.query('period==@period')
        skc_passes_period = self.skc_passes.query('period==@period')
        player_id_list = provider_passes_period['skc_player_id'].unique()

        # get default frame start
        DEFAULT_START[period]

        # apply convolution to estimate period start
        estimated_start_by_player = []
        for ply_id in player_id_list:  # noqa: B007
            skc_passes_frames_ply = skc_passes_period.query('skc_player_id==@ply_id')['frame_end'].values.astype(int)
            provider_passes_frames_ply = provider_passes_period.query('skc_player_id==@ply_id')[
                'skc_frame'
            ].values.astype(int)

            # filter-only take ply with more than MIN_PASS_PER_PERIOD
            if len(skc_passes_frames_ply) > MIN_PASS_PER_PERIOD[period]:
                skc_events_ply = np.bincount(skc_passes_frames_ply)
                provider_events_ply = np.bincount(provider_passes_frames_ply)

                # apply convolution
                discrete_linear_convolution_arr = signal.fftconvolve(
                    skc_events_ply, provider_events_ply[::-1], mode='full'
                )

                # get offset
                offset = np.argmax(discrete_linear_convolution_arr) - len(provider_events_ply) + 1
                estimated_start_by_player.append(offset)
        return int(np.percentile(estimated_start_by_player, 50))

    def apply_offset(self, period: int, offset: int) -> None:
        """Apply offset to all events of a given period

        Args:
            period (int): period to sync
            offset (int): offset to apply
        """
        self.provider_passes.loc[self.provider_passes['period'] == period, 'skc_frame'] += offset
        self.provider_events.loc[self.provider_events['period'] == period, 'skc_frame'] += offset

    def run(self) -> tuple:
        """Run the offset synchronization process."""
        for period in np.unique(self.skc_passes['period']):
            offset = self.get_period_start_estimation(period)
            self.apply_offset(period, offset)
        return self.provider_events
