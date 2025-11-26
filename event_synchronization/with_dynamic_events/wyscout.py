from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from event_synchronization.constants import PERIOD_STARTS
from event_synchronization.events_utils.wyscout import get_offsets_periods
from event_synchronization.with_dynamic_events.offset_manager import OffsetSyncManager
from event_synchronization.with_dynamic_events.utils import (
    apply_matching_duels,
    apply_output_format,
    match_bundle,
    preprocess_prov_events,
)

RM_WS_TYPES = [
    'corner',
    'free_kick',
    'goal_kick',
    'throw_in',
    'goalkeeper_exit',
    'shot_against',
    'game_interruption',
]


class WyscoutSyncDynamicEventsManager:
    def __init__(self, skc_events: pd.DataFrame, raw_wyscout_events: dict, wyscout_events_manager: Any) -> None:  # noqa: ANN401
        self.skc_events = skc_events
        self.raw_wyscout_events = raw_wyscout_events
        self.wyscout_events_manager = wyscout_events_manager

    def enrich_wyscout_events(self) -> pd.DataFrame:
        """Enrich wyscout events with skc_player_id and skc_frame"""
        wyscout_events = pd.json_normalize(self.raw_wyscout_events['events'], max_level=3)
        wyscout_events.columns = [c.replace('.', '_') for c in wyscout_events.columns]
        wyscout_events['skc_player_id'] = wyscout_events['player_id'].apply(
            lambda x: self.wyscout_events_manager.wyscout_id_to_skc_id.get(x)
        )
        wyscout_events['period'] = wyscout_events['matchPeriod'].apply(lambda x: int(x[0]))
        wyscout_events = wyscout_events.query('period in [1, 2, 3, 4]').copy()
        offsets_periods, use_match_timestamp = get_offsets_periods(self.raw_wyscout_events['events'])
        wyscout_events['skc_frame'] = wyscout_events.apply(
            lambda x: np.round(
                10
                * self.wyscout_events_manager.get_timestamp(
                    x, offsets_periods.get(x.period), x.period, use_match_timestamp
                )
            ),
            axis=1,
        )
        wyscout_events['skc_frame'] = wyscout_events.apply(
            lambda x: x.skc_frame + 10 * 60 * PERIOD_STARTS.get(x.period), axis=1
        )
        return wyscout_events

    def get_wyscout_passes(self, wyscout_events: pd.DataFrame) -> pd.DataFrame:
        wyscout_passes = wyscout_events[
            (wyscout_events['type_primary'] == 'pass') & (~wyscout_events['skc_player_id'].isna())
        ].copy()
        return wyscout_passes[['id', 'skc_frame', 'skc_player_id', 'period']]

    def add_skc_duel_opponent_id(self, wyscout_events: pd.DataFrame) -> pd.DataFrame:
        map_player_ids = wyscout_events[['skc_player_id', 'player_id']].drop_duplicates()
        # as dictionary
        map_dict = dict(zip(map_player_ids['skc_player_id'], map_player_ids['player_id']))
        inv_map_dict = {v: k for k, v in map_dict.items()}
        wyscout_events['skc_groundDuel_opponent_id'] = wyscout_events['groundDuel_opponent_id'].map(inv_map_dict)
        return wyscout_events

    def run(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        wyscout_events = self.enrich_wyscout_events()
        wyscout_passes = self.get_wyscout_passes(wyscout_events)
        wyscout_events_enriched = OffsetSyncManager(wyscout_passes, wyscout_events, self.skc_events).run()

        wyscout_events_sync, ws_events_to_match, pp_sorted, obe_sorted = preprocess_prov_events(
            wyscout_events_enriched, self.skc_events, 'wyscout'
        )

        # Init tracking of used events
        used_pp_ids = set[int]()
        duels_pp_used_ids = set[int]()

        # MATCH PP, DUELS
        pp_bundles = [
            match_bundle(row, ws_events_to_match, used_pp_ids, 'wyscout', duels_pp_used_ids)
            for _, row in pp_sorted.iterrows()
        ]
        mapping_pp_bundles = pd.DataFrame(pp_bundles)
        matched_duels, _ = apply_matching_duels(wyscout_events_sync, obe_sorted, duels_pp_used_ids, 'wyscout')

        skc_events_mapping, provider_events_mapping = apply_output_format(
            wyscout_events_sync, self.skc_events, mapping_pp_bundles, matched_duels, 'wyscout'
        )

        return skc_events_mapping, provider_events_mapping
