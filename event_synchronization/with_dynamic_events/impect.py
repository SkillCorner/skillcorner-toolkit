from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from event_synchronization.constants import PERIOD_STARTS
from event_synchronization.events_utils.impect import normalize_impect_response
from event_synchronization.with_dynamic_events.offset_manager import OffsetSyncManager
from event_synchronization.with_dynamic_events.utils import (
    apply_matching_duels,
    apply_output_format,
    match_bundle,
    preprocess_prov_events,
)

RM_IMP_TYPES = [
    'OUT',
    'GK_CATCH',
    'GK_SAVE',
    'THROW_IN',
    'GOAL_KICK',
    'CORNER',
    'FREE_KICK',
    'PENALTY',
    'KICK_OFF',
    'FINAL_WHISTLE',
    'NO_VIDEO',
    'FOUL',
]


class ImpectSyncDynamicEventsManager:
    def __init__(self, skc_events: pd.DataFrame, raw_impect_events: pd.DataFrame, impect_events_manager: Any) -> None:  # noqa: ANN401
        self.skc_events = skc_events
        self.raw_impect_events = normalize_impect_response(raw_impect_events)
        self.impect_events_manager = impect_events_manager

    def enrich_impect_events(self) -> pd.DataFrame:
        impect_events = pd.json_normalize(self.raw_impect_events, max_level=1)
        impect_events.columns = [c.replace('.', '_') for c in impect_events.columns]
        impect_events['skc_player_id'] = impect_events['player_id'].apply(
            lambda x: self.impect_events_manager.impect_ply_id_to_skc_ply_id.get(x)
        )
        impect_events = impect_events.query('periodId in [1, 2, 3, 4]').copy()
        impect_events['skc_frame'] = impect_events.apply(
            lambda x: np.round(10 * self.impect_events_manager.get_timestamp(x)), axis=1
        )
        impect_events = impect_events.rename(columns={'periodId': 'period'})
        impect_events['skc_frame'] = impect_events.apply(
            lambda x: x.skc_frame + 10 * 60 * PERIOD_STARTS.get(x.period), axis=1
        )
        return impect_events

    def get_impect_passes(self, impect_events: pd.DataFrame) -> pd.DataFrame:
        impect_passes = impect_events[(impect_events['actionType'] == 'PASS')]
        return impect_passes[['id', 'skc_frame', 'skc_player_id', 'period']]

    def run(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Unified interface for StatsBomb dynamic events."""

        imp_events_enriched = self.enrich_impect_events()
        imp_passes = self.get_impect_passes(imp_events_enriched)
        imp_events_enriched = OffsetSyncManager(imp_passes, imp_events_enriched, self.skc_events).run()

        imp_events_sync, imp_events_to_match, pp_sorted, obe_sorted = preprocess_prov_events(
            imp_events_enriched, self.skc_events, 'impect'
        )
        # Init tracking of used events
        used_pp_ids = set()
        duels_pp_used_ids = set()

        # MATCH PP, DUELS
        pp_bundles = [
            match_bundle(row, imp_events_to_match, used_pp_ids, 'impect', duels_pp_used_ids)
            for _, row in pp_sorted.iterrows()
        ]
        mapping_pp_bundles = pd.DataFrame(pp_bundles)

        matched_duels, _ = apply_matching_duels(imp_events_sync, obe_sorted, duels_pp_used_ids, 'impect')

        skc_events_mapping, provider_events_mapping = apply_output_format(
            imp_events_sync, self.skc_events, mapping_pp_bundles, matched_duels, 'impect'
        )

        return skc_events_mapping, provider_events_mapping
