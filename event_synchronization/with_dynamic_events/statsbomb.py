from __future__ import annotations

import numpy as np
import pandas as pd

from event_synchronization.constants import PERIOD_STARTS
from event_synchronization.with_dynamic_events.offset_manager import OffsetSyncManager
from event_synchronization.with_dynamic_events.utils import (
    apply_sb_output_format,
    match_bundle,
    match_duels,
    match_pressures_sb,
    preprocess_sb_events,
)

RM_SB_TYPES = [
    'Ball Receipt*',
    'Starting XI',
    'Half Start',
    'Dribbled Past',
    'Injury Stoppage',
    'Referee Ball-Drop',
    'Foul Won',
    'Substitution',
    'Tactical Shift',
    'Foul Committed',
    'Half End',
    'Player Off',
]

PASS_TYPE_ID = 30


class StatsbombSyncDynamicEventsManager:
    def __init__(
        self, skc_events: pd.DataFrame, raw_statsbomb_events: pd.DataFrame, statsbomb_events: pd.DataFrame
    ) -> None:
        self.skc_events = skc_events
        self.raw_statsbomb_events = raw_statsbomb_events
        self.statsbomb_events = statsbomb_events

    def enrich_sb_events(self) -> pd.DataFrame:
        """Enrich statsbomb events with skc_player_id and skc_frame"""
        sb_events = pd.json_normalize(self.raw_statsbomb_events, max_level=3)
        sb_events.columns = [c.replace('.', '_') for c in sb_events.columns]  # use the best format
        sb_events['skc_player_id'] = sb_events['player_id'].apply(
            lambda x: self.statsbomb_events.stb_ply_id_to_skc_ply_id.get(x)
        )
        sb_events = sb_events.query('period in [1, 2, 3, 4]').copy()  # filter penalty shootouts
        sb_events['skc_frame'] = sb_events.apply(
            lambda x: np.round(10 * self.statsbomb_events.get_timestamp(x)), axis=1
        )
        sb_events['skc_frame'] = sb_events.apply(lambda x: x.skc_frame + 10 * 60 * PERIOD_STARTS.get(x.period), axis=1)
        return sb_events

    def get_sb_passes(self, sb_events: pd.DataFrame) -> pd.DataFrame:
        set_pieces = ['Corner', 'Free Kick', 'Goal Kick', 'Throw-in']
        sb_passes = sb_events[
            (sb_events['type_id'] == PASS_TYPE_ID) & (~sb_events['pass_type_name'].isin(set_pieces))
        ].copy()
        return sb_passes[['id', 'skc_frame', 'skc_player_id', 'period']]

    def retropropagate(self, sb_passes: pd.DataFrame, sb_events: pd.DataFrame) -> pd.DataFrame:
        """Retropropagate event_id to previous Carry and Ball Receipt events of the same player in the same possession."""  # noqa: E501
        sb_events = sb_events.merge(
            sb_passes[['id', 'event_id', 'event_id_other', 'skc_player_id', 'skc_frame']], how='left'
        )
        event_with_skc_id = sb_events.query("event_id==event_id and type_name=='Pass'").copy()
        for _, event in event_with_skc_id.iterrows():
            # if the last Carry event has been done by the same player, copy event_id
            last_carry = sb_events.query("possession==@event.possession and type_name in ['Carry']").copy()
            last_carry = last_carry[last_carry['index'] < event['index']].copy()
            if len(last_carry):
                last_carry = last_carry.iloc[-1]
                if last_carry.player_id == event.player_id:
                    sb_events.loc[sb_events.id == last_carry.id, 'event_id'] = event.event_id

            last_receipt = sb_events.query("possession==@event.possession and type_name in ['Ball Receipt*']").copy()
            last_receipt = last_receipt[last_receipt['index'] < event['index']].copy()
            if len(last_receipt):
                last_receipt = last_receipt.iloc[-1]
                if last_receipt.player_id == event.player_id:
                    sb_events.loc[sb_events.id == last_receipt.id, 'event_id'] = event.event_id
        return sb_events

    def get_percentages(  # noqa: PLR0913
        self,
        mapping_pp_bundles: pd.DataFrame,
        used_pp_ids: set,
        sb_events_sync: pd.DataFrame,
        matched_pressure_sb: pd.DataFrame,
        matched_pressure_skc: pd.DataFrame,
        matched_duels: pd.DataFrame,
        sb_duels: pd.DataFrame,
    ) -> dict:
        pct_pp_matched = (
            (mapping_pp_bundles.query("pattern != 'None'").shape[0] / mapping_pp_bundles.shape[0] * 100)
            if mapping_pp_bundles.shape[0]
            else 0.0
        )
        pct_matched_sb = (
            matched_pressure_sb.dropna(subset=['event_id']).shape[0] / matched_pressure_sb.shape[0] * 100
            if 'event_id' in matched_pressure_sb.columns and matched_pressure_sb.shape[0]
            else 0.0
        )
        pct_matched_obe = (
            matched_pressure_skc.dropna(subset=['id']).shape[0] / matched_pressure_skc.shape[0] * 100
            if 'id' in matched_pressure_skc.columns and matched_pressure_skc.shape[0]
            else 0.0
        )
        pct_matched_duels = matched_duels.shape[0] / sb_duels.shape[0] * 100 if sb_duels.shape[0] else 0.0
        sb_pp_events = set(sb_events_sync.query("type_name not in ['Duel', 'Pressure']")['id'].values)

        pct_matched_pp_ids = round(len(used_pp_ids) / len(sb_pp_events) * 100)

        pressure_confirmed_match = matched_pressure_sb.merge(matched_pressure_skc, on=['event_id', 'id'], how='inner')
        pressure_used_ids = set(pressure_confirmed_match['id'].values)
        pct_matched_pressure_ids = round(len(pressure_used_ids) / matched_pressure_sb.shape[0] * 100)

        return {
            'pct_pp_matched_with_bundle': round(pct_pp_matched, 2),
            'pct_matched_pp_ids': round(pct_matched_pp_ids, 2),
            'pct_matched_sb': round(pct_matched_sb, 2),
            'pct_matched_obe': round(pct_matched_obe, 2),
            'pct_matched_pressure_ids': round(pct_matched_pressure_ids, 2),
            'pct_matched_duels': round(pct_matched_duels, 2),
        }

    def run(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Unified interface for StatsBomb dynamic events."""

        sb_events_enriched = self.enrich_sb_events()
        sb_passes = self.get_sb_passes(sb_events_enriched)
        sb_events_enriched = OffsetSyncManager(sb_passes, sb_events_enriched, self.skc_events).run()

        # Compute mappings
        sb_events_sync, sb_events_to_match, pp_sorted, sb_pressure, obe_sorted, sb_duels = preprocess_sb_events(
            sb_events_enriched, self.skc_events
        )

        # Init tracking of used events
        used_pp_ids = set()
        duels_used_ids = set()
        pressure_used_ids = set()

        # MATCH PP, PRESSURE, DUELS
        pp_bundles = [
            match_bundle(row, sb_events_to_match, used_pp_ids, 'statsbomb') for _, row in pp_sorted.iterrows()
        ]
        mapping_pp_bundles = pd.DataFrame(pp_bundles)

        if obe_sorted is None or obe_sorted.empty:
            # No on_ball_engagement in SKC => keep PP sync, but skip pressure/duel alignment.
            matched_duels = pd.DataFrame(columns=['event_id', 'id'])
            pressure_confirmed_match = pd.DataFrame(columns=['event_id', 'id'])
        else:
            matched_pressure_sb, matched_pressure_skc = match_pressures_sb(sb_pressure, obe_sorted)

            sb_tackles = sb_duels.query("duel_type_name == 'Tackle'").copy()
            matched_duels, duels_used_ids = match_duels(sb_tackles, obe_sorted, 'statsbomb', duels_used_ids)

            pressure_confirmed_match = matched_pressure_sb.merge(
                matched_pressure_skc, on=['event_id', 'id'], how='inner'
            )
            pressure_used_ids = pressure_used_ids.union(set(pressure_confirmed_match['id'].values))

        skc_events_mapping, statsbomb_events_mapping = apply_sb_output_format(
            sb_events_sync, self.skc_events, mapping_pp_bundles, matched_duels, pressure_confirmed_match
        )

        return skc_events_mapping, statsbomb_events_mapping
