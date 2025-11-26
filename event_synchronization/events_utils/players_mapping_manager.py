from __future__ import annotations

from typing import Any

MAGIC_EXPECTED_TEAMS = 2


class SkcPlayersMapping:
    """Class to manage the mapping of SKC players."""

    def __init__(self, match_data: dict) -> None:
        self.match_data = match_data

        self.skc_ply_id_to_ply = self.get_ply_id_to_ply()
        self.skc_team_id_to_jno_list = self.get_team_id_to_jno_list()

    def get_ply_id_to_ply(self) -> dict[int, dict]:
        """Get the mapping dict of player ID to player information.

        Returns:
            dict: dict where keys are player IDs and values are corresponding player dict.
        """
        return {ply['id']: ply for ply in self.match_data['players']}

    def get_team_id_to_jno_list(self) -> dict[int, list[int]]:
        """Get the mapping dict of SKC team ID to jersey number list.

        Returns:
            dict: dict where keys are SKC team IDs and values are lists of jersey numbers.
        """

        team_id_list = [team_info['id'] for team_info in [self.match_data['home_team'], self.match_data['away_team']]]
        return {
            team_id: [
                ply['number']
                for ply in self.skc_ply_id_to_ply.values()
                if ply['team_id'] == team_id and ply['start_time'] is not None
            ]
            for team_id in team_id_list
        }

    def get_team_jno_to_ply_id(self) -> dict[int, int]:
        """Get the mapping dict of jersey number to SKC player ID.

        Returns:
            dict: dict where keys are jersey numbers and values are SKC player IDs.
        """

        return {jno: ply_id for ply_id, ply in self.skc_ply_id_to_ply.items() for jno in ply['number']}

    def get_skc_team_id_to_provider_team_id(
        self, provider_team_id_to_jno_list: dict[int, list[int]]
    ) -> dict[int, int] | None:
        """Get the mapping dict of SKC team ID to provider team ID.

        Args:
            provider_team_id_to_jno_list (dict): dict of the jersey number list of the provider team.

        Returns:
            dict: dict of mapping from SKC team ID to provider team ID if the jno list of the two teams are the same.
            Otherwise, None.
        """

        skc_team_id_to_provider_team_id = jno_set_mapping(self.skc_team_id_to_jno_list, provider_team_id_to_jno_list)
        if skc_team_id_to_provider_team_id is not None:
            return skc_team_id_to_provider_team_id
        return None

    def get_provider_team_id_jno_to_skc_ply_id(
        self, skc_team_id_to_provider_team_id: dict[int, int]
    ) -> dict[tuple[int, int], int]:
        """Get the mapping dict of provider team ID and jersey number to SKC player ID.

        Args:
            skc_team_id_to_provider_team_id (dict): dict of mapping from SKC team ID to provider team ID.

        Returns:
            dict: dict where keys are tuples of provider team ID and jersey number and values are SKC player IDs.
        """

        return {
            (skc_team_id_to_provider_team_id[ply['team_id']], ply['number']): ply_id
            for ply_id, ply in self.skc_ply_id_to_ply.items()
        }

    def get_provider_id_to_skc_id(
        self,
        provider_ply_id_to_ply: dict[int, dict[str, Any]],
        provider_team_id_to_jno_list: dict[int, list[int]],
        key_jno: str,
    ) -> tuple[dict[int, int], dict[int, int]]:
        """Get the mapping dict of provider player ID to SKC player ID.

        Args:
            provider_ply_id_to_ply (dict): dict where keys are provider player IDs and values are player info.
            provider_team_id_to_jno_list (dict): dict of the jersey number list of the provider team.
            key_jno (str): key of jersey number in the player dict.

        Returns:
            tuple: tuple of two dicts where the first dict is mapping from provider team ID to SKC team ID,
                and the second dict is mapping from provider player ID to SKC player ID.
        """

        skc_team_id_to_provider_team_id = self.get_skc_team_id_to_provider_team_id(provider_team_id_to_jno_list)
        provider_team_id_to_skc_team_id = {v: k for k, v in skc_team_id_to_provider_team_id.items()}
        provider_team_id_jno_to_skc_ply_id = self.get_provider_team_id_jno_to_skc_ply_id(
            skc_team_id_to_provider_team_id
        )
        provider_ply_id_to_ply = {
            provider_ply_id: provider_team_id_jno_to_skc_ply_id[(provider_ply['team_id'], provider_ply[key_jno])]
            for provider_ply_id, provider_ply in provider_ply_id_to_ply.items()
        }
        return provider_team_id_to_skc_team_id, provider_ply_id_to_ply

    def get_provider_ply_id_to_skc_ply_id_with_known_team_id_mapping(
        self,
        provider_ply_id_to_ply: dict[int, dict[str, Any]],
        skc_team_id_to_provider_team_id: dict[int, int],
        key_jno: str,
    ) -> dict[int, int]:
        """Get the mapping dict of provider player ID to SKC player ID with known team ID mapping.

        Args:
            provider_ply_id_to_ply (dict): dict where keys are provider player IDs and values are player info
            skc_team_id_to_provider_team_id (dict): dict of mapping from SKC team ID to provider team ID.
            key_jno (str): key of jersey number in the player dict.

        Returns:
            dict: dict where keys are provider player IDs and values are SKC player IDs.
        """

        provider_team_id_jno_to_skc_ply_id = self.get_provider_team_id_jno_to_skc_ply_id(
            skc_team_id_to_provider_team_id
        )
        return {
            provider_ply_id: provider_team_id_jno_to_skc_ply_id.get(
                (provider_ply['team_id'], provider_ply[key_jno]), None
            )
            for provider_ply_id, provider_ply in provider_ply_id_to_ply.items()
        }


def jno_set_mapping(
    skc_team_id_to_jno_list: dict[int, list[int]],
    provider_team_id_to_jno_list: dict[int, list[int]],
) -> dict[int, int] | None:
    """Manage the mapping of the jersey number list of the two teams.
    Compare the jersey number list of two teams,
     and return the mapping if the jersey number list of the two teams are the same.

    Args:
        skc_team_id_to_jno_list (dict): The dictionary of the jersey number list of the SKC team.
        provider_team_id_to_jno_list (dict): The dictionary of the jersey number list of the provider team.

    Returns:
        dict: Dict of mapping from SKC team ID to provider team ID if the jno list of the two teams are the same.
            Otherwise, None.
    """

    skc_team_id_list = list(skc_team_id_to_jno_list.keys())
    if set(skc_team_id_to_jno_list[skc_team_id_list[0]]) == set(skc_team_id_to_jno_list[skc_team_id_list[1]]):
        return None

    provider_team_id_list = list(provider_team_id_to_jno_list.keys())
    if set(provider_team_id_to_jno_list[provider_team_id_list[0]]) == set(
        provider_team_id_to_jno_list[provider_team_id_list[1]]
    ):
        return None

    skc_team_id_to_provider_team_id = {}
    for skc_team_id, skc_jno_list in skc_team_id_to_jno_list.items():
        for provider_team_id, provider_jno_list in provider_team_id_to_jno_list.items():
            if set(skc_jno_list) == set(provider_jno_list):
                skc_team_id_to_provider_team_id[skc_team_id] = provider_team_id

    return None if len(skc_team_id_to_provider_team_id) != MAGIC_EXPECTED_TEAMS else skc_team_id_to_provider_team_id
