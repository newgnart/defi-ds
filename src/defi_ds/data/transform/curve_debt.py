import pandas as pd
import os


def borrower_state(df: pd.DataFrame) -> pd.DataFrame:
    """
    Manipulate borrower state data to ensure each user has debt data for each day.

    Args:
        df (pd.DataFrame): Input DataFrame which contains the borrower state data with fields:
            - user: str
            - time_stamp: datetime
            - debt: float
    Returns:
        pd.DataFrame: Manipulated DataFrame that has a row for each user for each day with fields:
            - user: str
            - date: datetime
            - debt: float
    """

    # Convert time_stamp to datetime
    if type(df["time_stamp"].iloc[0]) == str:
        df["time_stamp"] = pd.to_datetime(df["time_stamp"])

    # Extract date from time_stamp
    df["date"] = df["time_stamp"].dt.date

    # Get all unique dates and users
    all_dates = sorted(df["date"].unique())
    all_users = df["user"].unique()

    # Create a list to store the manipulated data
    manipulated_rows = []

    # For each date, ensure all users have debt data
    for date in all_dates:
        # Get data for this date
        date_data = df[df["date"] == date].copy()

        # For each user, get their last debt value up to this date
        for user in all_users:
            # Get all data for this user up to this date
            user_data_up_to_date = df[(df["user"] == user) & (df["date"] <= date)]

            if len(user_data_up_to_date) > 0:
                # Get the last row for this user up to this date
                last_user_row = user_data_up_to_date.iloc[-1]

                # Check if we already have data for this user on this date
                existing_data = date_data[date_data["user"] == user]

                if len(existing_data) > 0:
                    # Use the last row for this user on this date
                    final_row = existing_data.iloc[-1]
                else:
                    # Create a new row with the last known debt value for this user
                    final_row = last_user_row.copy()
                    # Update the time_stamp to be the last time on this date (or create a reasonable time)
                    last_time_on_date = (
                        date_data["time_stamp"].max()
                        if len(date_data) > 0
                        else pd.Timestamp(date)
                        + pd.Timedelta(hours=23, minutes=59, seconds=59)
                    )
                    final_row["time_stamp"] = last_time_on_date
                    final_row["date"] = date

                manipulated_rows.append(final_row)

    # Create the manipulated DataFrame
    manipulated_df = pd.DataFrame(manipulated_rows).drop(columns=["time_stamp"])

    # Sort by date and time_stamp
    manipulated_df = manipulated_df[manipulated_df["debt"] != 0].sort_values(
        ["date", "user"]
    )

    return manipulated_df


def decode_user_state_data(data: str) -> dict:
    """
    Decode Solidity function calldata string with arguments: uint256 collateral, uint256 debt, int256 n1, int256 n2, uint256 liquidation_discount

    Args:
        calldata_str: string containing hex calldata with '0x' prefix

    Returns:
        dict with decoded values: collateral, debt, n1, n2, liquidation_discount
    """

    def hex_to_int256(hex_str):
        """Convert hex string to int256, handling negative values"""
        value = int(hex_str, 16)
        return value if value < 2**255 else value - 2**256

    def hex_to_uint256(hex_str):
        """Convert hex string to uint256"""
        return int(hex_str, 16)

    # Extract each 32-byte argument (64 hex characters each)
    result = {
        "collateral": hex_to_uint256(data[2:66]),
        "debt": hex_to_uint256(data[66:130]),
        "n1": hex_to_int256(data[130:194]),
        "n2": hex_to_int256(data[194:258]),
        "liquidation_discount": hex_to_uint256(data[258:]),
    }

    return result
