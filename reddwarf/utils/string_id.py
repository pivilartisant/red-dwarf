def cast_int_to_string(votes: list) -> list:
    """Convert integer IDs to string IDs in votes."""
    for vote in votes:
        vote["participant_id"] = str(vote["participant_id"]) + "p"
        vote["statement_id"] = str(vote["statement_id"]) + "p"
    return votes

def recast_string_to_int(processed_data: dict) -> dict:
    """Convert string keys like '10p' to int keys like 10."""
    recasted_data = {}
    for key, value in processed_data.items():
        new_key = int(key.replace("p", ""))
        recasted_data[new_key] = value
    return recasted_data