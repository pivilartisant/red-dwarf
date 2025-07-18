import pytest
import numpy as np
from reddwarf.implementations.base import run_pipeline
from reddwarf.utils import matrix as MatrixUtils
from reddwarf.data_loader import Loader
from tests.fixtures import polis_convo_data

# This test will only match polismath for sub-100 participant convos.
@pytest.mark.parametrize("reducer", ["pca", "pacmap", "localmap"])
@pytest.mark.parametrize(
    "polis_convo_data", ["small-no-meta", "small-with-meta"], indirect=True
)
def test_run_pipeline_with_string_statement_and_participant_id(polis_convo_data):
    """Test that un_pipeline works with string participant IDs."""
    fixture = polis_convo_data

    # Expected values from the math_data
    expected_user_vote_counts = fixture.math_data["user-vote-counts"]
    expected_user_vote_counts = {int(k):v for k,v in expected_user_vote_counts.items()}
    
    # Load test data
    loader = Loader(filepaths=[f"{fixture.data_dir}/votes.json"])

    # Preprocess votes to convert IDs to strings
    preprocessed_votes = preprocess_votes(loader.votes_data)

    # Get the vote matrix for the preprocessed votes
    real_vote_matrix = MatrixUtils.generate_raw_matrix(votes=preprocessed_votes)
    actual_user_vote_counts = real_vote_matrix.count(axis="columns").to_dict()

    # Recast string keys to integers for comparison to insure data integrity
    actual_user_vote_counts_recasted = recast_string_to_int(actual_user_vote_counts)

    # Assert that the expected user vote counts match the actual counts
    assert expected_user_vote_counts == actual_user_vote_counts_recasted

   
def preprocess_votes(votes: list) -> list:
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