import pytest
import numpy as np
from reddwarf.implementations.base import run_pipeline
from reddwarf.utils import matrix as MatrixUtils
from reddwarf.data_loader import Loader
from tests.fixtures import polis_convo_data
from reddwarf.utils.string_id import cast_int_to_string, recast_string_to_int

@pytest.mark.parametrize("reducer", ["pca", "pacmap", "localmap"])
@pytest.mark.parametrize(
    "polis_convo_data", ["small-no-meta", "small-with-meta"], indirect=True
)
def test_run_pipeline_with_statement_and_participant_id_casting(reducer,polis_convo_data):
    """Test that run_pipeline supports string statement and participant id's and outputs expected data"""
    fixture = polis_convo_data

    # Load test data
    loader = Loader(filepaths=[f"{fixture.data_dir}/votes.json"])
    votes = loader.votes_data
    ### Test int to string casting for run_pipeline

    # Preprocess votes to convert participant and statement IDs to strings
    preprocessed_vote_data = cast_int_to_string(votes)

    # preprocessed clustering run_pipeine
    preprocessed_clustering_result = run_pipeline(
       votes = preprocessed_vote_data,
       reducer=reducer
    )

    # Get preprocessed vote dict
    preprocessed_vote_matrix = preprocessed_clustering_result.raw_vote_matrix.count(axis="columns").to_dict()

    # Expected values from preprocessed fixture
    preprocessed_mathdata_results = fixture.math_data["user-vote-counts"]
    preprocessed_mathdata_results = {str(k)+"p":v for k,v in preprocessed_mathdata_results.items()}
    assert 1

    assert preprocessed_vote_matrix == preprocessed_mathdata_results



@pytest.mark.parametrize("reducer", ["pca", "pacmap", "localmap"])
@pytest.mark.parametrize(
    "polis_convo_data", ["small-no-meta", "small-with-meta"], indirect=True
)
def test_run_pipeline_against_mathdata(reducer,polis_convo_data):
    """Test that run_pipeline outputs expected data"""

    fixture = polis_convo_data

    # Load test data
    loader = Loader(filepaths=[f"{fixture.data_dir}/votes.json"])
    votes = loader.votes_data


    # default clustering run_pipeline 
    default_clustering_result = run_pipeline(
        votes = votes,
        reducer=reducer,
        random_state=None
    )

    # Get default votes dict
    default_vote_matrix = default_clustering_result.raw_vote_matrix.count(axis="columns").to_dict()

    # Expected default values from fixture
    default_mathdata_results = fixture.math_data["user-vote-counts"]
    
    default_mathdata_results = {int(k):v for k,v in default_mathdata_results.items()}

    # default_clustering_result should equal default mathdata results 
    assert default_vote_matrix == default_mathdata_results


# This test will make sure the cast_int_to_string & recast_string_to_int are working correctly 
@pytest.mark.parametrize(
    "polis_convo_data", ["small-no-meta", "small-with-meta"], indirect=True
)
def test_casting_roundtrip_statement_and_participant_id(polis_convo_data):
    """Test that cating an into to string and string to int does not currupt expected data"""
    fixture = polis_convo_data
    
    # Load test data
    loader = Loader(filepaths=[f"{fixture.data_dir}/votes.json"])

    # Preprocess votes to convert IDs to strings
    preprocessed_votes = cast_int_to_string(loader.votes_data)
 
    
    # Get the vote matrix for the preprocessed votes
    real_vote_matrix = MatrixUtils.generate_raw_matrix(votes=preprocessed_votes)
    actual_user_vote_counts = real_vote_matrix.count(axis="columns").to_dict()

    # Recast string keys to integers for comparison to insure data integrity
    actual_user_vote_counts_recasted = recast_string_to_int(actual_user_vote_counts)

    # Expected values from fixture
    expected_user_vote_counts = fixture.math_data["user-vote-counts"]
    expected_user_vote_counts = {int(k):v for k,v in expected_user_vote_counts.items()}

    # Assert that the expected user vote counts match the actual counts
    assert expected_user_vote_counts == actual_user_vote_counts_recasted

   
