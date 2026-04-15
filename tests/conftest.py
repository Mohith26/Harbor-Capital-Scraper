"""Shared pytest fixtures for the self-learning engine tests."""
import os
import pytest
from pathlib import Path


FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir():
    return FIXTURES_DIR


@pytest.fixture
def sample_comp_files_dir():
    """Path to the 16 real sample files used for Phase 8 seeding + regression."""
    return Path(__file__).parent.parent / "sample comp files"
