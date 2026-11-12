def test_tantra_data_import():
    import tantra_data

    assert tantra_data.__version__ == "0.1.0"
    assert tantra_data.TantraDataService is not None
