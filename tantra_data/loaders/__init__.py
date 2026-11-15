"""tantra_data.loaders: offline data-loading surfaces for official Indian-market publications.

Loaders parse the raw publications (AMFI NAVAll.txt, NSE EOD bhavcopy CSVs, Yahoo Finance
daily history) into canonical rows and bars. They are wheel-optional: the modules import
without the ``tantra`` wheel and lazy-import its ``Bar``/``InstrumentId``/store types only
inside the functions that build bars.
"""
