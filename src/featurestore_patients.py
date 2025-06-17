import toml
import pandas as pd

import snowflake.snowpark.functions as sff
from snowflake.ml.feature_store import FeatureStore, CreationMode, Entity, FeatureView
from snowflake.snowpark.context import get_active_session

#Reference
#https://github.com/ncl-icb-analytics/snowpark_testing/blob/main/FeatureStore.qmd