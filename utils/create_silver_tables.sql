%sql
CREATE TABLE IF NOT EXISTS market_catalog.silver.sales
USING DELTA
LOCATION "abfss://silver@marketstorage1111.dfs.core.windows.net/sales"

%sql
CREATE TABLE IF NOT EXISTS market_catalog.silver.warranty
USING DELTA
LOCATION "abfss://silver@marketstorage1111.dfs.core.windows.net/warranty"

%sql
CREATE TABLE IF NOT EXISTS market_catalog.silver.products
USING DELTA
LOCATION "abfss://silver@marketstorage1111.dfs.core.windows.net/products"

%sql
CREATE TABLE IF NOT EXISTS market_catalog.silver.stores
USING DELTA
LOCATION "abfss://silver@marketstorage1111.dfs.core.windows.net/stores"

%sql
CREATE TABLE IF NOT EXISTS market_catalog.silver.category
USING DELTA
LOCATION "abfss://silver@marketstorage1111.dfs.core.windows.net/category"
