SHELL := /bin/bash

dbt-console: 
	docker-compose run dbt_console

.PHONY: dbt-console

SHELL := /bin/bash

# set default target
DBT_TARGET ?= dev
AWS_LAMBDA_ROLE ?= aws_lambda_cosmos_api_dev

sl-cosmos-api:
	dbt run-operation create_aws_cosmos_api \
	--profile cosmos \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt/

udfs:
	dbt run-operation create_udfs \
	--vars '{"UPDATE_UDFS_AND_SPS":True}' \
	--profile cosmos \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt/

complete:
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m 1+models/silver/streamline/core/complete \
	--profile cosmos \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt

streamline: sl-cosmos-api udfs 

blocks_history:
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m 1+models/streamline/genesis_backfill/streamline__blocks_genesis_backfill_ch1.sql \
	--profile cosmos \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt

tx_history:
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m 1+models/streamline/genesis_backfill/streamline__transactions_genesis_backfill_ch1.sql \
	--profile cosmos \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt

validators_history:
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m 1+models/streamline/genesis_backfill/streamline__validators_genesis_backfill_ch1.sql \
	--profile cosmos \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt

tx_realtime: 
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m 1+models/streamline/genesis_backfill/streamline__transactions_realtime_test.sql \
	--profile cosmos \
	--target dev \
	--profiles-dir ~/.dbt

validators_realtime: 
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m 1+models/streamline/genesis_backfill/tests/streamline__validators_realtime_test.sql \
	--profile cosmos \
	--target dev \
	--profiles-dir ~/.dbt