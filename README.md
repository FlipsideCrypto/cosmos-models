## Profile Set Up

#### Use the following within profiles.yml

----

```yml
cosmos:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: vna27887.us-east-1
      role: INTERNAL_DEV
      user: <USERNAME>
      password: <PASSWORD>
      region: us-east-1
      database: COSMOS_DEV
      warehouse: DBT_EMERGENCY
      schema: silver
      threads: 12
      client_session_keep_alive: False
      query_tag: <QUERY_TAG>
    prod:
      type: snowflake
      account: vna27887.us-east-1
      role: DBT_CLOUD_COSMOS
      user: <USERNAME>
      password: <PASSWORD>
      region: us-east-1
      database: COSMOS
      warehouse: DBT_EMERGENCY
      schema: silver
      threads: 12
      client_session_keep_alive: False
      query_tag: <QUERY_TAG>
```
### Variables

To control which external table environment a model references, as well as, whether a Stream is invoked at runtime using control variables:
* STREAMLINE_INVOKE_STREAMS
When True, invokes streamline on model run as normal
When False, NO-OP
* STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES
When True, uses DEV schema Streamline.Cosmos_DEV
When False, uses PROD schema Streamline.Cosmos

Default values are False

* Usage:
dbt run --var '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":True, "STREAMLINE_INVOKE_STREAMS":True}'  -m ...

### Resources:

* Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
* Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
* Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
* Find [dbt events](https://events.getdbt.com) near you
* Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

## Applying Model Tags

### Database / Schema level tags

Database and schema tags are applied via the `add_database_or_schema_tags` macro.  These tags are inherited by their downstream objects.  To add/modify tags call the appropriate tag set function within the macro.

```
{{ set_database_tag_value('SOME_DATABASE_TAG_KEY','SOME_DATABASE_TAG_VALUE') }}
{{ set_schema_tag_value('SOME_SCHEMA_TAG_KEY','SOME_SCHEMA_TAG_VALUE') }}
```

### Model tags

To add/update a model's snowflake tags, add/modify the `meta` model property under `config`.  Only table level tags are supported at this time via DBT.

```
{{ config(
    ...,
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'SOME_PURPOSE'
            }
        }
    },
    ...
) }}
```

By default, model tags are not pushed to snowflake on each load.  You can push a tag update for a model by specifying the `UPDATE_SNOWFLAKE_TAGS` project variable during a run.

```
dbt run --var '{"UPDATE_SNOWFLAKE_TAGS":True}' -s models/core/core__fact_swaps.sql
```

### Querying for existing tags on a model in snowflake

```
select *
from table(solana.information_schema.tag_references('solana.core.fact_blocks', 'table'));
```