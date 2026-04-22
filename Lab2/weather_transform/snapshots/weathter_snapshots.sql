{% snapshot weather_status_snapshot %}

{{
    config(
      target_schema='analytics',
      strategy='check',
      unique_key="location_name || '-' || date",
      check_cols=['temp_mean', 'weather_code'],
    )
}}

select * from {{ source('snowflake_raw', 'weather_historical') }}

{% endsnapshot %}