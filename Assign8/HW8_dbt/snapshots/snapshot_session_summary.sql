{% snapshot snapshot_session_summary %}
{{
    config(
      target_schema='snapshot',
      unique_key='sessionId',
      strategy='timestamp',
      updated_at='ts',
    )
}}
SELECT * FROM {{ ref('session_summary') }}
{% endsnapshot %}