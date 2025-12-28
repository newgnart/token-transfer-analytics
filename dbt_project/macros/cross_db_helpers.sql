{% macro bytes_to_hex(column_name) %}
  {#-
    Convert bytea/binary data to hex string with 0x prefix
    - PostgreSQL: Uses encode(column, 'hex')
    - Snowflake: Uses TO_VARCHAR(column, 'hex')
  -#}
  {% if target.type == 'postgres' %}
    '0x' || encode({{ column_name }}, 'hex')
  {% elif target.type == 'snowflake' %}
    '0x' || lower(to_varchar({{ column_name }}, 'hex'))
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}


{% macro safe_column(column_name, default_value='NULL') %}
  {#-
    Safely reference a column that may not exist in all environments
    Returns default_value if column doesn't exist

    Note: This checks if column exists in adapter's relation cache at compile time
  -#}
  {% set relation = adapter.get_relation(
      database=target.database,
      schema=target.schema,
      identifier=this.identifier if this is defined else ''
  ) %}

  {#- If relation doesn't exist yet (first run), assume column exists -#}
  {% if relation is none %}
    {{ column_name }}
  {% else %}
    {#- Check if column exists in the relation -#}
    {% set columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | list %}
    {% if column_name.upper() in columns or column_name.lower() in columns or column_name in columns %}
      {{ column_name }}
    {% else %}
      {{ default_value }} as {{ column_name }}
    {% endif %}
  {% endif %}
{% endmacro %}


{% macro dlt_column(column_name) %}
  {#-
    Reference DLT metadata columns with proper case handling
    - PostgreSQL: lowercase (_dlt_load_id)
    - Snowflake: uppercase (_DLT_LOAD_ID) if not quoted
  -#}
  {% if target.type == 'postgres' %}
    {{ column_name }}
  {% elif target.type == 'snowflake' %}
    {{ column_name | upper }}
  {% else %}
    {{ column_name }}
  {% endif %}
{% endmacro %}


{% macro trim_leading_zeros(string_expression) %}
  {#-
    Trim leading zeros from a string
    - PostgreSQL: TRIM(LEADING '0' FROM expression)
    - Snowflake: LTRIM(expression, '0')
  -#}
  {% if target.type == 'postgres' %}
    TRIM(LEADING '0' FROM {{ string_expression }})
  {% elif target.type == 'snowflake' %}
    LTRIM({{ string_expression }}, '0')
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}


{% macro hex_to_bigint(hex_string_expression) %}
  {#-
    Convert hex string (with '0x' prefix removed and leading zeros trimmed) to numeric
    Handles unsigned uint256 values correctly by treating them as unsigned integers

    - PostgreSQL: Uses ::bit(64)::bigint with unsigned conversion for negative values
    - Snowflake: Uses TRY_TO_NUMBER with XXXXXXXXXXXX format (16 hex digits = 64 bits)

    Input should be result of trim_leading_zeros(SUBSTRING(hex_column, 3))

    Note: Returns NUMERIC type (not BIGINT) to handle full uint64 range without overflow
  -#}
  {% if target.type == 'postgres' %}
    (CASE
        WHEN ('x' || LPAD({{ hex_string_expression }}, 16, '0'))::bit(64)::bigint < 0
        THEN ('x' || LPAD({{ hex_string_expression }}, 16, '0'))::bit(64)::bigint::numeric + 18446744073709551616::numeric
        ELSE ('x' || LPAD({{ hex_string_expression }}, 16, '0'))::bit(64)::bigint::numeric
    END)
  {% elif target.type == 'snowflake' %}
    try_to_number({{ hex_string_expression }}, 'XXXXXXXXXXXXXXXX')
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}


{% macro current_timestamp_func() %}
  {#-
    Get current timestamp
    - PostgreSQL: NOW()
    - Snowflake: CURRENT_TIMESTAMP() or SYSDATE()
  -#}
  {% if target.type == 'postgres' %}
    NOW()
  {% elif target.type == 'snowflake' %}
    CURRENT_TIMESTAMP()
  {% else %}
    CURRENT_TIMESTAMP
  {% endif %}
{% endmacro %}


{% macro date_to_integer_key(date_expression=none) %}
  {#-
    Convert date/timestamp to integer in YYYYMMDD format
    - PostgreSQL: TO_CHAR(date, 'YYYYMMDD')::INTEGER
    - Snowflake: TO_NUMBER(TO_VARCHAR(date, 'YYYYMMDD'))

    If date_expression is not provided, uses current timestamp
  -#}
  {% set date_expr = date_expression if date_expression else current_timestamp_func() %}

  {% if target.type == 'postgres' %}
    TO_CHAR({{ date_expr }}, 'YYYYMMDD')::INTEGER
  {% elif target.type == 'snowflake' %}
    TO_NUMBER(TO_VARCHAR({{ date_expr }}, 'YYYYMMDD'))
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}


{% macro substring_from(string_expression, start_position) %}
  {#-
    Extract substring from start_position to end of string
    - PostgreSQL: SUBSTRING(string FROM start_position)
    - Snowflake: SUBSTRING(string, start_position) requires length, so use large number

    start_position is 1-based index
  -#}
  {% if target.type == 'postgres' %}
    SUBSTRING({{ string_expression }}, {{ start_position }})
  {% elif target.type == 'snowflake' %}
    SUBSTRING({{ string_expression }}, {{ start_position }}, 1000)
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}
