{#
  Generate a deterministic MD5 surrogate key from one or more fields.

  Each field is normalized via UPPER(TRIM(COALESCE(field, ''))) and joined
  with a '|' delimiter, then hashed. This guarantees the same key for
  values that differ only in case or whitespace, and a stable key when
  any field is null (treated as empty string).

  Args:
    fields: list of column expressions to combine into the key.

  Example:
    {{ surrogate_key(['country', 'state', 'city']) }} as location_key
#}
{% macro surrogate_key(fields) %}
    md5(
        {%- for field in fields -%}
            upper(trim(coalesce({{ field }}, '')))
            {%- if not loop.last %} || '|' || {% endif -%}
        {%- endfor -%}
    )
{% endmacro %}
