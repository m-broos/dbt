# GitHub Copilot - Prompt: Create a dbt source model

## Goal
Create a dbt staging model for a source table, including:
- A SQL file (view materialization)
- A YAML metadata file
- Optional centralized documentation in column.md
- A suggestion to update sources.yml if needed

---

## UTF-8 and Character Encoding Requirements

- When importing or copying content (e.g., documentation blocks) from other markdown files:
  - **Validate** that all text is valid UTF-8 and contains only printable, readable characters.
  - **Remove or replace** any non-printable, non-UTF-8, or suspicious characters (such as BOM, control characters, or replacement characters).
  - If any questionable characters are detected, **flag the user for manual review** or skip the import for that block.
  - **Save all markdown and YAML files with UTF-8 encoding (without BOM)** to ensure compatibility with dbt and documentation tools.

---

## Step 1: SQL Model (View)

- Path:
  models/staging/co_in/view/<database_name>_<table_name>.sql

- Content:
  ```sql
  -- models/staging/co_in/view/<database_name>_<table_name>.sql

  {{ config(
      materialized='view',
  ) }}

  select
      <field_names_in_snake_case>
  from {{ source('<schema_name>', '<table_name>') }}
  ```

---

### Required SQL File Layout

Each generated SQL file must contain the following sections in this order:

1. A file header at the top, e.g.:
   ```
   -- -------------------------------------------------------------------------
   -- Model Name   : <model_name>
   -- File         : models/<layer>/co_in/<tables|views>/<model_name>.sql
   -- Layer        : <layer>
   -- Pipeline     : <pipeline_name>   # TODO: add pipeline name if not provided
   -- Description  : <Short description of the model>
   -- Author       : <author>
   -- Created      : <YYYY-MM-DD>
   -- -------------------------------------------------------------------------
   ```

2. A formatted header block labeled "Model Configuration":
   ```sql
   --------------------------------------------------------------
   -- Model Configuration
   --------------------------------------------------------------
   {{ config(
       materialized='view'
   ) }}
   ```

3. A second formatted header block labeled "SQL Statement":
   ```sql
   --------------------------------------------------------------
   -- SQL Statement
   --------------------------------------------------------------
   select ...
   ```

Notes:
- Always include both comment headers exactly as defined above.
- Do not extract descriptions dynamically from the SQL content.
- Do not insert inline comments inside the config block.
- Read the database_name in the model path from the source_table_dwh.md file. Search bei model name 
  and go to field *Database Name*. Take the first database base if there are more then one. 
  If the Database Name in "UNKNOWN" then use the Product Short Name from the source_table_dwh.md file.
---

## Step 2: YAML model metadata file

- Path:
  models/staging/co_in/properties/<database_name>_<table_name>.yml

- Content:
  ```yaml
  # models/staging/<schema_name>/properties/<database_name>_<table_name>.yml

  models:
    - name: <database_name>_<table_name>
      description: "<table description in english, lowercase>"
      meta:
        source_owner: "**[<Product Names>]('https://atp-web.rewe.local/details/product/<Product IDs>)**"
      columns:
        - name: <field_name>
          data_type: <data_type>
          description: "{{ doc('<field_name>') }}"  # if centrally documented
        - name: <field_name>
          data_type: <data_type>
          description: <english description>        # if not centrally documented
  ```

Notes:
- Include table description, Product Names and Product IDs from source_tables_dwh.md file.

---

## Step 3: column documentation (centralized by domain)

- If a field has a centralized documentation target (e.g., 'sap', 'co'), update the corresponding documentation file.

- Metadata tables may include a "Field doc file" column, e.g.:
  | Field name         | Data Type     | Description (en)                          | Field doc file |
  |--------------------|---------------|-------------------------------------------|----------------|
  | werbe_kz_art_id    | number(38,0)  | advertising characteristic article id     | sap            |
  | werbe_kz_art       | varchar(4)    | advertising characteristic description    | sap            |

- Path:
  /docs/columns_<field_doc_file>.md

- Content per field:
  ```jinja
  {% docs <field_name> %}
  <description in english, lowercase>
  {% enddocs %}
  ```

- Only add fields that are not already documented.
- Validate UTF-8 encoding before writing any doc blocks.

---

## Step 4: Check and update __sources_<schema_name>.yml

- Path:
  models/staging/<schema_name>/__sources_<schema_name>.yml

- If the table is not already listed, add this:
  ```yaml
  - name: <table_name>
    description: source table for <table_name>
    schema: <schema_name>
    tables:
      - name: <table_name>
        data_type: <data_type>
        description: source table for <table_name>
  ```

- If you're unsure whether the source is already present, ask the user to paste the current file.
- Do not add the source columns and descriptions here.
- Do not include the file path in comments.

---

## Style rules

- Use all lowercase
- Use snake_case for all field and table names
- No version number in YAML (handled in dbt project)
- Use file-level comments for readability if helpful

---

## Example metadata table

| Field name         | Data Type     | Description (en)                          | Field doc file |
|--------------------|---------------|-------------------------------------------|----------------|
| werbe_kz_art_id    | number(38,0)  | advertising characteristic article id     | sap            |
| werbe_kz_art       | varchar(4)    | advertising characteristic description    | sap            |
| werbe_kz_art_txt   | varchar(60)   | advertising characteristic text           | sap            |
| werbe_kz_art_prio  | number(38,0)  | advertising characteristic priority       | sap            |

---

## Task: Review and improve the structure and comments of this dbt model SQL file

**Context**: We are using dbt Cloud to build and maintain data models for a Snowflake data warehouse.

### Requirements:
- Ensure the file follows dbt and SQL best practices for layout, structure, and documentation.

### Section layout (only for major sections):
Use this header format:
```sql
--------------------------------------------------------------
-- Section Title
-- Brief description of the section purpose
--------------------------------------------------------------
```

Use this structure for:
- Warehouse Settings
- Model Configuration
- SQL Statement

**Note:** Do *not* use the header layout inside SQL blocks (e.g. CTEs or SELECTs).

### Inside SQL (CTEs, etc.):
Use this style for steps:
```sql
-- Step 1: Describe the purpose of the CTE
with my_cte as (
  ...
),

-- Step 2: Describe the transformation
another_cte as (
  ...
)
```

- Improve or add missing comments where needed.
- Do *not* add inline comments inside the {{ config(...) }} block.
- The file is functionally correct. Focus on structure, readability, and consistent formatting only.
