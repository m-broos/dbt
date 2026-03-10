# GitHub Copilot - Prompt: Create a dbt EDW Model from a SQL SELECT Statement

## Goal
Create a dbt EDW model from a given SQL SELECT statement, including:
- A SQL model file (with view or table materialization, as appropriate)
- A YAML metadata file for the model
- Documentation for each output field, using a docs reference to the source field in the format `{{ doc('<source field name>') }}`

---

## UTF-8 and Character Encoding Requirements

- When importing or copying content (e.g., documentation blocks) from other markdown files:
  - **Validate** that all text is valid UTF-8 and contains only printable, readable characters.
  - **Remove or replace** any non-printable, non-UTF-8, or suspicious characters (such as BOM, control characters, or replacement characters like ).
  - If any questionable characters are detected, **flag the user for manual review** or skip the import for that block.
  - **Save all markdown and YAML files with UTF-8 encoding (without BOM)** to ensure compatibility with dbt and documentation tools.

---

## Step 1: SQL Model (View or Table)

- **Input:** A SQL SELECT statement (not just a table name)
- **Path:**  
  - Place the SQL file in the appropriate directory:  
    `models/<layer>/<schema>/<tables|views>/<model_name>.sql`
    - `<layer>` is one of: `staging`, `intermediate`, or `marts` (the three dbt layers).
    - `<schema>` is the schema or subject area.
    - `<tables|views>` is the subfolder for tables or views.
    - `<model_name>` is the name of the model.
- **Content:**  
  - The SQL file must follow this layout:
    1. **Header Section:**  
       Add a standardized header at the top of the file, for example:
       ```
       -- -------------------------------------------------------------------------
       -- Model Name   : <model_name>
       -- File         : models/<layer>/<schema>/<tables|views>/<model_name>.sql
       -- Layer        : <layer>   -- (Extracted from the path: models/<layer>/...)
       -- Pipeline     : <pipeline_name>   # TODO: add pipeline name if not provided
       -- Description  : <Short description of the model>
       -- Author       : <author>
       -- Created      : <YYYY-MM-DD>
       -- -------------------------------------------------------------------------
       ```
       - **Note:** The `<layer>` value should be extracted from the file path (the first subfolder after `models/`). The three possible layers are: `staging`, `intermediate`, and `marts`.
    2. **If the model is a table:**  
       - Add a "Warehouse Settings" section at the top, e.g.:
         ```
         --------------------------------------------------------------
         -- Warehouse Settings
         --------------------------------------------------------------
         {% set warehouse = {
             'DEV': 'PRODUCT_CO_DEV_CUSTOM_X1',
             'TEST': 'PRODUCT_CO_DEV_CUSTOM_X1',
             'INT': 'PRODUCT_CO_INT_CUSTOM_ETL_TEC_X1',   # TODO: add customer INT warehouse
             'PROD': 'PRODUCT_CO_PROD_CUSTOM_ETL_TEC_X1'  # TODO: add customer PROD warehouse
         } %}
         ```
    3. A formatted header block labeled "Model Configuration" describing materialization, unique key, documentation persistence, and tags.
    4. The dbt `config` block.  
       - For tables, include `snowflake_warehouse=warehouse[env_var('DBT_ENV_TYPE')]` in the config.
       - For the `tags` parameter, use the pipeline name (which must be provided).  
       - If the pipeline name is not provided, add a hint: `# TODO: add pipeline name`.
    5. A formatted header block labeled "SQL Statement" describing the transformation.
    6. The SQL SELECT statement, adapted to use dbt Jinja syntax for **refs** (not sources).  
       - For example, use:  
         `from {{ ref('src_dma_dwh_f_bon_agg_flag_sco') }} as baf`
    7. **Write all SQL code in dbt style using lower case for keywords, functions, and identifiers.**

---

## Step 2: YAML Model File

- **Path:**  
  - Place the YAML file in the corresponding `models/<layer>/<schema>/properties/` directory, using the model name as the file name.
- **Content:**  
  - The YAML file must follow this layout:
    ```yaml
    models:
      - name: <model_name>
        description: "<Short description of the model>"
        columns:
          - name: <field_name>
            description: "{{ doc('<source_field_name>') }}"
    ```
  - For each output field in the SELECT statement:
    - If the field is an alias (e.g. `baf.tag_id as kal_tag_id`), use the alias as the field name and the source field (e.g. `tag_id`) for the docs reference.
    - If the field is not aliased, use the field name as is for both the column name and the docs reference.
    - If the source field name cannot be determined, use a placeholder: `"TODO: Add description for <field_name>"`

---

## Step 3: Documentation Check

- For each output field in the SELECT statement, check if a docs reference exists for the source field in the appropriate `docs/columns_*.md` file.
- **Before adding or updating documentation blocks, validate that the content is clean UTF-8 and free of unreadable or non-printable characters.**
- If not, add a comment in the YAML file or as output to the user indicating which fields are missing documentation.

---

## Example Input

```sql
select
      baf.tag_id as kal_tag_id,
      baf.ma_id,
      baf.t_id as trans_id,
      baf.kassen_nr as kassen_nr_id,
      baf.flag_sco
from dma_dwh.f_bon_agg_flag_sco as baf
where kal_tag_id >= '2025-03-01'
order by kal_tag_id, ma_id, trans_id, kassen_nr_id;
```
**Note:**  
Always write the SQL code in lower case dbt style.