
SQL_DICT = {
    'select_prd_pln_by_prcs_grp_id': """
        SELECT
            attr.attname AS column_name
        FROM
            pg_attribute AS attr
        INNER JOIN
            pg_stat_user_tables AS stat
        ON
            attr.attrelid = stat.relid
            AND stat.schemaname = '{schema_name}'
            AND stat.relname = '{table_name}'
        INNER JOIN
            pg_constraint cons
        ON
            attr.attnum = ANY (cons.conkey)
            AND cons.contype = 'p'
            AND cons.conrelid = stat.relid;
    ;
    """,
    'insert_mix_bd_prd_pln': """
    INSERT INTO
        {schema}.t_alg_prd_pln_for_ord_bo_ord_lk
    (SELECT
        {col_names}
        , date_trunc('second', localtimestamp) AS create_date
        , '{job_id}' AS create_user_id
        , '{program_name}' AS create_func_id
        , date_trunc('second', localtimestamp) AS update_date
        , '{job_id}' AS update_user_id
        , '{program_name}' AS update_func_id
        , 'N' AS del_flg
    FROM {schema}.t_alg_prd_pln_for_ord
    WHERE
        --except the exsited product plan with order
        prd_pln_id IS NOT NULL
        AND opt_grp_id = {opt_grp_id}
        {opt_grp_sub_id_cond}
        AND oprn_date = {oprn_date}
        AND del_flg = 'N'
    )
    ;
    """
}
