SELECT 
    SUM(wrE19.[Debit Amount]) AS [Debit Amount],
    SUM(wrE19.[Credit Amount]) AS [Credit Amount],
    MAX(wrE19.[General Ledger Key]) AS [General Ledger Key]
FROM (
    WITH all_fiscal_accounts AS (
        SELECT 
            gl.account_key AS account_key, 
            gl.organization_key AS org_key, 
            fm.fiscal_month_key AS fiscal_month_key, 
            fm.fiscal_year_key AS fiscal_year_key, 
            fm.begin_date AS begin_date, 
            fm.end_date AS end_date, 
            0 AS amount, 
            a.type AS acc_type, 
            gl.transaction_currency AS transaction_currency, 
            gl.local_currency AS local_currency
        FROM (
            SELECT DISTINCT account_key, organization_key, transaction_currency, local_currency 
            FROM [dbo].["aretum"."general_ledger"]
        ) gl
        CROSS JOIN [dbo].["aretum"."fiscal_month"] fm
        JOIN [dbo].["aretum"."account"] a ON a.account_key = gl.account_key
        WHERE fm.begin_date >= DATEADD(year, -4, GETDATE())
    ),re_accounts_cte AS (
        SELECT 
            gl.account_key AS account_key, 
            gl.organization_key AS org_key, 
            gl.fiscal_month_key AS fiscal_month_key, 
            MAX(fm.fiscal_year_key) AS fiscal_year_key, 
            MAX(fm.begin_date) AS begin_date, 
            MAX(fm.end_date) AS end_date,
            SUM(
                CASE    
                    WHEN a.type = 'E' THEN 
                        CAST(gl.debit_amount AS DECIMAL(18,2)) - CAST(gl.credit_amount AS DECIMAL(18,2))
                    WHEN a.type = 'R' THEN 
                        CAST(gl.credit_amount AS DECIMAL(18,2)) - CAST(gl.debit_amount AS DECIMAL(18,2))
                    ELSE 0 
                END
            ) AS amount,
            SUM(
                CASE    
                    WHEN a.type = 'E' THEN 
                        CAST(gl.local_debit_amount AS DECIMAL(18,2)) - CAST(gl.local_credit_amount AS DECIMAL(18,2))
                    WHEN a.type = 'R' THEN 
                        CAST(gl.local_credit_amount AS DECIMAL(18,2)) - CAST(gl.local_debit_amount AS DECIMAL(18,2))
                    ELSE 0 
                END
            ) AS local_amount,
            MAX(gl.transaction_currency) AS transaction_currency, 
            MAX(gl.local_currency) AS local_currency, 
            MAX(a.type) AS acc_type
        FROM [dbo].["aretum"."general_ledger"] gl
        JOIN [dbo].["aretum"."fiscal_month"] fm ON fm.fiscal_month_key = gl.fiscal_month_key
        JOIN [dbo].["aretum"."account"] a ON a.account_key = gl.account_key
        WHERE a.type IN ('E','R')
        GROUP BY gl.account_key, gl.organization_key, gl.fiscal_month_key  
        UNION ALL
        SELECT 
            afa.account_key, 
            afa.org_key, 
            afa.fiscal_month_key, 
            afa.fiscal_year_key, 
            afa.begin_date, 
            afa.end_date, 
            0 AS amount, 
            0 AS local_amount, 
            afa.transaction_currency, 
            afa.local_currency, 
            afa.acc_type
        FROM all_fiscal_accounts afa
        WHERE afa.acc_type IN ('E','R')
          AND NOT EXISTS (
            SELECT 1 
            FROM [dbo].["aretum"."general_ledger"] gl1
            WHERE gl1.account_key = afa.account_key 
              AND gl1.organization_key = afa.org_key 
              AND gl1.fiscal_month_key = afa.fiscal_month_key
          )
    ),
    totals AS (
        SELECT  
            x.account_key, 
            x.org_key, 
            x.acc_type AS account_type, 
            fm_next.fiscal_month_key, 
            x.fiscal_month_key AS prev_fiscal_month_key,
            SUM(x.amount) OVER (PARTITION BY x.account_key, x.org_key ORDER BY x.begin_date) AS amount,
            SUM(x.amount) OVER (PARTITION BY x.account_key, x.org_key ORDER BY x.begin_date) AS amount_c,
            SUM(x.local_amount) OVER (PARTITION BY x.account_key, x.org_key ORDER BY x.begin_date) AS local_amount,
            SUM(x.local_amount) OVER (PARTITION BY x.account_key, x.org_key ORDER BY x.begin_date) AS local_amount_c,
            x.transaction_currency,
            x.local_currency
        FROM (
            SELECT 
                gl.account_key, 
                gl.organization_key AS org_key, 
                gl.fiscal_month_key, 
                MAX(fm.begin_date) AS begin_date, 
                MAX(fm.end_date) AS end_date,
                SUM(
                    CASE    
                        WHEN a.type = 'A' THEN 
                            CAST(gl.debit_amount AS DECIMAL(18,2)) - CAST(gl.credit_amount AS DECIMAL(18,2))
                        WHEN a.type = 'L' THEN 
                            CAST(gl.credit_amount AS DECIMAL(18,2)) - CAST(gl.debit_amount AS DECIMAL(18,2))
                        ELSE 0 
                    END
                ) AS amount,
                SUM(
                    CASE    
                        WHEN a.type = 'A' THEN 
                            CAST(gl.local_debit_amount AS DECIMAL(18,2)) - CAST(gl.local_credit_amount AS DECIMAL(18,2))
                        WHEN a.type = 'L' THEN 
                            CAST(gl.local_credit_amount AS DECIMAL(18,2)) - CAST(gl.local_debit_amount AS DECIMAL(18,2))
                        ELSE 0 
                    END
                ) AS local_amount,
                MAX(gl.local_currency) AS local_currency,
                MAX(gl.transaction_currency) AS transaction_currency,
                MAX(a.type) AS acc_type
            FROM [dbo].["aretum"."general_ledger"] gl
            JOIN [dbo].["aretum"."fiscal_month"] fm ON fm.fiscal_month_key = gl.fiscal_month_key
            JOIN [dbo].["aretum"."account"] a ON a.account_key = gl.account_key
            WHERE a.type IN ('A','L')
            GROUP BY gl.account_key, gl.organization_key, gl.fiscal_month_key
            UNION ALL
            SELECT 
                afa.account_key, 
                afa.org_key, 
                afa.fiscal_month_key, 
                afa.begin_date, 
                afa.end_date, 
                0 AS amount, 
                0 AS local_amount, 
                afa.local_currency, 
                afa.transaction_currency, 
                afa.acc_type
            FROM all_fiscal_accounts afa
            WHERE afa.acc_type IN ('A','L')
              AND NOT EXISTS (
                SELECT 1
                FROM [dbo].["aretum"."general_ledger"] gl1
                WHERE gl1.account_key = afa.account_key 
                  AND gl1.organization_key = afa.org_key 
                  AND gl1.fiscal_month_key = afa.fiscal_month_key
              )
        ) x  
        JOIN [dbo].["aretum"."fiscal_month"] fm_next ON fm_next.begin_date = DATEADD(day, 1, x.end_date)
        UNION ALL
        SELECT  
            x.account_key, 
            x.org_key, 
            MAX(x.acc_type) AS account_type, 
            fm_next.fiscal_month_key, 
            x.fiscal_month_key AS prev_fiscal_month_key,
            CASE WHEN fm_next.period_number = 1 THEN 0 ELSE SUM(y.amount) END AS amount,
            SUM(y.amount) AS amount_c,
            CASE WHEN fm_next.period_number = 1 THEN 0 ELSE SUM(y.local_amount) END AS local_amount,
            SUM(y.local_amount) AS local_amount_c,
            MAX(x.transaction_currency) AS transaction_currency,
            MAX(x.local_currency) AS local_currency
        FROM re_accounts_cte x
        JOIN re_accounts_cte y ON x.account_key = y.account_key 
            AND x.org_key  = y.org_key 
            AND x.begin_date >= y.begin_date 
            AND x.fiscal_year_key = y.fiscal_year_key
        JOIN [dbo].["aretum"."fiscal_month"] fm_next ON fm_next.begin_date = DATEADD(day, 1, x.end_date)
        JOIN [dbo].["aretum"."fiscal_year"] fy ON fy.fiscal_year_key = fm_next.fiscal_year_key
        GROUP BY x.account_key, x.org_key, fm_next.fiscal_month_key, fm_next.period_number, x.fiscal_month_key
    )
    SELECT 
        [General Ledger Key], [Acct Key], [Customer Key], [Organization Key], [Project Key], [Person Key], [Fiscal Month Key], [Acct Code], [Acct Description], [Acct Type], [Acct Type Order], [Credit Amount], [Customer/Vendor Code], [Customer/Vendor Name], [Debit Amount], [GL Description], [Doc Number], [Doc Type], [Doc Type Description], [Doc Key], [Fin Org Code], [Fin Org Name], [Fiscal Period], [Post Date], [Quantity], [Transaction Date], [Legal Entity Org Code], [Legal Entity Org Name], [Person First Name], [Person Last Name], [Person Middle Initial], [Proj Code], [Proj Org Code], [Proj Org Name], [Proj Owning Org Code], [Proj Owning Org Name], [Reference], [BEGINNING_BALANCE], [ENDING_BALANCE], [Transaction Currency Code], [Local Currency Code], [Local Beginning Balance], [Local Credit Amount], [Local Debit Amount], [Local Ending Balance]
    FROM (
        SELECT 
            NULL AS [General Ledger Key], 
            t.account_key AS [Acct Key], 
            NULL AS [Customer Key], 
            t.org_key AS [Organization Key], 
            NULL AS [Project Key], 
            NULL AS [Person Key], 
            fm_prev.fiscal_month_key AS [Fiscal Month Key], 
            a.account_code AS [Acct Code], 
            a.description AS [Acct Description], 
            CASE a.type WHEN 'A' THEN 'Asset' WHEN 'E' THEN 'Expense' WHEN 'L' THEN 'Liability' WHEN 'R' THEN 'Revenue' ELSE 'Unknown Account Type' END AS [Acct Type],
            CASE a.type WHEN 'A' THEN 1 WHEN 'L' THEN 2 WHEN 'R' THEN 3 WHEN 'E' THEN 4 END AS [Acct Type Order],
            NULL AS [Credit Amount],
            NULL AS [Customer/Vendor Code],
            NULL AS [Customer/Vendor Name],
            NULL AS [Debit Amount],
            'Calculated Balance' AS [GL Description],
            NULL AS [Doc Number],
            NULL AS [Doc Key],
            NULL AS [Doc Type],
            NULL AS [Doc Type Description],
            org.customer_code AS [Fin Org Code],
            org.customer_name AS [Fin Org Name],
            CASE WHEN fm_prev.period_number < 10 THEN CONCAT(CONCAT(CONCAT(fy.name,'-'),'0'),CAST(fm_prev.period_number AS VARCHAR(4))) ELSE CONCAT(CONCAT(fy.name,'-'),CAST(fm_prev.period_number AS VARCHAR(4))) END AS [Fiscal Period],
            NULL AS [Post Date],
            NULL AS [Quantity],
            NULL AS [Transaction Date],
            le.customer_code AS [Legal Entity Org Code],
            le.customer_name AS [Legal Entity Org Name],
            NULL AS [Person First Name],
            NULL AS [Person Last Name],
            NULL AS [Person Middle Initial],
            NULL AS [Proj Code],
            NULL AS [Proj Org Code],
            NULL AS [Proj Org Name],
            NULL AS [Proj Owning Org Code],
            NULL AS [Proj Owning Org Name],
            NULL AS [Reference],
            t_p.amount AS [BEGINNING_BALANCE],
            t.amount AS [ENDING_BALANCE],
            ccc.iso_currency_code AS [Transaction Currency Code],
            lcc.iso_currency_code AS [Local Currency Code],
            t_p.local_amount AS [Local Beginning Balance],
            NULL AS [Local Credit Amount],
            NULL AS [Local Debit Amount],
            t.local_amount AS [Local Ending Balance]
        FROM totals t
        JOIN totals t_p ON t.account_key = t_p.account_key AND t.org_key = t_p.org_key AND t.prev_fiscal_month_key = t_p.fiscal_month_key
        JOIN [dbo].["aretum"."account"] a ON a.account_key = t.account_key
        JOIN [dbo].["aretum"."customer"] org ON org.customer_key = t.org_key
        JOIN [dbo].["aretum"."fiscal_month"] fm ON fm.fiscal_month_key = t.fiscal_month_key
        JOIN [dbo].["aretum"."fiscal_month"] fm_prev ON fm_prev.end_date = DATEADD(day, -1, fm.begin_date)
        JOIN [dbo].["aretum"."fiscal_year"] fy ON fy.fiscal_year_key = fm_prev.fiscal_year_key
        LEFT OUTER JOIN [dbo].["aretum"."customer"] le ON le.customer_key = org.legal_entity_key
        JOIN [dbo].["aretum"."currency_code"] ccc ON ccc.currency_code_key = t.transaction_currency
        JOIN [dbo].["aretum"."currency_code"] lcc ON lcc.currency_code_key = t.local_currency
        WHERE NOT (t_p.amount = 0 AND t.amount = 0)
          AND fm.begin_date >= DATEADD(year, -3, GETDATE())
        UNION ALL
        SELECT 
            gl.general_ledger_key AS [General Ledger Key], 
            a.account_key AS [Acct Key], 
            gl.customer_key AS [Customer Key], 
            gl.organization_key AS [Organization Key], 
            gl.project_key AS [Project Key], 
            gl.person_key AS [Person Key], 
            gl.fiscal_month_key AS [Fiscal Month Key], 
            a.account_code AS [Acct Code], 
            a.description AS [Acct Description], 
            CASE a.type WHEN 'A' THEN 'Asset' WHEN 'E' THEN 'Expense' WHEN 'L' THEN 'Liability' WHEN 'R' THEN 'Revenue' ELSE 'Unknown Account Type' END AS [Acct Type],
            CASE a.type WHEN 'A' THEN 1 WHEN 'L' THEN 2 WHEN 'R' THEN 3 WHEN 'E' THEN 4 END AS [Acct Type Order],
            gl.credit_amount AS [Credit Amount],
            cust.customer_code AS [Customer/Vendor Code],
            cust.customer_name AS [Customer/Vendor Name],
            gl.debit_amount AS [Debit Amount],
            gl.description AS [GL Description],
            gl.document_number AS [Doc Number],
            gl.general_ledger_key AS [Doc Key],
            CASE gl.feature 
                WHEN 0 THEN 'VI' WHEN 1 THEN 'VP' WHEN 2 THEN 'F2' WHEN 3 THEN 'CP' WHEN 4 THEN 'D'
                WHEN 5 THEN 'JE' WHEN 6 THEN 'BR' WHEN 7 THEN 'CI' WHEN 8 THEN 'LC' WHEN 9 THEN 'EC'
                WHEN 10 THEN 'FY' WHEN 11 THEN 'AL' WHEN 13 THEN 'FA' WHEN 14 THEN 'PILOB'
                ELSE 'Unknown Document Type'
            END AS [Doc Type],
            CASE gl.feature 
                WHEN 0 THEN 'Vendor Invoice' WHEN 1 THEN 'Vendor Payment' WHEN 2 THEN 'Unknown Feature 2'
                WHEN 3 THEN 'Customer Payment' WHEN 4 THEN 'Deposit' WHEN 5 THEN 'Journal Entry'
                WHEN 6 THEN 'Billing and Revenue Post' WHEN 7 THEN 'Invoice' WHEN 8 THEN 'Labor Cost Post'
                WHEN 9 THEN 'Expense Report Cost Post' WHEN 10 THEN 'General Ledger Closing'
                WHEN 11 THEN 'Cost Pool Post' WHEN 13 THEN 'Fixed Asset Post' WHEN 14 THEN 'Pay in Lieu of Benefits'
                ELSE 'Unknown Document Type'
            END AS [Doc Type Description],
            org.customer_code AS [Fin Org Code],
            org.customer_name AS [Fin Org Name],
            CASE WHEN fm.period_number < 10 THEN CONCAT(CONCAT(CONCAT(fy.name,'-'),'0'),CAST(fm.period_number AS VARCHAR(4))) ELSE CONCAT(CONCAT(fy.name,'-'),CAST(fm.period_number AS VARCHAR(4))) END AS [Fiscal Period],
            gl.post_date AS [Post Date],
            gl.quantity AS [Quantity],
            gl.transaction_date AS [Transaction Date],
            le.customer_code AS [Legal Entity Org Code],
            le.customer_name AS [Legal Entity Org Name],
            pers.first_name AS [Person First Name],
            pers.last_name AS [Person Last Name],
            pers.middle_initial AS [Person Middle Initial],
            proj.project_code AS [Proj Code],
            porg.customer_code AS [Proj Org Code],
            porg.customer_name AS [Proj Org Name],
            pown.customer_code AS [Proj Owning Org Code],
            pown.customer_name AS [Proj Owning Org Name],
            gl.reference AS [Reference],
            NULL AS [BEGINNING_BALANCE],
            NULL AS [ENDING_BALANCE],
            ccc.iso_currency_code AS [Transaction Currency Code],
            lcc.iso_currency_code AS [Local Currency Code],
            NULL AS [Local Beginning Balance],
            gl.local_credit_amount AS [Local Credit Amount],
            gl.local_debit_amount AS [Local Debit Amount],
            NULL AS [Local Ending Balance]
        FROM [dbo].["aretum"."general_ledger"] gl
        JOIN [dbo].["aretum"."account"] a ON a.account_key = gl.account_key
        JOIN [dbo].["aretum"."customer"] org ON org.customer_key = gl.organization_key
        LEFT OUTER JOIN [dbo].["aretum"."customer"] le ON le.customer_key = org.legal_entity_key
        LEFT OUTER JOIN [dbo].["aretum"."customer"] cust ON cust.customer_key = gl.customer_key
        JOIN [dbo].["aretum"."fiscal_month"] fm ON fm.fiscal_month_key = gl.fiscal_month_key
        JOIN [dbo].["aretum"."fiscal_year"] fy ON fy.fiscal_year_key = fm.fiscal_year_key
        LEFT OUTER JOIN [dbo].["aretum"."project"] proj ON proj.project_key = gl.project_key
        LEFT OUTER JOIN [dbo].["aretum"."customer"] porg ON porg.customer_key = proj.customer_key
        LEFT OUTER JOIN [dbo].["aretum"."customer"] pown ON pown.customer_key = proj.owning_customer_key
        LEFT OUTER JOIN [dbo].["aretum"."person"] pers ON pers.person_key = gl.person_key
        JOIN [dbo].["aretum"."currency_code"] ccc ON ccc.currency_code_key = gl.transaction_currency
        JOIN [dbo].["aretum"."currency_code"] lcc ON lcc.currency_code_key = gl.local_currency
        WHERE fm.begin_date >= DATEADD(year, -3, GETDATE())
    ) x 
    WHERE (
        EXISTS (SELECT 1 FROM [dbo].["aretum"."member"] WHERE person_key = '3896' AND role_key = 1)
        OR EXISTS (SELECT 1 FROM [dbo].["aretum"."org_access_person"] WHERE person_key = '3896' AND role_key = 33 AND global_access = 'Y')
        OR (
            x.[Organization Key] IN (
                SELECT v.customer_key 
                FROM [dbo].["aretum"."access_customer_view"] v 
                JOIN [dbo].["aretum"."org_access_person"] oap ON oap.org_access_person_key = v.org_access_person_key
                WHERE oap.person_key = '3896' AND oap.role_key = 33 AND oap.access_type = 2 AND legal_entity_ind = 'N'
                UNION ALL
                SELECT cust.customer_key 
                FROM [dbo].["aretum"."customer"] cust
                WHERE cust.legal_entity_key IN (
                    SELECT v.customer_key 
                    FROM [dbo].["aretum"."access_customer_view"] v 
                    JOIN [dbo].["aretum"."org_access_person"] oap ON oap.org_access_person_key = v.org_access_person_key
                    WHERE oap.person_key = '3896' AND oap.role_key = 33 AND oap.access_type = 2 AND legal_entity_ind = 'Y'
                )
            )
        )
    )
) wrE19
WHERE 
    (wrE19.[Fin Org Code] IN ('1.01.03.03.02MIR-DHS','1.01.03.01.02PAN-DHS','1.01.03.00.02.02-DHSDIR','1.01.03.00.02.01-DHSLEAD','1.01.03.00.02-DHS') AND wrE19.[Acct Description] IN ('Direct - IFF (Non-Billed)','Direct - Proj Mgmt Bns','Direct Consultant Labor (1099)','Direct Labor','Direct Subcontractor Labor','Other Direct Costs (ODCs)'))
--  AND wrE19.[Fiscal Period] = 'FY2025-05'
