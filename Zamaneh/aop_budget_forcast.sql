CREATE VIEW [dbo].[APOBudgetAndBussinessForcast]
AS
SELECT 
    wrE19.[Budget] AS "Budget",
	wrE19.[Fiscal Period] AS "Fiscal Period",
	wrE20.[Acct Fin Lv2 Code] AS "Acct Fin Lv2 Code",
	wrE20.[Acct Fin Lv2 Name] AS "Acct Fin Lv2 Name",
	wrE20.[Acct Fin Lv3 Code] AS "Acct Fin Lv3 Code",
	wrE20.[Acct Fin Lv3 Name] AS "Acct Fin Lv3 Name",
	wrE20.[Acct Fin Lv4 Code] AS "Acct Fin Lv4 Code",
	wrE20.[Acct Fin Lv4 Name] AS "Acct Fin Lv4 Name",
	wrE20.[Acct Fin Lv5 Code] AS "Acct Fin Lv5 Code",
	wrE20.[Acct Fin Lv5 Name] AS "Acct Fin Lv5 Name",
	wrE20.[Acct Fin Lv6 Code] AS "Acct Fin Lv6 Code",
	wrE20.[Acct Fin Lv6 Name] AS "Acct Fin Lv6 Name",
	wrE20.[Acct Type] AS "Acct Type",
	wrE19.[Organization Key] AS "Organization Key",
	wrE19.[Fiscal Month Key] AS "Fiscal Month Key",
	wrE19.[Actual / Budget Name] AS "Actual / Budget Name",
	wrE20.[ACCOUNT_KEY] AS "ACCOUNT_KEY",
	wrE9.[CUSTOMER_KEY] AS "CUSTOMER_KEY",
	wrE21.[Fiscal Month Key] AS "Fiscal Month Key2",
	wrE19.[Fin/Budget Org Code] AS "Fin/Budget Org Code"
FROM (
	SELECT 
		x.account_key AS [Acct Key], 
		x.organization_key AS [Organization Key], 
		x.fiscal_month_key AS [Fiscal Month Key], 
		x.account_code AS [Acct Code], 
		x.description AS [Acct Description], 
		x.acct_type_order AS [Acct Type Order], 
		x.acct_type AS [Acct Type], 
		x.fin_org_code AS [Fin/Budget Org Code], 
		x.fin_org_name AS [Fin/Budget Org Name], 
		x.fiscal_period AS [Fiscal Period], 
		x.le_org_code AS [Legal Entity Org Code], 
		x.le_org_name AS [Legal Entity Org Name], 
		x.act_budname AS [Actual / Budget Name], 
		SUM(x.actual) AS [Actual], 
		SUM(x.budget) AS [Budget], 
		SUM(x.net_amount) AS [Net Amount (Credit - Debit)], 
		x.elimination_flag AS [Elimination Org Indicator], 
		SUM(x.local_actual) AS [Local Actual], 
		SUM(x.local_net_amount) AS [Local Net Amount (Credit - Debit)], 
		x.local_currency_code AS [Local Currency Code]
	FROM (
		SELECT 
			gl.account_key,
			gl.organization_key,
			gl.fiscal_month_key,
			a.account_code,
			a.description,
			CASE a.type WHEN 'A' THEN 1 WHEN 'L' THEN 2 WHEN 'R' THEN 3 WHEN 'E' THEN 4 END AS acct_type_order,
			CASE a.type WHEN 'A' THEN 'Asset' WHEN 'E' THEN 'Expense' WHEN 'L' THEN 'Liability' WHEN 'R' THEN 'Revenue' ELSE 'Unknown Account Type' END AS acct_type,
			org.customer_code AS fin_org_code,
			org.customer_name AS fin_org_name,
			org.transact_elimination_flag AS elimination_flag,
			CASE WHEN fm.period_number < 10 THEN CONCAT(fy.name, '-0', CAST(fm.period_number AS VARCHAR(4))) ELSE CONCAT(fy.name, '-', CAST(fm.period_number AS VARCHAR(4))) END AS fiscal_period,
			le.customer_code AS le_org_code,
			le.customer_name AS le_org_name,
			'Actual' AS act_budname,
			CASE a.type WHEN 'R' THEN (gl.credit_amount - gl.debit_amount) ELSE (gl.debit_amount - gl.credit_amount) END AS actual,
			CASE a.type WHEN 'R' THEN (gl.local_credit_amount - gl.local_debit_amount) ELSE (gl.local_debit_amount - gl.local_credit_amount) END AS local_actual,
			0 AS budget,
			(gl.credit_amount - gl.debit_amount) AS net_amount,
			(gl.local_credit_amount - gl.local_debit_amount) AS local_net_amount,
			cc.iso_currency_code AS local_currency_code
		FROM [dbo].["aretum"."general_ledger"] gl
		JOIN [dbo].["aretum"."account"] a ON a.account_key = gl.account_key
		JOIN [dbo].["aretum"."customer"] org ON org.customer_key = gl.organization_key
		LEFT JOIN [dbo].["aretum"."customer"] le ON le.customer_key = org.legal_entity_key
		JOIN [dbo].["aretum"."fiscal_month"] fm ON fm.fiscal_month_key = gl.fiscal_month_key
		JOIN [dbo].["aretum"."fiscal_year"] fy ON fy.fiscal_year_key = fm.fiscal_year_key
		JOIN [dbo].["aretum"."currency_code"] cc ON cc.currency_code_key = gl.local_currency
		WHERE a.type IN ('R','E')
			AND fm.begin_date >= DATEADD(
                     month, -36,
                     DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1))
		
		UNION ALL
		
		SELECT 
			bd.account_key,
			c.customer_key,
			bd.fiscal_month_key,
			a.account_code,
			a.description,
			CASE a.type WHEN 'A' THEN 1 WHEN 'L' THEN 2 WHEN 'R' THEN 3 WHEN 'E' THEN 4 END AS acct_type_order,
			CASE a.type WHEN 'A' THEN 'Asset' WHEN 'E' THEN 'Expense' WHEN 'L' THEN 'Liability' WHEN 'R' THEN 'Revenue' ELSE 'Unknown Account Type' END AS acct_type,
			c.customer_code AS fin_org_code,
			c.customer_name AS fin_org_name,
			c.transact_elimination_flag AS elimination_flag,
			CASE WHEN fm.period_number < 10 THEN CONCAT(fy.name, '-0', CAST(fm.period_number AS VARCHAR(4))) ELSE CONCAT(fy.name, '-', CAST(fm.period_number AS VARCHAR(4))) END AS fiscal_period,
			cle.customer_code AS le_org_code,
			cle.customer_name AS le_org_name,
			bn.budget_name AS act_budname,
			0 AS actual,
			0 AS local_actual,
			bd.amount AS budget,
			CASE WHEN a.type = 'R' THEN bd.amount ELSE (bd.amount * (-1)) END AS net_amount,
			CASE WHEN a.type = 'R' THEN bd.amount ELSE (bd.amount * (-1)) END AS local_net_amount,
			cc.iso_currency_code AS local_currency_code
		FROM [dbo].["aretum"."budget_detail"] bd
		JOIN [dbo].["aretum"."account"] a ON a.account_key = bd.account_key
		JOIN [dbo].["aretum"."budget_organization"] bo ON bo.budget_organization_key = bd.budget_organization_key
		JOIN [dbo].["aretum"."budget"] b ON b.budget_key = bo.budget_key
		JOIN [dbo].["aretum"."budget_name"] bn ON bn.budget_name_key = b.budget_name_key
		JOIN [dbo].["aretum"."customer"] c ON c.customer_key = bo.customer_key
		JOIN [dbo].["aretum"."fiscal_month"] fm ON fm.fiscal_month_key = bd.fiscal_month_key
		JOIN [dbo].["aretum"."fiscal_year"] fy ON fy.fiscal_year_key = fm.fiscal_year_key
		LEFT JOIN [dbo].["aretum"."customer"] cle ON cle.customer_key = c.legal_entity_key
		LEFT JOIN [dbo].["aretum"."currency_code"] cc ON cc.currency_code_key = cle.currency_code_key
		WHERE b.type = 'I'
			AND fm.begin_date >= DATEADD(
                     month, -36,
                     DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1))
	) x
	WHERE 
		(EXISTS (SELECT 1 FROM [dbo].["aretum"."member"] WHERE person_key = 3896 AND role_key = 1))
		OR (EXISTS (SELECT 1 FROM [dbo].["aretum"."org_access_person"] WHERE person_key = 3896 AND role_key = 33 AND global_access = 'Y'))
		OR (x.organization_key IN (
			SELECT customer_key
			FROM [dbo].["aretum"."access_customer_view"] v
			JOIN [dbo].["aretum"."org_access_person"] oap ON oap.org_access_person_key = v.org_access_person_key
			WHERE oap.person_key = 3896 AND oap.role_key = 33 AND oap.access_type = 2 AND legal_entity_ind = 'N'
			UNION ALL
			SELECT customer_key
			FROM [dbo].["aretum"."customer"] cust
			WHERE cust.legal_entity_key IN (
				SELECT customer_key
				FROM [dbo].["aretum"."access_customer_view"] v
				JOIN [dbo].["aretum"."org_access_person"] oap ON oap.org_access_person_key = v.org_access_person_key
				WHERE oap.person_key = 3896 AND oap.role_key = 33 AND oap.access_type = 2 AND legal_entity_ind = 'Y'
			)
		))
	GROUP BY 
		x.account_key, x.organization_key, x.fiscal_month_key, x.account_code, x.description, x.acct_type_order, x.acct_type, x.fin_org_code, x.fin_org_name, x.fiscal_period, x.le_org_code, x.le_org_name, x.act_budname, x.elimination_flag, x.local_currency_code
) wrE19
INNER JOIN (
	SELECT a.account_key AS "ACCOUNT_KEY", a.account_code AS "Acct Code", a.description AS "Acct Description",
		CASE a.type WHEN 'A' THEN 'Asset' WHEN 'E' THEN 'Expense' WHEN 'L' THEN 'Liability' WHEN 'R' THEN 'Revenue' ELSE 'Unknown Account Type' END AS "Acct Type",
		CASE a.type WHEN 'A' THEN 1 WHEN 'L' THEN 2 WHEN 'R' THEN 3 WHEN 'E' THEN 4 END AS "Acct Type Order",
		a.active AS "Acct Active", a.entry_allowed AS "Acct Entry Allowed", a.begin_date AS "Acct Begin Date", a.end_date AS "Acct End Date",
		a.project_required AS "Acct Proj Required", acct_fin_tree1.account_code AS "Acct Fin Lv1 Code", acct_fin_tree1.description AS "Acct Fin Lv1 Name",
		coalesce(acct_fin_tree2.account_code, acct_fin_tree1.account_code) AS "Acct Fin Lv2 Code",
		coalesce(acct_fin_tree2.description, acct_fin_tree1.description) AS "Acct Fin Lv2 Name",
		coalesce(acct_fin_tree3.account_code, acct_fin_tree2.account_code, acct_fin_tree1.account_code) AS "Acct Fin Lv3 Code",
		coalesce(acct_fin_tree3.description, acct_fin_tree2.description, acct_fin_tree1.description) AS "Acct Fin Lv3 Name",
		coalesce(acct_fin_tree4.account_code, acct_fin_tree3.account_code, acct_fin_tree2.account_code, acct_fin_tree1.account_code) AS "Acct Fin Lv4 Code",
		coalesce(acct_fin_tree4.description, acct_fin_tree3.description, acct_fin_tree2.description, acct_fin_tree1.description) AS "Acct Fin Lv4 Name",
		coalesce(acct_fin_tree5.account_code, acct_fin_tree4.account_code, acct_fin_tree3.account_code, acct_fin_tree2.account_code, acct_fin_tree1.account_code) AS "Acct Fin Lv5 Code",
		coalesce(acct_fin_tree5.description, acct_fin_tree4.description, acct_fin_tree3.description, acct_fin_tree2.description, acct_fin_tree1.description) AS "Acct Fin Lv5 Name",
		coalesce(acct_fin_tree6.account_code, acct_fin_tree5.account_code, acct_fin_tree4.account_code, acct_fin_tree3.account_code, acct_fin_tree2.account_code, acct_fin_tree1.account_code) AS "Acct Fin Lv6 Code",
		coalesce(acct_fin_tree6.description, acct_fin_tree5.description, acct_fin_tree4.description, acct_fin_tree3.description, acct_fin_tree2.description, acct_fin_tree1.description) AS "Acct Fin Lv6 Name",
		coalesce(acct_fin_tree7.account_code, acct_fin_tree6.account_code, acct_fin_tree5.account_code, acct_fin_tree4.account_code, acct_fin_tree3.account_code, acct_fin_tree2.account_code, acct_fin_tree1.account_code) AS "Acct Fin Lv7 Code",
		coalesce(acct_fin_tree7.description, acct_fin_tree6.description, acct_fin_tree5.description, acct_fin_tree4.description, acct_fin_tree3.description, acct_fin_tree2.description, acct_fin_tree1.description) AS "Acct Fin Lv7 Name",
		coalesce(acct_fin_tree8.account_code, acct_fin_tree7.account_code, acct_fin_tree6.account_code, acct_fin_tree5.account_code, acct_fin_tree4.account_code, acct_fin_tree3.account_code, acct_fin_tree2.account_code, acct_fin_tree1.account_code) AS "Acct Fin Lv8 Code",
		coalesce(acct_fin_tree8.description, acct_fin_tree7.description, acct_fin_tree6.description, acct_fin_tree5.description, acct_fin_tree4.description, acct_fin_tree3.description, acct_fin_tree2.description, acct_fin_tree1.description) AS "Acct Fin Lv8 Name",
		coalesce(acct_fin_tree9.account_code, acct_fin_tree8.account_code, acct_fin_tree7.account_code, acct_fin_tree6.account_code, acct_fin_tree5.account_code, acct_fin_tree4.account_code, acct_fin_tree3.account_code, acct_fin_tree2.account_code, acct_fin_tree1.account_code) AS "Acct Fin Lv9 Code",
		coalesce(acct_fin_tree9.description, acct_fin_tree8.description, acct_fin_tree7.description, acct_fin_tree6.description, acct_fin_tree5.description, acct_fin_tree4.description, acct_fin_tree3.description, acct_fin_tree2.description, acct_fin_tree1.description) AS "Acct Fin Lv9 Name",
		coalesce(acct_fin_tree10.account_code, acct_fin_tree9.account_code, acct_fin_tree8.account_code, acct_fin_tree7.account_code, acct_fin_tree6.account_code, acct_fin_tree5.account_code, acct_fin_tree4.account_code, acct_fin_tree3.account_code, acct_fin_tree2.account_code, acct_fin_tree1.account_code) AS "Acct Fin Lv10 Code",
		coalesce(acct_fin_tree10.description, acct_fin_tree9.description, acct_fin_tree8.description, acct_fin_tree7.description, acct_fin_tree6.description, acct_fin_tree5.description, acct_fin_tree4.description, acct_fin_tree3.description, acct_fin_tree2.description, acct_fin_tree1.description) AS "Acct Fin Lv10 Name",
		acct_cp_tree1.account_code AS "Acct Cost Pool Lv1 Code", acct_cp_tree1.description AS "Acct Cost Pool Lv1 Name",
		CASE acct_fin_tree1.type WHEN 'A' THEN 'Asset' WHEN 'E' THEN 'Expense' WHEN 'L' THEN 'Liability' WHEN 'R' THEN 'Revenue' ELSE 'Unknown Account Type' END AS "Acct Fin Lv1 Type",
		coalesce(acct_cp_tree2.account_code, acct_cp_tree1.account_code) AS "Acct Cost Pool Lv2 Code",
		coalesce(acct_cp_tree2.description, acct_cp_tree1.description) AS "Acct Cost Pool Lv2 Name",
		CASE coalesce(acct_fin_tree2.type, acct_fin_tree1.type) WHEN 'A' THEN 'Asset' WHEN 'E' THEN 'Expense' WHEN 'L' THEN 'Liability' WHEN 'R' THEN 'Revenue' ELSE 'Unknown Account Type' END AS "Acct Fin Lv2 Type",
		a.hide_income_stmt_hdr  as "Acct Hide Income Stmt Hdr", a.category_1099         as "Acct Cat 1099",
		CASE ca.category WHEN 'O' THEN 'Operating Activities' WHEN 'I' THEN 'Investing Activities' WHEN 'F' THEN 'Financing Activities' WHEN 'C' THEN 'Cash' ELSE ' ' END as "Cash Flow Category",
		CASE WHEN ca.category in('O', 'I', 'F', 'C') THEN coalesce(rollup_account.account_code,a.account_code) ELSE rollup_account.account_code END as "CFR Account Code",
		CASE WHEN ca.category in('O', 'I', 'F', 'C') THEN coalesce(rollup_account.description,a.description) ELSE rollup_account.description END as "CFR Account Description"
	FROM [dbo].["aretum"."account"] a

left outer join (select acpt.node_key acct_key, max(case when p.tree_level = 0 then p.node_key else null end) acct_key_0, max(case when p.tree_level = 1 then p.node_key else null end) acct_key_1, max(case when p.tree_level = 2 then p.node_key else null end) acct_key_2, max(case when p.tree_level = 3 then p.node_key else null end) acct_key_3, max(case when p.tree_level = 4 then p.node_key else null end) acct_key_4, max(case when p.tree_level = 5 then p.node_key else null end) acct_key_5, max(case when p.tree_level = 6 then p.node_key else null end) acct_key_6, max(case when p.tree_level = 7 then p.node_key else null end) acct_key_7, max(case when p.tree_level = 8 then p.node_key else null end) acct_key_8, max(case when p.tree_level = 9 then p.node_key else null end) acct_key_9 from [dbo].["aretum"."acct_cost_pool_tree"] acpt join [dbo].["aretum"."acct_cost_pool_tree"] p on acpt.left_visit between p.left_visit and p.right_visit group by acpt.node_key) acct_cp_tree_accts on acct_cp_tree_accts.acct_key = a.account_key  left outer join [dbo].["aretum"."account"] acct_cp_tree1 on acct_cp_tree1.account_key = acct_cp_tree_accts.acct_key_0 left outer join [dbo].["aretum"."account"] acct_cp_tree2 on acct_cp_tree2.account_key = acct_cp_tree_accts.acct_key_1 left outer join [dbo].["aretum"."account"] acct_cp_tree3 on acct_cp_tree3.account_key = acct_cp_tree_accts.acct_key_2 left outer join [dbo].["aretum"."account"] acct_cp_tree4 on acct_cp_tree4.account_key = acct_cp_tree_accts.acct_key_3 left outer join [dbo].["aretum"."account"] acct_cp_tree5 on acct_cp_tree5.account_key = acct_cp_tree_accts.acct_key_4 left outer join [dbo].["aretum"."account"] acct_cp_tree6 on acct_cp_tree6.account_key = acct_cp_tree_accts.acct_key_5 left outer join [dbo].["aretum"."account"] acct_cp_tree7 on acct_cp_tree7.account_key = acct_cp_tree_accts.acct_key_6 left outer join [dbo].["aretum"."account"] acct_cp_tree8 on acct_cp_tree8.account_key = acct_cp_tree_accts.acct_key_7 left outer join [dbo].["aretum"."account"] acct_cp_tree9 on acct_cp_tree9.account_key = acct_cp_tree_accts.acct_key_8 left outer join [dbo].["aretum"."account"] acct_cp_tree10 on acct_cp_tree10.account_key = acct_cp_tree_accts.acct_key_9 left outer join (select aft.node_key acct_key, max(case when p.tree_level = 0 then p.node_key else null end) acct_key_0, max(case when p.tree_level = 1 then p.node_key else null end) acct_key_1, max(case when p.tree_level = 2 then p.node_key else null end) acct_key_2, max(case when p.tree_level = 3 then p.node_key else null end) acct_key_3, max(case when p.tree_level = 4 then p.node_key else null end) acct_key_4, max(case when p.tree_level = 5 then p.node_key else null end) acct_key_5, max(case when p.tree_level = 6 then p.node_key else null end) acct_key_6, max(case when p.tree_level = 7 then p.node_key else null end) acct_key_7, max(case when p.tree_level = 8 then p.node_key else null end) acct_key_8, max(case when p.tree_level = 9 then p.node_key else null end) acct_key_9 from [dbo].["aretum"."acct_fin_tree"] aft join [dbo].["aretum"."acct_fin_tree"] p on aft.left_visit between p.left_visit and p.right_visit group by aft.node_key) acct_fin_tree_accts on acct_fin_tree_accts.acct_key = a.account_key  left outer join [dbo].["aretum"."account"] acct_fin_tree1 on acct_fin_tree1.account_key = acct_fin_tree_accts.acct_key_0 left outer join [dbo].["aretum"."account"] acct_fin_tree2 on acct_fin_tree2.account_key = acct_fin_tree_accts.acct_key_1 left outer join [dbo].["aretum"."account"] acct_fin_tree3 on acct_fin_tree3.account_key = acct_fin_tree_accts.acct_key_2 left outer join [dbo].["aretum"."account"] acct_fin_tree4 on acct_fin_tree4.account_key = acct_fin_tree_accts.acct_key_3 left outer join [dbo].["aretum"."account"] acct_fin_tree5 on acct_fin_tree5.account_key = acct_fin_tree_accts.acct_key_4 left outer join [dbo].["aretum"."account"] acct_fin_tree6 on acct_fin_tree6.account_key = acct_fin_tree_accts.acct_key_5 left outer join [dbo].["aretum"."account"] acct_fin_tree7 on acct_fin_tree7.account_key = acct_fin_tree_accts.acct_key_6 left outer join [dbo].["aretum"."account"] acct_fin_tree8 on acct_fin_tree8.account_key = acct_fin_tree_accts.acct_key_7 left outer join [dbo].["aretum"."account"] acct_fin_tree9 on acct_fin_tree9.account_key = acct_fin_tree_accts.acct_key_8 left outer join [dbo].["aretum"."account"] acct_fin_tree10 on acct_fin_tree10.account_key = acct_fin_tree_accts.acct_key_9 left outer join [dbo].["aretum"."cashflow_account"] ca on a.account_key = ca.cashflow_account_key left outer join [dbo].["aretum"."account"] rollup_account on rollup_account.account_key = ca.rollup_account_key 

) wrE20 ON (
	wrE19.[Acct Key] = wrE20.[ACCOUNT_KEY]
)
INNER JOIN (
	SELECT 
		fm.fiscal_month_key AS "Fiscal Month Key", 
		fm.period_number AS "Fiscal Month", 
		fm.begin_date AS "Fiscal Month Begin Date", 
		fm.end_date AS "Fiscal Month End Date",
		CASE WHEN fm.period_number < 10 THEN CONCAT(fy.name, '-0', CAST(fm.period_number AS VARCHAR(4))) ELSE CONCAT(fy.name, '-', CAST(fm.period_number AS VARCHAR(4))) END AS "Fiscal Period",
		CASE WHEN fm.period_number IN (1,2,3) THEN 'Q1' WHEN fm.period_number IN (4,5,6) THEN 'Q2' WHEN fm.period_number IN (7,8,9) THEN 'Q3' WHEN fm.period_number IN (10,11,12) THEN 'Q4' END AS "Fiscal Quarter",
		fq.begin_date AS "Fiscal Quarter Begin Date", fq.end_date AS "Fiscal Quarter End Date",
		fy.name AS "Fiscal Year", fy.begin_date AS "Fiscal Year Begin Date", fy.end_date AS "Fiscal Year End Date"
	FROM [dbo].["aretum"."fiscal_month"] fm
	JOIN [dbo].["aretum"."fiscal_year"] fy ON fy.fiscal_year_key = fm.fiscal_year_key
	JOIN [dbo].["aretum"."fiscal_quarter"] fq ON fq.fiscal_quarter_key = fm.fiscal_quarter_key
) wrE21 ON (
	wrE19.[Fiscal Month Key] = wrE21.[Fiscal Month Key]
)
INNER JOIN (
	-- Your wrE9 query exactly as in your original
	-- WITH consol_tree_orgs AS (
	-- 	SELECT c.node_key org_key,
	-- 		max(CASE WHEN p.tree_level = 0 THEN p.node_key ELSE null END) org_key_0,
	-- 		max(CASE WHEN p.tree_level = 1 THEN p.node_key ELSE null END) org_key_1,
	-- 		max(CASE WHEN p.tree_level = 2 THEN p.node_key ELSE null END) org_key_2,
	-- 		max(CASE WHEN p.tree_level = 3 THEN p.node_key ELSE null END) org_key_3,
	-- 		max(CASE WHEN p.tree_level = 4 THEN p.node_key ELSE null END) org_key_4,
	-- 		max(CASE WHEN p.tree_level = 5 THEN p.node_key ELSE null END) org_key_5
	-- 	FROM [dbo].["aretum"."org_consolidation_tree"] c
	-- 	JOIN [dbo].["aretum"."org_consolidation_tree"] p ON c.left_visit BETWEEN p.left_visit AND p.right_visit
	-- 	GROUP BY c.node_key
	-- )
	SELECT c.customer_key AS "CUSTOMER_KEY", 
	c.email_1099			as "EMAIL_1099", c.vendor_1099			as "VENDOR_1099", c.recipient_name_1099	as "RECIPIENT_NAME_1099", c.federal_tax_id		as "FEDERAL_TAX_ID", c.federal_tax_id_type	as "FEDERAL_TAX_ID_TYPE", c.financial_org			as "FINANCIAL_ORG", c.legal_entity			as "LEGAL_ENTITY", c.active				as "ACTIVE", c.classification		as "CLASSIFICATION", c.customer_code			as "CUSTOMER_CODE", org_cost_pool_tree1.customer_code as "Org Cost Pool Lv1 Code", org_cost_pool_tree1.customer_name as "Org Cost Pool Lv1 Name", coalesce(org_cost_pool_tree2.customer_code, org_cost_pool_tree1.customer_code) as "Org Cost Pool Lv2 Code", coalesce(org_cost_pool_tree2.customer_name, org_cost_pool_tree1.customer_name) as "Org Cost Pool Lv2 Name", coalesce(org_cost_pool_tree3.customer_code, org_cost_pool_tree2.customer_code, org_cost_pool_tree1.customer_code) as "Org Cost Pool Lv3 Code", coalesce(org_cost_pool_tree3.customer_name, org_cost_pool_tree2.customer_name, org_cost_pool_tree1.customer_name) as "Org Cost Pool Lv3 Name", coalesce(org_cost_pool_tree4.customer_code, org_cost_pool_tree3.customer_code, org_cost_pool_tree2.customer_code, org_cost_pool_tree1.customer_code) as "Org Cost Pool Lv4 Code", coalesce(org_cost_pool_tree4.customer_name, org_cost_pool_tree3.customer_name, org_cost_pool_tree2.customer_name, org_cost_pool_tree1.customer_name) as "Org Cost Pool Lv4 Name", coalesce(org_cost_pool_tree5.customer_code, org_cost_pool_tree4.customer_code, org_cost_pool_tree3.customer_code, org_cost_pool_tree2.customer_code, org_cost_pool_tree1.customer_code) as "Org Cost Pool Lv5 Code", coalesce(org_cost_pool_tree5.customer_name, org_cost_pool_tree4.customer_name, org_cost_pool_tree3.customer_name, org_cost_pool_tree2.customer_name, org_cost_pool_tree1.customer_name) as "Org Cost Pool Lv5 Name", coalesce(org_cost_pool_tree6.customer_code, org_cost_pool_tree5.customer_code, org_cost_pool_tree4.customer_code, org_cost_pool_tree3.customer_code, org_cost_pool_tree2.customer_code, org_cost_pool_tree1.customer_code) as "Org Cost Pool Lv6 Code", coalesce(org_cost_pool_tree6.customer_name, org_cost_pool_tree5.customer_name, org_cost_pool_tree4.customer_name, org_cost_pool_tree3.customer_name, org_cost_pool_tree2.customer_name, org_cost_pool_tree1.customer_name) as "Org Cost Pool Lv6 Name", c.entry_allowed			as "ENTRY_ALLOWED", c.begin_date			as "BEGIN_DATE", c.end_date				as "END_DATE", c.account_number		as "ACCOUNT_NUMBER", org_fin_tree1.customer_code as "Org Fin Lv1 Code", org_fin_tree1.customer_name as "Org Fin Lv1 Name", coalesce(org_fin_tree2.customer_code, org_fin_tree1.customer_code) as "Org Fin Lv2 Code", coalesce(org_fin_tree2.customer_name, org_fin_tree1.customer_name) as "Org Fin Lv2 Name", coalesce(org_fin_tree3.customer_code, org_fin_tree2.customer_code, org_fin_tree1.customer_code) as "Org Fin Lv3 Code", coalesce(org_fin_tree3.customer_name, org_fin_tree2.customer_name, org_fin_tree1.customer_name) as "Org Fin Lv3 Name", coalesce(org_fin_tree4.customer_code, org_fin_tree3.customer_code, org_fin_tree2.customer_code, org_fin_tree1.customer_code) as "Org Fin Lv4 Code", coalesce(org_fin_tree4.customer_name, org_fin_tree3.customer_name, org_fin_tree2.customer_name, org_fin_tree1.customer_name) as "Org Fin Lv4 Name", coalesce(org_fin_tree5.customer_code, org_fin_tree4.customer_code, org_fin_tree3.customer_code, org_fin_tree2.customer_code, org_fin_tree1.customer_code) as "Org Fin Lv5 Code", coalesce(org_fin_tree5.customer_name, org_fin_tree4.customer_name, org_fin_tree3.customer_name, org_fin_tree2.customer_name, org_fin_tree1.customer_name) as "Org Fin Lv5 Name", coalesce(org_fin_tree6.customer_code, org_fin_tree5.customer_code, org_fin_tree4.customer_code, org_fin_tree3.customer_code, org_fin_tree2.customer_code, org_fin_tree1.customer_code) as "Org Fin Lv6 Code", coalesce(org_fin_tree6.customer_name, org_fin_tree5.customer_name, org_fin_tree4.customer_name, org_fin_tree3.customer_name, org_fin_tree2.customer_name, org_fin_tree1.customer_name) as "Org Fin Lv6 Name", c.industry				as "INDUSTRY", c.sic_code				as "SIC_CODE", c.customer_name			as "CUSTOMER_NAME", org_org_tree1.customer_code as "Org Org Lv1 Code", org_org_tree1.customer_name as "Org Org Lv1 Name", coalesce(org_org_tree2.customer_code, org_org_tree1.customer_code) as "Org Org Lv2 Code", coalesce(org_org_tree2.customer_name, org_org_tree1.customer_name) as "Org Org Lv2 Name", coalesce(org_org_tree3.customer_code, org_org_tree2.customer_code, org_org_tree1.customer_code) as "Org Org Lv3 Code", coalesce(org_org_tree3.customer_name, org_org_tree2.customer_name, org_org_tree1.customer_name) as "Org Org Lv3 Name", coalesce(org_org_tree4.customer_code, org_org_tree3.customer_code, org_org_tree2.customer_code, org_org_tree1.customer_code) as "Org Org Lv4 Code", coalesce(org_org_tree4.customer_name, org_org_tree3.customer_name, org_org_tree2.customer_name, org_org_tree1.customer_name) as "Org Org Lv4 Name", coalesce(org_org_tree5.customer_code, org_org_tree4.customer_code, org_org_tree3.customer_code, org_org_tree2.customer_code, org_org_tree1.customer_code) as "Org Org Lv5 Code", coalesce(org_org_tree5.customer_name, org_org_tree4.customer_name, org_org_tree3.customer_name, org_org_tree2.customer_name, org_org_tree1.customer_name) as "Org Org Lv5 Name", coalesce(org_org_tree6.customer_code, org_org_tree5.customer_code, org_org_tree4.customer_code, org_org_tree3.customer_code, org_org_tree2.customer_code, org_org_tree1.customer_code) as "Org Org Lv6 Code", coalesce(org_org_tree6.customer_name, org_org_tree5.customer_name, org_org_tree4.customer_name, org_org_tree3.customer_name, org_org_tree2.customer_name, org_org_tree1.customer_name) as "Org Org Lv6 Name", coalesce(consol_org_tree1.customer_code, le_consol_org_tree1.customer_code) as "Org Consolidation Lv1 Code", coalesce(consol_org_tree1.customer_name, le_consol_org_tree1.customer_name) as "Org Consolidation Lv1 Name", coalesce(consol_org_tree2.customer_code, consol_org_tree1.customer_code, le_consol_org_tree2.customer_code, le_consol_org_tree1.customer_code) as "Org Consolidation Lv2 Code", coalesce(consol_org_tree2.customer_name, consol_org_tree1.customer_name, le_consol_org_tree2.customer_name, le_consol_org_tree1.customer_name) as "Org Consolidation Lv2 Name", coalesce(consol_org_tree3.customer_code, consol_org_tree2.customer_code, consol_org_tree1.customer_code, le_consol_org_tree3.customer_code, le_consol_org_tree2.customer_code, le_consol_org_tree1.customer_code) as "Org Consolidation Lv3 Code", coalesce(consol_org_tree3.customer_name, consol_org_tree2.customer_name, consol_org_tree1.customer_name, le_consol_org_tree3.customer_name, le_consol_org_tree2.customer_name, le_consol_org_tree1.customer_name) as "Org Consolidation Lv3 Name", coalesce(consol_org_tree4.customer_code, consol_org_tree3.customer_code, consol_org_tree2.customer_code, consol_org_tree1.customer_code, le_consol_org_tree4.customer_code, le_consol_org_tree3.customer_code, le_consol_org_tree2.customer_code, le_consol_org_tree1.customer_code) as "Org Consolidation Lv4 Code", coalesce(consol_org_tree4.customer_name, consol_org_tree3.customer_name, consol_org_tree2.customer_name, consol_org_tree1.customer_name, le_consol_org_tree4.customer_name, le_consol_org_tree3.customer_name, le_consol_org_tree2.customer_name, le_consol_org_tree1.customer_name) as "Org Consolidation Lv4 Name", coalesce(consol_org_tree5.customer_code, consol_org_tree4.customer_code, consol_org_tree3.customer_code, consol_org_tree2.customer_code, consol_org_tree1.customer_code, le_consol_org_tree5.customer_code, le_consol_org_tree4.customer_code, le_consol_org_tree3.customer_code, le_consol_org_tree2.customer_code, le_consol_org_tree1.customer_code) as "Org Consolidation Lv5 Code", coalesce(consol_org_tree5.customer_name, consol_org_tree4.customer_name, consol_org_tree3.customer_name, consol_org_tree2.customer_name, consol_org_tree1.customer_name, le_consol_org_tree5.customer_name, le_consol_org_tree4.customer_name, le_consol_org_tree3.customer_name, le_consol_org_tree2.customer_name, le_consol_org_tree1.customer_name) as "Org Consolidation Lv5 Name", coalesce(consol_org_tree6.customer_code, consol_org_tree5.customer_code, consol_org_tree4.customer_code, consol_org_tree3.customer_code, consol_org_tree2.customer_code, consol_org_tree1.customer_code, le_consol_org_tree6.customer_code, le_consol_org_tree5.customer_code, le_consol_org_tree4.customer_code, le_consol_org_tree3.customer_code, le_consol_org_tree2.customer_code, le_consol_org_tree1.customer_code) as "Org Consolidation Lv6 Code", coalesce(consol_org_tree6.customer_name, consol_org_tree5.customer_name, consol_org_tree4.customer_name, consol_org_tree3.customer_name, consol_org_tree2.customer_name, consol_org_tree1.customer_name, le_consol_org_tree6.customer_name, le_consol_org_tree5.customer_name, le_consol_org_tree4.customer_name, le_consol_org_tree3.customer_name, le_consol_org_tree2.customer_name, le_consol_org_tree1.customer_name) as "Org Consolidation Lv6 Name", c.sector						as "SECTOR", c.customer_size					as "CUSTOMER_SIZE", c.stock_symbol					as "STOCK_SYMBOL", ct.customer_type				as "CUSTOMER_TYPE", c.user01                        as "Org User01", c.user02                        as "Org User02", c.user03                        as "Org User03", c.user04                        as "Org User04", c.user05                        as "Org User05", c.user06                        as "Org User06", c.user07                        as "Org User07", c.user08                        as "Org User08", c.user09                        as "Org User09", c.user10                        as "Org User10", c.user11                        as "Org User11", c.user12                        as "Org User12", c.user13                        as "Org User13", c.user14                        as "Org User14", c.user15                        as "Org User15", c.user16                        as "Org User16", c.user17                        as "Org User17", c.user18                        as "Org User18", c.user19                        as "Org User19", c.user20                        as "Org User20", cgl.customer_code               as "LE Default Post Org", cle.customer_code               as "Org's Legal Entity Code", c.transact_elimination_flag     as "Elimination Org Indicator" 
	FROM [dbo].["aretum"."customer"] c

	left outer join [dbo].["aretum"."customer_type"]  ct   on ct.customer_type_key = c.customer_type_key left outer join [dbo].["aretum"."customer"]       cgl  on cgl.customer_key = c.default_gl_post_org_key left outer join [dbo].["aretum"."customer"]       cle  on cle.customer_key = c.legal_entity_key left outer join (select c.node_key org_key, max(case when p.tree_level = 0 then p.node_key else null end) org_key_0, max(case when p.tree_level = 1 then p.node_key else null end) org_key_1, max(case when p.tree_level = 2 then p.node_key else null end) org_key_2, max(case when p.tree_level = 3 then p.node_key else null end) org_key_3, max(case when p.tree_level = 4 then p.node_key else null end) org_key_4, max(case when p.tree_level = 5 then p.node_key else null end) org_key_5 from [dbo].["aretum"."org_cost_pool_tree"] c join [dbo].["aretum"."org_cost_pool_tree"] p on c.left_visit between p.left_visit and p.right_visit group by c.node_key) org_cost_pool_tree_orgs on org_cost_pool_tree_orgs.org_key = c.customer_key  left outer join [dbo].["aretum"."customer"] org_cost_pool_tree1 on org_cost_pool_tree1.customer_key = org_cost_pool_tree_orgs.org_key_0 left outer join [dbo].["aretum"."customer"] org_cost_pool_tree2 on org_cost_pool_tree2.customer_key = org_cost_pool_tree_orgs.org_key_1 left outer join [dbo].["aretum"."customer"] org_cost_pool_tree3 on org_cost_pool_tree3.customer_key = org_cost_pool_tree_orgs.org_key_2 left outer join [dbo].["aretum"."customer"] org_cost_pool_tree4 on org_cost_pool_tree4.customer_key = org_cost_pool_tree_orgs.org_key_3 left outer join [dbo].["aretum"."customer"] org_cost_pool_tree5 on org_cost_pool_tree5.customer_key = org_cost_pool_tree_orgs.org_key_4 left outer join [dbo].["aretum"."customer"] org_cost_pool_tree6 on org_cost_pool_tree6.customer_key = org_cost_pool_tree_orgs.org_key_5 left outer join (select c.node_key org_key, max(case when p.tree_level = 0 then p.node_key else null end) org_key_0, max(case when p.tree_level = 1 then p.node_key else null end) org_key_1, max(case when p.tree_level = 2 then p.node_key else null end) org_key_2, max(case when p.tree_level = 3 then p.node_key else null end) org_key_3, max(case when p.tree_level = 4 then p.node_key else null end) org_key_4, max(case when p.tree_level = 5 then p.node_key else null end) org_key_5 from [dbo].["aretum"."org_fin_tree"] c join [dbo].["aretum"."org_fin_tree"] p on c.left_visit between p.left_visit and p.right_visit group by c.node_key) org_fin_tree_orgs on org_fin_tree_orgs.org_key = c.customer_key left outer join [dbo].["aretum"."customer"] org_fin_tree1 on org_fin_tree1.customer_key = org_fin_tree_orgs.org_key_0 left outer join [dbo].["aretum"."customer"] org_fin_tree2 on org_fin_tree2.customer_key = org_fin_tree_orgs.org_key_1 left outer join [dbo].["aretum"."customer"] org_fin_tree3 on org_fin_tree3.customer_key = org_fin_tree_orgs.org_key_2 left outer join [dbo].["aretum"."customer"] org_fin_tree4 on org_fin_tree4.customer_key = org_fin_tree_orgs.org_key_3 left outer join [dbo].["aretum"."customer"] org_fin_tree5 on org_fin_tree5.customer_key = org_fin_tree_orgs.org_key_4 left outer join [dbo].["aretum"."customer"] org_fin_tree6 on org_fin_tree6.customer_key = org_fin_tree_orgs.org_key_5 left outer join (select c.node_key org_key, max(case when p.tree_level = 0 then p.node_key else null end) org_key_0, max(case when p.tree_level = 1 then p.node_key else null end) org_key_1, max(case when p.tree_level = 2 then p.node_key else null end) org_key_2, max(case when p.tree_level = 3 then p.node_key else null end) org_key_3, max(case when p.tree_level = 4 then p.node_key else null end) org_key_4, max(case when p.tree_level = 5 then p.node_key else null end) org_key_5 from [dbo].["aretum"."org_tree"] c join [dbo].["aretum"."org_tree"] p on c.left_visit between p.left_visit and p.right_visit group by c.node_key) org_org_tree_orgs on org_org_tree_orgs.org_key = c.customer_key left outer join [dbo].["aretum"."customer"] org_org_tree1 on org_org_tree1.customer_key = org_org_tree_orgs.org_key_0 left outer join [dbo].["aretum"."customer"] org_org_tree2 on org_org_tree2.customer_key = org_org_tree_orgs.org_key_1 left outer join [dbo].["aretum"."customer"] org_org_tree3 on org_org_tree3.customer_key = org_org_tree_orgs.org_key_2 left outer join [dbo].["aretum"."customer"] org_org_tree4 on org_org_tree4.customer_key = org_org_tree_orgs.org_key_3 left outer join [dbo].["aretum"."customer"] org_org_tree5 on org_org_tree5.customer_key = org_org_tree_orgs.org_key_4 left outer join [dbo].["aretum"."customer"] org_org_tree6 on org_org_tree6.customer_key = org_org_tree_orgs.org_key_5  left outer join (SELECT c.node_key org_key,
			max(CASE WHEN p.tree_level = 0 THEN p.node_key ELSE null END) org_key_0,
			max(CASE WHEN p.tree_level = 1 THEN p.node_key ELSE null END) org_key_1,
			max(CASE WHEN p.tree_level = 2 THEN p.node_key ELSE null END) org_key_2,
			max(CASE WHEN p.tree_level = 3 THEN p.node_key ELSE null END) org_key_3,
			max(CASE WHEN p.tree_level = 4 THEN p.node_key ELSE null END) org_key_4,
			max(CASE WHEN p.tree_level = 5 THEN p.node_key ELSE null END) org_key_5
		FROM [dbo].["aretum"."org_consolidation_tree"] c
		JOIN [dbo].["aretum"."org_consolidation_tree"] p ON c.left_visit BETWEEN p.left_visit AND p.right_visit
		GROUP BY c.node_key
	)
    le_consol_tree_orgs on le_consol_tree_orgs.org_key = cle.customer_key left outer join [dbo].["aretum"."customer"] le_consol_org_tree1 on le_consol_org_tree1.customer_key = le_consol_tree_orgs.org_key_0 left outer join [dbo].["aretum"."customer"] le_consol_org_tree2 on le_consol_org_tree2.customer_key = le_consol_tree_orgs.org_key_1 left outer join [dbo].["aretum"."customer"] le_consol_org_tree3 on le_consol_org_tree3.customer_key = le_consol_tree_orgs.org_key_2 left outer join [dbo].["aretum"."customer"] le_consol_org_tree4 on le_consol_org_tree4.customer_key = le_consol_tree_orgs.org_key_3 left outer join [dbo].["aretum"."customer"] le_consol_org_tree5 on le_consol_org_tree5.customer_key = le_consol_tree_orgs.org_key_4 left outer join [dbo].["aretum"."customer"] le_consol_org_tree6 on le_consol_org_tree6.customer_key = le_consol_tree_orgs.org_key_5 left outer join 
    (SELECT c.node_key org_key,
			max(CASE WHEN p.tree_level = 0 THEN p.node_key ELSE null END) org_key_0,
			max(CASE WHEN p.tree_level = 1 THEN p.node_key ELSE null END) org_key_1,
			max(CASE WHEN p.tree_level = 2 THEN p.node_key ELSE null END) org_key_2,
			max(CASE WHEN p.tree_level = 3 THEN p.node_key ELSE null END) org_key_3,
			max(CASE WHEN p.tree_level = 4 THEN p.node_key ELSE null END) org_key_4,
			max(CASE WHEN p.tree_level = 5 THEN p.node_key ELSE null END) org_key_5
		FROM [dbo].["aretum"."org_consolidation_tree"] c
		JOIN [dbo].["aretum"."org_consolidation_tree"] p ON c.left_visit BETWEEN p.left_visit AND p.right_visit
		GROUP BY c.node_key
	)
     org_consol_tree_orgs on org_consol_tree_orgs.org_key = c.customer_key left outer join [dbo].["aretum"."customer"] consol_org_tree1 on consol_org_tree1.customer_key = org_consol_tree_orgs.org_key_0 left outer join [dbo].["aretum"."customer"] consol_org_tree2 on consol_org_tree2.customer_key = org_consol_tree_orgs.org_key_1 left outer join [dbo].["aretum"."customer"] consol_org_tree3 on consol_org_tree3.customer_key = org_consol_tree_orgs.org_key_2 left outer join [dbo].["aretum"."customer"] consol_org_tree4 on consol_org_tree4.customer_key = org_consol_tree_orgs.org_key_3 left outer join [dbo].["aretum"."customer"] consol_org_tree5 on consol_org_tree5.customer_key = org_consol_tree_orgs.org_key_4 left outer join [dbo].["aretum"."customer"] consol_org_tree6 on consol_org_tree6.customer_key = org_consol_tree_orgs.org_key_5
	WHERE c.customer_key IS NOT NULL
) wrE9 ON (
	wrE19.[Organization Key] = wrE9.[CUSTOMER_KEY]
)
