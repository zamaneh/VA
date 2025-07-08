-- Document Date	Is Between	2025
--                             2025



--List of tables in this report:
-- access_customer_view
-- org_access_person
-- member
-- currency_code
-- vendor_invoice_activity
-- account
-- address
-- person
-- customer
-- customer_address
-- payment_term
-- purchase_order
-- vendor_invoice
-- fin_document
-- fiscal_year
-- fiscal_month
-- fin_document_recurring


CREATE VIEW [dbo].[vw_DocumentsWithIntegration]
AS
SELECT 
    wrE19."Doc Number" AS "Doc Number",
    wrE19."Doc Date" AS "Doc Date",
    wrE19."Vendor Code" AS "Vendor Code",
    wrE19."Vendor Name" AS "Vendor Name",
    wrE19."Purchase Order Number" AS "Purchase Order Number",
    wrE19."Currency Code" AS "Currency Code",
    wrE19."Vendor Invoice Date" AS "Vendor Invoice Date",
    wrE19."Post Date" AS "Post Date",
    wrE19."Doc Status" AS "Doc Status",
    wrE19."Posted Date" AS "Posted Date",
    wrE19."Voiding or Voided Doc Number" AS "Voiding or Voided Doc Number",
    wrE19."Current Balance" AS "Current Balance",
    wrE19."Vendor Invoice Amount" AS "Vendor Invoice Amount",
    wrE20."INTEGRATION_KEY" AS "INTEGRATION_KEY",
    wrE20."INTEGRATION_VALUE_KEY" AS "INTEGRATION_VALUE_KEY",
    wrE19."Vendor Invoice Key" AS "Vendor Invoice Key",
    wrE20."Internal Key" AS "Internal Key"FROM (
    SELECT 
        i.integration_key AS "INTEGRATION_KEY",
        iv.integration_value_key AS "INTEGRATION_VALUE_KEY",
        i.integration_name AS "Integration Name",
        iv.value_type AS "Integration Value Type",
        iv.internal_key AS "Internal Key",
        iv.external_key AS "External Key"
    FROM [dbo].["aretum"."integration"] i
    JOIN [dbo].["aretum"."integration_value"] iv ON i.integration_key = iv.integration_key
) wrE20
FULL OUTER JOIN (
    SELECT 
        pfm.fiscal_month_key AS "FISCAL_MONTH_KEY",
        fd.fin_document_key AS "Vendor Invoice Key",
        fd.document_number AS "Doc Number",
        fd.reference AS "Vendor Invoice or Ref #",
        fd.document_date AS "Doc Date",
        pt.description AS "Payment Terms",
        vi.discount_amount AS "Discount Amount",
        vi.invoice_date AS "Vendor Invoice Date",
        vi.due_date AS "Due Date",
        DATEADD(day, ISNULL(pt.discount_days, 0), vi.invoice_date) AS "Discount Date",
        fd.description AS "Doc Description",
        vi.invoice_amount AS "Vendor Invoice Amount",
        po.document_number AS "Purchase Order Number",
        cv.customer_key AS "Vendor Key",
        cv.customer_code AS "Vendor Code",
        cv.customer_name AS "Vendor Name",
        fd.post_date AS "Post Date",
        CASE 
            WHEN pfm.period_number < 10 
                THEN CONCAT(CONCAT(CONCAT(pfy.name, '-'), '0'), CAST(pfm.period_number AS VARCHAR(4))) 
            ELSE CONCAT(CONCAT(pfy.name, '-'), CAST(pfm.period_number AS VARCHAR(4))) 
        END AS "Fiscal Period",
        fd.comments AS "Comment",
        fd.status AS "Doc Status",
        last_updated_by.first_name AS "Last Modified By First Name",
        last_updated_by.last_name AS "Last Modified By Last Name",
        last_updated_by.username AS "Last Modified By Username",
        fd.created_timestamp AS "Created Date",
        fd.last_updated_timestamp AS "Last Modified Date",
        fd.submitted_timestamp AS "Submitted Date",
        fd.posted_timestamp AS "Posted Date",
        created_by.first_name AS "Created By Person First Name",
        created_by.last_name AS "Created By Person Last Name",
        created_by.username AS "Created By Person Username",
        posted_by.first_name AS "Posted By Person First Name",
        posted_by.last_name AS "Posted By Person Last Name",
        posted_by.username AS "Posted By Person Username",
        submitted_by.first_name AS "Submitted By Person First Name",
        submitted_by.last_name AS "Submitted By Person Last Name",
        submitted_by.username AS "Submitted By Person Username",
        fd.legal_entity_key AS "Legal Entity Key",
        c.customer_code AS "Legal Entity Code",
        c.customer_name AS "Legal Entity Name",
        COALESCE(voiding.document_number, voided.document_number) AS "Voiding or Voided Doc Number",
        cvi.customer_key AS "Financial Org Key",
        cvi.customer_code AS "Financial Org Code",
        cvi.customer_name AS "Financial Org Name",
        vi.account_key AS "AP Account key",
        a.account_code AS "AP Account Code",
        a.description AS "AP Account Name",
        va.city AS "Remit To Address City",
        va.country AS "Remit To Address Country",
        va.street1 AS "Remit To Address Line1",
        va.street2 AS "Remit To Address Line2",
        va.street3 AS "Remit To Address Line3",
        va.postal_code AS "Remit To Address Postal Code",
        va.state_province AS "Remit To Address State",
        CASE 
            WHEN fd.posted_timestamp IS NULL THEN vi.invoice_amount 
            ELSE via.balance 
        END AS "Current Balance",
        CASE 
            WHEN fdr.fin_document_key IS NULL THEN 'No' 
            ELSE 'Yes' 
        END AS "Recurring",
        ofd.document_number AS "Originating Doc Number",
        fdr.sequence AS "Occurrence Number",
        fdr.max_occurrences AS "Total Recurrence Number",
        vi.payment_on_hold AS "Hold Payments",
        vi.hold_reason AS "Hold Payments Reason",
        pt.payment_term_key AS "Payment Term Key",
        COALESCE(cc.iso_currency_code, 'USD') AS "Currency Code",
        CASE 
            WHEN fd.posted_timestamp IS NULL THEN NULL 
            ELSE vi.local_discount_amount 
        END AS "Local Discount Amount",
        CASE 
            WHEN fd.posted_timestamp IS NULL THEN NULL 
            ELSE vi.local_invoice_amount 
        END AS "Local Vendor Invoice Amount",
        COALESCE(lcc.iso_currency_code, 'USD') AS "Local Currency Code",
        CASE 
            WHEN fd.posted_timestamp IS NULL THEN NULL 
            ELSE via.local_balance 
        END AS "Local Current Balance",
        CASE 
            WHEN vi.due_date < DATEADD(day, -90, GETDATE()) THEN '90+ days past due'
            WHEN vi.due_date < DATEADD(day, -60, GETDATE()) THEN '61-90 days past due'
            WHEN vi.due_date < DATEADD(day, -30, GETDATE()) THEN '31-60 days past due'
            WHEN vi.due_date < GETDATE() THEN '1-30 days past due'
            ELSE 'Current' 
        END AS "Aging",
        FORMAT(fd.document_date, 'MMM yyyy') AS "Invoice Month"
    FROM [dbo].["aretum"."fin_document"] fd
    LEFT OUTER JOIN [dbo].["aretum"."fin_document_recurring"] fdr ON fdr.fin_document_key = fd.fin_document_key
    LEFT OUTER JOIN [dbo].["aretum"."fin_document"] ofd ON ofd.fin_document_key = fdr.orig_fin_document_key
    JOIN [dbo].["aretum"."fiscal_month"] pfm ON fd.post_date BETWEEN pfm.begin_date AND pfm.end_date
    JOIN [dbo].["aretum"."fiscal_year"] pfy ON pfy.fiscal_year_key = pfm.fiscal_year_key
    LEFT OUTER JOIN [dbo].["aretum"."fin_document"] voiding ON voiding.fin_document_key = fd.voiding_fin_document_key
    LEFT OUTER JOIN [dbo].["aretum"."fin_document"] voided ON voided.fin_document_key = fd.voided_fin_document_key
    JOIN [dbo].["aretum"."vendor_invoice"] vi ON vi.fin_document_key = fd.fin_document_key
    LEFT OUTER JOIN [dbo].["aretum"."purchase_order"] po ON po.po_key = vi.po_key AND po.orig_po_key = po.po_key
    JOIN [dbo].["aretum"."customer"] c ON c.customer_key = fd.legal_entity_key
    JOIN [dbo].["aretum"."payment_term"] pt ON pt.payment_term_key = vi.payment_term_key
    JOIN [dbo].["aretum"."customer"] cv ON cv.customer_key = vi.vendor_org_key
    LEFT OUTER JOIN [dbo].["aretum"."customer_address"] ca ON ca.customer_key = cv.customer_key AND ca.default_remit_to = 'Y'
    LEFT OUTER JOIN [dbo].["aretum"."address"] va ON va.address_key = ca.address_key
    JOIN [dbo].["aretum"."person"] created_by ON created_by.person_key = fd.CREATED_BY
    LEFT OUTER JOIN [dbo].["aretum"."person"] posted_by ON posted_by.person_key = fd.posted_by
    LEFT OUTER JOIN [dbo].["aretum"."person"] submitted_by ON submitted_by.person_key = fd.submitted_by
    LEFT OUTER JOIN [dbo].["aretum"."person"] last_updated_by ON last_updated_by.person_key = fd.last_updated_by
    JOIN [dbo].["aretum"."customer"] cvi ON vi.organization_key = cvi.customer_key
    JOIN [dbo].["aretum"."account"] a ON a.account_key = vi.account_key
LEFT OUTER JOIN (
    SELECT 
        fin_document_key, 
        SUM(
            CAST(invoice_amount AS DECIMAL(18,2)) 
            - CAST(applied AS DECIMAL(18,2)) 
            - CAST(discount AS DECIMAL(18,2))
        ) AS balance, 
        SUM(
            CAST(local_invoice_amount AS DECIMAL(18,2)) 
            - CAST(local_applied AS DECIMAL(18,2)) 
            - CAST(local_discount AS DECIMAL(18,2))
        ) AS local_balance 
    FROM [dbo].["aretum"."vendor_invoice_activity"] 
    GROUP BY fin_document_key
) via ON via.fin_document_key = vi.fin_document_key

    LEFT OUTER JOIN [dbo].["aretum"."currency_code"] cc ON cc.currency_code_key = cv.currency_code_key
    LEFT OUTER JOIN [dbo].["aretum"."currency_code"] lcc ON lcc.currency_code_key = fd.local_currency
    WHERE fd.fin_document_key IS NOT NULL  
      AND (
            EXISTS (SELECT 1 FROM [dbo].["aretum"."member"] WHERE person_key = '3896' AND role_key = 1)
        OR  EXISTS (SELECT 1 FROM [dbo].["aretum"."org_access_person"] WHERE person_key = '3896' AND role_key IN (27,28,29) AND global_access = 'Y')
        OR  (c.customer_key IN (
                SELECT v.customer_key 
                FROM [dbo].["aretum"."access_customer_view"] v 
                JOIN [dbo].["aretum"."org_access_person"] oap ON oap.org_access_person_key = v.org_access_person_key 
                WHERE oap.person_key = '3896' AND oap.role_key IN (27,28,29) AND oap.access_type IN (2,3)
            ))
        OR (
                EXISTS (
                    SELECT 1
                    FROM [dbo].["aretum"."org_access_person"]
                    WHERE person_key = '3896' AND role_key = 56 AND access_type IN (2,6) AND global_access = 'Y'
                    GROUP BY person_key, role_key
                    HAVING COUNT(*) = 2
                )
            OR (
                    c.customer_key IN (
                        SELECT v.customer_key
                        FROM [dbo].["aretum"."access_customer_view"] v
                        JOIN [dbo].["aretum"."org_access_person"] oap ON oap.org_access_person_key = v.org_access_person_key
                        WHERE oap.person_key = '3896' AND oap.role_key = 56 AND oap.access_type IN (2,3)
                    )
                    AND EXISTS (
                        SELECT 1 FROM [dbo].["aretum"."org_access_person"]
                        WHERE person_key = '3896' AND role_key = 56 AND access_type = 6 AND global_access = 'Y'
                    )
                )
            OR (
                    EXISTS (
                        SELECT 1 FROM [dbo].["aretum"."org_access_person"]
                        WHERE person_key = '3896' AND role_key = 56 AND access_type = 2 AND global_access = 'Y'
                    )
                    AND cv.customer_key IN (
                        SELECT v.customer_key
                        FROM [dbo].["aretum"."access_customer_view"] v
                        JOIN [dbo].["aretum"."org_access_person"] oap ON oap.org_access_person_key = v.org_access_person_key
                        WHERE oap.person_key = '3896' AND oap.role_key = 56 AND oap.access_type = 6
                    )
                )
            OR (
                    c.customer_key IN (
                        SELECT v.customer_key
                        FROM [dbo].["aretum"."access_customer_view"] v
                        JOIN [dbo].["aretum"."org_access_person"] oap ON oap.org_access_person_key = v.org_access_person_key
                        WHERE oap.person_key = '3896' AND oap.role_key = 56 AND oap.access_type IN (2,3)
                    )
                    AND cv.customer_key IN (
                        SELECT v.customer_key
                        FROM [dbo].["aretum"."access_customer_view"] v
                        JOIN [dbo].["aretum"."org_access_person"] oap ON oap.org_access_person_key = v.org_access_person_key
                        WHERE oap.person_key = '3896' AND oap.role_key = 56 AND oap.access_type = 6
                    )
                )
            )
        )
) wrE19 ON (
    wrE20."Internal Key" = wrE19."Vendor Invoice Key"
)
GO
