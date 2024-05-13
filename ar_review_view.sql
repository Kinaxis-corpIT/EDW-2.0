create or replace view CITD_D4_DEV.S2_INT.AR_REVIEW_MKTG_CMO_DASHBOARD_2021_ACTUAL(
	YEAR,
	QUARTER,
	ACCOUNT_TERRITORY,
	ACTUAL,
	MEASURE
) as 

-- pre-transform for Target account engagement %
with engaged_acct as (
select
    year(TASK_DUE_DATE) as YEAR
    ,quarter(TASK_DUE_DATE) as QUARTER
    ,count(distinct ACCOUNT_ID) as ACTUAL
    ,'Engaged Accounts' as MEASURE_NEW
from "CITD_D4_DEV"."S2_INT"."MKTG_ACCOUNTS_WITH_AGGREGATE_ACTIVITIES"
where ACCOUNT_TYPE in ('Prospect', 'VAR Target Account')
and TASK_DUE_DATE > '2019-12-31'	-- AR20240129
and TASK_DUE_DATE between '2020-01-01' and current_date
group by 
     year(TASK_DUE_DATE)
    ,quarter(TASK_DUE_DATE)
),
a_run_cnt as (
select
    *
    ,count(*) over (order by CREATEDDATE asc rows between unbounded preceding and current row) as run_cnt
from "CITD_D4_DEV"."S1_LND"."SFDC_ACCOUNT2"
where TYPE in ('Prospect', 'VAR Target Account')
and	CREATEDDATE >= '2019-12-31'	-- AR20240129
and ISDELETED = 0
),
all_acct as (
select
year(CREATEDDATE) as YEAR
,quarter(CREATEDDATE) as QUARTER
, max(run_cnt) as ACTUAL
,'All Prospect Accounts' as MEASURE_NEW
from a_run_cnt
-- where year(CREATEDDATE) >= 2020
where CREATEDDATE >= '2019-12-31'		-- AR20240129
group by 
     year(CREATEDDATE)
    ,quarter(CREATEDDATE)
)

-- campaign members
select 
    year(MEMBER_ASSOCIATED_DATE) as YEAR
    ,quarter(MEMBER_ASSOCIATED_DATE) as QUARTER
    ,ACCOUNT_TERRITORY
    ,count(*) as ACTUAL
    ,'Campaign Members' as MEASURE
from "CITD_D4_DEV"."S2_INT"."SALES_SFDC_LEAD_AND_CONTACT_CAMPAIGN_PARTICIPATION"
where ACCOUNT_TYPE in ('Prospect', 'VAR Target Account')
and COMPANY_OR_ACCOUNT not like '%Kinaxis%'
and COMPANY_OR_ACCOUNT not like '%Migration%'
and CAMPAIGN_LEAD_SOURCE not in (
  'Community',
  'Inbound Inquiry',
  'Inside Sales',
  'Market Development',
  'Purchased List',
  'Sales - AE','Partner'
)
and MEMBER_STATUS not in (
  'Coffee Bar',
  'Received Promo Item',
  'Invited',
  'Sent',
  'Added External',
  'Data Collected'
)
and year(MEMBER_ASSOCIATED_DATE) >= 2020
group by 
     year(MEMBER_ASSOCIATED_DATE)
    ,quarter(MEMBER_ASSOCIATED_DATE)
    ,ACCOUNT_TERRITORY

union all

-- mql contacts
select 
    year(MEMBER_ASSOCIATED_DATE) as YEAR
    ,quarter(MEMBER_ASSOCIATED_DATE) as QUARTER
    ,ACCOUNT_TERRITORY
    ,count(CAMPAIGN_MEMBER_ID) as ACTUAL
    ,'MQLs (Contacts)' as MEASURE
from "CITD_D4_DEV"."S2_INT"."SALES_SFDC_LEAD_AND_CONTACT_CAMPAIGN_PARTICIPATION_MOST_RECENT_ASSOCIATION_IN_Q"
where ACCOUNT_TYPE in ('Prospect', 'VAR Target Account')
and CAMPAIGN_LEAD_SOURCE not in (
  'Community',
  'Inbound Inquiry',
  'Inside Sales',
  'Market Development',
  'Purchased List',
  'Sales - AE','Partner'
)
and MEMBER_STATUS not in (
  'Coffee Bar',
  'Received Promo Item',
  'Invited',
  'Sent',
  'Added External',
  'Data Collected'
)
and COMPANY_OR_ACCOUNT not in (
  'Kinaxis',
  'Kinaxis Inc',
  'Kinaxis Inc.',
  'Migration Account'
)
and MAPPED_STATUS in ('MQL','Recycle','Rejected')
-- and year(MEMBER_ASSOCIATED_DATE) >= 2020
and MEMBER_ASSOCIATED_DATE >= '2019-12-31' -- AR20240129
group by 
     year(MEMBER_ASSOCIATED_DATE)
    ,quarter(MEMBER_ASSOCIATED_DATE)
    ,ACCOUNT_TERRITORY

union all

-- mql accounts
select 
    year(MEMBER_ASSOCIATED_DATE) as YEAR
    ,quarter(MEMBER_ASSOCIATED_DATE) as QUARTER
    ,ACCOUNT_TERRITORY
    ,count(distinct COMPANY_OR_ACCOUNT) as ACTUAL
    ,'MQLs (Accounts)' as MEASURE
from "CITD_D4_DEV"."S2_INT"."SALES_SFDC_LEAD_AND_CONTACT_CAMPAIGN_PARTICIPATION_MOST_RECENT_ASSOCIATION_IN_Q"
where ACCOUNT_TYPE in ('Prospect', 'VAR Target Account')
and CAMPAIGN_LEAD_SOURCE not in (
  'Community',
  'Inbound Inquiry',
  'Inside Sales',
  'Market Development',
  'Purchased List',
  'Sales - AE','Partner'
)
and MEMBER_STATUS not in (
  'Coffee Bar',
  'Received Promo Item',
  'Invited',
  'Sent',
  'Added External',
  'Data Collected'
)
and COMPANY_OR_ACCOUNT not in (
  'Kinaxis',
  'Kinaxis Inc',
  'Kinaxis Inc.',
  'Migration Account'
)
and MAPPED_STATUS in ('MQL','Recycle','Rejected')
-- and year(MEMBER_ASSOCIATED_DATE) >= 2020
and MEMBER_ASSOCIATED_DATE >= '2019-12-31'	-- AR20240129
group by 
     year(MEMBER_ASSOCIATED_DATE)
    ,quarter(MEMBER_ASSOCIATED_DATE)
    ,ACCOUNT_TERRITORY

-- mels contacts--
-- mels accounts --

union all 

-- inbound inquiries
select 
    year(DATE) as YEAR
    ,quarter(DATE) as QUARTER
    ,'Total' as ACCOUNT_TERRITORY
    ,count(CAMPAIGN_MEMBER_ID) as ACTUAL
    ,'Inbound Inquiries' as MEASURE
from "CITD_D4_DEV"."S2_INT"."MKTG_INBOUND_INTEREST"
-- where year(DATE) >= 2020
where DATE >= '2019-12-31'	-- AR20240129
group by 
     year(DATE)
    ,quarter(DATE)

union all

-- Target account engagement %
select 
 all_acct.YEAR
,all_acct.QUARTER
,'Total' as ACCOUNT_TERRITORY
,avg((engaged_acct.ACTUAL / all_acct.ACTUAL) * 100) as ACTUAL
,'Target account engagement %' as MEASURE
from engaged_acct
inner join all_acct
    on engaged_acct.YEAR = all_acct.YEAR
    and engaged_acct.QUARTER = all_acct.QUARTER
group by 
     all_acct.YEAR
    ,all_acct.QUARTER

union all

-- SAL (Opps Mktg / BDR) 
select
 year(o.CREATEDDATE) as YEAR
,quarter(o.CREATEDDATE) as QUARTER
,a.ACCOUNT_TERRITORY__C as ACCOUNT_TERRITORY
,count(o.ID) as ACTUAL
,'SAL (Opps Mktg / BDR)' as MEASURE
from "CITD_D4_DEV"."S2_INT"."SALES_SFDC_OPPORTUNITIES_WITH_CURRENCY_CONV_FINAL" o
inner join "CITD_D4_DEV"."S1_LND"."SFDC_ACCOUNT2" a
    on o.ACCOUNTID = a.ID
where o.TOTAL_IMSA__C_CONVERTED > 0
and o.ASSIGN_BDR__C is not null
and o.TYPE = 'New Name Account'
-- and year(o.CREATEDDATE) >= 2020
and o.CREATEDDATE >= '2019-12-31'	-- AR20240129
and a.ISDELETED = 0
group by 
 year(o.CREATEDDATE)
,quarter(o.CREATEDDATE)
,a.ACCOUNT_TERRITORY__C 

union all

-- ISQL (Meetings)
select
 year(o.CREATEDDATE) as YEAR
,quarter(o.CREATEDDATE) as QUARTER
,a.ACCOUNT_TERRITORY__C as ACCOUNT_TERRITORY
,count(o.ID) as ACTUAL
,'ISQL (Meetings)' as MEASURE
from "CITD_D4_DEV"."S2_INT"."SALES_SFDC_OPPORTUNITIES_WITH_CURRENCY_CONV_FINAL" o
inner join "CITD_D4_DEV"."S1_LND"."SFDC_ACCOUNT2" a
    on o.ACCOUNTID = a.ID
where o.ASSIGN_BDR__C is not null
and o.TYPE = 'New Name Account'
-- and year(o.CREATEDDATE) >= 2020
and o.CREATEDDATE >= '2019-12-31'	-- AR20240129
and a.ISDELETED = 0
group by 
 year(o.CREATEDDATE)
,quarter(o.CREATEDDATE)
,a.ACCOUNT_TERRITORY__C 

-- Opps (Marketing Influenced) - #
-- Opps (Marketing Influenced) - IMSA
-- SAL (All) - #
-- Opps (All) - #
-- Opps (MKTG Campaign Inf.) - IMSA
-- Opps (MKTG Campaign Inf.) - #
;