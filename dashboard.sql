WITH last_paid_click AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM sessions AS s
    LEFT JOIN leads AS l
        ON
            s.visitor_id = l.visitor_id
            AND s.visit_date <= l.created_at
    WHERE
        LOWER(s.medium) IN (
            'cpc', 'cpm', 'cpp', 'cpa', 'youtube', 'tg', 'social'
        )
),

filtered_last_click AS (
    SELECT *
    FROM last_paid_click
    WHERE rn = 1
),

ad_costs AS (
    SELECT
        campaign_date::date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM (
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM ya_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM vk_ads
    ) AS ads
    GROUP BY 1, 2, 3, 4
),

agg AS (
    SELECT
        f.visit_date::date AS visit_date,
        f.utm_source,
        f.utm_medium,
        f.utm_campaign,
        COUNT(*) AS visitors_count,
        COUNT(f.lead_id) AS leads_count,
        COUNT(*) FILTER (
            WHERE f.closing_reason = 'Успешно реализовано' OR f.status_id = 142
        ) AS purchases_count,
        SUM(CASE
            WHEN f.closing_reason = 'Успешно реализовано' OR f.status_id = 142
                THEN f.amount
            ELSE 0
        END) AS revenue
    FROM filtered_last_click AS f
    GROUP BY 1, 2, 3, 4
)

SELECT
    a.visit_date,
    a.visitors_count,
    a.utm_source,
    a.utm_medium,
    a.utm_campaign,
    a.leads_count,
    a.purchases_count,
    a.revenue,
    (c.total_cost)::numeric AS total_cost
FROM agg AS a
LEFT JOIN ad_costs AS c
    ON
        a.visit_date = c.visit_date
        AND a.utm_source = c.utm_source
        AND a.utm_medium = c.utm_medium
        AND a.utm_campaign = c.utm_campaign
ORDER BY
    a.revenue DESC NULLS LAST,
    a.visit_date ASC,
    a.visitors_count DESC,
    a.utm_source ASC,
    a.utm_medium ASC,
    a.utm_campaign ASC
LIMIT 15;

--------
WITH paid_sessions AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign
    FROM sessions AS s
    WHERE
        LOWER(s.medium) IN (
            'cpc', 'cpm', 'cpp', 'cpa', 'youtube', 'tg', 'social'
        )
),

joined_data AS (
    SELECT
        ps.visitor_id,
        ps.visit_date,
        ps.utm_source,
        ps.utm_medium,
        ps.utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        ROW_NUMBER() OVER (
            PARTITION BY l.lead_id
            ORDER BY ps.visit_date DESC
        ) AS rn
    FROM paid_sessions AS ps
    LEFT JOIN leads AS l
        ON ps.visitor_id = l.visitor_id
        AND ps.visit_date <= l.created_at
)

SELECT
    visitor_id,
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    lead_id,
    created_at,
    amount,
    closing_reason,
    status_id
FROM joined_data
WHERE rn = 1 OR lead_id IS NULL
ORDER BY
    amount DESC NULLS LAST,
    visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 10;
