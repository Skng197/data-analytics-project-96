WITH ad_spend AS (
    -- Объединяем данные о затратах из vk_ads и ya_ads
    SELECT
        campaign_date::DATE AS visit_date,
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
        FROM
            vk_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM
            ya_ads
    ) combined_ads
    WHERE daily_spent > 0
    GROUP BY
        campaign_date::DATE, utm_source, utm_medium, utm_campaign
),
visit_data AS (
    -- Считаем количество визитов для каждой комбинации меток
    SELECT
        visit_date::DATE AS visit_date,
        "source" AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign,
        COUNT(*) AS visitors_count
    FROM
        sessions
    GROUP BY
        visit_date::DATE, "source", medium, campaign
),
lead_data AS (
    -- Считаем количество лидов и успешно закрытых лидов
    SELECT
        l.created_at::DATE AS visit_date,
        s."source" AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        COUNT(*) AS leads_count,
        SUM(CASE WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 THEN 1 ELSE 0 END) AS purchases_count,
        SUM(CASE WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 THEN l.amount ELSE 0 END) AS revenue
    FROM
        public.leads l
    LEFT JOIN
        sessions s
    ON
        l.visitor_id = s.visitor_id
    GROUP BY
        l.created_at::DATE, s."source", s.medium, s.campaign
),
aggregated_data AS (
    -- Объединяем все данные
    SELECT
        COALESCE(v.visit_date, l.visit_date, a.visit_date) AS visit_date,
        COALESCE(v.utm_source, l.utm_source, a.utm_source) AS utm_source,
        COALESCE(v.utm_medium, l.utm_medium, a.utm_medium) AS utm_medium,
        COALESCE(v.utm_campaign, l.utm_campaign, a.utm_campaign) AS utm_campaign,
        COALESCE(v.visitors_count, 0) AS visitors_count,
        COALESCE(a.total_cost, 0) AS total_cost,
        COALESCE(l.leads_count, 0) AS leads_count,
        COALESCE(l.purchases_count, 0) AS purchases_count,
        COALESCE(l.revenue, 0) AS revenue
    FROM
        visit_data v
    FULL OUTER JOIN
        lead_data l
    ON
        v.visit_date = l.visit_date
        AND v.utm_source = l.utm_source
        AND v.utm_medium = l.utm_medium
        AND v.utm_campaign = l.utm_campaign
    FULL OUTER JOIN
        ad_spend a
    ON
        COALESCE(v.visit_date, l.visit_date) = a.visit_date
        AND COALESCE(v.utm_source, l.utm_source) = a.utm_source
        AND COALESCE(v.utm_medium, l.utm_medium) = a.utm_medium
        AND COALESCE(v.utm_campaign, l.utm_campaign) = a.utm_campaign
)
-- Финальная выборка с сортировкой
SELECT *
FROM aggregated_data
ORDER BY
    CASE WHEN revenue IS NULL THEN 1 ELSE 0 END, -- NULL записи идут последними
    revenue DESC, -- По убыванию revenue
    visit_date ASC, -- От ранних к поздним
    visitors_count DESC, -- По убыванию visitors_count
    utm_source ASC, utm_medium ASC, utm_campaign ASC -- В алфавитном порядке
LIMIT 15; -- Берем топ-15 записей