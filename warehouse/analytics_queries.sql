SELECT COUNT(*)
FROM crop_health_analytics;
SELECT
    farm_id,
    region,
    crop,
    ndvi,
    soc,
    vegetation_health,
    crop_health_score,
    health_risk
FROM crop_health_analytics
LIMIT 10;
SELECT
    crop,
    ROUND(AVG(crop_health_score)::numeric, 2)
        AS avg_health_score
FROM crop_health_analytics
GROUP BY crop
ORDER BY avg_health_score DESC;
SELECT
    farm_id,
    crop,
    ndvi,
    soc,
    crop_health_score
FROM crop_health_analytics
WHERE health_risk = 'High Risk'
ORDER BY crop_health_score;
SELECT
    region,
    COUNT(*) AS observations,
    ROUND(AVG(ndvi)::numeric, 2) AS avg_ndvi,
    ROUND(AVG(soc)::numeric, 2) AS avg_soc
FROM crop_health_analytics
GROUP BY region
ORDER BY avg_ndvi DESC;