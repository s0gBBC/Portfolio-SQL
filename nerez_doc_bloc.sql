-- Удаляем временные таблицы, если они существуют
DROP TABLE IF EXISTS nom_priz;
DROP TABLE IF EXISTS type_doc;
DROP TABLE IF EXISTS nomm;
DROP TABLE IF EXISTS filtered_sales;
DROP TABLE IF EXISTS blocked_numbers;

-- Создаем временную таблицу с отфильтрованными данными
CREATE TEMPORARY TABLE filtered_sales
WITH (appendonly=true, compresstype=ZSTD, compresslevel=1)
ON COMMIT PRESERVE ROWS AS (
    SELECT DISTINCT
        act.regid,
        act.app_n,
        act.activation_dttm,
        act.sale_dttm,
        act.phone_num,
        act.sale_channel,
        act.sale_point_code
    FROM 
        uat_v_base.dapp_app_sales AS act
    WHERE 
        act.activation_dttm >= '2025-11-01'
        AND act.activation_dttm < '2025-12-01'
        AND (act.mrgn_id = 1 OR act.mrgn_id = 3 OR act.mrgn_id = 4)
        AND act.sale_channel <> 8 
        AND act.sale_channel <> 10
);

-- Создаем временную таблицу для абонентов
CREATE TEMPORARY TABLE nomm
WITH (appendonly=true, compresstype=ZSTD, compresslevel=1)
ON COMMIT PRESERVE ROWS AS (
    SELECT 
        abnt.regid,
        abnt.app_n,
        abnt.personal_manager,
        abnt.tp,
        abnt.region,
        abnt.mnp_port_in_dttm
    FROM 
        uat_v_base.aapp_abnt AS abnt
    WHERE 
    	(abnt.mrgn_id = 1 OR abnt.mrgn_id = 3 OR abnt.mrgn_id = 4) AND
        abnt.region IN (52, 53, 57, 54, 55, 56, 59, 60, 61, 62, 63, 64, 66) 
        AND abnt.activation_date >= '2025-11-01'
        AND abnt.activation_date < '2025-12-01'
);

-- Создаем временную таблицу для заблокированных номеров
CREATE TEMPORARY TABLE blocked_numbers AS (
    SELECT 
        sb.regid,
        sb.app_n,
        sb.mrgn_id,
        sb.date_to
    FROM 
        uat_v_base.dapp_sblo AS sb
    WHERE
		(sb.mrgn_id = 1 OR sb.mrgn_id = 3 OR sb.mrgn_id = 4) AND
        sb.code IN (32768, 32776, 32784, 32832, 32840, 32856, 33280)
        AND sb.date_to = '2040-01-01 00:00:00.000'  -- Проверяем на блокировку
);

-- Основной запрос для объединения данных и получения окончательной таблицы
CREATE TEMPORARY TABLE nom_priz
WITH (appendonly=true, compresstype=ZSTD, compresslevel=1)
ON COMMIT PRESERVE ROWS AS (
    SELECT 
        act.app_n,
        act.activation_dttm AS activation,
        act.sale_dttm AS registration,
        act.phone_num,
        act.sale_channel,
        act.sale_point_code,
        abnt.personal_manager,
        abnt.tp,
        abnt.region,
        mg.country,
        abnt.mnp_port_in_dttm,
        equip.equipment_model,
        f922.label AS equipment_name,
        company.full_name AS dealer_name,
        region.label AS region_name,
        personal.label AS personal_manager_name,
        fc.label AS tarif_plan,
        sale_channel.label AS sale_channel_name,
        sp.agent_name,  -- Добавляем столбец agent_name
        dsec.document_type,  -- Добавляем столбец document_type
        CASE 
            WHEN mg.country = 643.060 THEN 'rezident'
            WHEN mg.country = 643.067 THEN 'rezident'
            WHEN mg.country = 643.071 THEN 'rezident'
            WHEN mg.country IS NULL THEN 'rezident'
            ELSE 'nerezident'
        END AS priznak,
        CASE 
            WHEN blocked.regid IS NOT NULL THEN 'заблокирован'  -- Добавляем статус блокировки
            ELSE 'активирован'  -- Статус активации
        END AS status
    FROM 
        filtered_sales AS act
        
    INNER JOIN 
        uat_v_base.da_app_acc_con_cust_lnk AS cust_lnk ON act.regid = cust_lnk.regid AND act.app_n = cust_lnk.app_n
    LEFT JOIN 
        nomm AS abnt ON abnt.regid = act.regid AND abnt.app_n = act.app_n
    LEFT JOIN 
        uat_v_base.dapp_app_sales_attr AS equip ON equip.regid = act.regid AND equip.app_n = act.app_n
    LEFT JOIN 
        uat_v_base.sas_fmt_f99022c AS f922 ON f922."start" = equip.equipment_model
    LEFT JOIN 
        uat_v_base_sec.dsec_company AS company ON company.customer_n = cust_lnk.customer_n
    LEFT JOIN 
        uat_v_base.sas_fmt_fsrgc AS region ON region.start = abnt.region
    LEFT JOIN 
        uat_v_base.sas_fmt_persman AS personal ON personal.start = abnt.personal_manager
    LEFT JOIN 
        uat_v_base.sas_fmt_m117c AS sale_channel ON sale_channel.start = act.sale_channel
    LEFT JOIN 
        uat_v_base_sec.dsec_individual AS mg ON mg.regid = act.regid AND mg.customer_n = cust_lnk.customer_n
    LEFT JOIN 
        uat_v_base.dic_sale_point AS sp ON sp.sale_point_code = act.sale_point_code  -- Соединение для agent_name
    LEFT JOIN 
        uat_v_base.dica_sale_point AS sp1 ON sp1.sale_point_code = act.sale_point_code  -- Соединение для agent_name
    LEFT JOIN 
        uat_v_base.sas_fmt_f8000c AS fc ON fc.start = abnt.tp  -- Соединение для TP
    LEFT JOIN 
        uat_v_base_sec.dsec_individual AS dsec ON  dsec.regid = act.regid AND dsec.customer_n = cust_lnk.customer_n  -- Соединение для document_type
    LEFT JOIN 
        blocked_numbers AS blocked ON blocked.regid = act.regid AND blocked.app_n = act.app_n  -- Соединение для заблокированных номеров
    WHERE 
        abnt.region IN (52, 53, 57, 54, 55, 56, 59, 60, 61, 62, 63, 64, 66)
);

-- Запрос для получения данных из окончательной таблицы
SELECT DISTINCT
	phone_num,
    activation,
    registration,
    sale_channel,
    sale_point_code,
    personal_manager,
    mnp_port_in_dttm,
    equipment_name,
    dealer_name,
    country,
    region_name,
    personal_manager_name,
    tarif_plan,
    sale_channel_name,
    agent_name,
    document_type,
    priznak,
    status
FROM 
    nom_priz;