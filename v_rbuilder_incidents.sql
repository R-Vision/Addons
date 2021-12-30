CREATE OR REPLACE VIEW public.v_rbuilder_im_incidents AS
SELECT
    im_incident.id,
    (
        SELECT
            settings ->> 'sname'
        FROM
            companies
        WHERE
            id = im_incident.company_id
    ) as "company",
    im_incident.company_id,
    im_incident.uuid,
    im_incident.identifier,
    (
        SELECT
            login
        FROM
            users
        WHERE
            id = im_incident.creator_id
    ) as "user",
    (
        SELECT
            name
        FROM
            im_categories
        WHERE
            id = im_incident.category_id
    ) as "category",
    (
        SELECT
            name
        FROM
            im_catalog_types
        WHERE
            id = im_incident.incident_type_id
    ) as "type",
    (
        SELECT
            name
        FROM
            im_catalog_methods
        WHERE
            id = im_incident.methods_id
    ) as "method",
    (
        SELECT
            name
        FROM
            im_catalog_status
        WHERE
            id = im_incident.status_id
    ) as "status",
    (
        SELECT
            name
        FROM
            im_catalog_levels
        WHERE
            id = im_incident.levels_id
    ) as "level",
    (
        SELECT
            login
        FROM
            users
        WHERE
            id = im_incident.responsible_id
    ) as "responsible",
    im_incident.detection,
    im_incident.emergence,
    im_incident.creation,
    im_incident.updated,
    im_incident.plan,
    im_incident.completion,
    im_incident.description,
    im_incident.deleted,
    im_incident.archived
FROM
    im_incident;
GRANT SELECT ON TABLE public.v_rbuilder_im_incidents TO rvision_read_only;
CREATE OR REPLACE VIEW public.v_rbuilder_im_incidents_field_values AS
SELECT
    im_fields_values_only_inc_field.incident_id,
    im_fields_only_label_catalog_type.label as "field_label",
    case when im_fields_only_label_catalog_type.catalog_id is not null then (
        SELECT
            name
        FROM
            im_catalog_usercatalog
        WHERE
            catalog_id = im_fields_only_label_catalog_type.catalog_id
            and id = im_fields_values_only_inc_field.value :: int
    ) else im_fields_values_only_inc_field.value end as "value"
FROM
    (
        SELECT
            incident_id,
            field_id,
            value
        FROM
            im_fields_values
        WHERE
            value != ''
            and value IS NOT NULL
    ) as im_fields_values_only_inc_field
    left join (
        SELECT
            id,
            "label",
            catalog_id,
            "type"
        FROM
            im_fields
    ) as im_fields_only_label_catalog_type on im_fields_values_only_inc_field.field_id = im_fields_only_label_catalog_type.id
WHERE
    im_fields_only_label_catalog_type."type" not in ('timecounter', 'usertagfield');
GRANT SELECT ON TABLE public.v_rbuilder_im_incidents_field_values TO rvision_read_only;
CREATE OR REPLACE VIEW public.v_rbuilder_im_incidents_user_field_values AS
SELECT
    im_user_fields_values_only_inc_field_user.incident_id,
    im_fields_only_label_catalog_type.label as "field_label",
    (
        SELECT
            login
        FROM
            users
        WHERE
            id = im_user_fields_values_only_inc_field_user.user_id
    ) as "user"
FROM
    (
        SELECT
            id,
            incident_id,
            field_id,
            user_id
        FROM
            im_user_fields_values
    ) as im_user_fields_values_only_inc_field_user
    left join (
        SELECT
            id,
            "label",
            catalog_id,
            "type"
        FROM
            im_fields
    ) as im_fields_only_label_catalog_type on im_user_fields_values_only_inc_field_user.field_id = im_fields_only_label_catalog_type.id;
GRANT SELECT ON TABLE public.v_rbuilder_im_incidents_user_field_values TO rvision_read_only;
CREATE OR REPLACE VIEW public.v_rbuilder_im_incidents_devices AS
SELECT
    im_incident_device.incident_id,
    im_incident_device.type as "device_type",
    (
        SELECT
            hostname
        FROM
            am_devices
        WHERE
            id = im_incident_device.device_id
    ) as "device_hostname",
    am_devices_ifs_ips.ip :: inet as "device_ip"
FROM
    im_incident_device
    left join am_devices_ifs_ips on am_devices_ifs_ips.devices_id = im_incident_device.device_id;
GRANT SELECT ON TABLE public.v_rbuilder_im_incidents_devices TO rvision_read_only;
CREATE OR REPLACE VIEW public.v_rbuilder_im_incidents_disturbers AS
SELECT
    im_incident_disturber.incident_id,
    im_incident_disturber.type as "disterber_type",
    (
        SELECT
            login
        FROM
            am_users
        WHERE
            id = im_incident_disturber.user_id
    ) as "disterber_login",
    (
        SELECT
            name
        FROM
            am_users
        WHERE
            id = im_incident_disturber.user_id
    ) as "disterber"
FROM
    im_incident_disturber;
GRANT SELECT ON TABLE public.v_rbuilder_im_incidents_disturbers TO rvision_read_only;
CREATE OR REPLACE VIEW public.v_rbuilder_im_incidents_processes AS
SELECT
    im_incident_processes.incident_id,
    (
        SELECT
            name
        FROM
            am_processes
        WHERE
            id = im_incident_processes.process_id
    ) as "process"
FROM
    im_incident_processes;
GRANT SELECT ON TABLE public.v_rbuilder_im_incidents_processes TO rvision_read_only;
CREATE OR REPLACE VIEW public.v_rbuilder_im_incidents_assets AS
SELECT
    im_incidents_assets.incident_id,
    (
        SELECT
            name
        FROM
            am_assets
        WHERE
            id = im_incidents_assets.asset_id
    ) as "asset"
FROM
    im_incidents_assets;
GRANT SELECT ON TABLE public.v_rbuilder_im_incidents_assets TO rvision_read_only;
--Подумать над custom_assets
CREATE OR REPLACE VIEW public.v_rbuilder_im_incidents_informations AS
SELECT
    im_incidents_information.incident_id,
    (
        SELECT
            name
        FROM
            am_informationassets
        WHERE
            id = (
                SELECT
                    t.type_id
                FROM
                    am_information as t
                WHERE
                    id = im_incidents_information.id
                limit
                    1
            )
    ) as "information"
FROM
    im_incidents_information;
GRANT SELECT ON TABLE public.v_rbuilder_im_incidents_informations TO rvision_read_only;
--Подумать над локациями
CREATE OR REPLACE VIEW public.v_rbuilder_im_incidents_organizations AS
SELECT
    im_incidents_organizations.incident_id,
    (
        SELECT
            name
        FROM
            am_organization
        WHERE
            id = im_incidents_organizations.organization_id
    ) as "organization"
FROM
    im_incidents_organizations;
GRANT SELECT ON TABLE public.v_rbuilder_im_incidents_organizations TO rvision_read_only;
--Загружается только вместе
--Функция для извлечения даты из im_incident
CREATE OR REPLACE FUNCTION v_rbuilder_get_startdate_FROM_im_incident(text, text, integer) 
    RETURNS timestamp 
    LANGUAGE plpgsql 
    AS $function$ 
    #print_strict_params on
    DECLARE startdate timestamp;
    BEGIN 
        execute format(
        'SELECT %I'
        ' FROM %I WHERE id = %s',
        $1,
        $2,
        $3
    ) 
    INTO STRICT startdate;
    RETURN startdate;
    END;
    $function$;
--Функция для извлечения даты из всех остальных таблиц
CREATE OR REPLACE FUNCTION v_rbuilder_get_startdate_FROM_im_other(integer, text, integer) 
    RETURNS timestamp 
    LANGUAGE plpgsql 
    AS $function$ 
    #print_strict_params on
    DECLARE startdate timestamp;
    BEGIN 
        execute format(
        'SELECT value'
        ' FROM %I WHERE incident_id = %s and field_id = %s',
        $2,
        $3,
        $1
    ) 
    INTO STRICT startdate;
    RETURN startdate;
    END;
    $function$
    ;
--А теперь сама вьюшка
CREATE OR REPLACE VIEW public.v_rbuilder_im_incidents_timecounters AS 
with fields as (
    SELECT
        id,
        field_id_time_counter_start
    FROM
        im_fields
    WHERE
        "type" = 'timecounter'
),
inc_types as (
    SELECT
        im_incident.id as "incident_id",
        fields.id as "field_id",
        fields.field_id_time_counter_start
    FROM
        im_catalog_types_fields
        inner join fields on im_catalog_types_fields.field_id = fields.id
        inner join im_incident on im_incident.incident_type_id = im_catalog_types_fields.type_id
),
inc_categories as (
    SELECT
        im_incident.id as "incident_id",
        fields.id as "field_id",
        fields.field_id_time_counter_start
    FROM
        im_categories_fields
        inner join fields on im_categories_fields.field_id = fields.id
        inner join im_incident on im_incident.category_id = im_categories_fields.category_id
),
inc_with_counters as (
    SELECT
        *
    FROM
        inc_types
    union
    SELECT
        *
    FROM
        inc_categories
),
inc_all as (
SELECT
    inc_with_counters.incident_id,
        im_fields_only_label_catalog_type."label",
        case when (
            (
                im_fields_values_only_inc_field.value :: json ->> 'start'
            ) :: timestamp is null
            and (
                SELECT
                    stores_value_table
                FROM
                    im_fields
                WHERE
                    id = inc_with_counters.field_id_time_counter_start
            ) :: text = 'im_incident'
        ) then v_rbuilder_get_startdate_FROM_im_incident(
            (
                SELECT
                    "name"
                FROM
                    im_fields
                WHERE
                    id = inc_with_counters.field_id_time_counter_start
            ) :: text,
            (
                SELECT
                    stores_value_table
                FROM
                    im_fields
                WHERE
                    id = inc_with_counters.field_id_time_counter_start
            ) :: text,
            inc_with_counters.incident_id
            ) when (
            (
                im_fields_values_only_inc_field.value :: json ->> 'start'
            ) :: timestamp is null
            and (
                SELECT
                    stores_value_table
                FROM
                    im_fields
                WHERE
                    id = inc_with_counters.field_id_time_counter_start
            ) :: text != 'im_incident'
            ) then v_rbuilder_get_startdate_FROM_im_other (
                inc_with_counters.field_id_time_counter_start :: int,
                (
                    SELECT
                        stores_value_table
                    FROM
                        im_fields
                    WHERE
                        id = inc_with_counters.field_id_time_counter_start
                ) :: text,
                inc_with_counters.incident_id
            ) else (
                im_fields_values_only_inc_field.value :: json ->> 'start'
            ) :: timestamp end as "start_date",
            case when (
                im_fields_values_only_inc_field.value :: json ->> 'stop'
            ) :: timestamp is null then now() else (
                im_fields_values_only_inc_field.value :: json ->> 'stop'
            ) :: timestamp end as "stop_date",
            im_fields_only_label_catalog_type.time_counter_limit as "limit"
        FROM
            inc_with_counters
            left join (
                SELECT
                    incident_id,
                    field_id,
                    value
                FROM
                    im_fields_values
                WHERE
                    value != ''
                    and value IS NOT NULL
            ) as im_fields_values_only_inc_field on inc_with_counters.field_id = im_fields_values_only_inc_field.field_id
            and inc_with_counters.incident_id = im_fields_values_only_inc_field.incident_id
            inner join (
                SELECT
                    id,
                    manual_time_counter_start,
                    "label",
                    time_counter_limit
                FROM
                    im_fields
            ) as im_fields_only_label_catalog_type on im_fields_only_label_catalog_type.id = inc_with_counters.field_id
    )
SELECT
    incident_id,
    "label" as "field_label",
    (
        date_part('epoch', stop_date - start_date) * interval '1 second'
    ) :: interval(0) as "value",
    "limit"
FROM
    inc_all;
GRANT SELECT ON TABLE public.v_rbuilder_im_incidents_timecounters TO rvision_read_only;
